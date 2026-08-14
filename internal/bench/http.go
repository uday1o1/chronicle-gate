package bench

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/uday1o1/chronicle-gate/internal/spec"
)

type timingRecord struct {
	Round          int    `json:"round"`
	Target         string `json:"target"`
	Phase          string `json:"phase"`
	Ordinal        int    `json:"ordinal"`
	RequestID      string `json:"requestId"`
	OperationID    string `json:"operationId"`
	ScheduledNanos int64  `json:"scheduledNanos"`
	StartNanos     int64  `json:"startNanos"`
	FirstByteNanos int64  `json:"firstByteNanos"`
	EndNanos       int64  `json:"endNanos"`
	Status         int    `json:"status"`
	ResponseBytes  int64  `json:"responseBytes"`
	Success        bool   `json:"success"`
	Error          string `json:"error,omitempty"`
}

type failureKind string

const (
	failureInfrastructure failureKind = "INFRASTRUCTURE_ERROR"
	failureTimeout        failureKind = "TIMEOUT"
	failureUnresolved     failureKind = "UNRESOLVED"
)

type benchmarkError struct {
	kind failureKind
	err  error
}

func (failure *benchmarkError) Error() string { return failure.err.Error() }
func (failure *benchmarkError) Unwrap() error { return failure.err }

func runHTTPPhase(ctx context.Context, service *benchmarkService, workload spec.BenchmarkWorkload, round int, role, phase string, requests []RequestPlan, phaseStart time.Time) ([]timingRecord, error) {
	parsed, err := url.Parse(service.endpoint)
	if err != nil {
		return nil, &benchmarkError{kind: failureInfrastructure, err: err}
	}
	exactAddress := parsed.Host
	transport := newBenchmarkTransport(exactAddress, workload)
	defer transport.CloseIdleConnections()
	client := &http.Client{
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	results := make(chan timingRecord, len(requests))
	semaphore := make(chan struct{}, workload.Spec.Schedule.MaxInFlight)
	var group sync.WaitGroup
	var schedulingErr error
	for _, planned := range requests {
		targetTime := phaseStart.Add(time.Duration(planned.OffsetNanos))
		if wait := time.Until(targetTime); wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				timer.Stop()
				schedulingErr = ctx.Err()
			case <-timer.C:
			}
		}
		if schedulingErr != nil {
			break
		}
		lag := time.Since(targetTime)
		if lag > workload.Spec.Schedule.MaxScheduleLag.Duration {
			schedulingErr = &benchmarkError{kind: failureUnresolved, err: fmt.Errorf("request %s schedule lag %s exceeds %s", planned.RequestID, lag, workload.Spec.Schedule.MaxScheduleLag.Duration)}
			break
		}
		select {
		case semaphore <- struct{}{}:
		default:
			schedulingErr = &benchmarkError{kind: failureUnresolved, err: fmt.Errorf("request %s exhausted maxInFlight", planned.RequestID)}
		}
		if schedulingErr != nil {
			break
		}
		planned := planned
		group.Add(1)
		go func() {
			defer group.Done()
			defer func() { <-semaphore }()
			results <- executeRequest(ctx, client, service.endpoint, workload, round, role, phase, phaseStart, planned)
		}()
	}
	group.Wait()
	close(results)
	records := make([]timingRecord, 0, len(requests))
	for record := range results {
		records = append(records, record)
	}
	sort.Slice(records, func(left, right int) bool { return records[left].Ordinal < records[right].Ordinal })
	if schedulingErr != nil {
		if errors.Is(schedulingErr, context.Canceled) {
			return records, schedulingErr
		}
		if errors.Is(schedulingErr, context.DeadlineExceeded) {
			return records, &benchmarkError{kind: failureTimeout, err: schedulingErr}
		}
		return records, schedulingErr
	}
	if len(records) != len(requests) {
		return records, &benchmarkError{kind: failureUnresolved, err: fmt.Errorf("completed %d requests, want %d", len(records), len(requests))}
	}
	for index, record := range records {
		if record.Ordinal != index || record.RequestID != requests[index].RequestID {
			return records, &benchmarkError{kind: failureUnresolved, err: fmt.Errorf("request completion inventory does not match the persisted plan")}
		}
		if record.Error != "" {
			kind := failureInfrastructure
			if record.Error == context.DeadlineExceeded.Error() {
				kind = failureTimeout
			} else if record.Error == "unexpected status" || record.Error == "response body limit exceeded" {
				kind = failureUnresolved
			}
			return records, &benchmarkError{kind: kind, err: fmt.Errorf("request %s: %s", record.RequestID, record.Error)}
		}
	}
	return records, nil
}

func newBenchmarkTransport(exactAddress string, workload spec.BenchmarkWorkload) *http.Transport {
	dialer := &net.Dialer{Timeout: workload.Spec.Schedule.RequestTimeout.Duration, KeepAlive: 30 * time.Second}
	return &http.Transport{
		Proxy: nil,
		DialContext: func(dialContext context.Context, network, address string) (net.Conn, error) {
			if network != "tcp" || address != exactAddress {
				return nil, fmt.Errorf("benchmark dial escaped resolved endpoint: %s %s", network, address)
			}
			return dialer.DialContext(dialContext, network, address)
		},
		ForceAttemptHTTP2:      false,
		DisableCompression:     true,
		MaxIdleConns:           workload.Spec.Schedule.MaxInFlight,
		MaxIdleConnsPerHost:    workload.Spec.Schedule.MaxInFlight,
		MaxConnsPerHost:        workload.Spec.Schedule.MaxInFlight,
		IdleConnTimeout:        30 * time.Second,
		ResponseHeaderTimeout:  workload.Spec.Schedule.RequestTimeout.Duration,
		MaxResponseHeaderBytes: workload.Spec.Validity.MaxResponseHeaderBytes,
	}
}

func executeRequest(parent context.Context, client *http.Client, endpoint string, workload spec.BenchmarkWorkload, round int, role, phase string, phaseStart time.Time, planned RequestPlan) timingRecord {
	record := timingRecord{
		Round: round, Target: role, Phase: phase, Ordinal: planned.Ordinal,
		RequestID: planned.RequestID, OperationID: planned.OperationID, ScheduledNanos: planned.OffsetNanos,
	}
	requestContext, cancel := context.WithTimeout(parent, workload.Spec.Schedule.RequestTimeout.Duration)
	defer cancel()
	request, err := http.NewRequestWithContext(requestContext, planned.Method, endpoint+planned.Path, bytes.NewBufferString(planned.Body))
	if err != nil {
		record.Error = err.Error()
		return record
	}
	for name, value := range planned.Headers {
		request.Header.Set(name, value)
	}
	firstByte := time.Time{}
	request = request.WithContext(httptrace.WithClientTrace(request.Context(), &httptrace.ClientTrace{
		GotFirstResponseByte: func() { firstByte = time.Now() },
	}))
	started := time.Now()
	record.StartNanos = started.Sub(phaseStart).Nanoseconds()
	response, err := client.Do(request)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(requestContext.Err(), context.DeadlineExceeded) {
			record.Error = context.DeadlineExceeded.Error()
		} else {
			record.Error = err.Error()
		}
		record.EndNanos = time.Since(phaseStart).Nanoseconds()
		return record
	}
	if !firstByte.IsZero() {
		record.FirstByteNanos = firstByte.Sub(phaseStart).Nanoseconds()
	}
	record.Status = response.StatusCode
	limited := io.LimitReader(response.Body, workload.Spec.Validity.MaxResponseBodyBytes+1)
	written, readErr := io.Copy(io.Discard, limited)
	closeErr := response.Body.Close()
	record.EndNanos = time.Since(phaseStart).Nanoseconds()
	record.ResponseBytes = written
	if readErr != nil || closeErr != nil {
		record.Error = errors.Join(readErr, closeErr).Error()
		return record
	}
	if written > workload.Spec.Validity.MaxResponseBodyBytes {
		record.Error = "response body limit exceeded"
		return record
	}
	if !slices.Contains(planned.ExpectedCodes, response.StatusCode) {
		record.Error = "unexpected status"
		return record
	}
	record.Success = true
	return record
}
