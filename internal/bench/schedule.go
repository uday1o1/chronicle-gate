// Package bench implements the isolated ChronicleGate performance path.
package bench

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/uday1o1/chronicle-gate/internal/spec"
)

const scheduleAlgorithm = spec.BenchmarkScheduleAlgorithm

type Plan struct {
	Algorithm string      `json:"algorithm"`
	Rounds    []RoundPlan `json:"rounds"`
	SHA256    string      `json:"sha256"`
}

type RoundPlan struct {
	Index       int           `json:"index"`
	First       string        `json:"first"`
	Warmup      []RequestPlan `json:"warmup"`
	Measurement []RequestPlan `json:"measurement"`
}

type RequestPlan struct {
	Ordinal       int               `json:"ordinal"`
	OffsetNanos   int64             `json:"offsetNanos"`
	OperationID   string            `json:"operationId"`
	Method        string            `json:"method"`
	Path          string            `json:"path"`
	Headers       map[string]string `json:"headers"`
	Body          string            `json:"body"`
	RequestID     string            `json:"requestId"`
	ExpectedCodes []int             `json:"expectedCodes"`
}

func BuildPlan(workload spec.BenchmarkWorkload) (Plan, error) {
	if violations := spec.ValidateBenchmarkWorkload(workload); len(violations) != 0 {
		return Plan{}, fmt.Errorf("invalid benchmark workload at %s: %s", violations[0].Pointer, violations[0].Message)
	}
	rounds := workload.Spec.Schedule.Rounds
	orders := make([]string, rounds)
	for index := range orders {
		if index < rounds/2 {
			orders[index] = "baseline"
		} else {
			orders[index] = "candidate"
		}
	}
	orderRandom := newSplitMix64(uint64(workload.Spec.Schedule.OrderSeed))
	for index := len(orders) - 1; index > 0; index-- {
		selected := int(orderRandom.bounded(uint64(index + 1)))
		orders[index], orders[selected] = orders[selected], orders[index]
	}
	requestRandom := newSplitMix64(uint64(workload.Spec.Schedule.RequestSeed))
	plan := Plan{Algorithm: scheduleAlgorithm, Rounds: make([]RoundPlan, rounds)}
	for index := range plan.Rounds {
		roundRandom := newSplitMix64(requestRandom.next())
		warmup, err := buildRequests(index, "warmup", workload.Spec.Schedule.RatePerSecond, workload.Spec.Schedule.Warmup.Duration, workload.Spec.Operations, roundRandom)
		if err != nil {
			return Plan{}, err
		}
		measurement, err := buildRequests(index, "measurement", workload.Spec.Schedule.RatePerSecond, workload.Spec.Schedule.Measurement.Duration, workload.Spec.Operations, roundRandom)
		if err != nil {
			return Plan{}, err
		}
		plan.Rounds[index] = RoundPlan{Index: index, First: orders[index], Warmup: warmup, Measurement: measurement}
	}
	document, err := json.Marshal(struct {
		Algorithm string      `json:"algorithm"`
		Rounds    []RoundPlan `json:"rounds"`
	}{Algorithm: plan.Algorithm, Rounds: plan.Rounds})
	if err != nil {
		return Plan{}, fmt.Errorf("encode benchmark plan: %w", err)
	}
	digest := sha256.Sum256(document)
	plan.SHA256 = hex.EncodeToString(digest[:])
	return plan, nil
}

func buildRequests(round int, phase string, rate int, duration time.Duration, operations []spec.BenchmarkOperation, random *splitMix64) ([]RequestPlan, error) {
	count := int64(duration) * int64(rate) / 1_000_000_000
	if count < 0 || count > math.MaxInt {
		return nil, fmt.Errorf("%s request count overflows", phase)
	}
	totalWeight := uint64(0)
	for _, operation := range operations {
		totalWeight += uint64(operation.Weight)
	}
	requests := make([]RequestPlan, int(count))
	for ordinal := range requests {
		if int64(ordinal) > math.MaxInt64/1_000_000_000 {
			return nil, fmt.Errorf("request offset overflows")
		}
		offset := int64(ordinal) * 1_000_000_000 / int64(rate)
		selection := random.bounded(totalWeight)
		operation := operations[len(operations)-1]
		cursor := uint64(0)
		for _, candidate := range operations {
			cursor += uint64(candidate.Weight)
			if selection < cursor {
				operation = candidate
				break
			}
		}
		headers := make(map[string]string, len(operation.Headers)+1)
		for key, value := range operation.Headers {
			headers[key] = value
		}
		requestID := fmt.Sprintf("r%02d-%s-%06d", round, phase, ordinal)
		headers["X-Chronicle-Benchmark-Request"] = requestID
		requests[ordinal] = RequestPlan{
			Ordinal: ordinal, OffsetNanos: offset, OperationID: operation.ID, Method: operation.Method,
			Path: operation.Path, Headers: headers, Body: operation.Body, RequestID: requestID,
			ExpectedCodes: append([]int(nil), operation.ExpectedStatuses...),
		}
	}
	return requests, nil
}

type splitMix64 struct {
	state uint64
}

func newSplitMix64(seed uint64) *splitMix64 {
	return &splitMix64{state: seed}
}

func (random *splitMix64) next() uint64 {
	random.state += 0x9e3779b97f4a7c15
	value := random.state
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	return value ^ (value >> 31)
}

func (random *splitMix64) bounded(bound uint64) uint64 {
	if bound == 0 {
		panic("zero random bound")
	}
	threshold := -bound % bound
	for {
		value := random.next()
		if value >= threshold {
			return value % bound
		}
	}
}
