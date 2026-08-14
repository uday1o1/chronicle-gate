package bench

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"runtime"
	"time"

	"github.com/moby/moby/api/types/container"
	mobyclient "github.com/moby/moby/client"
	chronruntime "github.com/uday1o1/chronicle-gate/internal/runtime"
)

type resourceSample struct {
	Round              int    `json:"round"`
	Target             string `json:"target"`
	OffsetNanos        int64  `json:"offsetNanos"`
	ReadAt             string `json:"readAt"`
	CPUTotalUsageNanos uint64 `json:"cpuTotalUsageNanos"`
	MemoryUsageBytes   uint64 `json:"memoryUsageBytes"`
	MemoryLimitBytes   uint64 `json:"memoryLimitBytes"`
	PIDs               uint64 `json:"pids"`
	ThrottleAvailable  bool   `json:"throttleAvailable"`
	ThrottledPeriods   uint64 `json:"throttledPeriods,omitempty"`
	ThrottledTimeNanos uint64 `json:"throttledTimeNanos,omitempty"`
}

func (service *benchmarkService) sample(ctx context.Context, round int, role string, offset time.Duration) (resourceSample, error) {
	docker, err := mobyclient.New(mobyclient.FromEnv)
	if err != nil {
		return resourceSample{}, err
	}
	defer func() { _ = docker.Close() }()
	response, err := docker.ContainerStats(ctx, service.container.GetContainerID(), mobyclient.ContainerStatsOptions{Stream: false})
	if err != nil {
		return resourceSample{}, err
	}
	defer func() { _ = response.Body.Close() }()
	var statistics container.StatsResponse
	decoder := json.NewDecoder(io.LimitReader(response.Body, 1<<20))
	if err := decoder.Decode(&statistics); err != nil {
		return resourceSample{}, fmt.Errorf("decode Docker resource sample: %w", err)
	}
	return resourceSample{
		Round: round, Target: role, OffsetNanos: offset.Nanoseconds(), ReadAt: statistics.Read.UTC().Format(time.RFC3339Nano),
		CPUTotalUsageNanos: statistics.CPUStats.CPUUsage.TotalUsage,
		MemoryUsageBytes:   statistics.MemoryStats.Usage, MemoryLimitBytes: statistics.MemoryStats.Limit,
		PIDs:               statistics.PidsStats.Current,
		ThrottleAvailable:  statistics.OSType == "linux",
		ThrottledPeriods:   statistics.CPUStats.ThrottlingData.ThrottledPeriods,
		ThrottledTimeNanos: statistics.CPUStats.ThrottlingData.ThrottledTime,
	}, nil
}

func summarizeResources(samples []resourceSample, requireThrottle bool) (ResourceSummary, error) {
	if len(samples) < 2 {
		return ResourceSummary{}, fmt.Errorf("resource summary requires at least two samples")
	}
	first := samples[0]
	if first.MemoryLimitBytes == 0 || first.PIDs == 0 {
		return ResourceSummary{}, fmt.Errorf("first resource sample is incomplete")
	}
	summary := ResourceSummary{
		Samples:           len(samples),
		MemoryLimitBytes:  first.MemoryLimitBytes,
		ThrottleAvailable: true,
	}
	previous := first
	for index, sample := range samples {
		if sample.MemoryLimitBytes != first.MemoryLimitBytes || sample.PIDs == 0 {
			return ResourceSummary{}, fmt.Errorf("resource sample %d has inconsistent limits or process count", index)
		}
		if index > 0 && (sample.CPUTotalUsageNanos < previous.CPUTotalUsageNanos ||
			sample.ThrottledPeriods < previous.ThrottledPeriods ||
			sample.ThrottledTimeNanos < previous.ThrottledTimeNanos) {
			return ResourceSummary{}, fmt.Errorf("resource counters moved backward at sample %d", index)
		}
		if sample.MemoryUsageBytes > summary.MaxMemoryUsageBytes {
			summary.MaxMemoryUsageBytes = sample.MemoryUsageBytes
		}
		if sample.PIDs > summary.MaxPIDs {
			summary.MaxPIDs = sample.PIDs
		}
		summary.ThrottleAvailable = summary.ThrottleAvailable && sample.ThrottleAvailable
		previous = sample
	}
	if requireThrottle && !summary.ThrottleAvailable {
		return ResourceSummary{}, fmt.Errorf("publication evidence requires Linux throttling counters")
	}
	last := samples[len(samples)-1]
	summary.CPUUsageDeltaNanos = last.CPUTotalUsageNanos - first.CPUTotalUsageNanos
	summary.ThrottledPeriodsDelta = last.ThrottledPeriods - first.ThrottledPeriods
	summary.ThrottledTimeDeltaNanos = last.ThrottledTimeNanos - first.ThrottledTimeNanos
	return summary, nil
}

func benchmarkHostProvenance(ctx context.Context) (map[string]any, error) {
	if err := chronruntime.ConfigureDockerHost(ctx); err != nil {
		return nil, err
	}
	docker, err := mobyclient.New(mobyclient.FromEnv)
	if err != nil {
		return nil, err
	}
	defer func() { _ = docker.Close() }()
	info, err := docker.Info(ctx, mobyclient.InfoOptions{})
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"hostOS": runtime.GOOS, "hostArchitecture": runtime.GOARCH, "hostLogicalCPUs": runtime.NumCPU(),
		"dockerOperatingSystem": info.Info.OperatingSystem, "dockerArchitecture": info.Info.Architecture,
		"dockerCPUs": info.Info.NCPU, "dockerMemoryBytes": info.Info.MemTotal, "dockerKernelVersion": info.Info.KernelVersion,
	}, nil
}
