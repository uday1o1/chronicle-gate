package bench

import (
	"fmt"
	"math"
	"slices"
)

type TrialSummary struct {
	Round      int             `json:"round"`
	Target     string          `json:"target"`
	Requests   int             `json:"requests"`
	Successes  int             `json:"successes"`
	P50Nanos   int64           `json:"p50Nanos"`
	P95Nanos   int64           `json:"p95Nanos"`
	P99Nanos   int64           `json:"p99Nanos"`
	Throughput float64         `json:"throughputPerSecond"`
	ErrorRate  float64         `json:"errorRate"`
	Valid      bool            `json:"valid"`
	Reason     string          `json:"reason,omitempty"`
	Resource   ResourceSummary `json:"resource"`
}

type ResourceSummary struct {
	Samples                 int    `json:"samples"`
	CPUUsageDeltaNanos      uint64 `json:"cpuUsageDeltaNanos"`
	MaxMemoryUsageBytes     uint64 `json:"maxMemoryUsageBytes"`
	MemoryLimitBytes        uint64 `json:"memoryLimitBytes"`
	MaxPIDs                 uint64 `json:"maxPids"`
	ThrottleAvailable       bool   `json:"throttleAvailable"`
	ThrottledPeriodsDelta   uint64 `json:"throttledPeriodsDelta"`
	ThrottledTimeDeltaNanos uint64 `json:"throttledTimeDeltaNanos"`
}

type TargetSummary struct {
	Target     string          `json:"target"`
	Requests   int             `json:"requests"`
	Successes  int             `json:"successes"`
	P50Nanos   int64           `json:"p50Nanos"`
	P95Nanos   int64           `json:"p95Nanos"`
	P99Nanos   int64           `json:"p99Nanos"`
	Throughput float64         `json:"throughputPerSecond"`
	ErrorRate  float64         `json:"errorRate"`
	Resource   ResourceSummary `json:"resource"`
}

type Analysis struct {
	Algorithm                 string    `json:"algorithm"`
	BootstrapSeed             int64     `json:"bootstrapSeed"`
	BootstrapResamples        int       `json:"bootstrapResamples"`
	Confidence                float64   `json:"confidence"`
	BlockSize                 int       `json:"blockSize"`
	AbsoluteP95DeltasNanos    []int64   `json:"absoluteP95DeltasNanos"`
	RelativeP95Deltas         []float64 `json:"relativeP95Deltas"`
	MeanAbsoluteP95DeltaNanos float64   `json:"meanAbsoluteP95DeltaNanos"`
	MeanRelativeP95Delta      float64   `json:"meanRelativeP95Delta"`
	LowerRelativeCI           float64   `json:"lowerRelativeCI"`
	UpperRelativeCI           float64   `json:"upperRelativeCI"`
	LowerIndex                int       `json:"lowerIndex"`
	UpperIndex                int       `json:"upperIndex"`
	AbsoluteThresholdNanos    int64     `json:"absoluteThresholdNanos"`
	RelativeThreshold         float64   `json:"relativeThreshold"`
	Regression                bool      `json:"regression"`
}

func Summarize(round int, target string, latencies []int64, successes int, seconds float64) (TrialSummary, error) {
	if len(latencies) == 0 || seconds <= 0 || successes < 0 || successes > len(latencies) {
		return TrialSummary{}, fmt.Errorf("trial summary has incomplete samples")
	}
	return TrialSummary{
		Round: round, Target: target, Requests: len(latencies), Successes: successes,
		P50Nanos: nearestRank(latencies, 0.50), P95Nanos: nearestRank(latencies, 0.95), P99Nanos: nearestRank(latencies, 0.99),
		Throughput: float64(successes) / seconds, ErrorRate: float64(len(latencies)-successes) / float64(len(latencies)), Valid: true,
	}, nil
}

func summarizeTarget(target string, latencies []int64, successes int, seconds float64, trials []TrialSummary) (TargetSummary, error) {
	if len(latencies) == 0 || len(trials) == 0 || seconds <= 0 || successes < 0 || successes > len(latencies) {
		return TargetSummary{}, fmt.Errorf("target summary has incomplete samples")
	}
	resource := ResourceSummary{ThrottleAvailable: true}
	for _, trial := range trials {
		resource.Samples += trial.Resource.Samples
		resource.CPUUsageDeltaNanos += trial.Resource.CPUUsageDeltaNanos
		resource.ThrottledPeriodsDelta += trial.Resource.ThrottledPeriodsDelta
		resource.ThrottledTimeDeltaNanos += trial.Resource.ThrottledTimeDeltaNanos
		resource.ThrottleAvailable = resource.ThrottleAvailable && trial.Resource.ThrottleAvailable
		if trial.Resource.MaxMemoryUsageBytes > resource.MaxMemoryUsageBytes {
			resource.MaxMemoryUsageBytes = trial.Resource.MaxMemoryUsageBytes
		}
		if trial.Resource.MaxPIDs > resource.MaxPIDs {
			resource.MaxPIDs = trial.Resource.MaxPIDs
		}
		if resource.MemoryLimitBytes == 0 {
			resource.MemoryLimitBytes = trial.Resource.MemoryLimitBytes
		} else if resource.MemoryLimitBytes != trial.Resource.MemoryLimitBytes {
			return TargetSummary{}, fmt.Errorf("target %s resource limits changed between trials", target)
		}
	}
	return TargetSummary{
		Target: target, Requests: len(latencies), Successes: successes,
		P50Nanos: nearestRank(latencies, 0.50), P95Nanos: nearestRank(latencies, 0.95), P99Nanos: nearestRank(latencies, 0.99),
		Throughput: float64(successes) / seconds, ErrorRate: float64(len(latencies)-successes) / float64(len(latencies)), Resource: resource,
	}, nil
}

func Analyze(baseline, candidate []TrialSummary, seed int64, resamples int, confidence, relativeThreshold float64, absoluteThreshold int64) (Analysis, error) {
	if len(baseline) == 0 || len(baseline) != len(candidate) || resamples <= 0 || confidence <= 0 || confidence >= 1 || absoluteThreshold < 0 {
		return Analysis{}, fmt.Errorf("paired analysis inputs are invalid")
	}
	result := Analysis{
		Algorithm: specBootstrapAlgorithm, BootstrapSeed: seed, BootstrapResamples: resamples, Confidence: confidence, BlockSize: 1,
		AbsoluteP95DeltasNanos: make([]int64, len(baseline)), RelativeP95Deltas: make([]float64, len(baseline)),
		AbsoluteThresholdNanos: absoluteThreshold, RelativeThreshold: relativeThreshold,
	}
	absoluteSum := int64(0)
	for index := range baseline {
		if baseline[index].Round != candidate[index].Round || baseline[index].P95Nanos <= 0 {
			return Analysis{}, fmt.Errorf("round %d is not a valid pair", index)
		}
		delta := candidate[index].P95Nanos - baseline[index].P95Nanos
		if delta > 0 && absoluteSum > math.MaxInt64-delta || delta < 0 && absoluteSum < math.MinInt64-delta {
			return Analysis{}, fmt.Errorf("absolute p95 delta sum overflows")
		}
		absoluteSum += delta
		result.AbsoluteP95DeltasNanos[index] = delta
		result.RelativeP95Deltas[index] = float64(delta) / float64(baseline[index].P95Nanos)
		result.MeanRelativeP95Delta += result.RelativeP95Deltas[index]
	}
	result.MeanAbsoluteP95DeltaNanos = float64(absoluteSum) / float64(len(baseline))
	result.MeanRelativeP95Delta /= float64(len(baseline))
	random := newSplitMix64(uint64(seed))
	bootstrap := make([]float64, resamples)
	for replicate := range bootstrap {
		sum := float64(0)
		for range result.RelativeP95Deltas {
			sum += result.RelativeP95Deltas[random.bounded(uint64(len(result.RelativeP95Deltas)))]
		}
		bootstrap[replicate] = sum / float64(len(result.RelativeP95Deltas))
	}
	slices.Sort(bootstrap)
	alpha := (1 - confidence) / 2
	result.LowerIndex = nearestRankIndex(len(bootstrap), alpha)
	result.UpperIndex = nearestRankIndex(len(bootstrap), 1-alpha)
	result.LowerRelativeCI = bootstrap[result.LowerIndex]
	result.UpperRelativeCI = bootstrap[result.UpperIndex]
	if absoluteThreshold > 0 && int64(len(baseline)) > math.MaxInt64/absoluteThreshold {
		return Analysis{}, fmt.Errorf("absolute threshold multiplication overflows")
	}
	result.Regression = absoluteSum >= absoluteThreshold*int64(len(baseline)) && result.LowerRelativeCI > relativeThreshold
	return result, nil
}

const specBootstrapAlgorithm = "paired-percentile-bootstrap-v1"

func nearestRank(values []int64, quantile float64) int64 {
	ordered := append([]int64(nil), values...)
	slices.Sort(ordered)
	return ordered[nearestRankIndex(len(ordered), quantile)]
}

func nearestRankIndex(length int, quantile float64) int {
	index := int(math.Ceil(quantile*float64(length))) - 1
	if index < 0 {
		return 0
	}
	if index >= length {
		return length - 1
	}
	return index
}
