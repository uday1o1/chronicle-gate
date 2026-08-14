package bench

import (
	"fmt"
	"math"
	"reflect"
	"slices"
)

const (
	AbsoluteP95DeltaUnit = "nanoseconds"
	RelativeP95DeltaUnit = "ratio"
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
	AbsoluteP95DeltaUnit      string    `json:"absoluteP95DeltaUnit"`
	RelativeP95DeltaUnit      string    `json:"relativeP95DeltaUnit"`
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

type P95Pair struct {
	Round             int   `json:"round"`
	BaselineP95Nanos  int64 `json:"baselineP95Nanos"`
	CandidateP95Nanos int64 `json:"candidateP95Nanos"`
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
	if len(baseline) == 0 || len(baseline) != len(candidate) || resamples <= 0 || confidence <= 0 || confidence >= 1 || !finite(confidence) || relativeThreshold < 0 || !finite(relativeThreshold) || absoluteThreshold < 0 {
		return Analysis{}, fmt.Errorf("paired analysis inputs are invalid")
	}
	result := Analysis{
		Algorithm: specBootstrapAlgorithm, BootstrapSeed: seed, BootstrapResamples: resamples, Confidence: confidence, BlockSize: 1,
		AbsoluteP95DeltaUnit: AbsoluteP95DeltaUnit, RelativeP95DeltaUnit: RelativeP95DeltaUnit,
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

func PairP95Trials(trials []TrialSummary, rounds int) ([]P95Pair, error) {
	if rounds <= 0 || len(trials) != rounds*2 {
		return nil, fmt.Errorf("paired p95 inventory has %d trials over %d rounds", len(trials), rounds)
	}
	pairs := make([]P95Pair, rounds)
	baselineSeen := make([]bool, rounds)
	candidateSeen := make([]bool, rounds)
	for round := range pairs {
		pairs[round].Round = round
	}
	for _, trial := range trials {
		if trial.Round < 0 || trial.Round >= rounds || trial.P95Nanos <= 0 {
			return nil, fmt.Errorf("invalid p95 trial for round %d and target %q", trial.Round, trial.Target)
		}
		switch trial.Target {
		case "baseline":
			if baselineSeen[trial.Round] {
				return nil, fmt.Errorf("duplicate baseline p95 trial for round %d", trial.Round)
			}
			baselineSeen[trial.Round] = true
			pairs[trial.Round].BaselineP95Nanos = trial.P95Nanos
		case "candidate":
			if candidateSeen[trial.Round] {
				return nil, fmt.Errorf("duplicate candidate p95 trial for round %d", trial.Round)
			}
			candidateSeen[trial.Round] = true
			pairs[trial.Round].CandidateP95Nanos = trial.P95Nanos
		default:
			return nil, fmt.Errorf("unexpected p95 trial target %q", trial.Target)
		}
	}
	for round := range pairs {
		if !baselineSeen[round] || !candidateSeen[round] {
			return nil, fmt.Errorf("incomplete p95 pair for round %d", round)
		}
	}
	return pairs, nil
}

func RecomputeAnalysis(pairs []P95Pair, configuration Analysis) (Analysis, error) {
	if configuration.Algorithm != specBootstrapAlgorithm || configuration.BlockSize != 1 || configuration.AbsoluteP95DeltaUnit != AbsoluteP95DeltaUnit || configuration.RelativeP95DeltaUnit != RelativeP95DeltaUnit {
		return Analysis{}, fmt.Errorf("paired analysis metadata is invalid")
	}
	if len(pairs) == 0 {
		return Analysis{}, fmt.Errorf("paired analysis has no p95 pairs")
	}
	baseline := make([]TrialSummary, len(pairs))
	candidate := make([]TrialSummary, len(pairs))
	for index, pair := range pairs {
		if pair.Round != index || pair.BaselineP95Nanos <= 0 || pair.CandidateP95Nanos <= 0 {
			return Analysis{}, fmt.Errorf("p95 pair %d is invalid", index)
		}
		baseline[index] = TrialSummary{Round: pair.Round, Target: "baseline", P95Nanos: pair.BaselineP95Nanos}
		candidate[index] = TrialSummary{Round: pair.Round, Target: "candidate", P95Nanos: pair.CandidateP95Nanos}
	}
	return Analyze(
		baseline,
		candidate,
		configuration.BootstrapSeed,
		configuration.BootstrapResamples,
		configuration.Confidence,
		configuration.RelativeThreshold,
		configuration.AbsoluteThresholdNanos,
	)
}

func ValidateAnalysis(pairs []P95Pair, recorded Analysis) error {
	if !analysisFinite(recorded) || recorded.LowerRelativeCI > recorded.UpperRelativeCI {
		return fmt.Errorf("paired analysis contains invalid numeric evidence")
	}
	recomputed, err := RecomputeAnalysis(pairs, recorded)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(recomputed, recorded) {
		return fmt.Errorf("paired analysis does not match deterministic recomputation")
	}
	return nil
}

func analysisFinite(analysis Analysis) bool {
	values := []float64{
		analysis.Confidence,
		analysis.MeanAbsoluteP95DeltaNanos,
		analysis.MeanRelativeP95Delta,
		analysis.LowerRelativeCI,
		analysis.UpperRelativeCI,
		analysis.RelativeThreshold,
	}
	values = append(values, analysis.RelativeP95Deltas...)
	for _, value := range values {
		if !finite(value) {
			return false
		}
	}
	return true
}

func finite(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
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
