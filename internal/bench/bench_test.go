package bench

import (
	"math"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/uday1o1/chronicle-gate/internal/spec"
)

func TestScheduleGoldenAndBalancedPairs(t *testing.T) {
	workload := validWorkload()
	plan, err := BuildPlan(workload)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Algorithm != "chronicle-schedule-v1" || len(plan.Rounds) != 4 || len(plan.SHA256) != 64 {
		t.Fatalf("plan = %#v", plan)
	}
	baselineFirst := 0
	wantOrder := []string{"candidate", "baseline", "candidate", "baseline"}
	for _, round := range plan.Rounds {
		if round.First != wantOrder[round.Index] {
			t.Fatalf("round %d first = %s, want %s", round.Index, round.First, wantOrder[round.Index])
		}
		if round.First == "baseline" {
			baselineFirst++
		}
		if len(round.Warmup) != 2 || len(round.Measurement) != 4 {
			t.Fatalf("round request inventory = %d/%d", len(round.Warmup), len(round.Measurement))
		}
		for index, request := range round.Measurement {
			if request.OffsetNanos != int64(index)*500_000_000 {
				t.Fatalf("request %d offset = %d", index, request.OffsetNanos)
			}
		}
	}
	if baselineFirst != 2 {
		t.Fatalf("baseline-first rounds = %d", baselineFirst)
	}
	again, err := BuildPlan(workload)
	if err != nil || !reflect.DeepEqual(plan, again) {
		t.Fatalf("schedule is not reproducible: err=%v", err)
	}
	random := newSplitMix64(0)
	if got := random.next(); got != 0xe220a8397b1dcdaf {
		t.Fatalf("SplitMix64 first vector = %016x", got)
	}
	if got := random.next(); got != 0x6e789e6aa1b965f4 {
		t.Fatalf("SplitMix64 second vector = %016x", got)
	}
	rejection := newSplitMix64(3)
	if got := rejection.bounded(0x8000000000000001); got != 0x33466f8a7b81a988 {
		t.Fatalf("rejection-sampled vector = %016x", got)
	}

	weighted := validWorkload()
	weighted.Spec.Operations = append(weighted.Spec.Operations, spec.BenchmarkOperation{
		ID: "weighted", Weight: 3, Method: "GET", Path: "/weighted", Headers: map[string]string{}, ExpectedStatuses: []int{200},
	})
	weightedPlan, err := BuildPlan(weighted)
	if err != nil {
		t.Fatal(err)
	}
	wantOperations := [][]string{
		{"work", "work", "work", "work"},
		{"weighted", "weighted", "weighted", "weighted"},
		{"weighted", "weighted", "weighted", "work"},
		{"weighted", "work", "weighted", "weighted"},
	}
	for round, requests := range weightedPlan.Rounds {
		for index, request := range requests.Measurement {
			if request.OperationID != wantOperations[round][index] {
				t.Fatalf("weighted selection round %d request %d = %s", round, index, request.OperationID)
			}
		}
	}
}

func TestPairedBootstrapAAndSlowdown(t *testing.T) {
	baseline := []TrialSummary{{Round: 0, P95Nanos: 10}, {Round: 1, P95Nanos: 10}, {Round: 2, P95Nanos: 10}, {Round: 3, P95Nanos: 10}}
	aa, err := Analyze(baseline, baseline, 9, 1000, 0.95, 0.5, 5)
	if err != nil || aa.Regression || aa.LowerRelativeCI != 0 || aa.UpperRelativeCI != 0 || aa.AbsoluteP95DeltaUnit != AbsoluteP95DeltaUnit || aa.RelativeP95DeltaUnit != RelativeP95DeltaUnit {
		t.Fatalf("A/A analysis = %#v, err=%v", aa, err)
	}
	candidate := []TrialSummary{{Round: 0, P95Nanos: 30}, {Round: 1, P95Nanos: 30}, {Round: 2, P95Nanos: 30}, {Round: 3, P95Nanos: 30}}
	slow, err := Analyze(baseline, candidate, 9, 1000, 0.95, 0.5, 20)
	if err != nil || !slow.Regression || slow.MeanAbsoluteP95DeltaNanos != 20 || slow.MeanRelativeP95Delta != 2 || slow.LowerRelativeCI != 2 {
		t.Fatalf("slow analysis = %#v, err=%v", slow, err)
	}
	repeated, err := Analyze(baseline, candidate, 9, 1000, 0.95, 0.5, 20)
	if err != nil || !reflect.DeepEqual(slow, repeated) {
		t.Fatal("paired bootstrap is not deterministic")
	}
}

func TestPairedAnalysisRecomputationFailsClosed(t *testing.T) {
	trials := []TrialSummary{
		{Round: 1, Target: "candidate", P95Nanos: 30},
		{Round: 0, Target: "baseline", P95Nanos: 10},
		{Round: 1, Target: "baseline", P95Nanos: 10},
		{Round: 0, Target: "candidate", P95Nanos: 30},
	}
	pairs, err := PairP95Trials(trials, 2)
	if err != nil || !reflect.DeepEqual(pairs, []P95Pair{{Round: 0, BaselineP95Nanos: 10, CandidateP95Nanos: 30}, {Round: 1, BaselineP95Nanos: 10, CandidateP95Nanos: 30}}) {
		t.Fatalf("p95 pairs = %#v, err=%v", pairs, err)
	}
	analysis, err := Analyze(
		[]TrialSummary{{Round: 0, P95Nanos: 10}, {Round: 1, P95Nanos: 10}},
		[]TrialSummary{{Round: 0, P95Nanos: 30}, {Round: 1, P95Nanos: 30}},
		9, 1000, 0.95, 0.5, 20,
	)
	if err != nil || ValidateAnalysis(pairs, analysis) != nil {
		t.Fatalf("valid paired analysis failed: %#v, err=%v", analysis, err)
	}
	tampered := analysis
	tampered.MeanRelativeP95Delta++
	if err := ValidateAnalysis(pairs, tampered); err == nil {
		t.Fatal("tampered relative point estimate passed recomputation")
	}
	tampered = analysis
	tampered.LowerRelativeCI = math.NaN()
	if err := ValidateAnalysis(pairs, tampered); err == nil {
		t.Fatal("non-finite confidence bound passed recomputation")
	}
	duplicate := append(append([]TrialSummary(nil), trials[:3]...), trials[1])
	if _, err := PairP95Trials(duplicate, 2); err == nil {
		t.Fatal("duplicate and incomplete p95 inventory passed pairing")
	}
}

func TestBenchmarkHumanReportsAlignEstimateIntervalAndUnits(t *testing.T) {
	report := Report{
		RunID: "bench-test", Classification: "PASS", State: "COMPLETE", EvidenceScope: "local-development",
		Plan: PlanEvidence{Rounds: 4},
		Analysis: Analysis{
			MeanAbsoluteP95DeltaNanos: 12.5,
			MeanRelativeP95Delta:      0.125,
			Confidence:                0.9,
			LowerRelativeCI:           0.1,
			UpperRelativeCI:           0.2,
		},
	}
	textReport := renderText(report)
	for _, want := range []string{
		"mean paired absolute p95 delta: 12.50 ns",
		"mean paired relative p95 delta: 12.500000%",
		"90.0% confidence interval for mean paired relative p95 delta: [10.000000%, 20.000000%]",
	} {
		if !strings.Contains(textReport, want) {
			t.Fatalf("text report omits %q:\n%s", want, textReport)
		}
	}
	htmlReport, err := renderHTML(report)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"Mean paired absolute p95 delta</dt><dd>12.50 ns",
		"Mean paired relative p95 delta</dt><dd>12.500000%",
		"90.0% confidence interval for mean paired relative p95 delta</dt><dd>[10.000000%, 20.000000%]",
	} {
		if !strings.Contains(string(htmlReport), want) {
			t.Fatalf("HTML report omits %q:\n%s", want, htmlReport)
		}
	}
}

func TestBenchmarkTargetRejectsInstrumentation(t *testing.T) {
	workload := validWorkload()
	target := validTarget("sha256:" + repeat("a", 64))
	target.Spec.Services[0].Environment["OTEL_EXPORTER_OTLP_ENDPOINT"] = "http://collector"
	if violations := ValidateInputs(workload, target, target, true); len(violations) == 0 {
		t.Fatal("instrumented benchmark target passed validation")
	}
}

func TestPublicationRejectsNonportableLocalImages(t *testing.T) {
	workload := validWorkload()
	workload.Spec.EvidenceScope = "publication"
	target := validTarget("sha256:" + repeat("a", 64))
	violations := ValidateInputs(workload, target, target, true)
	found := false
	for _, violation := range violations {
		found = found || violation.Rule == "publication_image"
	}
	if !found {
		t.Fatalf("publication local-image violations = %#v", violations)
	}
}

func TestBenchmarkTransportDisablesCompression(t *testing.T) {
	transport := newBenchmarkTransport("127.0.0.1:1234", validWorkload())
	defer transport.CloseIdleConnections()
	if !transport.DisableCompression || transport.Proxy != nil || transport.ForceAttemptHTTP2 {
		t.Fatalf("transport measured-path policy = %#v", transport)
	}
}

func TestResourceSummaryRejectsBackwardCounters(t *testing.T) {
	samples := []resourceSample{
		{CPUTotalUsageNanos: 100, MemoryUsageBytes: 20, MemoryLimitBytes: 100, PIDs: 1, ThrottleAvailable: true, ThrottledPeriods: 3, ThrottledTimeNanos: 5},
		{CPUTotalUsageNanos: 150, MemoryUsageBytes: 30, MemoryLimitBytes: 100, PIDs: 2, ThrottleAvailable: true, ThrottledPeriods: 4, ThrottledTimeNanos: 7},
	}
	summary, err := summarizeResources(samples, true)
	if err != nil || summary.CPUUsageDeltaNanos != 50 || summary.MaxMemoryUsageBytes != 30 || summary.MaxPIDs != 2 {
		t.Fatalf("resource summary = %#v, err=%v", summary, err)
	}
	samples[1].CPUTotalUsageNanos = 99
	if _, err := summarizeResources(samples, false); err == nil {
		t.Fatal("backward CPU counter passed validation")
	}
}

func TestOutputBudgetCountsEveryRegularArtifact(t *testing.T) {
	directory := t.TempDir()
	if err := os.WriteFile(directory+"/one", make([]byte, 10), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := enforceOutputBudget(directory, 14, 4); err != nil {
		t.Fatalf("exact output budget failed: %v", err)
	}
	if err := enforceOutputBudget(directory, 13, 4); err == nil {
		t.Fatal("oversized output passed aggregate budget")
	}
}

func TestResourceSampleScheduleUsesAbsoluteIntervals(t *testing.T) {
	offsets := resourceSampleOffsets(2*time.Second, 250*time.Millisecond)
	want := []time.Duration{0, 250 * time.Millisecond, 500 * time.Millisecond, 750 * time.Millisecond, time.Second, 1250 * time.Millisecond, 1500 * time.Millisecond, 1750 * time.Millisecond}
	if !reflect.DeepEqual(offsets, want) {
		t.Fatalf("resource sample offsets = %v, want %v", offsets, want)
	}
	if offsets := resourceSampleOffsets(time.Second, 0); offsets != nil {
		t.Fatalf("invalid resource sample interval produced %v", offsets)
	}
}

func TestBoundedCommandBufferFailsClosed(t *testing.T) {
	buffer := newBoundedCommandBuffer(3)
	if _, err := buffer.Write([]byte("abcd")); err == nil || string(buffer.Bytes()) != "abc" {
		t.Fatalf("bounded buffer = %q, err=%v", buffer.String(), err)
	}
	if normalizeBenchmarkArchitecture("aarch64") != "arm64" || normalizeBenchmarkArchitecture("x86_64") != "amd64" {
		t.Fatal("Docker architecture aliases were not normalized")
	}
}

func validWorkload() spec.BenchmarkWorkload {
	duration := func(value time.Duration) spec.Duration { return spec.Duration{Duration: value} }
	return spec.BenchmarkWorkload{
		APIVersion: spec.APIVersion, Kind: "BenchmarkWorkload", Metadata: spec.Metadata{Name: "benchmark"},
		Spec: spec.BenchmarkWorkloadSpec{
			Service: "benchmark-api", EvidenceScope: "local-development",
			Operations:             []spec.BenchmarkOperation{{ID: "work", Weight: 1, Method: "POST", Path: "/work", Headers: map[string]string{}, Body: "{}", ExpectedStatuses: []int{200}}},
			Schedule:               spec.BenchmarkSchedule{Algorithm: spec.BenchmarkScheduleAlgorithm, RatePerSecond: 2, Warmup: duration(time.Second), Measurement: duration(2 * time.Second), RequestTimeout: duration(time.Second), MaxInFlight: 2, MaxScheduleLag: duration(10 * time.Millisecond), Rounds: 4, OrderSeed: 1, RequestSeed: 2},
			Analysis:               spec.BenchmarkAnalysis{Algorithm: spec.BenchmarkBootstrapAlgorithm, BootstrapSeed: 3, BootstrapResamples: 1000, Confidence: 0.95, BlockSize: 1, MinRelativeP95Delta: 0.5, MinAbsoluteP95Delta: duration(time.Millisecond)},
			Validity:               spec.BenchmarkValidity{MaxResponseHeaderBytes: 32768, MaxResponseBodyBytes: 1 << 20, MaxRawTimingBytes: 1 << 20, MaxResourceBytes: 1 << 20, MaxOutputBytes: 2 << 20},
			ResourceSampleInterval: duration(250 * time.Millisecond),
		},
	}
}

func validTarget(image string) spec.Target {
	duration := func(value time.Duration) spec.Duration { return spec.Duration{Duration: value} }
	return spec.Target{
		APIVersion: spec.APIVersion, Kind: "Target", Metadata: spec.Metadata{Name: "benchmark"},
		Spec: spec.TargetSpec{DatabaseSchemaVersion: "none-v1", Services: []spec.Service{{
			Name: "benchmark-api", Image: image, Command: []string{}, Args: []string{}, Environment: map[string]string{}, SecretEnvironment: map[string]string{},
			Health: spec.Health{Type: "http", Path: "/healthz", Port: 8080, Timeout: duration(time.Second), Interval: duration(100 * time.Millisecond)},
			Probe:  spec.ProbeDeclaration{Enabled: false}, Resources: spec.Resources{CPUs: 1, MemoryBytes: 128 << 20, PIDs: 64}, Dependencies: []string{},
		}}},
	}
}

func repeat(value string, count int) string {
	result := ""
	for range count {
		result += value
	}
	return result
}
