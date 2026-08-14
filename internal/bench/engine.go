package bench

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/uday1o1/chronicle-gate/internal/artifact"
	"github.com/uday1o1/chronicle-gate/internal/spec"
)

type Config struct {
	Workload               spec.BenchmarkWorkload
	Baseline               spec.Target
	Candidate              spec.Target
	Output                 string
	DevelopmentLocalImages bool
	DedicatedHost          bool
}

type PlanEvidence struct {
	Algorithm string `json:"algorithm"`
	SHA256    string `json:"sha256"`
	Rounds    int    `json:"rounds"`
}

type ValidityCheck struct {
	ID      string `json:"id"`
	Status  string `json:"status"`
	Message string `json:"message"`
}

type Report struct {
	APIVersion      string                  `json:"apiVersion"`
	Kind            string                  `json:"kind"`
	RunID           string                  `json:"runId"`
	State           string                  `json:"state"`
	Classification  string                  `json:"classification"`
	StartedAt       string                  `json:"startedAt"`
	CompletedAt     string                  `json:"completedAt"`
	EvidenceScope   string                  `json:"evidenceScope"`
	Plan            PlanEvidence            `json:"plan"`
	Targets         []targetIdentity        `json:"targets"`
	Trials          []TrialSummary          `json:"trials"`
	Aggregates      []TargetSummary         `json:"aggregates"`
	Analysis        Analysis                `json:"analysis"`
	ValidityChecks  []ValidityCheck         `json:"validityChecks"`
	Instrumentation instrumentationEvidence `json:"instrumentation"`
	Environment     map[string]any          `json:"environment"`
	Artifacts       map[string]string       `json:"artifacts"`
	Error           string                  `json:"error,omitempty"`
	MaxOutputBytes  int64                   `json:"-"`
}

func Run(ctx context.Context, config Config) Report {
	started := time.Now().UTC()
	report := Report{
		APIVersion: spec.APIVersion, Kind: "BenchmarkResult", RunID: newBenchmarkRunID(), State: "RUNNING",
		Classification: "UNRESOLVED", StartedAt: started.Format(time.RFC3339Nano), EvidenceScope: config.Workload.Spec.EvidenceScope,
		Targets: []targetIdentity{}, Trials: []TrialSummary{}, Aggregates: []TargetSummary{},
		ValidityChecks: []ValidityCheck{}, Artifacts: benchmarkArtifactPaths(),
		Analysis:       emptyAnalysis(config.Workload),
		MaxOutputBytes: config.Workload.Spec.Validity.MaxOutputBytes,
	}
	plan, err := BuildPlan(config.Workload)
	if err != nil {
		return finishWithoutArtifacts(report, failureUnresolved, err)
	}
	report.Plan = PlanEvidence{Algorithm: plan.Algorithm, SHA256: plan.SHA256, Rounds: len(plan.Rounds)}
	if err := artifact.PrepareDirectory(config.Output); err != nil {
		return finishWithoutArtifacts(report, failureInfrastructure, err)
	}
	if err := artifact.WriteJSON(filepath.Join(config.Output, "execution-plan.json"), plan); err != nil {
		return finishWithArtifacts(report, config.Output, failureInfrastructure, err, nil, nil)
	}
	rawTimings, err := newStreamArtifact(config.Output, "raw-timings", config.Workload.Spec.Validity.MaxRawTimingBytes)
	if err != nil {
		return finishWithArtifacts(report, config.Output, failureInfrastructure, err, nil, nil)
	}
	rawResources, err := newStreamArtifact(config.Output, "resource-samples", config.Workload.Spec.Validity.MaxResourceBytes)
	if err != nil {
		rawTimings.discard()
		return finishWithArtifacts(report, config.Output, failureInfrastructure, err, nil, nil)
	}
	host, err := benchmarkHostProvenance(ctx)
	if err != nil {
		return finishWithArtifacts(report, config.Output, failureInfrastructure, err, rawTimings, rawResources)
	}
	report.Environment = host
	report.Environment["evidenceBoundary"] = "Comparative local evidence does not generalize to production capacity."
	if config.Workload.Spec.EvidenceScope == "publication" {
		publicationEvidence, publicationErr := validatePublicationHost(ctx, config.DedicatedHost)
		report.Environment["publicationHost"] = publicationEvidence
		if publicationErr != nil {
			return finishWithArtifacts(report, config.Output, failureInfrastructure, publicationErr, rawTimings, rawResources)
		}
	}
	baselineIdentity, err := materializeTarget(ctx, "baseline", config.Baseline)
	if err != nil {
		return finishWithArtifacts(report, config.Output, failureInfrastructure, err, rawTimings, rawResources)
	}
	candidateIdentity, err := materializeTarget(ctx, "candidate", config.Candidate)
	if err != nil {
		return finishWithArtifacts(report, config.Output, failureInfrastructure, err, rawTimings, rawResources)
	}
	report.Targets = []targetIdentity{baselineIdentity, candidateIdentity}
	environment, err := newBenchmarkEnvironment(ctx, report.RunID)
	if err != nil {
		return finishWithArtifacts(report, config.Output, failureInfrastructure, err, rawTimings, rawResources)
	}
	report.Environment["network"] = environment.NetworkName
	baselineTrials := make([]TrialSummary, 0, len(plan.Rounds))
	candidateTrials := make([]TrialSummary, 0, len(plan.Rounds))
	baselineLatencies := []int64{}
	candidateLatencies := []int64{}
	baselineSuccesses := 0
	candidateSuccesses := 0
	instrumentationSet := false
	publicationIdleWindows := []hostIdleEvidence{}
	var runErr error
	for _, round := range plan.Rounds {
		roles := []string{round.First, oppositeRole(round.First)}
		for _, role := range roles {
			if ctx.Err() != nil {
				runErr = ctx.Err()
				break
			}
			target, identity := config.Baseline, baselineIdentity
			if role == "candidate" {
				target, identity = config.Candidate, candidateIdentity
			}
			if config.Workload.Spec.EvidenceScope == "publication" {
				idleEvidence, idleErr := verifyPublicationTrialIdle(ctx, round.Index, role)
				publicationIdleWindows = append(publicationIdleWindows, idleEvidence)
				report.Environment["publicationTrialIdleWindows"] = publicationIdleWindows
				if idleErr != nil {
					runErr = &benchmarkError{kind: failureInfrastructure, err: idleErr}
					break
				}
			}
			outcome, evidence, trialErr := executeTrial(ctx, environment, config.Workload, round, role, target.Spec.Services[0], identity, rawTimings, rawResources)
			mergeInstrumentation(&report.Instrumentation, evidence, &instrumentationSet)
			if trialErr != nil {
				runErr = trialErr
				break
			}
			summary := outcome.Summary
			report.Trials = append(report.Trials, summary)
			if role == "baseline" {
				baselineTrials = append(baselineTrials, summary)
				baselineLatencies = append(baselineLatencies, outcome.Latencies...)
				baselineSuccesses += summary.Successes
			} else {
				candidateTrials = append(candidateTrials, summary)
				candidateLatencies = append(candidateLatencies, outcome.Latencies...)
				candidateSuccesses += summary.Successes
			}
		}
		if runErr != nil {
			break
		}
	}
	report.State = "CLEANING"
	cleanupContext, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	cleanupErr := environment.cleanup(cleanupContext)
	cancel()
	if cleanupErr != nil {
		runErr = errors.Join(runErr, cleanupErr)
		report.Classification = "INFRASTRUCTURE_ERROR"
		report.State = "INFRASTRUCTURE_ERROR"
	} else if runErr == nil {
		measurementSeconds := config.Workload.Spec.Schedule.Measurement.Seconds() * float64(len(plan.Rounds))
		baselineAggregate, baselineErr := summarizeTarget("baseline", baselineLatencies, baselineSuccesses, measurementSeconds, baselineTrials)
		candidateAggregate, candidateErr := summarizeTarget("candidate", candidateLatencies, candidateSuccesses, measurementSeconds, candidateTrials)
		if baselineErr != nil || candidateErr != nil {
			runErr = &benchmarkError{kind: failureUnresolved, err: errors.Join(baselineErr, candidateErr)}
		}
		if runErr == nil {
			report.Aggregates = []TargetSummary{baselineAggregate, candidateAggregate}
		}
		if runErr == nil {
			analysis, analysisErr := Analyze(
				baselineTrials, candidateTrials, config.Workload.Spec.Analysis.BootstrapSeed,
				config.Workload.Spec.Analysis.BootstrapResamples, config.Workload.Spec.Analysis.Confidence,
				config.Workload.Spec.Analysis.MinRelativeP95Delta, config.Workload.Spec.Analysis.MinAbsoluteP95Delta.Nanoseconds(),
			)
			if analysisErr != nil {
				runErr = &benchmarkError{kind: failureUnresolved, err: analysisErr}
			} else {
				report.Analysis = analysis
				report.State = "COMPLETE"
				report.Classification = "PASS"
				if analysis.Regression {
					report.Classification = "PERFORMANCE_REGRESSION"
				}
				report.ValidityChecks = append(report.ValidityChecks, ValidityCheck{ID: "complete-paired-inventory", Status: "PASS", Message: fmt.Sprintf("%d paired rounds completed under the locked schedule", len(plan.Rounds))})
			}
		}
	}
	if runErr != nil && cleanupErr == nil {
		classifyRunError(ctx, &report, runErr)
	}
	if !instrumentationSet {
		report.Instrumentation.Claim = "No measured container started, so instrumentation absence was not established."
	}
	report.CompletedAt = time.Now().UTC().Format(time.RFC3339Nano)
	if report.Error == "" && runErr != nil {
		report.Error = runErr.Error()
	}
	return finishWithArtifacts(report, config.Output, "", nil, rawTimings, rawResources)
}

type trialOutcome struct {
	Summary   TrialSummary
	Latencies []int64
}

func executeTrial(ctx context.Context, environment *benchmarkEnvironment, workload spec.BenchmarkWorkload, round RoundPlan, role string, declaration spec.Service, identity targetIdentity, timings, resources *streamArtifact) (trialOutcome, instrumentationEvidence, error) {
	service, evidence, err := startBenchmarkService(ctx, environment, role, round.Index, declaration, identity)
	if err != nil {
		return trialOutcome{}, evidence, &benchmarkError{kind: failureInfrastructure, err: err}
	}
	cleanup := func() error {
		cleanupContext, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		return service.terminate(cleanupContext)
	}
	fail := func(cause error) (trialOutcome, instrumentationEvidence, error) {
		if cleanupErr := cleanup(); cleanupErr != nil {
			return trialOutcome{}, evidence, &benchmarkError{kind: failureInfrastructure, err: errors.Join(cause, cleanupErr)}
		}
		return trialOutcome{}, evidence, cause
	}
	healthContext, healthCancel := context.WithTimeout(ctx, declaration.Health.Timeout.Duration)
	err = service.health(healthContext, declaration.Health.Path)
	healthCancel()
	if err != nil {
		return fail(&benchmarkError{kind: failureInfrastructure, err: fmt.Errorf("pre-warmup health: %w", err)})
	}
	warmupStart := time.Now()
	warmup, err := runHTTPPhase(ctx, service, workload, round.Index, role, "warmup", round.Warmup, warmupStart)
	for _, record := range warmup {
		if writeErr := timings.write(record); writeErr != nil {
			return fail(&benchmarkError{kind: failureInfrastructure, err: writeErr})
		}
	}
	if err != nil {
		return fail(err)
	}
	measurementEvidence, err := service.inspect(ctx, declaration)
	if err != nil {
		return fail(&benchmarkError{kind: failureInfrastructure, err: err})
	}
	evidence = measurementEvidence
	measurementStart := time.Now()
	sampleChannel := make(chan sampleOutcome, 1)
	go sampleResources(ctx, service, workload, round.Index, role, measurementStart, sampleChannel)
	measurement, measurementErr := runHTTPPhase(ctx, service, workload, round.Index, role, "measurement", round.Measurement, measurementStart)
	samples := <-sampleChannel
	for _, record := range measurement {
		if writeErr := timings.write(record); writeErr != nil {
			return fail(&benchmarkError{kind: failureInfrastructure, err: writeErr})
		}
	}
	for _, sample := range samples.values {
		if writeErr := resources.write(sample); writeErr != nil {
			return fail(&benchmarkError{kind: failureInfrastructure, err: writeErr})
		}
	}
	if measurementErr != nil {
		return fail(measurementErr)
	}
	if samples.err != nil {
		return fail(&benchmarkError{kind: failureInfrastructure, err: samples.err})
	}
	minimumSamples := int(workload.Spec.Schedule.Measurement.Duration/workload.Spec.ResourceSampleInterval.Duration) - 1
	if minimumSamples < 2 {
		minimumSamples = 2
	}
	if len(samples.values) < minimumSamples {
		return fail(&benchmarkError{kind: failureInfrastructure, err: fmt.Errorf("resource sampler produced %d samples, want at least %d", len(samples.values), minimumSamples)})
	}
	healthContext, healthCancel = context.WithTimeout(ctx, declaration.Health.Timeout.Duration)
	err = service.health(healthContext, declaration.Health.Path)
	healthCancel()
	if err != nil {
		return fail(&benchmarkError{kind: failureInfrastructure, err: fmt.Errorf("post-measurement health: %w", err)})
	}
	if err := cleanup(); err != nil {
		return trialOutcome{}, evidence, &benchmarkError{kind: failureInfrastructure, err: err}
	}
	latencies := make([]int64, len(measurement))
	successes := 0
	for index, record := range measurement {
		latencies[index] = record.EndNanos - record.StartNanos
		if record.Success {
			successes++
		}
	}
	resourceSummary, err := summarizeResources(samples.values, workload.Spec.EvidenceScope == "publication")
	if err != nil {
		return trialOutcome{}, evidence, &benchmarkError{kind: failureInfrastructure, err: err}
	}
	summary, err := Summarize(round.Index, role, latencies, successes, workload.Spec.Schedule.Measurement.Seconds())
	if err != nil {
		return trialOutcome{}, evidence, &benchmarkError{kind: failureUnresolved, err: err}
	}
	summary.Resource = resourceSummary
	return trialOutcome{Summary: summary, Latencies: latencies}, evidence, nil
}

type sampleOutcome struct {
	values []resourceSample
	err    error
}

func sampleResources(ctx context.Context, service *benchmarkService, workload spec.BenchmarkWorkload, round int, role string, started time.Time, output chan<- sampleOutcome) {
	deadline := started.Add(workload.Spec.Schedule.Measurement.Duration)
	values := []resourceSample{}
	for {
		now := time.Now()
		if now.After(deadline) {
			output <- sampleOutcome{values: values}
			return
		}
		sample, err := service.sample(ctx, round, role, now.Sub(started))
		if err != nil {
			output <- sampleOutcome{values: values, err: err}
			return
		}
		values = append(values, sample)
		wait := workload.Spec.ResourceSampleInterval.Duration
		if remaining := time.Until(deadline); remaining < wait {
			wait = remaining
		}
		if wait <= 0 {
			continue
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			output <- sampleOutcome{values: values, err: ctx.Err()}
			return
		case <-timer.C:
		}
	}
}
