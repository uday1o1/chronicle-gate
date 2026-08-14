package bench

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"html/template"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	mobyclient "github.com/moby/moby/client"
	"github.com/uday1o1/chronicle-gate/internal/artifact"
	chronruntime "github.com/uday1o1/chronicle-gate/internal/runtime"
	"github.com/uday1o1/chronicle-gate/internal/spec"
)

func emptyAnalysis(workload spec.BenchmarkWorkload) Analysis {
	return Analysis{
		Algorithm: spec.BenchmarkBootstrapAlgorithm, BootstrapSeed: workload.Spec.Analysis.BootstrapSeed,
		BootstrapResamples: workload.Spec.Analysis.BootstrapResamples, Confidence: workload.Spec.Analysis.Confidence,
		BlockSize: 1, AbsoluteP95DeltasNanos: []int64{}, RelativeP95Deltas: []float64{},
		AbsoluteThresholdNanos: workload.Spec.Analysis.MinAbsoluteP95Delta.Nanoseconds(),
		RelativeThreshold:      workload.Spec.Analysis.MinRelativeP95Delta,
	}
}

func oppositeRole(role string) string {
	if role == "baseline" {
		return "candidate"
	}
	return "baseline"
}

func mergeInstrumentation(target *instrumentationEvidence, next instrumentationEvidence, initialized *bool) {
	if !*initialized {
		*target = next
		*initialized = true
		return
	}
	target.HarnessCorrectnessInstrumentationAbsent = target.HarnessCorrectnessInstrumentationAbsent && next.HarnessCorrectnessInstrumentationAbsent
	target.ProbeDisabled = target.ProbeDisabled && next.ProbeDisabled
	target.TelemetryEnvironmentAbsent = target.TelemetryEnvironmentAbsent && next.TelemetryEnvironmentAbsent
	target.SecretMountsAbsent = target.SecretMountsAbsent && next.SecretMountsAbsent
	target.DockerHealthcheckDisabled = target.DockerHealthcheckDisabled && next.DockerHealthcheckDisabled
	target.AutomaticCompressionDisabled = target.AutomaticCompressionDisabled && next.AutomaticCompressionDisabled
	target.TargetBinaryInternalsProven = target.TargetBinaryInternalsProven && next.TargetBinaryInternalsProven
	target.Claim = next.Claim
}

func classifyRunError(ctx context.Context, report *Report, err error) {
	report.Error = err.Error()
	if errors.Is(ctx.Err(), context.Canceled) {
		report.State = "INTERRUPTED"
		report.Classification = "UNRESOLVED"
		return
	}
	var typed *benchmarkError
	if errors.As(err, &typed) {
		report.Classification = string(typed.kind)
		report.State = string(typed.kind)
		return
	}
	if errors.Is(err, context.DeadlineExceeded) {
		report.State = "TIMEOUT"
		report.Classification = "TIMEOUT"
		return
	}
	report.State = "INFRASTRUCTURE_ERROR"
	report.Classification = "INFRASTRUCTURE_ERROR"
}

func finishWithoutArtifacts(report Report, kind failureKind, err error) Report {
	report.Classification = string(kind)
	report.State = string(kind)
	report.Error = err.Error()
	report.CompletedAt = time.Now().UTC().Format(time.RFC3339Nano)
	return report
}

func finishWithArtifacts(report Report, root string, kind failureKind, runErr error, timings, resources *streamArtifact) Report {
	if kind != "" && runErr != nil {
		report.Classification = string(kind)
		report.State = string(kind)
		report.Error = runErr.Error()
	}
	if report.CompletedAt == "" {
		report.CompletedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	var artifactErr error
	if timings != nil {
		if err := timings.publish(root, "raw-timings.ndjson"); err != nil {
			artifactErr = errors.Join(artifactErr, fmt.Errorf("publish raw timings: %w", err))
			timings.discard()
		}
	}
	if resources != nil {
		if err := resources.publish(root, "resource-samples.ndjson"); err != nil {
			artifactErr = errors.Join(artifactErr, fmt.Errorf("publish resource samples: %w", err))
			resources.discard()
		}
	}
	if artifactErr != nil {
		report.Classification = "INFRASTRUCTURE_ERROR"
		report.State = "INFRASTRUCTURE_ERROR"
		report.Error = errors.Join(runErr, artifactErr).Error()
	}
	if report.Environment == nil {
		report.Environment = map[string]any{}
	}
	if err := artifact.WriteJSON(filepath.Join(root, "environment.json"), report.Environment); err != nil {
		artifactErr = errors.Join(artifactErr, err)
	}
	textReport := renderText(report)
	if err := artifact.WriteFile(filepath.Join(root, "report.txt"), []byte(textReport)); err != nil {
		artifactErr = errors.Join(artifactErr, err)
	}
	htmlReport, err := renderHTML(report)
	if err != nil {
		artifactErr = errors.Join(artifactErr, err)
	} else if err := artifact.WriteFile(filepath.Join(root, "report.html"), htmlReport); err != nil {
		artifactErr = errors.Join(artifactErr, err)
	}
	if artifactErr != nil {
		report.Classification = "INFRASTRUCTURE_ERROR"
		report.State = "INFRASTRUCTURE_ERROR"
		report.Error = errors.Join(runErr, artifactErr).Error()
		report.CompletedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	if err := artifact.WriteJSON(filepath.Join(root, "benchmark.json"), report); err != nil {
		report.Classification = "INFRASTRUCTURE_ERROR"
		report.State = "INFRASTRUCTURE_ERROR"
		report.Error = errors.Join(runErr, artifactErr, err).Error()
		return report
	}
	if err := enforceOutputBudget(root, report.MaxOutputBytes, 4<<10); err != nil {
		report.Classification = "INFRASTRUCTURE_ERROR"
		report.State = "INFRASTRUCTURE_ERROR"
		report.Error = errors.Join(runErr, artifactErr, err).Error()
		_ = artifact.WriteJSON(filepath.Join(root, "benchmark.json"), report)
		return report
	}
	if err := artifact.WriteChecksums(root, map[string]struct{}{"checksums.sha256": {}}); err != nil {
		report.Classification = "INFRASTRUCTURE_ERROR"
		report.State = "INFRASTRUCTURE_ERROR"
		report.Error = errors.Join(runErr, artifactErr, err).Error()
		_ = artifact.WriteJSON(filepath.Join(root, "benchmark.json"), report)
	}
	return report
}

func enforceOutputBudget(root string, maximum, checksumReserve int64) error {
	if maximum <= 0 {
		return fmt.Errorf("benchmark output budget is not positive")
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return fmt.Errorf("inspect benchmark output budget: %w", err)
	}
	total := int64(0)
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			return fmt.Errorf("inspect benchmark artifact %q: %w", entry.Name(), err)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("benchmark output contains non-regular entry %q", entry.Name())
		}
		if info.Size() > maximum-total {
			return fmt.Errorf("benchmark output exceeds %d bytes before checksums", maximum)
		}
		total += info.Size()
	}
	if checksumReserve > maximum-total {
		return fmt.Errorf("benchmark output leaves no bounded checksum budget within %d bytes", maximum)
	}
	return nil
}

func benchmarkArtifactPaths() map[string]string {
	return map[string]string{
		"result": "benchmark.json", "plan": "execution-plan.json", "timings": "raw-timings.ndjson",
		"resources": "resource-samples.ndjson", "environment": "environment.json", "text": "report.txt",
		"html": "report.html", "checksums": "checksums.sha256",
	}
}

func renderText(report Report) string {
	return fmt.Sprintf("ChronicleGate benchmark %s\nclassification: %s\nstate: %s\npaired rounds: %d\nmean p95 delta: %.0f ns\nrelative 95%% CI: [%.6f, %.6f]\nevidence scope: %s\nlimitation: local comparative results do not generalize to production capacity.\n",
		report.RunID, report.Classification, report.State, report.Plan.Rounds, report.Analysis.MeanAbsoluteP95DeltaNanos,
		report.Analysis.LowerRelativeCI, report.Analysis.UpperRelativeCI, report.EvidenceScope)
}

func renderHTML(report Report) ([]byte, error) {
	page := template.Must(template.New("benchmark").Parse(`<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>ChronicleGate benchmark</title></head>
<body><main><h1>ChronicleGate benchmark</h1><dl>
<dt>Run</dt><dd>{{.RunID}}</dd><dt>Classification</dt><dd>{{.Classification}}</dd>
<dt>State</dt><dd>{{.State}}</dd><dt>Paired rounds</dt><dd>{{.Plan.Rounds}}</dd>
<dt>Mean p95 delta</dt><dd>{{printf "%.0f" .Analysis.MeanAbsoluteP95DeltaNanos}} ns</dd>
<dt>Relative confidence interval</dt><dd>[{{printf "%.6f" .Analysis.LowerRelativeCI}}, {{printf "%.6f" .Analysis.UpperRelativeCI}}]</dd>
<dt>Evidence scope</dt><dd>{{.EvidenceScope}}</dd></dl>
<p>Local comparative results do not generalize to production capacity.</p></main></body></html>
`))
	var output bytes.Buffer
	if err := page.Execute(&output, report); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}

type hostIdleEvidence struct {
	Round                 int     `json:"round,omitempty"`
	Target                string  `json:"target,omitempty"`
	StartedAt             string  `json:"startedAt"`
	CompletedAt           string  `json:"completedAt"`
	WindowNanos           int64   `json:"windowNanos"`
	IdleFraction          float64 `json:"idleFraction"`
	StartLoadOne          float64 `json:"startLoadOne"`
	EndLoadOne            float64 `json:"endLoadOne"`
	MaxNormalizedLoad     float64 `json:"maxNormalizedLoad"`
	MinimumIdleFraction   float64 `json:"minimumIdleFraction"`
	MaximumNormalizedLoad float64 `json:"maximumNormalizedLoad"`
}

type hostCPUCounter struct {
	idle  uint64
	total uint64
}

func validatePublicationHost(ctx context.Context, dedicated bool) (map[string]any, error) {
	evidence := map[string]any{
		"dedicatedHostAttested": dedicated,
		"nativeLinuxRequired":   true,
		"localDockerRequired":   true,
		"sharedCIForbidden":     true,
	}
	if !dedicated {
		return evidence, fmt.Errorf("publication evidence requires explicit dedicated-host attestation")
	}
	if runtime.GOOS != "linux" {
		return evidence, fmt.Errorf("publication evidence requires a native Linux host")
	}
	if os.Getenv("CI") != "" || os.Getenv("GITHUB_ACTIONS") != "" {
		return evidence, fmt.Errorf("publication evidence is forbidden on shared CI runners")
	}
	if _, err := os.Stat("/.dockerenv"); err == nil {
		return evidence, fmt.Errorf("publication evidence cannot run from inside a container")
	}
	if err := chronruntime.ConfigureDockerHost(ctx); err != nil {
		return evidence, err
	}
	if !strings.HasPrefix(os.Getenv("DOCKER_HOST"), "unix://") {
		return evidence, fmt.Errorf("publication evidence requires a local Unix-socket Docker endpoint")
	}
	docker, err := mobyclient.New(mobyclient.FromEnv)
	if err != nil {
		return evidence, err
	}
	defer func() { _ = docker.Close() }()
	if err := requireNoRunningContainers(ctx, docker); err != nil {
		return evidence, err
	}
	idle, err := sampleHostIdle(ctx, 30*time.Second)
	evidence["idlePreflight"] = idle
	if err != nil {
		return evidence, err
	}
	return evidence, nil
}

func requireNoRunningContainers(ctx context.Context, docker *mobyclient.Client) error {
	listed, err := docker.ContainerList(ctx, mobyclient.ContainerListOptions{})
	if err != nil {
		return fmt.Errorf("inspect publication Docker activity: %w", err)
	}
	if len(listed.Items) != 0 {
		return fmt.Errorf("publication idle preflight found %d running Docker containers", len(listed.Items))
	}
	return nil
}

func verifyPublicationTrialIdle(ctx context.Context, round int, target string) (hostIdleEvidence, error) {
	docker, err := mobyclient.New(mobyclient.FromEnv)
	if err != nil {
		return hostIdleEvidence{}, fmt.Errorf("create publication Docker client: %w", err)
	}
	defer func() { _ = docker.Close() }()
	if err := requireNoRunningContainers(ctx, docker); err != nil {
		return hostIdleEvidence{}, err
	}
	evidence, err := sampleHostIdle(ctx, 2*time.Second)
	evidence.Round = round
	evidence.Target = target
	return evidence, err
}

func sampleHostIdle(ctx context.Context, window time.Duration) (hostIdleEvidence, error) {
	const minimumIdle = 0.90
	const maximumLoad = 0.25
	startedAt := time.Now().UTC()
	startCPU, err := readHostCPUCounter()
	if err != nil {
		return hostIdleEvidence{}, err
	}
	startLoad, err := readHostLoadOne()
	if err != nil {
		return hostIdleEvidence{}, err
	}
	timer := time.NewTimer(window)
	select {
	case <-ctx.Done():
		timer.Stop()
		return hostIdleEvidence{}, ctx.Err()
	case <-timer.C:
	}
	endCPU, err := readHostCPUCounter()
	if err != nil {
		return hostIdleEvidence{}, err
	}
	endLoad, err := readHostLoadOne()
	if err != nil {
		return hostIdleEvidence{}, err
	}
	if endCPU.total <= startCPU.total || endCPU.idle < startCPU.idle {
		return hostIdleEvidence{}, fmt.Errorf("publication CPU counters are not monotonic")
	}
	idleFraction := float64(endCPU.idle-startCPU.idle) / float64(endCPU.total-startCPU.total)
	maximumNormalized := math.Max(startLoad, endLoad) / float64(runtime.NumCPU())
	evidence := hostIdleEvidence{
		StartedAt: startedAt.Format(time.RFC3339Nano), CompletedAt: time.Now().UTC().Format(time.RFC3339Nano), WindowNanos: window.Nanoseconds(),
		IdleFraction: idleFraction, StartLoadOne: startLoad, EndLoadOne: endLoad, MaxNormalizedLoad: maximumNormalized,
		MinimumIdleFraction: minimumIdle, MaximumNormalizedLoad: maximumLoad,
	}
	if idleFraction < minimumIdle || maximumNormalized > maximumLoad {
		return evidence, fmt.Errorf("publication host is not idle: idle %.4f, normalized load %.4f", idleFraction, maximumNormalized)
	}
	return evidence, nil
}

func readHostCPUCounter() (hostCPUCounter, error) {
	document, err := readBoundedProcFile("/proc/stat", 1<<20)
	if err != nil {
		return hostCPUCounter{}, fmt.Errorf("read publication CPU counters: %w", err)
	}
	line, _, found := strings.Cut(string(document), "\n")
	fields := strings.Fields(line)
	if !found || len(fields) < 9 || fields[0] != "cpu" {
		return hostCPUCounter{}, fmt.Errorf("/proc/stat has no complete aggregate CPU line")
	}
	values := make([]uint64, 8)
	for index := range values {
		value, err := strconv.ParseUint(fields[index+1], 10, 64)
		if err != nil {
			return hostCPUCounter{}, fmt.Errorf("parse /proc/stat CPU field %d: %w", index, err)
		}
		values[index] = value
	}
	total := uint64(0)
	for _, value := range values {
		if value > math.MaxUint64-total {
			return hostCPUCounter{}, fmt.Errorf("/proc/stat aggregate CPU counters overflow")
		}
		total += value
	}
	if values[3] > math.MaxUint64-values[4] {
		return hostCPUCounter{}, fmt.Errorf("/proc/stat idle counters overflow")
	}
	return hostCPUCounter{idle: values[3] + values[4], total: total}, nil
}

func readHostLoadOne() (float64, error) {
	document, err := readBoundedProcFile("/proc/loadavg", 4<<10)
	if err != nil {
		return 0, fmt.Errorf("read publication load average: %w", err)
	}
	fields := strings.Fields(string(document))
	if len(fields) < 1 {
		return 0, fmt.Errorf("/proc/loadavg is empty")
	}
	value, err := strconv.ParseFloat(fields[0], 64)
	if err != nil || math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
		return 0, fmt.Errorf("parse /proc/loadavg one-minute value")
	}
	return value, nil
}

func readBoundedProcFile(path string, limit int64) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	document, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(document)) > limit {
		return nil, fmt.Errorf("%s exceeds %d bytes", path, limit)
	}
	return document, nil
}

func newBenchmarkRunID() string {
	value := make([]byte, 8)
	if _, err := rand.Read(value); err != nil {
		return fmt.Sprintf("bench-%d", time.Now().UnixNano())
	}
	return "bench-" + hex.EncodeToString(value)
}
