//go:build integration

package benchmark_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/uday1o1/chronicle-gate/internal/artifact"
	"github.com/uday1o1/chronicle-gate/internal/bench"
	"github.com/uday1o1/chronicle-gate/internal/spec"
)

func TestPublicBenchmarkAAndSlowdownGates(t *testing.T) {
	repository, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	workload := filepath.Join(repository, "benchmarks/workloads/order-api.yaml")
	baseline := filepath.Join(repository, "examples/order-lifecycle/targets/generated/benchmark-baseline.yaml")
	candidate := filepath.Join(repository, "examples/order-lifecycle/targets/generated/benchmark-candidate.yaml")
	for _, testCase := range []struct {
		name           string
		candidate      string
		wantExit       int
		classification string
	}{
		{"aa-first", baseline, 0, "PASS"},
		{"aa-second", baseline, 0, "PASS"},
		{"slow-first", candidate, 2, "PERFORMANCE_REGRESSION"},
		{"slow-second", candidate, 2, "PERFORMANCE_REGRESSION"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			output := filepath.Join(repository, "run", "benchmark-"+testCase.name)
			_ = os.RemoveAll(output)
			t.Cleanup(func() { _ = os.RemoveAll(output) })
			report, stdout, stderr, exit := runBenchmark(t, repository, workload, baseline, testCase.candidate, output)
			if exit != testCase.wantExit || report.Classification != testCase.classification || report.State != "COMPLETE" {
				t.Fatalf("exit=%d report=%#v\nstdout=%s\nstderr=%s", exit, report, stdout, stderr)
			}
			if strings.TrimSpace(stderr) != "" {
				t.Fatalf("JSON benchmark wrote stderr: %s", stderr)
			}
			if len(report.Trials) != 8 || report.Plan.Rounds != 4 {
				t.Fatalf("trial inventory = %d over %d rounds", len(report.Trials), report.Plan.Rounds)
			}
			if len(report.Aggregates) != 2 || report.Aggregates[0].Requests != 160 || report.Aggregates[1].Requests != 160 {
				t.Fatalf("aggregate inventory = %#v", report.Aggregates)
			}
			for _, trial := range report.Trials {
				if trial.Resource.Samples < 2 || trial.Resource.MemoryLimitBytes == 0 || trial.Resource.MaxPIDs == 0 {
					t.Fatalf("trial resource summary = %#v", trial.Resource)
				}
			}
			if !report.Instrumentation.HarnessCorrectnessInstrumentationAbsent || !report.Instrumentation.DockerHealthcheckDisabled || !report.Instrumentation.AutomaticCompressionDisabled || !report.Instrumentation.TargetBinaryInternalsProven {
				t.Fatalf("instrumentation evidence = %#v", report.Instrumentation)
			}
			if report.Analysis.Regression != (testCase.classification == "PERFORMANCE_REGRESSION") {
				t.Fatalf("analysis regression = %t", report.Analysis.Regression)
			}
			if err := artifact.VerifyChecksums(output); err != nil {
				t.Fatal(err)
			}
			resultDocument, err := os.ReadFile(filepath.Join(output, "benchmark.json"))
			if err != nil {
				t.Fatal(err)
			}
			if err := spec.ValidateBenchmarkResultJSON(resultDocument); err != nil {
				t.Fatal(err)
			}
			assertLineCount(t, filepath.Join(output, "raw-timings.ndjson"), 400)
			assertMinimumLineCount(t, filepath.Join(output, "resource-samples.ndjson"), 56)
			assertNoDockerResources(t, report.RunID)
		})
	}
}

func runBenchmark(t *testing.T, repository, workload, baseline, candidate, output string) (bench.Report, string, string, int) {
	t.Helper()
	command := exec.Command(filepath.Join(repository, "bin/chronicle"), "bench", "--workload", workload, "--baseline", baseline, "--candidate", candidate, "--out", output, "--development-local-images", "--json")
	command.Dir = repository
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	err := command.Run()
	exit := 0
	if err != nil {
		var exitError *exec.ExitError
		if !errors.As(err, &exitError) {
			t.Fatal(err)
		}
		exit = exitError.ExitCode()
	}
	var report bench.Report
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("decode benchmark JSON: %v\n%s", err, stdout.String())
	}
	return report, stdout.String(), stderr.String(), exit
}

func assertLineCount(t *testing.T, path string, want int) {
	t.Helper()
	document, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := bytes.Count(document, []byte{'\n'}); got != want {
		t.Fatalf("%s lines = %d, want %d", path, got, want)
	}
}

func assertMinimumLineCount(t *testing.T, path string, minimum int) {
	t.Helper()
	document, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := bytes.Count(document, []byte{'\n'}); got < minimum {
		t.Fatalf("%s lines = %d, want at least %d", path, got, minimum)
	}
}

func assertNoDockerResources(t *testing.T, runID string) {
	t.Helper()
	for _, arguments := range [][]string{
		{"ps", "-a", "--filter", "label=dev.chronicle.run=" + runID, "--format", "{{.ID}}"},
		{"network", "ls", "--filter", "label=dev.chronicle.run=" + runID, "--format", "{{.ID}}"},
	} {
		document, err := exec.Command("docker", arguments...).CombinedOutput()
		if err != nil {
			t.Fatalf("docker %v: %v: %s", arguments, err, document)
		}
		if strings.TrimSpace(string(document)) != "" {
			t.Fatalf("benchmark resources leaked for %s: %s", runID, document)
		}
	}
}
