package app

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/uday1o1/chronicle-gate/internal/engine"
)

func TestRunCLIMapsConfirmedRegression(t *testing.T) {
	t.Parallel()
	repository := filepath.Join("..", "..")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	called := false
	code := Execute(context.Background(), []string{
		"run",
		"--scenario", filepath.Join(repository, "examples", "order-lifecycle", "scenarios", "r1-offset-rewind.yaml"),
		"--baseline", filepath.Join(repository, "examples", "order-lifecycle", "targets", "baseline.yaml"),
		"--candidate", filepath.Join(repository, "examples", "order-lifecycle", "targets", "candidate.yaml"),
		"--out", filepath.Join(t.TempDir(), "run"),
		"--json",
	}, &stdout, &stderr, Dependencies{Run: func(_ context.Context, _ engine.Config) engine.Report {
		called = true
		return engine.Report{APIVersion: "chronicle.dev/v1alpha1", Kind: "Result", RunID: "test", State: "COMPLETE", Classification: "SEMANTIC_REGRESSION"}
	}})
	if !called {
		t.Fatal("run dependency was not invoked")
	}
	if code != ExitRegression {
		t.Fatalf("Execute() code = %d, stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	var report engine.Report
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("run output is not one JSON document: %v\n%s", err, stdout.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr contaminated JSON: %s", stderr.String())
	}
}

func TestRunCLIMapsInterruptedState(t *testing.T) {
	repository := filepath.Join("..", "..")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := Execute(context.Background(), []string{
		"run", "--scenario", filepath.Join(repository, "examples/order-lifecycle/scenarios/r1-offset-rewind.yaml"),
		"--baseline", filepath.Join(repository, "examples/order-lifecycle/targets/baseline.yaml"),
		"--candidate", filepath.Join(repository, "examples/order-lifecycle/targets/candidate.yaml"),
		"--out", filepath.Join(t.TempDir(), "run"), "--json",
	}, &stdout, &stderr, Dependencies{Run: func(context.Context, engine.Config) engine.Report {
		return engine.Report{APIVersion: "chronicle.dev/v1alpha1", Kind: "Result", RunID: "interrupted", State: "INTERRUPTED", Classification: "UNRESOLVED"}
	}})
	if code != ExitInterrupted {
		t.Fatalf("interrupted run exit=%d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
}
