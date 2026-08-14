package app

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
)

func TestBenchmarkRefusesExistingOutputBeforeExecution(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	output := t.TempDir()
	code := Execute(context.Background(), []string{
		"bench", "--workload", filepath.Join(output, "workload.yaml"),
		"--baseline", filepath.Join(output, "baseline.yaml"),
		"--candidate", filepath.Join(output, "candidate.yaml"),
		"--out", output, "--json",
	}, &stdout, &stderr, Dependencies{})
	if code != ExitInvalidInput {
		t.Fatalf("bench existing-output exit = %d, want %d; stdout=%s stderr=%s", code, ExitInvalidInput, stdout.String(), stderr.String())
	}
}
