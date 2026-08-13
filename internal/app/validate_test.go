package app

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestValidateCLIUsesNoDocker(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell sentinel is POSIX-specific")
	}

	directory := t.TempDir()
	marker := filepath.Join(directory, "docker-called")
	sentinel := filepath.Join(directory, "docker")
	script := "#!/bin/sh\nprintf called > \"" + marker + "\"\nexit 99\n"
	if err := os.WriteFile(sentinel, []byte(script), 0o700); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	t.Setenv("PATH", directory)

	scenario := filepath.Join("..", "..", "examples", "order-lifecycle", "scenarios", "r1-offset-rewind.yaml")
	target := filepath.Join("..", "..", "examples", "order-lifecycle", "targets", "baseline.yaml")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := Execute(context.Background(), []string{"validate", "--scenario", scenario, "--target", target, "--json"}, &stdout, &stderr, Dependencies{})
	if code != ExitSuccess {
		t.Fatalf("Execute() code = %d, want success; stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	var report validationReport
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("validation output is not JSON: %v\n%s", err, stdout.String())
	}
	if report.Status != "valid" || len(report.Violations) != 0 {
		t.Fatalf("unexpected validation report: %#v", report)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("validate invoked Docker; marker error = %v", err)
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %s", stderr.String())
	}
}

func TestValidateCLIInvalidJSONIsSelfContained(t *testing.T) {
	t.Parallel()

	scenario := filepath.Join("..", "..", "tests", "fixtures", "validation", "invalid", "unknown-field.scenario.yaml")
	target := filepath.Join("..", "..", "examples", "order-lifecycle", "targets", "baseline.yaml")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := Execute(context.Background(), []string{"validate", "--scenario", scenario, "--target", target, "--json"}, &stdout, &stderr, Dependencies{})
	if code != ExitInvalidInput {
		t.Fatalf("Execute() code = %d, want invalid input", code)
	}
	var report validationReport
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("validation output is not one JSON document: %v\n%s", err, stdout.String())
	}
	if report.Status != "invalid" || len(report.Violations) == 0 {
		t.Fatalf("unexpected validation report: %#v", report)
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr contaminated JSON response: %s", stderr.String())
	}
}
