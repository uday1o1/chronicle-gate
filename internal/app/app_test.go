package app

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/uday1o1/chronicle-gate/internal/buildinfo"
	"github.com/uday1o1/chronicle-gate/internal/doctor"
)

func TestVersionJSON(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := Execute(context.Background(), []string{"version", "--json"}, &stdout, &stderr, Dependencies{
		Build: buildinfo.Info{Version: "1.2.3", Commit: "abc123", BuildDate: "2026-08-13T00:00:00Z"},
	})
	if code != ExitSuccess {
		t.Fatalf("Execute() code = %d, want %d; stderr = %s", code, ExitSuccess, stderr.String())
	}
	var payload map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
		t.Fatalf("version output is not JSON: %v\n%s", err, stdout.String())
	}
	if got := payload["schemaVersion"]; got != "chronicle.dev/version/v1alpha1" {
		t.Fatalf("schemaVersion = %v", got)
	}
}

func TestInvalidCommandJSON(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := Execute(context.Background(), []string{"not-a-command", "--json"}, &stdout, &stderr, Dependencies{})
	if code != ExitInvalidInput {
		t.Fatalf("Execute() code = %d, want %d", code, ExitInvalidInput)
	}
	var payload struct {
		SchemaVersion string `json:"schemaVersion"`
		Error         struct {
			Kind string `json:"kind"`
		} `json:"error"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
		t.Fatalf("error output is not JSON: %v\n%s", err, stdout.String())
	}
	if payload.Error.Kind != "INVALID_INPUT" {
		t.Fatalf("error kind = %q", payload.Error.Kind)
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr contaminated JSON response: %s", stderr.String())
	}
}

func TestDoctorFailureJSONAndExitCode(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	checker := doctor.New(doctor.Options{
		Workspace:             t.TempDir(),
		ImageLockPath:         filepath.Join(t.TempDir(), "missing-images.lock.json"),
		DockerBinary:          filepath.Join(t.TempDir(), "missing-docker"),
		MinimumAvailableBytes: 1,
	})
	code := Execute(context.Background(), []string{"doctor", "--json"}, &stdout, &stderr, Dependencies{Doctor: checker})
	if code != ExitInfrastructure {
		t.Fatalf("Execute() code = %d, want %d", code, ExitInfrastructure)
	}
	var report doctor.Report
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("doctor failure output is not JSON: %v\n%s", err, stdout.String())
	}
	if report.Status != doctor.StatusFail {
		t.Fatalf("doctor status = %q, want fail", report.Status)
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr contaminated JSON response: %s", stderr.String())
	}
}
