package doctor

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

type fakeResponse struct {
	stdout string
	stderr string
	err    error
}

type fakeRunner struct {
	mu        sync.Mutex
	responses map[string]fakeResponse
}

func (runner *fakeRunner) Run(_ context.Context, name string, args ...string) ([]byte, []byte, error) {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	key := name + " " + strings.Join(args, " ")
	response, exists := runner.responses[key]
	if !exists {
		return nil, nil, errors.New("unexpected command: " + key)
	}
	return []byte(response.stdout), []byte(response.stderr), response.err
}

func TestNormalizeArchitecture(t *testing.T) {
	t.Parallel()

	tests := map[string]string{
		"arm64":   "arm64",
		"aarch64": "arm64",
		"amd64":   "amd64",
		"x86_64":  "amd64",
		"s390x":   "s390x",
	}
	for input, want := range tests {
		if got := NormalizeArchitecture(input); got != want {
			t.Errorf("NormalizeArchitecture(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestCheckerStableOrderAndSuccess(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	lockPath := filepath.Join(directory, "images.lock.json")
	digestA := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	digestB := "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	digestC := "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	lock := `{"schemaVersion":"chronicle.dev/images-lock/v1alpha1","resolvedAt":"2026-08-13","images":[{"name":"example","role":"runtime","source":"example.invalid/image:v1","reference":"example.invalid/image@` + digestA + `","indexDigest":"` + digestA + `","platforms":{"linux/amd64":"` + digestB + `","linux/arm64":"` + digestC + `"},"hardening":{"capDrop":["ALL"],"capAdd":{"linux/amd64":[],"linux/arm64":[]}},"reason":"test"}]}`
	if err := os.WriteFile(lockPath, []byte(lock), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	manifest := `{"manifests":[{"digest":"` + digestB + `","platform":{"os":"linux","architecture":"amd64"}},{"digest":"` + digestC + `","platform":{"os":"linux","architecture":"arm64"}}]}`
	runner := &fakeRunner{responses: map[string]fakeResponse{
		"docker version --format {{json .Server}}": {
			stdout: `{"Version":"29.5.2","Os":"linux","Arch":"aarch64"}`,
		},
		"docker manifest inspect example.invalid/image@" + digestA: {stdout: manifest},
	}}
	report := New(Options{
		Workspace:             directory,
		ImageLockPath:         lockPath,
		MinimumAvailableBytes: 1,
		Runner:                runner,
	}).Run(context.Background())

	if report.Status != StatusPass {
		t.Fatalf("report status = %q, want %q: %#v", report.Status, StatusPass, report.Checks)
	}
	wantIDs := []string{
		"host.workspace_disk",
		"host.loopback_port",
		"docker.reachable",
		"docker.server_os",
		"docker.server_architecture",
		"images.lock",
		"image.example",
	}
	if len(report.Checks) != len(wantIDs) {
		t.Fatalf("len(checks) = %d, want %d", len(report.Checks), len(wantIDs))
	}
	for index, want := range wantIDs {
		if got := report.Checks[index].ID; got != want {
			t.Errorf("checks[%d].ID = %q, want %q", index, got, want)
		}
	}
}

func TestCheckerSkipsDockerDependents(t *testing.T) {
	t.Parallel()

	runner := &fakeRunner{responses: map[string]fakeResponse{
		"docker version --format {{json .Server}}": {stderr: "permission denied", err: errors.New("exit status 1")},
	}}
	report := New(Options{
		Workspace:             t.TempDir(),
		ImageLockPath:         filepath.Join(t.TempDir(), "missing.json"),
		MinimumAvailableBytes: 1,
		Runner:                runner,
	}).Run(context.Background())

	if report.Status != StatusFail {
		t.Fatalf("report status = %q, want %q", report.Status, StatusFail)
	}
	if got := report.Checks[3].Status; got != StatusSkip {
		t.Errorf("docker OS status = %q, want skip", got)
	}
	if got := report.Checks[4].Status; got != StatusSkip {
		t.Errorf("docker architecture status = %q, want skip", got)
	}
}
