//go:build integration

package integration_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/uday1o1/chronicle-gate/internal/artifact"
	"github.com/uday1o1/chronicle-gate/internal/bundle"
	"github.com/uday1o1/chronicle-gate/internal/engine"
	"github.com/uday1o1/chronicle-gate/internal/runlog"
	"github.com/uday1o1/chronicle-gate/internal/spec"
)

func TestR1PublicCLI(t *testing.T) {
	repository, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	output, err := os.MkdirTemp(filepath.Join(repository, "run"), "integration-r1-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(output) })
	report, stdout, stderr, exitCode := runCLI(t, repository, output,
		filepath.Join(repository, "examples/order-lifecycle/targets/generated/baseline.yaml"),
		filepath.Join(repository, "examples/order-lifecycle/targets/generated/candidate.yaml"),
	)
	if exitCode != 2 {
		t.Fatalf("chronicle exit = %d\nstdout=%s\nstderr=%s", exitCode, stdout, stderr)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("machine-readable run wrote stderr: %s", stderr)
	}
	if report.Classification != "SEMANTIC_REGRESSION" || report.FailureSignature == nil {
		t.Fatalf("unexpected report classification: %#v", report)
	}
	expectedSignatureDocument, err := os.ReadFile(filepath.Join(repository, "examples/order-lifecycle/expected/r1-signature.json"))
	if err != nil {
		t.Fatal(err)
	}
	var expectedSignature engine.FailureSignature
	if err := json.Unmarshal(expectedSignatureDocument, &expectedSignature); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(*report.FailureSignature, expectedSignature) {
		t.Fatalf("failure signature changed\nwant: %#v\ngot:  %#v", expectedSignature, *report.FailureSignature)
	}
	if report.Baseline == nil || len(report.Baseline.InvariantRows) != 0 || len(report.Baseline.Deliveries) != 2 {
		t.Fatalf("baseline evidence is incomplete: %#v", report.Baseline)
	}
	if len(report.Candidate) != 3 {
		t.Fatalf("candidate attempts = %d, want 3", len(report.Candidate))
	}
	for _, attempt := range append([]engine.AttemptEvidence{*report.Baseline}, report.Candidate...) {
		if len(attempt.Deliveries) != 2 || attempt.Deliveries[0].Topic != attempt.Deliveries[1].Topic || attempt.Deliveries[0].Partition != attempt.Deliveries[1].Partition || attempt.Deliveries[0].Offset != attempt.Deliveries[1].Offset {
			t.Fatalf("attempt %s did not prove exact physical redelivery: %#v", attempt.AttemptID, attempt.Deliveries)
		}
		if attempt.Rewind.OldCommitted != 1 || attempt.Rewind.Verified != 0 || attempt.Rewind.FinalCommitted != 1 {
			t.Fatalf("attempt %s rewind evidence = %#v", attempt.AttemptID, attempt.Rewind)
		}
		if attempt.SchemaAfterHealth == "" || attempt.SchemaAfterHealth != attempt.SchemaAfterObservation {
			t.Fatalf("attempt %s schema fingerprints differ", attempt.AttemptID)
		}
	}
	for _, attempt := range report.Candidate {
		if attempt.Signature == nil || attempt.Signature.Digest != report.FailureSignature.Digest {
			t.Fatalf("candidate signature is unconfirmed: %#v", attempt.Signature)
		}
	}
	assertNoRunResources(t, report.RunID)
	assertPrivateArtifacts(t, output)
	assertAuthoritativeJournal(t, output, "COMPLETE")
	assertResultContract(t, output)
	assertPublicReportFormats(t, repository, output)
}

func TestNoisyR1MinimizesAndBundleReplays(t *testing.T) {
	repository, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	root, err := os.MkdirTemp(filepath.Join(repository, "run"), "integration-minimize-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	output := filepath.Join(root, "original")
	report, stdout, stderr, exitCode := runCLIWithScenario(t, repository, output,
		filepath.Join(repository, "examples/order-lifecycle/scenarios/r1-offset-rewind-noisy.yaml"),
		filepath.Join(repository, "examples/order-lifecycle/targets/generated/baseline.yaml"),
		filepath.Join(repository, "examples/order-lifecycle/targets/generated/candidate.yaml"), false,
	)
	if exitCode != 2 || report.Classification != "SEMANTIC_REGRESSION" || report.FailureSignature == nil {
		t.Fatalf("noisy run exit=%d report=%#v\nstdout=%s\nstderr=%s", exitCode, report, stdout, stderr)
	}
	if report.Confirmations != 2 {
		t.Fatalf("original confirmations = %d, want 2", report.Confirmations)
	}
	if report.Minimization.Status != "complete" || report.Minimization.Minimality != "proven" || report.Minimization.OriginalEvents != 2 || report.Minimization.FinalEvents != 1 || report.Minimization.OriginalActions != 7 || report.Minimization.FinalActions != 6 || report.Minimization.Trials == 0 {
		t.Fatalf("unexpected minimization: %#v", report.Minimization)
	}
	minimizedDocument, err := os.ReadFile(filepath.Join(output, "minimized/scenario.json"))
	if err != nil {
		t.Fatal(err)
	}
	minimized, err := spec.DecodeScenarioJSON(minimizedDocument)
	if err != nil {
		t.Fatal(err)
	}
	if len(minimized.Spec.Events) != 1 || len(minimized.Spec.Steps) != 6 {
		t.Fatalf("minimized scenario still contains noise: events=%d steps=%d", len(minimized.Spec.Events), len(minimized.Spec.Steps))
	}
	bundlePath := filepath.Join(output, "reproduction.zip")
	archive, err := bundle.Open(bundlePath)
	if err != nil {
		t.Fatal(err)
	}
	bundleHash := archive.SHA256
	if err := archive.Close(); err != nil {
		t.Fatal(err)
	}
	removeImages(t, report.Baseline.AuthoredImage, report.Candidate[0].AuthoredImage)
	replayOutput := filepath.Join(root, "replay")
	replayed, replayStdout, replayStderr, replayExit := replayCLI(t, repository, bundlePath, replayOutput)
	if replayExit != 2 || replayed.Classification != "SEMANTIC_REGRESSION" || replayed.FailureSignature == nil || replayed.FailureSignature.Digest != report.FailureSignature.Digest {
		t.Fatalf("replay exit=%d report=%#v\nstdout=%s\nstderr=%s", replayExit, replayed, replayStdout, replayStderr)
	}
	if replayed.Replay == nil || replayed.Replay.SourceBundleSHA256 != bundleHash || replayed.Replay.ExpectedSignature != report.FailureSignature.Digest {
		t.Fatalf("replay provenance is incomplete: %#v", replayed.Replay)
	}
	assertNoRunResources(t, report.RunID)
	assertNoRunResources(t, replayed.RunID)
	assertPrivateArtifacts(t, output)
	assertPrivateArtifacts(t, replayOutput)
	assertAuthoritativeJournal(t, output, "COMPLETE")
	assertAuthoritativeJournal(t, replayOutput, "COMPLETE")
	assertResultContract(t, output)
	assertResultContract(t, replayOutput)
}

func removeImages(t *testing.T, images ...string) {
	t.Helper()
	arguments := append([]string{"image", "rm", "--force"}, images...)
	output, err := exec.Command("docker", arguments...).CombinedOutput()
	if err != nil {
		t.Fatalf("remove images before replay: %v: %s", err, output)
	}
}

func TestFlakyCandidateIsNeverMinimized(t *testing.T) {
	repository, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	root, err := os.MkdirTemp(filepath.Join(repository, "run"), "integration-flaky-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	output := filepath.Join(root, "artifacts")
	report, stdout, stderr, exitCode := runCLIWithScenario(t, repository, output,
		filepath.Join(repository, "examples/order-lifecycle/scenarios/r1-offset-rewind.yaml"),
		filepath.Join(repository, "examples/order-lifecycle/targets/generated/baseline.yaml"),
		filepath.Join(repository, "examples/order-lifecycle/targets/generated/flaky.yaml"), false,
	)
	if exitCode != 5 || report.Classification != "FLAKY" || report.FailureSignature != nil {
		t.Fatalf("flaky run exit=%d report=%#v\nstdout=%s\nstderr=%s", exitCode, report, stdout, stderr)
	}
	if report.Minimization.Status != "skipped" || report.Minimization.Trials != 0 {
		t.Fatalf("flaky run was minimized: %#v", report.Minimization)
	}
	if _, err := os.Stat(filepath.Join(output, "reproduction.zip")); !os.IsNotExist(err) {
		t.Fatalf("flaky run created a reproduction bundle: %v", err)
	}
	assertNoRunResources(t, report.RunID)
	assertAuthoritativeJournal(t, output, "FLAKY")
	assertResultContract(t, output)
}

func TestFailureCleanupUsesExactRunScope(t *testing.T) {
	repository, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	root, err := os.MkdirTemp(filepath.Join(repository, "run"), "integration-failure-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	targetDocument, err := os.ReadFile(filepath.Join(repository, "examples/order-lifecycle/targets/generated/baseline.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.SplitN(string(targetDocument), "sha256:", 2)
	if len(parts) != 2 || len(parts[1]) < 64 {
		t.Fatal("generated target contains no image ID")
	}
	brokenDocument := parts[0] + "sha256:" + strings.Repeat("0", 64) + parts[1][64:]
	brokenTarget := filepath.Join(root, "missing-image.yaml")
	if err := os.WriteFile(brokenTarget, []byte(brokenDocument), 0o600); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(root, "artifacts")
	report, _, _, exitCode := runCLI(t, repository, output, brokenTarget, brokenTarget)
	if exitCode != 4 || report.Classification != "INFRASTRUCTURE_ERROR" {
		t.Fatalf("failure exit=%d classification=%s error=%s", exitCode, report.Classification, report.Error)
	}
	assertNoRunResources(t, report.RunID)
}

func runCLI(t *testing.T, repository, output, baseline, candidate string) (engine.Report, string, string, int) {
	return runCLIWithScenario(t, repository, output, filepath.Join(repository, "examples/order-lifecycle/scenarios/r1-offset-rewind.yaml"), baseline, candidate, true)
}

func runCLIWithScenario(t *testing.T, repository, output, scenario, baseline, candidate string, noMinimize bool) (engine.Report, string, string, int) {
	t.Helper()
	arguments := []string{
		"run",
		"--scenario", scenario,
		"--baseline", baseline,
		"--candidate", candidate,
		"--out", output,
		"--development-local-images",
		"--json",
	}
	if noMinimize {
		arguments = append(arguments, "--no-minimize")
	}
	command := exec.Command(filepath.Join(repository, "bin/chronicle"), arguments...)
	command.Dir = repository
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	err := command.Run()
	exitCode := 0
	if err != nil {
		var exitError *exec.ExitError
		if !errors.As(err, &exitError) {
			t.Fatalf("execute chronicle: %v", err)
		}
		exitCode = exitError.ExitCode()
	}
	var report engine.Report
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("decode run JSON: %v\n%s", err, stdout.String())
	}
	return report, stdout.String(), stderr.String(), exitCode
}

func replayCLI(t *testing.T, repository, bundlePath, output string) (engine.Report, string, string, int) {
	t.Helper()
	command := exec.Command(filepath.Join(repository, "bin/chronicle"), "replay", "--bundle", bundlePath, "--out", output, "--json")
	command.Dir = repository
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	err := command.Run()
	exitCode := 0
	if err != nil {
		var exitError *exec.ExitError
		if !errors.As(err, &exitError) {
			t.Fatalf("execute replay: %v", err)
		}
		exitCode = exitError.ExitCode()
	}
	var report engine.Report
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("decode replay JSON: %v\n%s", err, stdout.String())
	}
	return report, stdout.String(), stderr.String(), exitCode
}

func assertNoRunResources(t *testing.T, runID string) {
	t.Helper()
	for _, arguments := range [][]string{
		{"ps", "-a", "--filter", "label=dev.chronicle.run=" + runID, "--format", "{{.ID}}"},
		{"network", "ls", "--filter", "label=dev.chronicle.run=" + runID, "--format", "{{.ID}}"},
	} {
		output, err := exec.Command("docker", arguments...).CombinedOutput()
		if err != nil {
			t.Fatalf("docker cleanup query failed: %v: %s", err, output)
		}
		if strings.TrimSpace(string(output)) != "" {
			t.Fatalf("run %s leaked Docker resources: %s", runID, output)
		}
	}
}

func assertPrivateArtifacts(t *testing.T, root string) {
	t.Helper()
	err := filepath.Walk(root, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		want := os.FileMode(0o600)
		if info.IsDir() {
			want = 0o700
		}
		if info.Mode().Perm() != want {
			return fmt.Errorf("%s mode = %o, want %o", path, info.Mode().Perm(), want)
		}
		if !info.IsDir() {
			document, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			for _, secretMarker := range [][]byte{[]byte("postgres://"), []byte("postgresql://"), []byte("CHRONICLE_DATABASE_DSN")} {
				if bytes.Contains(document, secretMarker) {
					return fmt.Errorf("%s contains a database credential marker", path)
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func assertAuthoritativeJournal(t *testing.T, root, state string) {
	t.Helper()
	events, truncated, err := runlog.Read(filepath.Join(root, "events.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	actual, terminal := runlog.FinalState(events, truncated)
	if !terminal || actual != state {
		t.Fatalf("journal terminal=%t state=%s, want %s", terminal, actual, state)
	}
}

func assertResultContract(t *testing.T, root string) {
	t.Helper()
	result, err := spec.LoadResult(filepath.Join(root, "result.json"))
	if err != nil {
		t.Fatal(err)
	}
	if violations := spec.ValidateResult(result); len(violations) != 0 {
		t.Fatalf("result contract violations: %#v", violations)
	}
	if err := artifact.VerifyChecksums(root); err != nil {
		t.Fatal(err)
	}
}

func assertPublicReportFormats(t *testing.T, repository, root string) {
	t.Helper()
	for _, format := range []string{"text", "json", "junit", "html"} {
		output, err := exec.Command(filepath.Join(repository, "bin/chronicle"), "report", "--result", root, "--format", format).CombinedOutput()
		if err != nil {
			t.Fatalf("render %s report: %v: %s", format, err, output)
		}
		if len(output) == 0 {
			t.Fatalf("render %s report returned no output", format)
		}
	}
}
