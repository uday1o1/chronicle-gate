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
	"syscall"
	"testing"
	"time"

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

func TestR2PublicCLIAndOfflineBundleReplay(t *testing.T) {
	repository, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	root, err := os.MkdirTemp(filepath.Join(repository, "run"), "integration-r2-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	baselinePath := filepath.Join(repository, "examples/order-lifecycle/targets/generated/r2-baseline.yaml")
	candidatePath := filepath.Join(repository, "examples/order-lifecycle/targets/generated/r2-candidate.yaml")
	output := filepath.Join(root, "original")
	report, stdout, stderr, exitCode := runCLIWithScenario(t, repository, output,
		filepath.Join(repository, "examples/order-lifecycle/scenarios/r2-crash-after-effect.yaml"), baselinePath, candidatePath, false,
	)
	if exitCode != 2 || report.Classification != "EXTERNAL_EFFECT_REGRESSION" || report.FailureSignature == nil {
		t.Fatalf("R2 exit=%d report=%#v\nstdout=%s\nstderr=%s", exitCode, report, stdout, stderr)
	}
	expectedDocument, err := os.ReadFile(filepath.Join(repository, "examples/order-lifecycle/expected/r2-signature.json"))
	if err != nil {
		t.Fatal(err)
	}
	var expected engine.FailureSignature
	if err := json.Unmarshal(expectedDocument, &expected); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(*report.FailureSignature, expected) || report.Confirmations != 2 {
		t.Fatalf("R2 signature or confirmations changed: signature=%#v confirmations=%d", report.FailureSignature, report.Confirmations)
	}
	if report.Baseline == nil || report.Baseline.Effects == nil || len(report.Baseline.Effects.Entries) != 1 {
		t.Fatalf("R2 baseline effect evidence is incomplete: %#v", report.Baseline)
	}
	for _, attempt := range append([]engine.AttemptEvidence{*report.Baseline}, report.Candidate...) {
		assertPreciseAttempt(t, attempt, 2)
		if len(attempt.Observations) != 1 || attempt.Observations[0].Type != "effects" || attempt.Observations[0].Identity.StepID != "observe-effects" || attempt.Observations[0].Identity.ObserverID != "capture-effects" || attempt.Observations[0].Identity.Occurrence != 1 || attempt.Observations[0].SHA256 == "" {
			t.Fatalf("R2 canonical effect observation is incomplete: %#v", attempt.Observations)
		}
	}
	for _, attempt := range report.Candidate {
		if attempt.Effects == nil || len(attempt.Effects.Entries) != 2 || attempt.Signature == nil || attempt.Signature.Digest != expected.Digest {
			t.Fatalf("R2 candidate evidence is incomplete: %#v", attempt)
		}
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
	removeTargetImages(t, baselinePath, candidatePath)
	replayOutput := filepath.Join(root, "replay")
	replayed, replayStdout, replayStderr, replayExit := replayCLI(t, repository, bundlePath, replayOutput)
	if replayExit != 2 || replayed.Classification != "EXTERNAL_EFFECT_REGRESSION" || replayed.FailureSignature == nil || replayed.FailureSignature.Digest != expected.Digest {
		t.Fatalf("R2 replay exit=%d report=%#v\nstdout=%s\nstderr=%s", replayExit, replayed, replayStdout, replayStderr)
	}
	if replayed.Replay == nil || replayed.Replay.SourceBundleSHA256 != bundleHash || replayed.Replay.ExpectedSignature != expected.Digest {
		t.Fatalf("R2 replay provenance is incomplete: %#v", replayed.Replay)
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

func TestR4ObserversRegistryControlAndOfflineReplay(t *testing.T) {
	repository, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	root, err := os.MkdirTemp(filepath.Join(repository, "run"), "integration-r4-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	baselinePath := filepath.Join(repository, "examples/order-lifecycle/targets/generated/r4-baseline.yaml")
	candidatePath := filepath.Join(repository, "examples/order-lifecycle/targets/generated/r4-candidate.yaml")
	output := filepath.Join(root, "regression")
	report, stdout, stderr, exitCode := runCLIWithScenario(t, repository, output,
		filepath.Join(repository, "examples/order-lifecycle/scenarios/r4-schema-default-drift.yaml"), baselinePath, candidatePath, true,
	)
	if exitCode != 2 || report.Classification != "SEMANTIC_REGRESSION" || report.FailureSignature == nil {
		t.Fatalf("R4 exit=%d report=%#v\nstdout=%s\nstderr=%s", exitCode, report, stdout, stderr)
	}
	expectedDocument, err := os.ReadFile(filepath.Join(repository, "examples/order-lifecycle/expected/r4-signature.json"))
	if err != nil {
		t.Fatal(err)
	}
	var expected engine.FailureSignature
	if err := json.Unmarshal(expectedDocument, &expected); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(*report.FailureSignature, expected) || report.Confirmations != 2 {
		t.Fatalf("R4 signature or confirmations changed: signature=%#v confirmations=%d", report.FailureSignature, report.Confirmations)
	}
	for _, attempt := range append([]engine.AttemptEvidence{*report.Baseline}, report.Candidate...) {
		assertR4Attempt(t, attempt)
	}

	controlOutput := filepath.Join(root, "control")
	control, controlStdout, controlStderr, controlExit := runCLIWithScenario(t, repository, controlOutput,
		filepath.Join(repository, "examples/order-lifecycle/scenarios/r4-explicit-default-control.yaml"), baselinePath, candidatePath, true,
	)
	if controlExit != 0 || control.Classification != "PASS" || control.FailureSignature != nil {
		t.Fatalf("R4 control exit=%d report=%#v\nstdout=%s\nstderr=%s", controlExit, control, controlStdout, controlStderr)
	}
	for _, attempt := range append([]engine.AttemptEvidence{*control.Baseline}, control.Candidate...) {
		assertR4Attempt(t, attempt)
	}

	schemaOutput := filepath.Join(root, "schema-regression")
	schemaReport, schemaStdout, schemaStderr, schemaExit := runCLIWithScenario(t, repository, schemaOutput,
		filepath.Join(repository, "examples/order-lifecycle/scenarios/r4-invalid-output-schema.yaml"), baselinePath, candidatePath, true,
	)
	if schemaExit != 2 || schemaReport.Classification != "SCHEMA_REGRESSION" || schemaReport.FailureSignature == nil || schemaReport.Confirmations != 2 {
		t.Fatalf("R4 schema exit=%d report=%#v\nstdout=%s\nstderr=%s", schemaExit, schemaReport, schemaStdout, schemaStderr)
	}
	schemaSignature := schemaReport.FailureSignature
	if schemaSignature.ObservationID != "fulfillment-output" || schemaSignature.Pointer != "/0/event/data" || schemaSignature.RowKey != "schemas/fulfillment-ready.schema.json" || schemaSignature.Expected != true || schemaSignature.Actual != false {
		t.Fatalf("R4 schema signature is incomplete: %#v", schemaSignature)
	}
	if schemaReport.Baseline == nil || len(schemaReport.Baseline.Observations) != 1 || !schemaReport.Baseline.Observations[0].RawSchemaValid {
		t.Fatalf("R4 schema baseline evidence is incomplete: %#v", schemaReport.Baseline)
	}
	for _, attempt := range schemaReport.Candidate {
		if attempt.Status != "COMPLETE" || len(attempt.Observations) != 1 || attempt.Observations[0].RawSchemaValid || attempt.Signature == nil || attempt.Signature.Digest != schemaSignature.Digest {
			t.Fatalf("R4 candidate schema evidence is incomplete: %#v", attempt)
		}
	}
	invalidBaselineOutput := filepath.Join(root, "invalid-baseline")
	invalidBaseline, invalidStdout, invalidStderr, invalidExit := runCLIWithScenario(t, repository, invalidBaselineOutput,
		filepath.Join(repository, "examples/order-lifecycle/scenarios/r4-invalid-output-schema.yaml"), candidatePath, candidatePath, true,
	)
	if invalidExit != 5 || invalidBaseline.Classification != "UNRESOLVED" || invalidBaseline.FailureSignature != nil || len(invalidBaseline.Candidate) != 0 {
		t.Fatalf("invalid baseline exit=%d report=%#v\nstdout=%s\nstderr=%s", invalidExit, invalidBaseline, invalidStdout, invalidStderr)
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
	removeTargetImages(t, baselinePath, candidatePath)
	replayOutput := filepath.Join(root, "replay")
	replayed, replayStdout, replayStderr, replayExit := replayCLI(t, repository, bundlePath, replayOutput)
	if replayExit != 2 || replayed.Classification != "SEMANTIC_REGRESSION" || replayed.FailureSignature == nil || !reflect.DeepEqual(*replayed.FailureSignature, expected) {
		t.Fatalf("R4 replay exit=%d report=%#v\nstdout=%s\nstderr=%s", replayExit, replayed, replayStdout, replayStderr)
	}
	if replayed.Replay == nil || replayed.Replay.SourceBundleSHA256 != bundleHash || replayed.Replay.ExpectedSignature != expected.Digest {
		t.Fatalf("R4 replay provenance is incomplete: %#v", replayed.Replay)
	}
	for _, run := range []engine.Report{report, control, schemaReport, invalidBaseline, replayed} {
		assertNoRunResources(t, run.RunID)
	}
	for _, directory := range []string{output, controlOutput, schemaOutput, invalidBaselineOutput, replayOutput} {
		assertPrivateArtifacts(t, directory)
		assertResultContract(t, directory)
	}
	for _, directory := range []string{output, controlOutput, schemaOutput, replayOutput} {
		assertAuthoritativeJournal(t, directory, "COMPLETE")
	}
	assertAuthoritativeJournal(t, invalidBaselineOutput, "UNRESOLVED")
	assertNormalizationReportFormats(t, repository, output, 12)
}

func assertR4Attempt(t *testing.T, attempt engine.AttemptEvidence) {
	t.Helper()
	if attempt.Status != "COMPLETE" || len(attempt.Observations) != 3 || len(attempt.Registry) != 1 {
		t.Fatalf("R4 attempt evidence is incomplete: %#v", attempt)
	}
	wantIdentity := [][3]any{
		{"observe-sql", "fulfillment-sql", 1},
		{"observe-kafka", "fulfillment-output", 1},
		{"observe-http", "fulfillment-http", 1},
	}
	for index, observation := range attempt.Observations {
		want := wantIdentity[index]
		if observation.Identity.StepID != want[0] || observation.Identity.ObserverID != want[1] || observation.Identity.Occurrence != want[2] || observation.SHA256 == "" || len(observation.Applied) != 1 || observation.Applied[0].AffectedCount != 1 {
			t.Fatalf("R4 observation %d is incomplete: %#v", index, observation)
		}
	}
	registered := attempt.Registry[0]
	if registered.LogicalSubject != "fulfillment-requested-value" || registered.EffectiveMode != "BACKWARD" || len(registered.Versions) != 2 || len(registered.Versions[1].PredecessorVersions) != 1 || len(registered.Versions[1].CompatibilityChecks) != 1 || !registered.Versions[1].CompatibilityChecks[0] {
		t.Fatalf("R4 Registry evidence is incomplete: %#v", registered)
	}
}

func TestManualSynchronousCommitControl(t *testing.T) {
	repository, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	root, err := os.MkdirTemp(filepath.Join(repository, "run"), "integration-manual-commit-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	baseline := filepath.Join(repository, "examples/order-lifecycle/targets/generated/r2-baseline.yaml")
	candidate := filepath.Join(repository, "examples/order-lifecycle/targets/generated/r2-candidate.yaml")
	report, stdout, stderr, exitCode := runCLIWithScenario(t, repository, filepath.Join(root, "artifacts"),
		filepath.Join(repository, "examples/order-lifecycle/scenarios/manual-offset-commit-control.yaml"), baseline, candidate, true,
	)
	if exitCode != 0 || report.Classification != "PASS" {
		t.Fatalf("manual commit exit=%d report=%#v\nstdout=%s\nstderr=%s", exitCode, report, stdout, stderr)
	}
	for _, attempt := range append([]engine.AttemptEvidence{*report.Baseline}, report.Candidate...) {
		assertPreciseAttempt(t, attempt, 1)
		if attempt.CheckpointMode != "manual-commit-control" || attempt.CommittedWhileBlocked == nil || *attempt.CommittedWhileBlocked != 0 || attempt.FinalCommitted == nil || *attempt.FinalCommitted != 1 {
			t.Fatalf("manual commit offset proof is incomplete: %#v", attempt)
		}
	}
	assertNoRunResources(t, report.RunID)
	assertPrivateArtifacts(t, filepath.Join(root, "artifacts"))
	assertAuthoritativeJournal(t, filepath.Join(root, "artifacts"), "COMPLETE")
	assertResultContract(t, filepath.Join(root, "artifacts"))
}

func assertPreciseAttempt(t *testing.T, attempt engine.AttemptEvidence, deliveries int) {
	t.Helper()
	if len(attempt.ProbeDeliveries) != deliveries || len(attempt.ProbeCapabilities) == 0 || attempt.Quiescence == nil || attempt.FinalCommitted == nil || *attempt.FinalCommitted != 1 {
		t.Fatalf("precise attempt evidence is incomplete: %#v", attempt)
	}
	for _, receipt := range attempt.ProbeDeliveries {
		if receipt.Topic != attempt.Published.Topic || receipt.Partition != attempt.Published.Partition || receipt.Offset != attempt.Published.Offset || receipt.Key != attempt.Published.Key || receipt.EventSHA256 != attempt.Published.EventHash {
			t.Fatalf("attempt %s receipt is not the published record: %#v", attempt.AttemptID, receipt)
		}
	}
	for name, passed := range attempt.Quiescence.Conditions {
		if !passed {
			t.Fatalf("attempt %s quiescence condition %s failed", attempt.AttemptID, name)
		}
	}
}

func removeTargetImages(t *testing.T, paths ...string) {
	t.Helper()
	seen := map[string]struct{}{}
	images := []string{}
	for _, path := range paths {
		target, err := spec.LoadTarget(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, service := range target.Spec.Services {
			if _, exists := seen[service.Image]; !exists {
				seen[service.Image] = struct{}{}
				images = append(images, service.Image)
			}
		}
	}
	removeImages(t, images...)
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

func TestAbruptCLIInterruptionCleansExactRunScope(t *testing.T) {
	repository, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for iteration := 0; iteration < 2; iteration++ {
		root, err := os.MkdirTemp(filepath.Join(repository, "run"), "integration-interrupt-")
		if err != nil {
			t.Fatal(err)
		}
		output := filepath.Join(root, "artifacts")
		command := exec.Command(filepath.Join(repository, "bin/chronicle"),
			"run",
			"--scenario", filepath.Join(repository, "examples/order-lifecycle/scenarios/r1-offset-rewind.yaml"),
			"--baseline", filepath.Join(repository, "examples/order-lifecycle/targets/generated/baseline.yaml"),
			"--candidate", filepath.Join(repository, "examples/order-lifecycle/targets/generated/candidate.yaml"),
			"--out", output,
			"--development-local-images",
			"--no-minimize",
			"--json",
		)
		command.Dir = repository
		var stdout bytes.Buffer
		var stderr bytes.Buffer
		command.Stdout = &stdout
		command.Stderr = &stderr
		if err := command.Start(); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- command.Wait() }()
		runID := waitForRunEnvironment(t, output, command.Process, done, &stdout, &stderr)
		if err := command.Process.Signal(os.Interrupt); err != nil {
			t.Fatal(err)
		}
		select {
		case waitErr := <-done:
			var exitError *exec.ExitError
			if !errors.As(waitErr, &exitError) || exitError.ExitCode() != 130 {
				t.Fatalf("interrupted CLI exit error = %v\nstdout=%s\nstderr=%s", waitErr, stdout.String(), stderr.String())
			}
		case <-time.After(60 * time.Second):
			_ = command.Process.Signal(syscall.SIGKILL)
			t.Fatal("interrupted CLI did not exit within 60 seconds")
		}
		var report engine.Report
		if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
			t.Fatalf("decode interrupted report: %v\n%s", err, stdout.String())
		}
		if report.RunID != runID || report.State != "INTERRUPTED" || report.Classification != "UNRESOLVED" {
			t.Fatalf("interrupted report = %#v", report)
		}
		if report.Error == "" {
			t.Fatal("interrupted report has no cause")
		}
		events, truncated, err := runlog.Read(filepath.Join(output, "events.ndjson"))
		if err != nil {
			t.Fatal(err)
		}
		if runlog.IsComplete(events, truncated) {
			t.Fatal("interrupted journal incorrectly claims COMPLETE")
		}
		state, terminal := runlog.FinalState(events, truncated)
		if !terminal || state != "INTERRUPTED" {
			t.Fatalf("interrupted journal terminal=%t state=%s", terminal, state)
		}
		assertNoRunResources(t, runID)
		for _, volume := range report.Environment.OwnedVolumes {
			if output, err := exec.Command("docker", "volume", "inspect", volume).CombinedOutput(); err == nil {
				t.Fatalf("owned volume %s remains after interruption: %s", volume, output)
			}
		}
		if err := os.RemoveAll(root); err != nil {
			t.Fatal(err)
		}
	}
}

func waitForRunEnvironment(t *testing.T, output string, process *os.Process, done <-chan error, stdout, stderr *bytes.Buffer) string {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		document, err := os.ReadFile(filepath.Join(output, "run.json"))
		if err == nil {
			var metadata struct {
				RunID string `json:"runId"`
			}
			if json.Unmarshal(document, &metadata) == nil && metadata.RunID != "" {
				listed, listErr := exec.Command("docker", "ps", "-a", "--filter", "label=dev.chronicle.run="+metadata.RunID, "--format", "{{.ID}}").CombinedOutput()
				if listErr != nil {
					t.Fatalf("list active run containers: %v: %s", listErr, listed)
				}
				if len(strings.Fields(string(listed))) >= 2 {
					return metadata.RunID
				}
			}
		}
		select {
		case err := <-done:
			t.Fatalf("CLI exited before the interruption point: %v\nstdout=%s\nstderr=%s", err, stdout.String(), stderr.String())
		default:
		}
		time.Sleep(100 * time.Millisecond)
	}
	_ = process.Signal(syscall.SIGKILL)
	t.Fatal("CLI did not start the broker and database within 90 seconds")
	return ""
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
	command.Dir = filepath.Dir(output)
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
		{"volume", "ls", "--filter", "label=dev.chronicle.run=" + runID, "--format", "{{.Name}}"},
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

func assertNormalizationReportFormats(t *testing.T, repository, root string, want int) {
	t.Helper()
	for _, format := range []string{"text", "json", "junit", "html"} {
		output, err := exec.Command(filepath.Join(repository, "bin/chronicle"), "report", "--result", root, "--format", format).CombinedOutput()
		if err != nil {
			t.Fatalf("render %s normalization report: %v: %s", format, err, output)
		}
		if !bytes.Contains(output, []byte("sql-updated-at")) || !bytes.Contains(output, []byte("fulfillment-sql")) {
			t.Fatalf("%s report omits applied normalization evidence: %s", format, output)
		}
		if format == "json" {
			var rendered struct {
				Normalizations []json.RawMessage `json:"normalizations"`
			}
			if err := json.Unmarshal(output, &rendered); err != nil {
				t.Fatal(err)
			}
			if len(rendered.Normalizations) != want {
				t.Fatalf("json normalization summaries = %d, want %d", len(rendered.Normalizations), want)
			}
		}
	}
}
