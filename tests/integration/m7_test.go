//go:build integration

package integration_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/uday1o1/chronicle-gate/internal/bundle"
	"github.com/uday1o1/chronicle-gate/internal/engine"
	"github.com/uday1o1/chronicle-gate/internal/observe"
)

func TestR7ConnectedOutboxCrashAndOfflineReplay(t *testing.T) {
	repository, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	root, err := os.MkdirTemp(filepath.Join(repository, "run"), "integration-r7-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	baselinePath := filepath.Join(repository, "examples/order-lifecycle/targets/generated/r7-baseline.yaml")
	candidatePath := filepath.Join(repository, "examples/order-lifecycle/targets/generated/r7-candidate.yaml")
	output := filepath.Join(root, "regression")
	report, stdout, stderr, exitCode := runCLIWithScenario(t, repository, output,
		filepath.Join(repository, "examples/order-lifecycle/scenarios/r7-outbox-crash-after-ack.yaml"), baselinePath, candidatePath, true,
	)
	if exitCode != 2 || report.Classification != "EXTERNAL_EFFECT_REGRESSION" || report.FailureSignature == nil || report.Confirmations != 2 {
		t.Fatalf("R7 exit=%d report=%#v\nstdout=%s\nstderr=%s", exitCode, report, stdout, stderr)
	}
	expectedDocument, err := os.ReadFile(filepath.Join(repository, "examples/order-lifecycle/expected/r7-signature.json"))
	if err != nil {
		t.Fatal(err)
	}
	var expected engine.FailureSignature
	if err := json.Unmarshal(expectedDocument, &expected); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(*report.FailureSignature, expected) {
		t.Fatalf("R7 signature changed\nwant: %#v\ngot:  %#v", expected, *report.FailureSignature)
	}
	if report.Baseline == nil {
		t.Fatal("R7 report omitted baseline evidence")
	}
	assertR7CrashAttempt(t, *report.Baseline, false)
	if len(report.Candidate) != 3 {
		t.Fatalf("R7 candidate attempts = %d, want 3", len(report.Candidate))
	}
	for _, attempt := range report.Candidate {
		assertR7CrashAttempt(t, attempt, true)
		if attempt.Signature == nil || attempt.Signature.Digest != expected.Digest {
			t.Fatalf("R7 candidate signature is unconfirmed: %#v", attempt.Signature)
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
	if replayExit != 2 || replayed.Classification != "EXTERNAL_EFFECT_REGRESSION" || replayed.FailureSignature == nil || !reflect.DeepEqual(*replayed.FailureSignature, expected) {
		t.Fatalf("R7 replay exit=%d report=%#v\nstdout=%s\nstderr=%s", replayExit, replayed, replayStdout, replayStderr)
	}
	if replayed.Replay == nil || replayed.Replay.SourceBundleSHA256 != bundleHash || replayed.Replay.ExpectedSignature != expected.Digest {
		t.Fatalf("R7 replay provenance is incomplete: %#v", replayed.Replay)
	}
	assertNoRunResources(t, report.RunID)
	assertNoRunResources(t, replayed.RunID)
	for _, directory := range []string{output, replayOutput} {
		assertPrivateArtifacts(t, directory)
		assertAuthoritativeJournal(t, directory, "COMPLETE")
		assertResultContract(t, directory)
	}
}

func TestM7NearbyControlMatrix(t *testing.T) {
	repository, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	root, err := os.MkdirTemp(filepath.Join(repository, "run"), "integration-m7-controls-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })

	r7Baseline := filepath.Join(repository, "examples/order-lifecycle/targets/generated/r7-baseline.yaml")
	r7Candidate := filepath.Join(repository, "examples/order-lifecycle/targets/generated/r7-candidate.yaml")
	controlOutput := filepath.Join(root, "unrelated-orders")
	control, stdout, stderr, exitCode := runCLIWithScenario(t, repository, controlOutput,
		filepath.Join(repository, "examples/order-lifecycle/scenarios/r7-unrelated-orders-control.yaml"), r7Baseline, r7Candidate, true,
	)
	if exitCode != 0 || control.Classification != "PASS" || control.FailureSignature != nil {
		t.Fatalf("R7 control exit=%d report=%#v\nstdout=%s\nstderr=%s", exitCode, control, stdout, stderr)
	}
	for _, attempt := range append([]engine.AttemptEvidence{*control.Baseline}, control.Candidate...) {
		assertR7ControlAttempt(t, attempt)
	}

	r1Output := filepath.Join(root, "single-delivery")
	r1, stdout, stderr, exitCode := runCLIWithScenario(t, repository, r1Output,
		filepath.Join(repository, "examples/order-lifecycle/scenarios/r1-single-delivery-control.yaml"),
		filepath.Join(repository, "examples/order-lifecycle/targets/generated/baseline.yaml"),
		filepath.Join(repository, "examples/order-lifecycle/targets/generated/candidate.yaml"), true,
	)
	if exitCode != 0 || r1.Classification != "PASS" || r1.FailureSignature != nil {
		t.Fatalf("R1 single-delivery control exit=%d report=%#v\nstdout=%s\nstderr=%s", exitCode, r1, stdout, stderr)
	}
	for _, attempt := range append([]engine.AttemptEvidence{*r1.Baseline}, r1.Candidate...) {
		if attempt.Status != "COMPLETE" || len(attempt.Observations) != 1 || attempt.Observations[0].Count != 1 || len(attempt.InvariantRows) != 0 {
			t.Fatalf("R1 single-delivery evidence is incomplete: %#v", attempt)
		}
	}

	r4Output := filepath.Join(root, "transport-metadata")
	r4, stdout, stderr, exitCode := runCLIWithScenario(t, repository, r4Output,
		filepath.Join(repository, "examples/order-lifecycle/scenarios/r4-explicit-default-control.yaml"),
		filepath.Join(repository, "examples/order-lifecycle/targets/generated/r4-baseline.yaml"),
		filepath.Join(repository, "examples/order-lifecycle/targets/generated/r4-metadata-candidate.yaml"), true,
	)
	if exitCode != 0 || r4.Classification != "PASS" || r4.FailureSignature != nil || r4.Baseline == nil {
		t.Fatalf("R4 transport control exit=%d report=%#v\nstdout=%s\nstderr=%s", exitCode, r4, stdout, stderr)
	}
	baselineRecord := kafkaControlRecord(t, *r4.Baseline)
	for _, attempt := range r4.Candidate {
		candidateRecord := kafkaControlRecord(t, attempt)
		if baselineRecord.Timestamp == candidateRecord.Timestamp || reflect.DeepEqual(baselineRecord.TopLevelJSONKeyOrder, candidateRecord.TopLevelJSONKeyOrder) || reflect.DeepEqual(baselineRecord.TraceContextFingerprints, candidateRecord.TraceContextFingerprints) {
			t.Fatalf("R4 control did not retain distinct transport metadata: baseline=%#v candidate=%#v", baselineRecord, candidateRecord)
		}
		if r4.Baseline.Observations[1].SHA256 != attempt.Observations[1].SHA256 {
			t.Fatalf("transport-only differences changed the semantic observation hash")
		}
	}

	for _, run := range []engine.Report{control, r1, r4} {
		assertNoRunResources(t, run.RunID)
	}
	for _, directory := range []string{controlOutput, r1Output, r4Output} {
		assertPrivateArtifacts(t, directory)
		assertAuthoritativeJournal(t, directory, "COMPLETE")
		assertResultContract(t, directory)
	}
}

func assertR7CrashAttempt(t *testing.T, attempt engine.AttemptEvidence, defective bool) {
	t.Helper()
	if attempt.Status != "COMPLETE" || len(attempt.ServiceImages) != 5 || len(attempt.Outbox) != 1 || len(attempt.OutboxPublishes) != 2 || len(attempt.EffectProjection) == 0 || len(attempt.TopicBounds) != 8 || len(attempt.GroupOffsets) != 7 || attempt.Quiescence == nil || attempt.SchemaAfterHealth == "" || attempt.SchemaAfterHealth != attempt.SchemaAfterObservation {
		t.Fatalf("R7 crash attempt evidence is incomplete: %#v", attempt)
	}
	if attempt.Outbox[0].PublishAttempts != 2 || !attempt.Outbox[0].Published {
		t.Fatalf("R7 outbox row did not prove the crash window: %#v", attempt.Outbox[0])
	}
	first, second := attempt.OutboxPublishes[0], attempt.OutboxPublishes[1]
	if first.Sequence != 1 || second.Sequence != 2 || first.Attempt != 1 || second.Attempt != 2 || first.Offset != 0 || second.Offset != 1 || first.LogicalEventID != second.LogicalEventID || first.EmittedEventID != first.LogicalEventID {
		t.Fatalf("R7 durable publication evidence is incomplete: %#v", attempt.OutboxPublishes)
	}
	if defective {
		if second.EmittedEventID == second.LogicalEventID || len(attempt.EffectProjection) != 2 {
			t.Fatalf("R7 defective attempt did not expose unstable identity and two effects: %#v", attempt)
		}
	} else if second.EmittedEventID != second.LogicalEventID || len(attempt.EffectProjection) != 1 {
		t.Fatalf("R7 baseline did not preserve stable identity and one effect: %#v", attempt)
	}
	assertAllQuiescenceConditions(t, attempt)
}

func assertR7ControlAttempt(t *testing.T, attempt engine.AttemptEvidence) {
	t.Helper()
	if attempt.Status != "COMPLETE" || len(attempt.Outbox) != 2 || len(attempt.OutboxPublishes) != 2 || len(attempt.EffectProjection) != 2 || len(attempt.TopicBounds) != 8 || len(attempt.GroupOffsets) != 7 {
		t.Fatalf("R7 unrelated-order control evidence is incomplete: %#v", attempt)
	}
	for _, state := range attempt.Outbox {
		if !state.Published || state.PublishAttempts != 1 {
			t.Fatalf("R7 control retried an unrelated order: %#v", state)
		}
	}
	for _, publication := range attempt.OutboxPublishes {
		if publication.Attempt != 1 || publication.EmittedEventID != publication.LogicalEventID {
			t.Fatalf("R7 control changed a stable event identity: %#v", publication)
		}
	}
	assertAllQuiescenceConditions(t, attempt)
}

func assertAllQuiescenceConditions(t *testing.T, attempt engine.AttemptEvidence) {
	t.Helper()
	if attempt.Quiescence == nil {
		t.Fatal("attempt omitted quiescence evidence")
	}
	for condition, passed := range attempt.Quiescence.Conditions {
		if !passed {
			t.Fatalf("attempt %s failed quiescence condition %s", attempt.AttemptID, condition)
		}
	}
}

func kafkaControlRecord(t *testing.T, attempt engine.AttemptEvidence) observe.BrokerRecord {
	t.Helper()
	if attempt.Status != "COMPLETE" || len(attempt.Observations) != 3 || attempt.Observations[1].Source.Kafka == nil || len(attempt.Observations[1].Source.Kafka.Records) != 1 {
		t.Fatalf("R4 transport control evidence is incomplete: %#v", attempt)
	}
	return attempt.Observations[1].Source.Kafka.Records[0]
}
