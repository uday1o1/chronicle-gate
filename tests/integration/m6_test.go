//go:build integration

package integration_test

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/uday1o1/chronicle-gate/internal/bundle"
	"github.com/uday1o1/chronicle-gate/internal/engine"
)

func TestM6ControlledCorpusAndOfflineReplay(t *testing.T) {
	repository, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	root, err := os.MkdirTemp(filepath.Join(repository, "run"), "integration-m6-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	baseline := filepath.Join(repository, "examples/order-lifecycle/targets/generated/state-baseline.yaml")
	r3Candidate := filepath.Join(repository, "examples/order-lifecycle/targets/generated/state-r3-candidate.yaml")
	r5Candidate := filepath.Join(repository, "examples/order-lifecycle/targets/generated/state-r5-candidate.yaml")
	r6Candidate := filepath.Join(repository, "examples/order-lifecycle/targets/generated/state-r6-candidate.yaml")

	r3 := runM6Case(t, repository, filepath.Join(root, "r3"), "r3-stale-aggregate-overwrite.yaml", baseline, r3Candidate, 2, loadM6Signature(t, repository, "r3-signature.json"))
	r3Control := runM6Case(t, repository, filepath.Join(root, "r3-control"), "r3-monotonic-version-control.yaml", baseline, r3Candidate, 0, nil)
	r5Payment := runM6Case(t, repository, filepath.Join(root, "r5-payment"), "r5-payment-first-control.yaml", baseline, r5Candidate, 0, nil)
	r5Inventory := runM6Case(t, repository, filepath.Join(root, "r5-inventory"), "r5-inventory-first-regression.yaml", baseline, r5Candidate, 2, loadM6Signature(t, repository, "r5-signature.json"))
	r6 := runM6Case(t, repository, filepath.Join(root, "r6"), "r6-late-cancellation.yaml", baseline, r6Candidate, 2, loadM6Signature(t, repository, "r6-signature.json"))
	r6Control := runM6Case(t, repository, filepath.Join(root, "r6-control"), "r6-on-time-cancellation-control.yaml", baseline, r6Candidate, 0, nil)

	for _, report := range []engine.Report{r3, r3Control, r5Payment, r5Inventory, r6, r6Control} {
		assertControlledAttempts(t, report)
		assertNoRunResources(t, report.RunID)
	}
	assertReleaseOrder(t, r5Payment, []string{"payment-confirmed-payment-first", "inventory-reserved-payment-first"})
	assertReleaseOrder(t, r5Inventory, []string{"inventory-reserved-inventory-first", "payment-confirmed-inventory-first"})
	assertR6TemporalEvidence(t, r6)
	assertTemporalReportFormats(t, repository, filepath.Join(root, "r6"))

	replayM6Bundle(t, repository, filepath.Join(root, "r3"), filepath.Join(root, "r3-replay"), baseline, r3Candidate, *r3.FailureSignature)
	replayM6Bundle(t, repository, filepath.Join(root, "r5-inventory"), filepath.Join(root, "r5-replay"), baseline, r5Candidate, *r5Inventory.FailureSignature)
	replayM6Bundle(t, repository, filepath.Join(root, "r6"), filepath.Join(root, "r6-replay"), baseline, r6Candidate, *r6.FailureSignature)
}

func runM6Case(t *testing.T, repository, output, scenarioName, baseline, candidate string, expectedExit int, expected *engine.FailureSignature) engine.Report {
	t.Helper()
	report, stdout, stderr, exitCode := runCLIWithScenario(t, repository, output,
		filepath.Join(repository, "examples/order-lifecycle/scenarios", scenarioName), baseline, candidate, true,
	)
	if exitCode != expectedExit || report.State != "COMPLETE" {
		t.Fatalf("%s exit=%d report=%#v\nstdout=%s\nstderr=%s", scenarioName, exitCode, report, stdout, stderr)
	}
	if expected == nil {
		if report.Classification != "PASS" || report.FailureSignature != nil {
			t.Fatalf("%s did not pass: %#v", scenarioName, report)
		}
	} else if report.Classification != "SEMANTIC_REGRESSION" || report.FailureSignature == nil || !reflect.DeepEqual(*report.FailureSignature, *expected) || report.Confirmations != 2 {
		t.Fatalf("%s signature or confirmations changed: %#v", scenarioName, report)
	}
	assertPrivateArtifacts(t, output)
	assertAuthoritativeJournal(t, output, "COMPLETE")
	assertResultContract(t, output)
	return report
}

func loadM6Signature(t *testing.T, repository, name string) *engine.FailureSignature {
	t.Helper()
	document, err := os.ReadFile(filepath.Join(repository, "examples/order-lifecycle/expected", name))
	if err != nil {
		t.Fatal(err)
	}
	var signature engine.FailureSignature
	if err := json.Unmarshal(document, &signature); err != nil {
		t.Fatal(err)
	}
	return &signature
}

func assertControlledAttempts(t *testing.T, report engine.Report) {
	t.Helper()
	if report.Baseline == nil || len(report.Candidate) != 3 {
		t.Fatalf("controlled attempt inventory is incomplete: %#v", report)
	}
	for _, attempt := range append([]engine.AttemptEvidence{*report.Baseline}, report.Candidate...) {
		if attempt.Status != "COMPLETE" || len(attempt.Publications) != 2 || len(attempt.Deliveries) != 2 || len(attempt.ProbeDeliveries) != 2 || len(attempt.CheckpointReleases) != 2 || len(attempt.AggregateTransitions) != 2 || attempt.Quiescence == nil {
			t.Fatalf("controlled attempt %s is incomplete: %#v", attempt.AttemptID, attempt)
		}
		if len(attempt.ProbeCapabilities) != 1 || attempt.ProbeCapabilities[0].MaxControlledInFlight != 2 {
			t.Fatalf("controlled attempt %s omits probe capacity", attempt.AttemptID)
		}
		for _, stream := range attempt.ControlledTopology {
			if stream.ProbeCapacity != 2 || stream.ConsumerCapacity != 1 || stream.Partition != 0 {
				t.Fatalf("controlled topology is unsafe: %#v", stream)
			}
		}
		for index, transition := range attempt.AggregateTransitions {
			if transition.Sequence != int64(index+1) || transition.EventID != attempt.CheckpointReleases[index].Checkpoint.EventID || transition.SourceOffset != attempt.CheckpointReleases[index].Offset {
				t.Fatalf("controlled transition order differs from release order: %#v", attempt)
			}
		}
		for name, passed := range attempt.Quiescence.Conditions {
			if !passed {
				t.Fatalf("controlled attempt %s quiescence %s failed", attempt.AttemptID, name)
			}
		}
		if attempt.SchemaAfterHealth == "" || attempt.SchemaAfterHealth != attempt.SchemaAfterObservation {
			t.Fatalf("controlled attempt %s schema fingerprint changed", attempt.AttemptID)
		}
	}
}

func assertReleaseOrder(t *testing.T, report engine.Report, expected []string) {
	t.Helper()
	for _, attempt := range append([]engine.AttemptEvidence{*report.Baseline}, report.Candidate...) {
		for index, eventID := range expected {
			if attempt.CheckpointReleases[index].Checkpoint.EventID != eventID || attempt.CheckpointReleases[index].Order != index+1 {
				t.Fatalf("attempt %s release order = %#v, want %#v", attempt.AttemptID, attempt.CheckpointReleases, expected)
			}
		}
		if attempt.AggregateTransitions[0].SourceTopic == attempt.AggregateTransitions[1].SourceTopic {
			t.Fatalf("attempt %s claimed cross-stream control on one topic", attempt.AttemptID)
		}
	}
}

func assertR6TemporalEvidence(t *testing.T, report engine.Report) {
	t.Helper()
	for _, attempt := range append([]engine.AttemptEvidence{*report.Baseline}, report.Candidate...) {
		if len(attempt.LogicalClockTransitions) != 1 {
			t.Fatalf("attempt %s clock evidence = %#v", attempt.AttemptID, attempt.LogicalClockTransitions)
		}
		clock := attempt.LogicalClockTransitions[0]
		if clock.Intended != clock.Acknowledged || clock.Acknowledged != "2026-08-13T13:00:00Z" {
			t.Fatalf("attempt %s clock acknowledgement = %#v", attempt.AttemptID, clock)
		}
		prior, late := attempt.AggregateTransitions[0], attempt.AggregateTransitions[1]
		eventTime, eventErr := time.Parse(time.RFC3339Nano, late.EventTime)
		deliveryTime, deliveryErr := time.Parse(time.RFC3339Nano, late.DeliveryLogicalTime)
		watermark, watermarkErr := time.Parse(time.RFC3339Nano, clock.Acknowledged)
		if eventErr != nil || deliveryErr != nil || watermarkErr != nil || !eventTime.Before(watermark) || deliveryTime.Before(watermark) || late.AggregateVersion <= prior.AggregateVersion || late.SourceOffset <= prior.SourceOffset {
			t.Fatalf("attempt %s does not prove event-time lateness independently: %#v", attempt.AttemptID, attempt)
		}
	}
}

func assertTemporalReportFormats(t *testing.T, repository, output string) {
	t.Helper()
	for _, format := range []string{"json", "text", "junit", "html"} {
		command := exec.Command(filepath.Join(repository, "bin/chronicle"), "report", "--result", output, "--format", format)
		document, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("render %s temporal report: %v: %s", format, err, document)
		}
		for _, expected := range [][]byte{[]byte("2026-08-13T11:00:00Z"), []byte("2026-08-13T13:00:00Z"), []byte("order-cancelled-v2-late")} {
			if !bytes.Contains(document, expected) {
				t.Fatalf("%s report omits temporal evidence %q: %s", format, expected, document)
			}
		}
	}
}

func replayM6Bundle(t *testing.T, repository, sourceOutput, replayOutput, baseline, candidate string, expected engine.FailureSignature) {
	t.Helper()
	bundlePath := filepath.Join(sourceOutput, "reproduction.zip")
	archive, err := bundle.Open(bundlePath)
	if err != nil {
		t.Fatal(err)
	}
	bundleHash := archive.SHA256
	if err := archive.Close(); err != nil {
		t.Fatal(err)
	}
	removeTargetImages(t, baseline, candidate)
	replayed, stdout, stderr, exitCode := replayCLI(t, repository, bundlePath, replayOutput)
	if exitCode != 2 || replayed.Classification != "SEMANTIC_REGRESSION" || replayed.FailureSignature == nil || !reflect.DeepEqual(*replayed.FailureSignature, expected) {
		t.Fatalf("M6 replay exit=%d report=%#v\nstdout=%s\nstderr=%s", exitCode, replayed, stdout, stderr)
	}
	if replayed.Replay == nil || replayed.Replay.SourceBundleSHA256 != bundleHash || replayed.Replay.ExpectedSignature != expected.Digest {
		t.Fatalf("M6 replay provenance is incomplete: %#v", replayed.Replay)
	}
	assertControlledAttempts(t, replayed)
	assertNoRunResources(t, replayed.RunID)
	assertPrivateArtifacts(t, replayOutput)
	assertAuthoritativeJournal(t, replayOutput, "COMPLETE")
	assertResultContract(t, replayOutput)
}
