package report

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/uday1o1/chronicle-gate/internal/artifact"
)

func TestRenderEscapesAndMapsJUnitOutcomes(t *testing.T) {
	value := Document{APIVersion: "chronicle.dev/v1alpha1", Kind: "Result", RunID: "run-<script>", State: "COMPLETE", Classification: "SEMANTIC_REGRESSION", Error: "bad <value>"}
	html, err := Render(value, "html")
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(html, []byte("<script>")) || !bytes.Contains(html, []byte("&lt;script&gt;")) {
		t.Fatalf("HTML is not escaped: %s", html)
	}
	junit, err := Render(value, "junit")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(junit, []byte("failures=\"1\"")) || !bytes.Contains(junit, []byte("<failure")) {
		t.Fatalf("regression is not a JUnit failure: %s", junit)
	}
	value.Classification = "INFRASTRUCTURE_ERROR"
	junit, err = Render(value, "junit")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(junit, []byte("errors=\"1\"")) || !bytes.Contains(junit, []byte("<error")) {
		t.Fatalf("infrastructure outcome is not a JUnit error: %s", junit)
	}
}

func TestEveryRendererIsRejectedWhenItContainsResolvedSecret(t *testing.T) {
	value := Document{APIVersion: "chronicle.dev/v1alpha1", Kind: "Result", RunID: "run-1", State: "INFRASTRUCTURE_ERROR", Classification: "INFRASTRUCTURE_ERROR", Error: "canary-secret-value"}
	for _, format := range []string{"json", "text", "junit", "html"} {
		document, err := Render(value, format)
		if err != nil {
			t.Fatal(err)
		}
		if err := artifact.ValidatePublic(document, []string{"canary-secret-value"}); err == nil {
			t.Fatalf("%s renderer exposed resolved secret", format)
		}
	}
}

func TestEveryRendererIncludesAppliedNormalizationEvidence(t *testing.T) {
	attempt := json.RawMessage(`{
  "attemptId":"baseline-0",
  "observations":[{
    "identity":{"stepId":"observe-sql","observerId":"projection","occurrence":1},
    "appliedNormalization":[{"ruleId":"updated-at","type":"timestamp","authoredPointer":"/rows/0/updated_at","affectedCount":1}]
  }]
}`)
	value := Document{
		APIVersion: "chronicle.dev/v1alpha1", Kind: "Result", RunID: "run-1", State: "COMPLETE", Classification: "PASS",
		Baseline: attempt,
	}
	for _, format := range []string{"json", "text", "junit", "html"} {
		document, err := Render(value, format)
		if err != nil {
			t.Fatal(err)
		}
		for _, expected := range [][]byte{[]byte("updated-at"), []byte("observe-sql"), []byte("projection")} {
			if !bytes.Contains(document, expected) {
				t.Fatalf("%s report omits normalization evidence %q: %s", format, expected, document)
			}
		}
	}
}

func TestNormalizationSummaryIsIdempotentAcrossReportRoundTrips(t *testing.T) {
	attempt := json.RawMessage(`{"attemptId":"baseline-0","observations":[{"identity":{"stepId":"observe","observerId":"projection","occurrence":1},"appliedNormalization":[{"ruleId":"updated-at","type":"timestamp","authoredPointer":"/time","affectedCount":1}]}]}`)
	value := Document{APIVersion: "chronicle.dev/v1alpha1", Kind: "Result", RunID: "run-1", State: "COMPLETE", Classification: "PASS", Baseline: attempt}
	first, err := Render(value, "json")
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := Decode(first)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Render(decoded, "json")
	if err != nil {
		t.Fatal(err)
	}
	roundTrip, err := Decode(second)
	if err != nil {
		t.Fatal(err)
	}
	if len(roundTrip.Normalizations) != 1 {
		t.Fatalf("normalization summaries after round trip = %d, want 1", len(roundTrip.Normalizations))
	}
}

func TestEveryRendererSeparatesEventTimeFromPhysicalDelivery(t *testing.T) {
	attempt := json.RawMessage(`{
  "attemptId":"candidate-0",
  "controlMode":"event-time",
  "aggregateTransitions":[{
    "sequence":2,
    "eventId":"cancel-late",
    "eventTime":"2026-08-13T11:00:00Z",
    "deliveryLogicalTime":"2026-08-13T13:00:00Z",
    "aggregateVersion":2,
    "disposition":"applied",
    "resultingStatus":"cancelled",
    "sourceTopic":"cg.run.candidate.order-lifecycle",
    "sourcePartition":0,
    "sourceOffset":1
  }]
}`)
	value := Document{
		APIVersion: "chronicle.dev/v1alpha1", Kind: "Result", RunID: "run-1", State: "COMPLETE",
		Classification: "SEMANTIC_REGRESSION", Candidate: []json.RawMessage{attempt},
	}
	for _, format := range []string{"json", "text", "junit", "html"} {
		document, err := Render(value, format)
		if err != nil {
			t.Fatal(err)
		}
		for _, expected := range [][]byte{
			[]byte("2026-08-13T11:00:00Z"), []byte("2026-08-13T13:00:00Z"),
			[]byte("cancel-late"), []byte("order-lifecycle"),
		} {
			if !bytes.Contains(document, expected) {
				t.Fatalf("%s report omits controlled temporal evidence %q: %s", format, expected, document)
			}
		}
	}
}
