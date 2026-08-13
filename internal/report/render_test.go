package report

import (
	"bytes"
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
