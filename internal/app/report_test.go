package app

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/uday1o1/chronicle-gate/internal/report"
	"github.com/uday1o1/chronicle-gate/internal/runlog"
)

func TestReportUsesJournalAsCompletionAuthority(t *testing.T) {
	root := t.TempDir()
	value := report.Document{
		APIVersion: "chronicle.dev/v1alpha1", Kind: "Result", RunID: "run-1", State: "COMPLETE",
		Classification: "PASS", StartedAt: "2026-01-01T00:00:00Z", CompletedAt: "2026-01-01T00:00:01Z",
		Environment: json.RawMessage(`{}`), Candidate: []json.RawMessage{}, Violations: []report.Violation{},
		Minimization: report.Minimization{Status: "skipped", Minimality: "unavailable", AcceptedTransforms: []string{}, Rejections: []report.Rejection{}},
	}
	document, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "result.json"), document, 0o600); err != nil {
		t.Fatal(err)
	}
	journal, err := runlog.Open(filepath.Join(root, "events.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.State("REPORTING", ""); err != nil {
		t.Fatal(err)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := Execute(context.Background(), []string{"report", "--result", root, "--json"}, &stdout, &stderr, Dependencies{})
	if code != ExitUnresolved {
		t.Fatalf("report exit=%d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	var effective report.Document
	if err := json.Unmarshal(stdout.Bytes(), &effective); err != nil {
		t.Fatal(err)
	}
	if effective.State != "INTERRUPTED" || effective.Classification != "UNRESOLVED" {
		t.Fatalf("effective result = %#v", effective)
	}
}
