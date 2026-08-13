package runlog

import (
	"os"
	"path/filepath"
	"testing"
)

func TestJournalIsContiguousAndComplete(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.ndjson")
	journal, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	operationID, err := journal.Before("PROVISIONING", "start_environment", map[string]any{"runId": "run-1"})
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.After("PROVISIONING", operationID, "start_environment", "ok", "", nil); err != nil {
		t.Fatal(err)
	}
	if err := journal.State("COMPLETE", ""); err != nil {
		t.Fatal(err)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	events, truncated, err := Read(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 3 || truncated || !IsComplete(events, truncated) {
		t.Fatalf("events=%d truncated=%t complete=%t", len(events), truncated, IsComplete(events, truncated))
	}
}

func TestJournalRecoversOnlyTruncatedFinalLine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.ndjson")
	document := []byte("{\"schemaVersion\":\"chronicle.dev/run-event/v1alpha1\",\"sequence\":1,\"timestamp\":\"2026-01-01T00:00:00Z\",\"state\":\"VALIDATING\",\"phase\":\"state\",\"operationId\":\"state-000001\",\"operation\":\"transition\"}\n{\"sequence\":")
	if err := os.WriteFile(path, document, 0o600); err != nil {
		t.Fatal(err)
	}
	events, truncated, err := Read(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || !truncated || IsComplete(events, truncated) {
		t.Fatalf("events=%d truncated=%t", len(events), truncated)
	}
}

func TestJournalRejectsInteriorCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.ndjson")
	if err := os.WriteFile(path, []byte("not-json\n{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Read(path); err == nil {
		t.Fatal("expected corrupt journal rejection")
	}
}

func TestJournalFailsClosedOnResolvedSecret(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.ndjson")
	journal, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	journal.SetSecretValues([]string{"canary-secret-value"})
	if err := journal.State("INFRASTRUCTURE_ERROR", "canary-secret-value"); err == nil {
		t.Fatal("expected secret-bearing journal event rejection")
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	document, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(document) != 0 {
		t.Fatalf("journal persisted secret event: %s", document)
	}
}
