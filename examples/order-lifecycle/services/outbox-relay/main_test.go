package main

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestCandidateChangesOnlyRetryEventID(t *testing.T) {
	logicalID := "order-created-request-1"
	if got := emittedEventID("candidate-r7", logicalID, "order-1", 1); got != logicalID {
		t.Fatalf("first candidate ID = %q", got)
	}
	retry := emittedEventID("candidate-r7", logicalID, "order-1", 2)
	if retry == logicalID || retry != emittedEventID("candidate-r7", logicalID, "order-1", 3) {
		t.Fatalf("candidate retry ID is not stable: %q", retry)
	}
	if got := emittedEventID("baseline", logicalID, "order-1", 2); got != logicalID {
		t.Fatalf("baseline retry ID = %q", got)
	}
	original := []byte(`{"specversion":"1.0","id":"order-created-request-1","aggregateid":"order-1","data":{"amount":1250}}`)
	rewritten, err := rewriteEventID(original, retry)
	if err != nil {
		t.Fatal(err)
	}
	var before, after map[string]any
	if err := json.Unmarshal(original, &before); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(rewritten, &after); err != nil {
		t.Fatal(err)
	}
	delete(before, "id")
	delete(after, "id")
	beforeJSON, _ := json.Marshal(before)
	afterJSON, _ := json.Marshal(after)
	if !bytes.Equal(beforeJSON, afterJSON) {
		t.Fatalf("retry changed business payload: %s != %s", beforeJSON, afterJSON)
	}
}
