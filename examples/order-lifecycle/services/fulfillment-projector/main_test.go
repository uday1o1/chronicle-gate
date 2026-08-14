package main

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestMetadataVariantChangesWireOnly(t *testing.T) {
	event := cloudEvent{
		SpecVersion: "1.0", ID: "event-1", Source: "/projector", Type: "dev.chronicle.fulfillment.ready",
		Subject: "order-1", Time: "2026-08-13T12:00:00Z", DataContentType: "application/json",
		AggregateID: "order-1", Data: map[string]any{"orderId": "order-1", "status": "ready", "fulfillmentMode": "standard"},
	}
	baseline, err := encodeFulfillmentOutput(event, "baseline-r4")
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := encodeFulfillmentOutput(event, "candidate-r4-metadata")
	if err != nil {
		t.Fatal(err)
	}
	if string(baseline) == string(candidate) {
		t.Fatal("metadata variant did not change JSON wire order")
	}
	var left, right any
	if err := json.Unmarshal(baseline, &left); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(candidate, &right); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(left, right) {
		t.Fatalf("wire variant changed semantics: %#v != %#v", left, right)
	}
	if traceContext(event.ID, "baseline-r4") == traceContext(event.ID, "candidate-r4-metadata") {
		t.Fatal("metadata variant did not change trace context")
	}
}
