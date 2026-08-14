package main

import (
	"encoding/json"
	"testing"
	"time"
)

func TestOrderCreatedIdentityAndPayload(t *testing.T) {
	now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	event := cloudEvent{
		SpecVersion: "1.0", ID: "order-created-request-1", Source: "/order-api",
		Type: "dev.chronicle.order.created", Subject: "order-1", Time: now.Format(time.RFC3339Nano),
		DataContentType: "application/json", AggregateID: "order-1", AggregateVersion: 1,
		Data: map[string]any{"orderId": "order-1", "amount": int64(1250)},
	}
	document, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	var decoded cloudEvent
	if err := json.Unmarshal(document, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.ID != "order-created-request-1" || decoded.AggregateID != "order-1" || decoded.AggregateVersion != 1 {
		t.Fatalf("unexpected event: %#v", decoded)
	}
}
