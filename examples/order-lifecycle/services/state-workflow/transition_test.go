package main

import (
	"testing"
	"time"
)

func TestStaleVersionDefectAndMonotonicControl(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	initial := aggregateState{AggregateID: "order-1", Version: 2, Status: "confirmed", Watermark: now, LastEventTime: now}
	stale := domainEvent{AggregateID: "order-1", AggregateVersion: 1, Type: "dev.chronicle.order.updated", Data: map[string]any{"status": "created"}}
	baseline, err := applyTransition("baseline", initial, stale, now.Add(-time.Minute), now)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := applyTransition("candidate-r3", initial, stale, now.Add(-time.Minute), now)
	if err != nil {
		t.Fatal(err)
	}
	if baseline.Disposition != "ignored_stale" || baseline.State.Version != 2 || candidate.State.Version != 1 || candidate.State.Status != "created" {
		t.Fatalf("stale transition mismatch: baseline=%#v candidate=%#v", baseline, candidate)
	}
	monotonic := stale
	monotonic.AggregateVersion = 3
	monotonic.Data = map[string]any{"status": "shipped"}
	for _, variant := range []string{"baseline", "candidate-r3"} {
		got, err := applyTransition(variant, initial, monotonic, now, now)
		if err != nil || got.State.Version != 3 || got.State.Status != "shipped" {
			t.Fatalf("monotonic control %s = %#v, %v", variant, got, err)
		}
	}
}

func TestCrossStreamOrdersExposeOnlyCandidateInventoryFirst(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	payment := domainEvent{AggregateID: "order-1", AggregateVersion: 1, Type: "dev.chronicle.payment.confirmed"}
	inventory := domainEvent{AggregateID: "order-1", AggregateVersion: 1, Type: "dev.chronicle.inventory.reserved"}
	for _, test := range []struct {
		name       string
		order      []domainEvent
		wantStatus string
	}{
		{name: "payment-first", order: []domainEvent{payment, inventory}, wantStatus: "ready"},
		{name: "inventory-first", order: []domainEvent{inventory, payment}, wantStatus: "payment_received"},
	} {
		t.Run(test.name, func(t *testing.T) {
			state := aggregateState{}
			for _, event := range test.order {
				got, err := applyTransition("candidate-r5", state, event, now, now)
				if err != nil {
					t.Fatal(err)
				}
				state = got.State
			}
			if state.Status != test.wantStatus {
				t.Fatalf("status = %q, want %q", state.Status, test.wantStatus)
			}
		})
	}
}

func TestLateCancellationIsIndependentOfVersionMonotonicity(t *testing.T) {
	t.Parallel()
	watermark := time.Date(2026, 8, 13, 13, 0, 0, 0, time.UTC)
	state := aggregateState{AggregateID: "order-1", Version: 1, Status: "active", Watermark: watermark.Add(-time.Hour), LastEventTime: watermark.Add(-time.Hour)}
	event := domainEvent{AggregateID: "order-1", AggregateVersion: 2, Type: "dev.chronicle.order.cancelled"}
	baseline, err := applyTransition("baseline", state, event, watermark.Add(-2*time.Hour), watermark)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := applyTransition("candidate-r6", state, event, watermark.Add(-2*time.Hour), watermark)
	if err != nil {
		t.Fatal(err)
	}
	if baseline.Disposition != "ignored_late" || baseline.State.Version != 1 || candidate.State.Version != 2 || candidate.State.Status != "cancelled" {
		t.Fatalf("late transition mismatch: baseline=%#v candidate=%#v", baseline, candidate)
	}
	onTime, err := applyTransition("candidate-r6", state, event, watermark.Add(time.Hour), watermark)
	if err != nil || onTime.State.Status != "cancelled" || onTime.State.Version != 2 {
		t.Fatalf("on-time control = %#v, %v", onTime, err)
	}
}
