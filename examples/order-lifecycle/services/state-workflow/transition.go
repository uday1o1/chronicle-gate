package main

import (
	"errors"
	"fmt"
	"time"
)

type aggregateState struct {
	AggregateID       string
	Version           int64
	Status            string
	PaymentReceived   bool
	InventoryReceived bool
	Watermark         time.Time
	LastEventTime     time.Time
}

type domainEvent struct {
	ID               string         `json:"id"`
	Type             string         `json:"type"`
	Time             string         `json:"time"`
	AggregateID      string         `json:"aggregateid"`
	AggregateVersion int64          `json:"aggregateversion"`
	Data             map[string]any `json:"data"`
}

type transition struct {
	State       aggregateState
	Disposition string
}

func applyTransition(variant string, current aggregateState, event domainEvent, eventTime, deliveryTime time.Time) (transition, error) {
	if event.AggregateID == "" || event.AggregateVersion <= 0 || eventTime.IsZero() || deliveryTime.IsZero() {
		return transition{}, errors.New("controlled event identity, version, and time are required")
	}
	if current.AggregateID == "" {
		current = aggregateState{AggregateID: event.AggregateID, Status: "pending", Watermark: deliveryTime, LastEventTime: eventTime}
	}
	if current.AggregateID != event.AggregateID {
		return transition{}, errors.New("controlled event aggregate does not match loaded state")
	}
	next := current
	if deliveryTime.After(next.Watermark) {
		next.Watermark = deliveryTime
	}
	disposition := "applied"
	switch event.Type {
	case "dev.chronicle.order.updated":
		status, ok := event.Data["status"].(string)
		if !ok || status == "" {
			return transition{}, errors.New("order update requires data.status")
		}
		if event.AggregateVersion <= current.Version && variant != "candidate-r3" {
			disposition = "ignored_stale"
			break
		}
		next.Version = event.AggregateVersion
		next.Status = status
		next.LastEventTime = eventTime
	case "dev.chronicle.payment.confirmed":
		next.PaymentReceived = true
		next.Version = max(current.Version, event.AggregateVersion)
		next.LastEventTime = eventTime
		if variant == "candidate-r5" {
			next.Status = "payment_received"
		} else if next.InventoryReceived {
			next.Status = "ready"
		} else {
			next.Status = "payment_received"
		}
	case "dev.chronicle.inventory.reserved":
		next.InventoryReceived = true
		next.Version = max(current.Version, event.AggregateVersion)
		next.LastEventTime = eventTime
		if next.PaymentReceived {
			next.Status = "ready"
		} else {
			next.Status = "inventory_received"
		}
	case "dev.chronicle.order.cancelled":
		if event.AggregateVersion <= current.Version {
			return transition{}, fmt.Errorf("cancellation version %d does not advance aggregate version %d", event.AggregateVersion, current.Version)
		}
		if eventTime.Before(deliveryTime) && variant != "candidate-r6" {
			disposition = "ignored_late"
			break
		}
		next.Version = event.AggregateVersion
		next.Status = "cancelled"
		next.LastEventTime = eventTime
	default:
		return transition{}, fmt.Errorf("unsupported controlled event type %q", event.Type)
	}
	return transition{State: next, Disposition: disposition}, nil
}
