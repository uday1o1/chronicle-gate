package main

import "testing"

func TestDerivedIDsAreStableAndParentScoped(t *testing.T) {
	first := derivedID("order-created-1", "dev.chronicle.payment.requested", "order-1")
	if first == "" || first != derivedID("order-created-1", "dev.chronicle.payment.requested", "order-1") {
		t.Fatalf("derived ID is unstable: %q", first)
	}
	for _, changed := range []string{
		derivedID("order-created-2", "dev.chronicle.payment.requested", "order-1"),
		derivedID("order-created-1", "dev.chronicle.inventory.requested", "order-1"),
		derivedID("order-created-1", "dev.chronicle.payment.requested", "order-2"),
	} {
		if changed == first {
			t.Fatal("derived identity ignored a required input")
		}
	}
}
