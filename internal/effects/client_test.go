package effects

import (
	"reflect"
	"testing"
)

func TestProjectExcludesPhysicalBrokerIdentity(t *testing.T) {
	left := Observation{Entries: []Entry{{Kind: "payment_capture", BusinessKey: "order-1", Amount: 1250, IdempotencyKey: "event-1", SourceTopic: "cg.left.payments", SourceOffset: 0, Sequence: 1}}}
	right := Observation{Entries: []Entry{{Kind: "payment_capture", BusinessKey: "order-1", Amount: 1250, IdempotencyKey: "event-1", SourceTopic: "cg.right.payments", SourceOffset: 7, Sequence: 9}}}
	if !reflect.DeepEqual(Project(left), Project(right)) {
		t.Fatalf("physical broker evidence changed the semantic projection: %#v != %#v", Project(left), Project(right))
	}
}
