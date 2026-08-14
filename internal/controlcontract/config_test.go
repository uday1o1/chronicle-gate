package controlcontract

import (
	"reflect"
	"testing"
)

func TestConfigEncodingIsDeterministicAndStrict(t *testing.T) {
	t.Parallel()
	config := Config{APIVersion: APIVersion, ProbeCapacity: 2, ConsumerCapacity: 1, Streams: []Stream{
		{LogicalTopic: "payments", Partition: 0, GroupSuffix: "order-workflow.payments", ClientSuffix: "payments", Bindings: []Binding{{EventID: "payment-1", StepID: "publish-payment"}}},
		{LogicalTopic: "inventory", Partition: 0, GroupSuffix: "order-workflow.inventory", ClientSuffix: "inventory", Bindings: []Binding{{EventID: "inventory-1", StepID: "publish-inventory"}}},
	}}
	document, digest, err := Encode(config)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := Decode(document)
	if err != nil {
		t.Fatal(err)
	}
	second, secondDigest, err := Encode(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(document, second) || digest != secondDigest || digest == "" {
		t.Fatalf("controlled runtime encoding is unstable")
	}
	if _, err := Decode(append(document, []byte("{}")...)); err == nil {
		t.Fatal("trailing JSON was accepted")
	}
}

func TestConfigRejectsDuplicateTopicEventBindings(t *testing.T) {
	t.Parallel()
	config := Config{APIVersion: APIVersion, ProbeCapacity: 2, ConsumerCapacity: 1, Streams: []Stream{{
		LogicalTopic: "payments", Partition: 0, GroupSuffix: "group", ClientSuffix: "client",
		Bindings: []Binding{{EventID: "event-1", StepID: "step-1"}, {EventID: "event-1", StepID: "step-2"}},
	}}}
	if _, _, err := Encode(config); err == nil {
		t.Fatal("duplicate topic/event binding was accepted")
	}
}
