package observe

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/uday1o1/chronicle-gate/internal/spec"
)

func TestStrictJSONRejectsDuplicateAndTrailingValues(t *testing.T) {
	for _, document := range []string{`{"a":1,"a":2}`, `{"a":1} {"b":2}`} {
		if _, err := DecodeStrictJSON([]byte(document)); err == nil {
			t.Fatalf("accepted invalid JSON %s", document)
		}
	}
}

func TestKafkaProjectionPreservesHeaderBytesAndChecksKey(t *testing.T) {
	root := t.TempDir()
	schema := []byte(`{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","additionalProperties":false,"required":["orderId"],"properties":{"orderId":{"type":"string"}}}`)
	if err := os.WriteFile(filepath.Join(root, "payload.json"), schema, 0o600); err != nil {
		t.Fatal(err)
	}
	event := []byte(`{"specversion":"1.0","id":"e1","source":"/orders/1","type":"example","subject":"1","time":"2026-08-13T00:00:00Z","datacontenttype":"application/json","aggregateid":"1","data":{"orderId":"1"}}`)
	record := &kgo.Record{Key: []byte("1"), Value: event, Offset: 4, Timestamp: time.Now(), Headers: []kgo.RecordHeader{
		{Key: "traceparent", Value: []byte("ignored")},
		{Key: string([]byte{0xff}), Value: []byte{0xff, 0x00}},
		{Key: "business", Value: []byte{0xfe, 0x00}},
	}}
	projection, excluded, err := projectRecord(root, spec.KafkaObservation{SchemaFile: "payload.json"}, record)
	if err != nil {
		t.Fatal(err)
	}
	headers, ok := projection["headers"].([]Header)
	if !ok || len(headers) != 2 || len(excluded) != 1 {
		t.Fatalf("unexpected header projection: %#v excluded=%#v", projection, excluded)
	}
	left, _ := headers[0].Value.Decode()
	right, _ := headers[1].Value.Decode()
	if reflect.DeepEqual(left, right) || !reflect.DeepEqual(left, []byte{0xff, 0x00}) || !reflect.DeepEqual(right, []byte{0xfe, 0x00}) {
		t.Fatalf("header bytes collapsed: %x %x", left, right)
	}
	record.Key = []byte("wrong")
	if _, _, err := projectRecord(root, spec.KafkaObservation{SchemaFile: "payload.json"}, record); err == nil {
		t.Fatal("wrong aggregate key was accepted")
	}
	record.Key = []byte("1")
	record.Value = []byte(`{"specversion":"1.0","id":"e1","source":"/orders/1","type":"example","subject":"1","time":"2026-08-13T00:00:00Z","datacontenttype":"application/json","aggregateid":"1","data":{}}`)
	_, _, err = projectRecord(root, spec.KafkaObservation{SchemaFile: "payload.json"}, record)
	var payloadFailure *PayloadValidationError
	if !errors.As(err, &payloadFailure) || payloadFailure.SchemaFile != "payload.json" || payloadFailure.Offset != 4 {
		t.Fatalf("payload validation error is not typed: %T %v", err, err)
	}
}

func TestKafkaPhysicalMetadataPreservesWireEvidence(t *testing.T) {
	document := []byte(`{"type":"example","id":"e1","data":{"orderId":"1"}}`)
	order, err := topLevelJSONKeyOrder(document)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(order, []string{"type", "id", "data"}) {
		t.Fatalf("wire order was not preserved: %v", order)
	}
	fingerprints := traceContextFingerprints([]kgo.RecordHeader{
		{Key: "business", Value: []byte("retained-semantically")},
		{Key: "traceparent", Value: []byte("00-a-b-01")},
		{Key: "traceparent", Value: []byte("00-c-d-01")},
	})
	if len(fingerprints) != 2 || fingerprints[0].WireIndex != 1 || fingerprints[1].WireIndex != 2 || fingerprints[0].SHA256 == fingerprints[1].SHA256 {
		t.Fatalf("trace fingerprints lost identity or order: %#v", fingerprints)
	}
}

func TestSQLOrderingRejectsMissingOrDescendingKeys(t *testing.T) {
	if err := verifySQLOrdering([]map[string]any{{"id": "b"}, {"id": "a"}}, []string{"id"}); err == nil {
		t.Fatal("descending rows were accepted")
	}
	if err := verifySQLOrdering([]map[string]any{{"other": "a"}}, []string{"id"}); err == nil {
		t.Fatal("missing order key was accepted")
	}
}
