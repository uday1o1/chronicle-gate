package observe

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/uday1o1/chronicle-gate/internal/spec"
)

func TestByteStringPreservesInvalidUTF8(t *testing.T) {
	left := EncodeBytes([]byte{0xff, 0x00})
	right := EncodeBytes([]byte{0xfe, 0x00})
	if reflect.DeepEqual(left, right) {
		t.Fatal("distinct invalid UTF-8 collapsed")
	}
	decoded, err := left.Decode()
	if err != nil || !reflect.DeepEqual(decoded, []byte{0xff, 0x00}) {
		t.Fatalf("byte round trip: %x %v", decoded, err)
	}
}

func TestNormalizationIsIdempotentAndAudited(t *testing.T) {
	rules := []spec.Normalization{
		{ID: "time", Type: "timestamp", Pointer: "/rows/0/time"},
		{ID: "sort", Type: "stableOrder", Pointer: "/rows", Keys: []string{"id"}},
	}
	value := map[string]any{"rows": []any{
		map[string]any{"id": "b", "time": "2026-08-13T00:00:01Z"},
		map[string]any{"id": "a", "time": "2026-08-13T00:00:02Z"},
	}}
	first, evidence, err := Normalize(value, rules)
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := Normalize(first, rules)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := Canonical(first)
	b, _ := Canonical(second)
	if string(a) != string(b) {
		t.Fatalf("normalization is not idempotent\n%s\n%s", a, b)
	}
	if len(evidence) != 2 || evidence[0].AffectedCount != 1 || evidence[1].AffectedCount != 1 {
		t.Fatalf("unexpected normalization evidence: %#v", evidence)
	}
}

func TestNormalizationRejectsMalformedArrayPointer(t *testing.T) {
	value := map[string]any{"rows": []any{map[string]any{"id": "a"}}}
	_, _, err := Normalize(value, []spec.Normalization{{ID: "bad", Type: "replace", Pointer: "/rows/not-an-index", Token: "x"}})
	if err == nil {
		t.Fatal("malformed array pointer was treated as an absent path")
	}
}

func TestComparisonModesAndTolerance(t *testing.T) {
	identity := Identity{StepID: "observe", ObserverID: "values", Occurrence: 1}
	rule := spec.Normalization{ID: "amount", Type: "numericTolerance", Pointer: "/0/amount", Tolerance: floatPointer(0.1)}
	leftValue := []any{map[string]any{"id": "a", "amount": json.Number("1.00")}}
	rightValue := []any{map[string]any{"id": "a", "amount": json.Number("1.05")}}
	left, _ := NewEvidence(identity, "sql", "ordered", Source{}, leftValue, []AppliedNormalization{{RuleID: "amount", AffectedCount: 1}}, nil)
	right, _ := NewEvidence(identity, "sql", "ordered", Source{}, rightValue, []AppliedNormalization{{RuleID: "amount", AffectedCount: 1}}, nil)
	differences, err := Compare(left, right, []spec.Normalization{rule}, "")
	if err != nil || len(differences) != 0 {
		t.Fatalf("tolerant compare: %#v %v", differences, err)
	}

	setLeft, _ := NewEvidence(identity, "kafka", "multiset", Source{}, []any{"a", "a"}, nil, nil)
	setRight, _ := NewEvidence(identity, "kafka", "multiset", Source{}, []any{"a"}, nil, nil)
	differences, err = Compare(setLeft, setRight, nil, "")
	if err != nil || len(differences) != 1 {
		t.Fatalf("multiset compare: %#v %v", differences, err)
	}
}

func floatPointer(value float64) *float64 { return &value }
