package observe

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/uday1o1/chronicle-gate/internal/spec"
)

func FuzzObservationCanonicalization(f *testing.F) {
	f.Add("left", "right", int64(2))
	f.Fuzz(func(t *testing.T, left, right string, number int64) {
		if len(left)+len(right) > 4096 {
			t.Skip()
		}
		first := map[string]any{"a": left, "b": right, "n": json.Number(stringInt(number))}
		second := map[string]any{"n": json.Number(stringInt(number)), "b": right, "a": left}
		leftCanonical, leftErr := Canonical(first)
		rightCanonical, rightErr := Canonical(second)
		if leftErr != nil || rightErr != nil {
			return
		}
		if string(leftCanonical) != string(rightCanonical) {
			t.Fatal("canonicalization depends on map insertion order")
		}
		encoded := EncodeBytes([]byte(left))
		decoded, err := encoded.Decode()
		if err != nil || !reflect.DeepEqual(decoded, []byte(left)) {
			t.Fatalf("binary representation is not reversible: %v", err)
		}
	})
}

func FuzzNormalizationIdempotence(f *testing.F) {
	f.Add("b", "a")
	f.Fuzz(func(t *testing.T, left, right string) {
		if left == right || len(left)+len(right) > 4096 {
			t.Skip()
		}
		value := map[string]any{"rows": []any{map[string]any{"id": left}, map[string]any{"id": right}}}
		rules := []spec.Normalization{{ID: "sort", Type: "stableOrder", Pointer: "/rows", Keys: []string{"id"}}}
		first, _, err := Normalize(value, rules)
		if err != nil {
			return
		}
		second, _, err := Normalize(first, rules)
		if err != nil {
			t.Fatal(err)
		}
		a, _ := Canonical(first)
		b, _ := Canonical(second)
		if string(a) != string(b) {
			t.Fatal("normalization is not idempotent")
		}
	})
}

func stringInt(value int64) string {
	document, _ := json.Marshal(value)
	return string(document)
}
