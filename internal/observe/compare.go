package observe

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"

	"github.com/uday1o1/chronicle-gate/internal/spec"
)

type Difference struct {
	Pointer  string `json:"pointer"`
	RowKey   string `json:"rowKey"`
	Expected any    `json:"expected"`
	Actual   any    `json:"actual"`
	Message  string `json:"message"`
}

func Compare(baseline, candidate Evidence, rules []spec.Normalization, keyPointer string) ([]Difference, error) {
	if baseline.Identity != candidate.Identity {
		return nil, fmt.Errorf("observation identity differs: %s != %s", baseline.Identity.String(), candidate.Identity.String())
	}
	if baseline.Type != candidate.Type || baseline.Mode != candidate.Mode {
		return nil, fmt.Errorf("observation type or mode differs")
	}
	if len(baseline.Applied) != len(candidate.Applied) {
		return []Difference{{Pointer: "/appliedNormalization", Expected: len(baseline.Applied), Actual: len(candidate.Applied), Message: "normalization inventory differs"}}, nil
	}
	for index := range baseline.Applied {
		left, right := baseline.Applied[index], candidate.Applied[index]
		if left.RuleID != right.RuleID || left.AffectedCount == 0 {
			return nil, fmt.Errorf("baseline normalization %q is ineffective or mismatched", left.RuleID)
		}
		if left.AffectedCount != right.AffectedCount {
			return []Difference{{Pointer: left.AuthoredPointer, Expected: left.AffectedCount, Actual: right.AffectedCount, Message: "normalization applicability differs"}}, nil
		}
	}
	var left, right any
	if err := decodeCanonical(baseline.Canonical, &left); err != nil {
		return nil, err
	}
	if err := decodeCanonical(candidate.Canonical, &right); err != nil {
		return nil, err
	}
	switch baseline.Mode {
	case "ordered":
		return compareValue(left, right, "", toleranceMap(rules))
	case "keyed":
		return compareKeyed(left, right, keyPointer, toleranceMap(rules))
	case "set", "multiset":
		return compareSet(left, right, baseline.Mode == "multiset")
	default:
		return nil, fmt.Errorf("unsupported comparison mode %q", baseline.Mode)
	}
}

func compareValue(left, right any, pointer string, tolerances map[string]float64) ([]Difference, error) {
	if tolerance, ok := tolerances[pointer]; ok {
		leftNumber, leftOK := number(left)
		rightNumber, rightOK := number(right)
		if !leftOK || !rightOK {
			return nil, fmt.Errorf("numeric tolerance %s does not select numbers", pointer)
		}
		if math.Abs(leftNumber-rightNumber) <= tolerance {
			return nil, nil
		}
		return []Difference{{Pointer: pointer, Expected: left, Actual: right, Message: "numeric values exceed tolerance"}}, nil
	}
	switch a := left.(type) {
	case map[string]any:
		b, ok := right.(map[string]any)
		if !ok {
			return mismatch(pointer, left, right), nil
		}
		keys := make([]string, 0, len(a)+len(b))
		seen := map[string]struct{}{}
		for key := range a {
			seen[key] = struct{}{}
			keys = append(keys, key)
		}
		for key := range b {
			if _, exists := seen[key]; !exists {
				keys = append(keys, key)
			}
		}
		sort.Strings(keys)
		var differences []Difference
		for _, key := range keys {
			av, aok := a[key]
			bv, bok := b[key]
			child := pointer + "/" + escape(key)
			if !aok || !bok {
				differences = append(differences, Difference{Pointer: child, Expected: av, Actual: bv, Message: "object field presence differs"})
				continue
			}
			part, err := compareValue(av, bv, child, tolerances)
			if err != nil {
				return nil, err
			}
			differences = append(differences, part...)
		}
		return differences, nil
	case []any:
		b, ok := right.([]any)
		if !ok {
			return mismatch(pointer, left, right), nil
		}
		var differences []Difference
		limit := len(a)
		if len(b) < limit {
			limit = len(b)
		}
		for index := 0; index < limit; index++ {
			part, err := compareValue(a[index], b[index], pointer+"/"+strconv.Itoa(index), tolerances)
			if err != nil {
				return nil, err
			}
			differences = append(differences, part...)
		}
		if len(a) != len(b) {
			differences = append(differences, Difference{Pointer: pointer, Expected: len(a), Actual: len(b), Message: "array length differs"})
		}
		return differences, nil
	default:
		if scalarEqual(left, right) {
			return nil, nil
		}
		return mismatch(pointer, left, right), nil
	}
}

func compareSet(left, right any, multiset bool) ([]Difference, error) {
	a, aok := left.([]any)
	b, bok := right.([]any)
	if !aok || !bok {
		return nil, fmt.Errorf("set comparison requires arrays")
	}
	counts := func(values []any) (map[string]int, map[string]any, error) {
		result, originals := map[string]int{}, map[string]any{}
		for _, value := range values {
			document, err := Canonical(value)
			if err != nil {
				return nil, nil, err
			}
			key := string(document)
			result[key]++
			originals[key] = value
		}
		return result, originals, nil
	}
	ac, ao, err := counts(a)
	if err != nil {
		return nil, err
	}
	bc, bo, err := counts(b)
	if err != nil {
		return nil, err
	}
	keys := map[string]struct{}{}
	for key := range ac {
		keys[key] = struct{}{}
	}
	for key := range bc {
		keys[key] = struct{}{}
	}
	ordered := make([]string, 0, len(keys))
	for key := range keys {
		ordered = append(ordered, key)
	}
	sort.Strings(ordered)
	var differences []Difference
	for _, key := range ordered {
		leftCount, rightCount := ac[key], bc[key]
		if !multiset {
			if leftCount > 0 {
				leftCount = 1
			}
			if rightCount > 0 {
				rightCount = 1
			}
		}
		if leftCount != rightCount {
			value := ao[key]
			if value == nil {
				value = bo[key]
			}
			differences = append(differences, Difference{Pointer: "/records", RowKey: key, Expected: map[string]any{"value": value, "count": leftCount}, Actual: map[string]any{"value": value, "count": rightCount}, Message: "set membership differs"})
		}
	}
	return differences, nil
}

func compareKeyed(left, right any, pointer string, tolerances map[string]float64) ([]Difference, error) {
	a, aok := left.([]any)
	b, bok := right.([]any)
	if !aok || !bok {
		return nil, fmt.Errorf("keyed comparison requires arrays")
	}
	index := func(values []any) (map[string]any, error) {
		result := map[string]any{}
		for position, value := range values {
			_, _, key, exists, err := resolvePointer(value, pointer)
			if err != nil || !exists {
				return nil, fmt.Errorf("record %d has no key at %q", position, pointer)
			}
			document, err := Canonical(key)
			if err != nil {
				return nil, err
			}
			text := string(document)
			if _, duplicate := result[text]; duplicate {
				return nil, fmt.Errorf("duplicate keyed value %s", text)
			}
			result[text] = value
		}
		return result, nil
	}
	ai, err := index(a)
	if err != nil {
		return nil, err
	}
	bi, err := index(b)
	if err != nil {
		return nil, err
	}
	keys := map[string]struct{}{}
	for key := range ai {
		keys[key] = struct{}{}
	}
	for key := range bi {
		keys[key] = struct{}{}
	}
	ordered := make([]string, 0, len(keys))
	for key := range keys {
		ordered = append(ordered, key)
	}
	sort.Strings(ordered)
	var differences []Difference
	for _, key := range ordered {
		av, aok := ai[key]
		bv, bok := bi[key]
		if !aok || !bok {
			differences = append(differences, Difference{Pointer: "/records", RowKey: key, Expected: av, Actual: bv, Message: "keyed record presence differs"})
			continue
		}
		part, err := compareValue(av, bv, "", tolerances)
		if err != nil {
			return nil, err
		}
		for index := range part {
			part[index].RowKey = key
		}
		differences = append(differences, part...)
	}
	return differences, nil
}

func toleranceMap(rules []spec.Normalization) map[string]float64 {
	values := map[string]float64{}
	for _, rule := range rules {
		if rule.Type == "numericTolerance" && rule.Tolerance != nil {
			values[rule.Pointer] = *rule.Tolerance
		}
	}
	return values
}

func decodeCanonical(document []byte, target *any) error {
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.UseNumber()
	return decoder.Decode(target)
}

func mismatch(pointer string, expected, actual any) []Difference {
	return []Difference{{Pointer: pointer, Expected: expected, Actual: actual, Message: "normalized values differ"}}
}

func number(value any) (float64, bool) {
	switch typed := value.(type) {
	case json.Number:
		converted, err := typed.Float64()
		return converted, err == nil
	case float64:
		return typed, true
	case float32:
		return float64(typed), true
	case int:
		return float64(typed), true
	case int64:
		return float64(typed), true
	default:
		return 0, false
	}
}

func escape(value string) string {
	value = bytes.NewBufferString(value).String()
	value = string(bytes.ReplaceAll([]byte(value), []byte("~"), []byte("~0")))
	return string(bytes.ReplaceAll([]byte(value), []byte("/"), []byte("~1")))
}
