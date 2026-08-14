package observe

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/uday1o1/chronicle-gate/internal/spec"
)

const timestampToken = "<chronicle-timestamp>"

// Normalize applies the declared rules to a deep copy and emits auditable evidence.
func Normalize(value any, rules []spec.Normalization) (any, []AppliedNormalization, error) {
	document, err := Canonical(value)
	if err != nil {
		return nil, nil, err
	}
	var normalized any
	decoder := json.NewDecoder(strings.NewReader(string(document)))
	decoder.UseNumber()
	if err := decoder.Decode(&normalized); err != nil {
		return nil, nil, fmt.Errorf("copy observation before normalization: %w", err)
	}
	orderedRules := append([]spec.Normalization(nil), rules...)
	sort.SliceStable(orderedRules, func(left, right int) bool {
		return orderedRules[left].Type == "stableOrder" && orderedRules[right].Type != "stableOrder"
	})
	applied := make([]AppliedNormalization, 0, len(orderedRules))
	for _, rule := range orderedRules {
		before, err := Canonical(normalized)
		if err != nil {
			return nil, nil, err
		}
		affected, err := applyRule(&normalized, rule)
		if err != nil {
			return nil, nil, fmt.Errorf("normalization %q: %w", rule.ID, err)
		}
		after, err := Canonical(normalized)
		if err != nil {
			return nil, nil, err
		}
		beforeHash, afterHash := sha256.Sum256(before), sha256.Sum256(after)
		applied = append(applied, AppliedNormalization{
			RuleID: rule.ID, Type: rule.Type, AuthoredPointer: rule.Pointer,
			AffectedPointers: affected, AffectedCount: len(affected),
			BeforeSHA256: hex.EncodeToString(beforeHash[:]), AfterSHA256: hex.EncodeToString(afterHash[:]),
		})
	}
	return normalized, applied, nil
}

func applyRule(root *any, rule spec.Normalization) ([]string, error) {
	parent, token, current, exists, err := resolvePointer(*root, rule.Pointer)
	if err != nil || !exists {
		return nil, err
	}
	switch rule.Type {
	case "remove":
		if rule.Pointer == "" {
			return nil, fmt.Errorf("cannot remove the document root")
		}
		if err := removeAt(parent, token); err != nil {
			return nil, err
		}
		return []string{rule.Pointer}, nil
	case "replace":
		if scalarEqual(current, rule.Token) {
			return nil, nil
		}
		if err := replaceAt(root, parent, token, rule.Token); err != nil {
			return nil, err
		}
		return []string{rule.Pointer}, nil
	case "timestamp":
		if current == timestampToken {
			return nil, nil
		}
		text, ok := current.(string)
		if !ok {
			return nil, fmt.Errorf("timestamp pointer does not select a string")
		}
		if _, err := time.Parse(time.RFC3339Nano, text); err != nil {
			return nil, fmt.Errorf("timestamp pointer is not RFC3339: %w", err)
		}
		if err := replaceAt(root, parent, token, timestampToken); err != nil {
			return nil, err
		}
		return []string{rule.Pointer}, nil
	case "stableOrder":
		values, ok := current.([]any)
		if !ok {
			return nil, fmt.Errorf("stableOrder pointer does not select an array")
		}
		keyed := make([]struct {
			key   string
			value any
		}, len(values))
		seen := map[string]struct{}{}
		for index, value := range values {
			object, ok := value.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("stableOrder element %d is not an object", index)
			}
			parts := make([]any, len(rule.Keys))
			for keyIndex, key := range rule.Keys {
				field, exists := object[key]
				if !exists {
					return nil, fmt.Errorf("stableOrder element %d has no key %q", index, key)
				}
				parts[keyIndex] = field
			}
			encoded, err := Canonical(parts)
			if err != nil {
				return nil, err
			}
			keyed[index] = struct {
				key   string
				value any
			}{string(encoded), value}
			if _, duplicate := seen[keyed[index].key]; duplicate {
				return nil, fmt.Errorf("stableOrder composite key is duplicated")
			}
			seen[keyed[index].key] = struct{}{}
		}
		sort.Slice(keyed, func(left, right int) bool { return keyed[left].key < keyed[right].key })
		ordered := make([]any, len(keyed))
		changed := false
		for index := range keyed {
			ordered[index] = keyed[index].value
			if !scalarEqual(ordered[index], values[index]) {
				changed = true
			}
		}
		if !changed {
			return nil, nil
		}
		if err := replaceAt(root, parent, token, ordered); err != nil {
			return nil, err
		}
		return []string{rule.Pointer}, nil
	case "numericTolerance":
		return nil, nil
	default:
		return nil, fmt.Errorf("unsupported normalization type %q", rule.Type)
	}
}

func resolvePointer(root any, pointer string) (parent any, token string, value any, exists bool, err error) {
	if pointer == "" {
		return nil, "", root, true, nil
	}
	parts := strings.Split(strings.TrimPrefix(pointer, "/"), "/")
	current := root
	for index, encoded := range parts {
		part := strings.ReplaceAll(strings.ReplaceAll(encoded, "~1", "/"), "~0", "~")
		last := index == len(parts)-1
		switch typed := current.(type) {
		case map[string]any:
			next, found := typed[part]
			if !found {
				return nil, "", nil, false, nil
			}
			if last {
				return typed, part, next, true, nil
			}
			current = next
		case []any:
			position, conversionErr := strconv.Atoi(part)
			if conversionErr != nil || position < 0 {
				return nil, "", nil, false, fmt.Errorf("invalid array pointer token %q", part)
			}
			if position >= len(typed) {
				return nil, "", nil, false, nil
			}
			if last {
				return typed, part, typed[position], true, nil
			}
			current = typed[position]
		default:
			return nil, "", nil, false, nil
		}
	}
	return nil, "", nil, false, nil
}

func replaceAt(root *any, parent any, token string, value any) error {
	if parent == nil {
		*root = value
		return nil
	}
	switch typed := parent.(type) {
	case map[string]any:
		typed[token] = value
	case []any:
		var position int
		if _, err := fmt.Sscanf(token, "%d", &position); err != nil || position < 0 || position >= len(typed) {
			return fmt.Errorf("invalid array pointer token %q", token)
		}
		typed[position] = value
	default:
		return fmt.Errorf("pointer parent cannot be replaced")
	}
	return nil
}

func removeAt(parent any, token string) error {
	switch typed := parent.(type) {
	case map[string]any:
		delete(typed, token)
	case []any:
		return fmt.Errorf("array element removal requires an object parent in v1")
	default:
		return fmt.Errorf("pointer parent cannot be removed")
	}
	return nil
}

func scalarEqual(left, right any) bool {
	a, errA := Canonical(left)
	b, errB := Canonical(right)
	return errA == nil && errB == nil && string(a) == string(b)
}
