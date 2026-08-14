package spec

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"go.yaml.in/yaml/v3"
)

const MaxContractBytes = 2 << 20

func LoadScenario(path string) (Scenario, error) {
	var value Scenario
	if err := loadYAML(path, "Scenario", &value); err != nil {
		return Scenario{}, err
	}
	return value, nil
}

func LoadTarget(path string) (Target, error) {
	var value Target
	if err := loadYAML(path, "Target", &value); err != nil {
		return Target{}, err
	}
	return value, nil
}

func LoadWorkload(path string) (Workload, error) {
	var value Workload
	if err := loadYAML(path, "Workload", &value); err != nil {
		return Workload{}, err
	}
	return value, nil
}

func LoadBenchmarkWorkload(path string) (BenchmarkWorkload, error) {
	var value BenchmarkWorkload
	if err := loadYAML(path, "BenchmarkWorkload", &value); err != nil {
		return BenchmarkWorkload{}, err
	}
	return value, nil
}

func LoadResult(path string) (Result, error) {
	var value Result
	if err := loadJSON(path, "Result", &value); err != nil {
		return Result{}, err
	}
	return value, nil
}

func LoadBundle(path string) (Bundle, error) {
	var value Bundle
	if err := loadJSON(path, "Bundle", &value); err != nil {
		return Bundle{}, err
	}
	return value, nil
}

func DecodeBundleJSON(document []byte) (Bundle, error) {
	return decodeJSONDocument(document, "Bundle", "bundle manifest", Bundle{})
}

func DecodeScenarioJSON(document []byte) (Scenario, error) {
	return decodeJSONDocument(document, "Scenario", "scenario", Scenario{})
}

func DecodeTargetJSON(document []byte) (Target, error) {
	return decodeJSONDocument(document, "Target", "target", Target{})
}

// ValidateBenchmarkResultJSON validates a standalone benchmark result artifact.
func ValidateBenchmarkResultJSON(document []byte) error {
	var raw any
	decoder := json.NewDecoder(bytes.NewReader(document))
	if err := decoder.Decode(&raw); err != nil {
		return fmt.Errorf("parse benchmark result: %w", err)
	}
	if err := ensureJSONEOF(decoder, "benchmark result"); err != nil {
		return err
	}
	return validateSchema("BenchmarkResult", raw)
}

func decodeJSONDocument[T any](document []byte, kind, label string, zero T) (T, error) {
	var raw any
	decoder := json.NewDecoder(bytes.NewReader(document))
	if err := decoder.Decode(&raw); err != nil {
		return zero, fmt.Errorf("parse %s: %w", label, err)
	}
	if err := ensureJSONEOF(decoder, label); err != nil {
		return zero, err
	}
	if err := validateSchema(kind, raw); err != nil {
		return zero, err
	}
	var value T
	decoder = json.NewDecoder(bytes.NewReader(document))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return zero, fmt.Errorf("strictly decode %s: %w", label, err)
	}
	return value, ensureJSONEOF(decoder, label)
}

func readBounded(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	reader := io.LimitReader(file, MaxContractBytes+1)
	document, readErr := io.ReadAll(reader)
	closeErr := file.Close()
	if readErr != nil {
		return nil, fmt.Errorf("read %s: %w", path, readErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close %s: %w", path, closeErr)
	}
	if len(document) > MaxContractBytes {
		return nil, fmt.Errorf("%s exceeds the %d byte contract limit", path, MaxContractBytes)
	}
	return document, nil
}

func loadYAML(path string, kind string, destination any) error {
	document, err := readBounded(path)
	if err != nil {
		return err
	}

	raw, err := rawYAML(document)
	if err != nil {
		return fmt.Errorf("parse %s: %w", path, err)
	}
	if err := validateSchema(kind, raw); err != nil {
		return err
	}

	decoder := yaml.NewDecoder(bytes.NewReader(document))
	decoder.KnownFields(true)
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("strictly decode %s: %w", path, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("%s contains multiple YAML documents", path)
		}
		return fmt.Errorf("decode trailing YAML in %s: %w", path, err)
	}
	return nil
}

func rawYAML(document []byte) (any, error) {
	decoder := yaml.NewDecoder(bytes.NewReader(document))
	var node yaml.Node
	if err := decoder.Decode(&node); err != nil {
		return nil, err
	}
	var trailing yaml.Node
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, fmt.Errorf("multiple YAML documents are forbidden")
		}
		return nil, err
	}
	if len(node.Content) != 1 {
		return nil, fmt.Errorf("document root is empty")
	}
	return yamlNodeToJSON(node.Content[0])
}

func yamlNodeToJSON(node *yaml.Node) (any, error) {
	if node.Alias != nil || node.Kind == yaml.AliasNode || node.Anchor != "" {
		return nil, fmt.Errorf("YAML aliases and anchors are forbidden at line %d", node.Line)
	}
	switch node.Kind {
	case yaml.MappingNode:
		object := make(map[string]any, len(node.Content)/2)
		for index := 0; index < len(node.Content); index += 2 {
			key := node.Content[index]
			if key.Kind != yaml.ScalarNode || key.Tag != "!!str" {
				return nil, fmt.Errorf("mapping key at line %d must be a string", key.Line)
			}
			if _, exists := object[key.Value]; exists {
				return nil, fmt.Errorf("duplicate mapping key %q at line %d", key.Value, key.Line)
			}
			value, err := yamlNodeToJSON(node.Content[index+1])
			if err != nil {
				return nil, err
			}
			object[key.Value] = value
		}
		return object, nil
	case yaml.SequenceNode:
		values := make([]any, len(node.Content))
		for index, child := range node.Content {
			value, err := yamlNodeToJSON(child)
			if err != nil {
				return nil, err
			}
			values[index] = value
		}
		return values, nil
	case yaml.ScalarNode:
		switch node.Tag {
		case "!!str":
			return node.Value, nil
		case "!!bool":
			return node.Value == "true", nil
		case "!!null":
			return nil, nil
		case "!!int":
			var value int64
			if err := node.Decode(&value); err != nil {
				return nil, err
			}
			return value, nil
		case "!!float":
			var value float64
			if err := node.Decode(&value); err != nil {
				return nil, err
			}
			return value, nil
		default:
			return nil, fmt.Errorf("YAML tag %q is unsupported at line %d", node.Tag, node.Line)
		}
	default:
		return nil, fmt.Errorf("YAML node kind %d is unsupported at line %d", node.Kind, node.Line)
	}
}

func loadJSON(path string, kind string, destination any) error {
	document, err := readBounded(path)
	if err != nil {
		return err
	}
	var raw any
	decoder := json.NewDecoder(bytes.NewReader(document))
	if err := decoder.Decode(&raw); err != nil {
		return fmt.Errorf("parse %s: %w", path, err)
	}
	if err := ensureJSONEOF(decoder, path); err != nil {
		return err
	}
	if err := validateSchema(kind, raw); err != nil {
		return err
	}
	decoder = json.NewDecoder(bytes.NewReader(document))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("strictly decode %s: %w", path, err)
	}
	return ensureJSONEOF(decoder, path)
}

func ensureJSONEOF(decoder *json.Decoder, path string) error {
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("%s contains multiple JSON values", path)
		}
		return fmt.Errorf("decode trailing JSON in %s: %w", path, err)
	}
	return nil
}
