package spec

import (
	"encoding/json"
	"path/filepath"
	"reflect"
	"testing"
)

func FuzzScenarioContracts(f *testing.F) {
	scenarioPath := repositoryPath("examples", "order-lifecycle", "scenarios", "r1-offset-rewind.yaml")
	targetPath := repositoryPath("examples", "order-lifecycle", "targets", "baseline.yaml")
	scenario, err := LoadScenario(scenarioPath)
	if err != nil {
		f.Fatal(err)
	}
	target, err := LoadTarget(targetPath)
	if err != nil {
		f.Fatal(err)
	}
	seed, err := json.Marshal(scenario)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Fuzz(func(t *testing.T, document []byte) {
		if len(document) > 128<<10 {
			t.Skip()
		}
		decoded, err := DecodeScenarioJSON(document)
		if err != nil {
			return
		}
		encoded, err := json.Marshal(decoded)
		if err != nil {
			t.Fatal(err)
		}
		roundTrip, err := DecodeScenarioJSON(encoded)
		if err != nil {
			t.Fatalf("accepted scenario did not round trip: %v", err)
		}
		if !reflect.DeepEqual(decoded, roundTrip) {
			t.Fatal("accepted scenario lost supported fields during round trip")
		}
		if violations := ValidateScenarioAndTarget(decoded, target, filepath.Dir(scenarioPath)); len(violations) == 0 {
			steps := make(map[string]Step, len(decoded.Spec.Steps))
			for _, step := range decoded.Spec.Steps {
				steps[step.ID] = step
			}
			if _, cyclic := dependencyAncestors(steps); cyclic != "" {
				t.Fatalf("semantically accepted scenario contains a cycle at %q", cyclic)
			}
		}
	})
}

func FuzzResultAndBundleContracts(f *testing.F) {
	for mode, path := range map[byte]string{
		0: repositoryPath("examples", "order-lifecycle", "expected", "result.json"),
		1: repositoryPath("examples", "order-lifecycle", "expected", "bundle.json"),
	} {
		document, err := readBounded(path)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(mode, document)
	}
	f.Fuzz(func(t *testing.T, mode byte, document []byte) {
		if len(document) > 128<<10 {
			t.Skip()
		}
		switch mode % 2 {
		case 0:
			value, err := decodeJSONDocument(document, "Result", "result", Result{})
			if err != nil {
				return
			}
			encoded, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			roundTrip, err := decodeJSONDocument(encoded, "Result", "result", Result{})
			if err != nil || !reflect.DeepEqual(value, roundTrip) {
				t.Fatalf("accepted Result lost fields: %v", err)
			}
		case 1:
			value, err := DecodeBundleJSON(document)
			if err != nil {
				return
			}
			encoded, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			roundTrip, err := DecodeBundleJSON(encoded)
			if err != nil || !reflect.DeepEqual(value, roundTrip) {
				t.Fatalf("accepted Bundle lost fields: %v", err)
			}
		}
	})
}
