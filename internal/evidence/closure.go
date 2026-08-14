package evidence

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/uday1o1/chronicle-gate/internal/spec"
)

func FindSemanticCase(corpus Corpus, id string) (SemanticCase, error) {
	for _, item := range corpus.SemanticCases {
		if item.ID == id {
			return item, nil
		}
	}
	return SemanticCase{}, fmt.Errorf("semantic case %q is not declared", id)
}

func FindBenchmarkCase(corpus Corpus, id string) (BenchmarkCase, error) {
	for _, item := range corpus.BenchmarkCases {
		if item.ID == id {
			return item, nil
		}
	}
	return BenchmarkCase{}, fmt.Errorf("benchmark case %q is not declared", id)
}

func SemanticInputs(repository, corpusPath string, item SemanticCase) ([]InputFile, error) {
	paths := []string{corpusPath, "config/images.lock.json", item.Scenario, item.Baseline, item.Candidate}
	if item.ExpectedSignature != nil {
		paths = append(paths, *item.ExpectedSignature)
	}
	scenario, err := spec.LoadScenario(filepath.Join(repository, filepath.FromSlash(item.Scenario)))
	if err != nil {
		return nil, err
	}
	root := filepath.ToSlash(filepath.Dir(item.Scenario))
	for _, observation := range scenario.Spec.Observations {
		if observation.SQL != nil {
			paths = append(paths, filepath.ToSlash(filepath.Join(root, observation.SQL.QueryFile)))
		}
		if observation.Kafka != nil {
			paths = append(paths, filepath.ToSlash(filepath.Join(root, observation.Kafka.SchemaFile)))
		}
	}
	for _, invariant := range scenario.Spec.Invariants {
		paths = append(paths, filepath.ToSlash(filepath.Join(root, invariant.QueryFile)))
	}
	for _, event := range scenario.Spec.Events {
		if event.Registry != nil {
			for _, history := range event.Registry.History {
				paths = append(paths, filepath.ToSlash(filepath.Join(root, history)))
			}
		}
		if event.DataSchema != "" && !strings.Contains(event.DataSchema, "://") {
			paths = append(paths, filepath.ToSlash(filepath.Join(root, event.DataSchema)))
		}
	}
	return inputInventory(repository, paths)
}

func BenchmarkInputs(repository, corpusPath string, item BenchmarkCase) ([]InputFile, error) {
	return inputInventory(repository, []string{corpusPath, "config/images.lock.json", item.Workload, item.Baseline, item.Candidate})
}

func inputInventory(repository string, paths []string) ([]InputFile, error) {
	unique := map[string]struct{}{}
	inputs := make([]InputFile, 0, len(paths))
	for _, path := range paths {
		path = filepath.ToSlash(filepath.Clean(path))
		if !safeRepositoryPath(path) {
			return nil, fmt.Errorf("unsafe evidence input path %q", path)
		}
		if _, exists := unique[path]; exists {
			continue
		}
		unique[path] = struct{}{}
		full := filepath.Join(repository, filepath.FromSlash(path))
		info, err := os.Lstat(full)
		if err != nil {
			return nil, fmt.Errorf("inspect evidence input %s: %w", path, err)
		}
		if !info.Mode().IsRegular() || info.Size() > maxEvidenceDocumentBytes {
			return nil, fmt.Errorf("evidence input %s is not a bounded regular file", path)
		}
		digest, err := HashFile(full)
		if err != nil {
			return nil, fmt.Errorf("hash evidence input %s: %w", path, err)
		}
		inputs = append(inputs, InputFile{Path: path, SHA256: digest})
	}
	sort.Slice(inputs, func(left, right int) bool { return inputs[left].Path < inputs[right].Path })
	return inputs, nil
}
