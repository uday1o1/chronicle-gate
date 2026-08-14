package spec

import (
	"path/filepath"
	"testing"
)

func TestBenchmarkWorkloadExampleAndHeaderRejection(t *testing.T) {
	workload, err := LoadBenchmarkWorkload(filepath.Join("..", "..", "benchmarks", "workloads", "order-api.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if violations := ValidateBenchmarkWorkload(workload); len(violations) != 0 {
		t.Fatalf("benchmark workload violations = %#v", violations)
	}
	workload.Spec.Operations[0].Headers["aCcEpT-eNcOdInG"] = "gzip"
	if violations := ValidateBenchmarkWorkload(workload); len(violations) == 0 {
		t.Fatal("case-insensitive Accept-Encoding passed validation")
	}
}
