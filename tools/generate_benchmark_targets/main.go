package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/uday1o1/chronicle-gate/internal/imagelock"
	"github.com/uday1o1/chronicle-gate/internal/spec"
	"go.yaml.in/yaml/v3"
)

func main() {
	baseline := flag.String("baseline-image", "", "exact baseline image ID")
	candidate := flag.String("candidate-image", "", "exact candidate image ID")
	output := flag.String("out", "examples/order-lifecycle/targets/generated", "output directory")
	flag.Parse()
	for name, image := range map[string]string{"baseline": *baseline, "candidate": *candidate} {
		if !imagelock.IsLocalImageID(image) {
			fatalf("%s benchmark image %q is not an exact local image ID", name, image)
		}
	}
	if err := os.MkdirAll(*output, 0o700); err != nil {
		fatalf("create benchmark target output: %v", err)
	}
	for name, image := range map[string]string{"benchmark-baseline.yaml": *baseline, "benchmark-candidate.yaml": *candidate} {
		if err := writeTarget(filepath.Join(*output, name), image); err != nil {
			fatalf("write %s: %v", name, err)
		}
	}
}

func writeTarget(path, image string) error {
	target := spec.Target{
		APIVersion: spec.APIVersion, Kind: "Target",
		Metadata: spec.Metadata{Name: "benchmark-api", Description: "Repository-trusted stdlib-only benchmark target."},
		Spec: spec.TargetSpec{DatabaseSchemaVersion: "benchmark-none-v1", Services: []spec.Service{{
			Name: "benchmark-api", Image: image, Command: []string{}, Args: []string{},
			Environment: map[string]string{}, SecretEnvironment: map[string]string{}, Dependencies: []string{},
			Health:    spec.Health{Type: "http", Path: "/healthz", Port: 8080, Timeout: duration("20s"), Interval: duration("100ms")},
			Probe:     spec.ProbeDeclaration{Enabled: false},
			Resources: spec.Resources{CPUs: 1, MemoryBytes: 128 << 20, PIDs: 64},
		}}},
	}
	document, err := yaml.Marshal(target)
	if err != nil {
		return err
	}
	return os.WriteFile(path, document, 0o600)
}

func duration(value string) spec.Duration {
	parsed, err := time.ParseDuration(value)
	if err != nil {
		fatalf("parse duration: %v", err)
	}
	return spec.Duration{Duration: parsed}
}

func fatalf(format string, values ...any) {
	_, _ = fmt.Fprintf(os.Stderr, format+"\n", values...)
	os.Exit(1)
}
