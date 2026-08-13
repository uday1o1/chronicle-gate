package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/uday1o1/chronicle-gate/internal/imagelock"
	"github.com/uday1o1/chronicle-gate/internal/spec"
	"go.yaml.in/yaml/v3"
)

func main() {
	baselineImage := flag.String("baseline-image", "", "content-addressed baseline Docker image ID")
	candidateImage := flag.String("candidate-image", "", "content-addressed candidate Docker image ID")
	output := flag.String("out", "examples/order-lifecycle/targets/generated", "generated target directory")
	flag.Parse()

	for name, image := range map[string]string{"baseline": *baselineImage, "candidate": *candidateImage} {
		if !imagelock.IsLocalImageID(image) {
			fatalf("%s image %q is not an exact sha256 Docker image ID", name, image)
		}
	}
	if err := os.MkdirAll(*output, 0o700); err != nil {
		fatalf("create output directory: %v", err)
	}
	if err := writeTarget(filepath.Join(*output, "baseline.yaml"), *baselineImage); err != nil {
		fatalf("write baseline target: %v", err)
	}
	if err := writeTarget(filepath.Join(*output, "candidate.yaml"), *candidateImage); err != nil {
		fatalf("write candidate target: %v", err)
	}
}

func writeTarget(path string, image string) error {
	target := spec.Target{
		APIVersion: spec.APIVersion,
		Kind:       "Target",
		Metadata:   spec.Metadata{Name: "order-lifecycle", Description: "Repository-trusted local R1 projector image."},
		Spec: spec.TargetSpec{
			DatabaseSchemaVersion: "order-lifecycle-v1",
			Services: []spec.Service{{
				Name:              "fulfillment-projector",
				Image:             image,
				Command:           []string{},
				Args:              []string{},
				Environment:       map[string]string{},
				SecretEnvironment: map[string]string{},
				Health:            spec.Health{Type: "http", Path: "/healthz", Port: 8080, Timeout: duration("20s"), Interval: duration("250ms")},
				Probe:             spec.ProbeDeclaration{Enabled: false},
				Resources:         spec.Resources{CPUs: 1, MemoryBytes: 256 << 20, PIDs: 128},
				Dependencies:      []string{},
			}},
		},
	}
	document, err := yaml.Marshal(target)
	if err != nil {
		return fmt.Errorf("marshal target: %w", err)
	}
	if err := os.WriteFile(path, document, 0o600); err != nil {
		return err
	}
	return nil
}

func duration(value string) spec.Duration {
	parsed, err := spec.ParseDuration(value)
	if err != nil {
		fatalf("parse built-in duration %q: %v", value, err)
	}
	return parsed
}

func fatalf(format string, values ...any) {
	_, _ = fmt.Fprintf(os.Stderr, format+"\n", values...)
	os.Exit(1)
}
