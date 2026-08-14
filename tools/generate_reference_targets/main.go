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
	flakyImage := flag.String("flaky-image", "", "content-addressed deterministic flaky Docker image ID")
	r4BaselineImage := flag.String("r4-baseline-image", "", "content-addressed R4 baseline Docker image ID")
	r4CandidateImage := flag.String("r4-candidate-image", "", "content-addressed R4 candidate Docker image ID")
	workflowBaselineImage := flag.String("workflow-baseline-image", "", "content-addressed baseline workflow Docker image ID")
	workflowCandidateImage := flag.String("workflow-candidate-image", "", "content-addressed R2 workflow Docker image ID")
	effectSinkImage := flag.String("effect-sink-image", "", "content-addressed effect sink Docker image ID")
	output := flag.String("out", "examples/order-lifecycle/targets/generated", "generated target directory")
	flag.Parse()

	for name, image := range map[string]string{
		"baseline": *baselineImage, "candidate": *candidateImage, "flaky": *flakyImage,
		"R4 baseline": *r4BaselineImage, "R4 candidate": *r4CandidateImage,
		"workflow baseline": *workflowBaselineImage, "workflow candidate": *workflowCandidateImage, "effect sink": *effectSinkImage,
	} {
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
	if err := writeTarget(filepath.Join(*output, "flaky.yaml"), *flakyImage); err != nil {
		fatalf("write flaky target: %v", err)
	}
	if err := writeTarget(filepath.Join(*output, "r4-baseline.yaml"), *r4BaselineImage); err != nil {
		fatalf("write R4 baseline target: %v", err)
	}
	if err := writeTarget(filepath.Join(*output, "r4-candidate.yaml"), *r4CandidateImage); err != nil {
		fatalf("write R4 candidate target: %v", err)
	}
	if err := writePreciseTarget(filepath.Join(*output, "r2-baseline.yaml"), *workflowBaselineImage, *effectSinkImage); err != nil {
		fatalf("write R2 baseline target: %v", err)
	}
	if err := writePreciseTarget(filepath.Join(*output, "r2-candidate.yaml"), *workflowCandidateImage, *effectSinkImage); err != nil {
		fatalf("write R2 candidate target: %v", err)
	}
}

func writePreciseTarget(path, workflowImage, effectSinkImage string) error {
	probeCheckpoints := []string{"before_handler", "after_state_load", "after_external_effect", "after_db_commit", "before_offset_commit", "after_offset_commit"}
	target := spec.Target{
		APIVersion: spec.APIVersion,
		Kind:       "Target",
		Metadata:   spec.Metadata{Name: "order-lifecycle-r2", Description: "Repository-trusted local precise crash target."},
		Spec: spec.TargetSpec{
			DatabaseSchemaVersion: "order-lifecycle-v1",
			Services: []spec.Service{
				{
					Name: "order-workflow", Image: workflowImage, Command: []string{}, Args: []string{}, Environment: map[string]string{}, SecretEnvironment: map[string]string{},
					Health:    spec.Health{Type: "http", Path: "/healthz", Port: 8080, Timeout: duration("20s"), Interval: duration("250ms")},
					Probe:     spec.ProbeDeclaration{Enabled: true, ProtocolVersion: "chronicle-probe/v1alpha1", CommitMode: "manual_sync", MaxControlledInFlight: 1, Checkpoints: probeCheckpoints, LogicalClock: true},
					Resources: spec.Resources{CPUs: 1, MemoryBytes: 256 << 20, PIDs: 128}, Dependencies: []string{"effect-sink"},
				},
				{
					Name: "effect-sink", Image: effectSinkImage, Command: []string{}, Args: []string{}, Environment: map[string]string{}, SecretEnvironment: map[string]string{},
					Health: spec.Health{Type: "http", Path: "/healthz", Port: 8080, Timeout: duration("20s"), Interval: duration("250ms")},
					Probe:  spec.ProbeDeclaration{Enabled: false}, Resources: spec.Resources{CPUs: 0.5, MemoryBytes: 128 << 20, PIDs: 64}, Dependencies: []string{},
				},
			},
		},
	}
	document, err := yaml.Marshal(target)
	if err != nil {
		return fmt.Errorf("marshal precise target: %w", err)
	}
	return os.WriteFile(path, document, 0o600)
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
