package app

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/uday1o1/chronicle-gate/internal/bundle"
	"github.com/uday1o1/chronicle-gate/internal/engine"
	cruntime "github.com/uday1o1/chronicle-gate/internal/runtime"
	"github.com/uday1o1/chronicle-gate/internal/spec"
)

func newReplayCommand(runner func(context.Context, engine.Config) engine.Report) *cobra.Command {
	var bundlePath string
	var outputPath string
	var machineReadable bool
	command := &cobra.Command{
		Use:   "replay",
		Short: "Verify and replay a reproduction bundle",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			if _, err := os.Lstat(outputPath); err == nil {
				return newCommandError(ExitInvalidInput, "INVALID_OUTPUT", fmt.Errorf("replay output %q must not exist", outputPath), false)
			} else if !os.IsNotExist(err) {
				return newCommandError(ExitInvalidInput, "INVALID_OUTPUT", err, false)
			}
			archive, err := bundle.Open(bundlePath)
			if err != nil {
				return newCommandError(ExitInvalidInput, "INVALID_BUNDLE", err, false)
			}
			defer func() { _ = archive.Close() }()
			parent := filepath.Dir(outputPath)
			if err := os.MkdirAll(parent, 0o700); err != nil {
				return newCommandError(ExitInfrastructure, "OUTPUT_ERROR", err, false)
			}
			stagingRoot, err := os.MkdirTemp(parent, ".chronicle-replay-staging-")
			if err != nil {
				return newCommandError(ExitInfrastructure, "OUTPUT_ERROR", err, false)
			}
			defer func() { _ = os.RemoveAll(stagingRoot) }()
			staging := filepath.Join(stagingRoot, "inputs")
			if err := archive.Extract(staging); err != nil {
				return newCommandError(ExitInvalidInput, "INVALID_BUNDLE", err, false)
			}
			if !machineReadable {
				if err := writeReplayPlan(command, archive); err != nil {
					return newCommandError(ExitInfrastructure, "OUTPUT_ERROR", err, false)
				}
			}
			scenarioPath := filepath.Join(staging, filepath.FromSlash(archive.Manifest.Scenario))
			baselinePath := filepath.Join(staging, filepath.FromSlash(archive.Manifest.Targets[0]))
			candidatePath := filepath.Join(staging, filepath.FromSlash(archive.Manifest.Targets[1]))
			scenario, err := loadReplayScenario(scenarioPath)
			if err != nil {
				return newCommandError(ExitInvalidInput, "INVALID_BUNDLE", err, false)
			}
			baseline, err := loadReplayTarget(baselinePath)
			if err != nil {
				return newCommandError(ExitInvalidInput, "INVALID_BUNDLE", err, false)
			}
			candidate, err := loadReplayTarget(candidatePath)
			if err != nil {
				return newCommandError(ExitInvalidInput, "INVALID_BUNDLE", err, false)
			}
			scenarioRoot := filepath.Join(staging, "scenario")
			options := spec.ValidationOptions{AllowLocalImageIDs: archive.Manifest.Nonportable}
			violations := append(spec.ValidateScenarioAndTargetWithOptions(scenario, baseline, scenarioRoot, options), spec.ValidateScenarioAndTargetWithOptions(scenario, candidate, scenarioRoot, options)...)
			violations = append(violations, spec.CompareTargets(baseline, candidate, scenario.Spec.Comparison.AllowedTargetDifferences)...)
			if len(violations) != 0 {
				return newCommandError(ExitInvalidInput, "INVALID_BUNDLE", fmt.Errorf("bundled contracts are invalid at %s: %s", violations[0].Pointer, violations[0].Message), false)
			}
			if err := engine.ValidateVerticalSlice(scenario, baseline); err != nil {
				return newCommandError(ExitInvalidInput, "INVALID_BUNDLE", err, false)
			}
			if err := engine.ValidateVerticalSlice(scenario, candidate); err != nil {
				return newCommandError(ExitInvalidInput, "INVALID_BUNDLE", err, false)
			}
			if err := cruntime.ConfigureDockerHost(command.Context()); err != nil {
				return newCommandError(ExitInfrastructure, "DOCKER_ERROR", err, false)
			}
			if err := archive.LoadImages(command.Context()); err != nil {
				return newCommandError(ExitInfrastructure, "IMAGE_LOAD_ERROR", err, false)
			}
			report := runner(command.Context(), engine.Config{
				Scenario: scenario, Baseline: baseline, Candidate: candidate,
				ScenarioRoot: scenarioRoot, Output: outputPath,
				ImageLock: filepath.Join(staging, "environment.lock.json"), ScenarioPath: scenarioPath,
				BaselinePath: baselinePath, CandidatePath: candidatePath, NoMinimize: true,
				SourceBundleSHA256: archive.SHA256, ExpectedSignature: archive.Manifest.ExpectedSignature,
			})
			if machineReadable {
				if err := json.NewEncoder(command.OutOrStdout()).Encode(report); err != nil {
					return newCommandError(ExitInfrastructure, "OUTPUT_ERROR", err, false)
				}
			} else {
				if _, err := fmt.Fprintf(command.OutOrStdout(), "replay %s: %s\n", report.RunID, report.Classification); err != nil {
					return newCommandError(ExitInfrastructure, "OUTPUT_ERROR", err, false)
				}
			}
			if report.State == "INTERRUPTED" {
				return newCommandError(ExitInterrupted, "INTERRUPTED", fmt.Errorf("replay was interrupted"), true)
			}
			switch report.Classification {
			case "PASS":
				return nil
			case "SEMANTIC_REGRESSION", "SCHEMA_REGRESSION", "EXTERNAL_EFFECT_REGRESSION", "PERFORMANCE_REGRESSION":
				return newCommandError(ExitRegression, report.Classification, fmt.Errorf("replay confirmed %s", report.Classification), true)
			case "INFRASTRUCTURE_ERROR":
				return newCommandError(ExitInfrastructure, report.Classification, fmt.Errorf("replay failed: %s", report.Error), true)
			default:
				return newCommandError(ExitUnresolved, report.Classification, fmt.Errorf("replay ended as %s", report.Classification), true)
			}
		},
	}
	command.Flags().StringVar(&bundlePath, "bundle", "", "reproduction ZIP path")
	command.Flags().StringVar(&outputPath, "out", "", "new output directory for replay artifacts")
	command.Flags().BoolVar(&machineReadable, "json", false, "emit machine-readable JSON")
	_ = command.MarkFlagRequired("bundle")
	_ = command.MarkFlagRequired("out")
	return command
}

func writeReplayPlan(command *cobra.Command, archive *bundle.Archive) error {
	if _, err := fmt.Fprintf(command.OutOrStdout(), "verified bundle %s\n", archive.SHA256); err != nil {
		return err
	}
	for _, image := range archive.Manifest.Images {
		if _, err := fmt.Fprintf(command.OutOrStdout(), "image: %s\n", image.Reference); err != nil {
			return err
		}
	}
	_, err := fmt.Fprintf(command.OutOrStdout(), "resources: %.1f CPUs, %d bytes memory, %d bytes disk\n", archive.Manifest.Resources.CPUs, archive.Manifest.Resources.MemoryBytes, archive.Manifest.Resources.DiskBytes)
	return err
}

func loadReplayScenario(path string) (spec.Scenario, error) {
	document, err := os.ReadFile(path)
	if err != nil {
		return spec.Scenario{}, err
	}
	return spec.DecodeScenarioJSON(document)
}

func loadReplayTarget(path string) (spec.Target, error) {
	document, err := os.ReadFile(path)
	if err != nil {
		return spec.Target{}, err
	}
	return spec.DecodeTargetJSON(document)
}
