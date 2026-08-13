package app

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/uday1o1/chronicle-gate/internal/engine"
	"github.com/uday1o1/chronicle-gate/internal/spec"
)

func newRunCommand(runner func(context.Context, engine.Config) engine.Report) *cobra.Command {
	var scenarioPath string
	var baselinePath string
	var candidatePath string
	var outputPath string
	var imageLockPath string
	var developmentLocalImages bool
	var noMinimize bool
	var machineReadable bool
	command := &cobra.Command{
		Use:   "run",
		Short: "Run sequential semantic qualification",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			scenario, scenarioErr := spec.LoadScenario(scenarioPath)
			baseline, baselineErr := spec.LoadTarget(baselinePath)
			candidate, candidateErr := spec.LoadTarget(candidatePath)
			violations := loadViolations(scenarioErr, baselineErr, candidateErr)
			if len(violations) == 0 {
				options := spec.ValidationOptions{AllowLocalImageIDs: developmentLocalImages}
				root := filepath.Dir(scenarioPath)
				violations = append(violations, spec.ValidateScenarioAndTargetWithOptions(scenario, baseline, root, options)...)
				violations = append(violations, spec.ValidateScenarioAndTargetWithOptions(scenario, candidate, root, options)...)
				violations = append(violations, spec.CompareTargets(baseline, candidate, scenario.Spec.Comparison.AllowedTargetDifferences)...)
				if err := engine.ValidateVerticalSlice(scenario, baseline); err != nil {
					violations = append(violations, spec.Violation{Document: "scenario", Pointer: "/spec", Rule: "milestone_2_vertical_slice", Message: err.Error()})
				}
				if err := engine.ValidateVerticalSlice(scenario, candidate); err != nil {
					violations = append(violations, spec.Violation{Document: "candidate", Pointer: "/spec", Rule: "milestone_2_vertical_slice", Message: err.Error()})
				}
			}
			if len(violations) > 0 {
				report := validationReport{SchemaVersion: validationReportSchema, Status: "invalid", Documents: []string{scenarioPath, baselinePath, candidatePath}, Violations: violations}
				if err := writeValidationReport(command, report, machineReadable); err != nil {
					return newCommandError(ExitInfrastructure, "OUTPUT_ERROR", err, false)
				}
				return newCommandError(ExitInvalidInput, "INVALID_INPUT", fmt.Errorf("authored run contracts are invalid"), true)
			}

			report := runner(command.Context(), engine.Config{
				Scenario: scenario, Baseline: baseline, Candidate: candidate, ScenarioRoot: filepath.Dir(scenarioPath),
				Output: outputPath, ImageLock: imageLockPath, ScenarioPath: scenarioPath, BaselinePath: baselinePath,
				CandidatePath: candidatePath, NoMinimize: noMinimize,
			})
			if err := writeRunReport(command, report, machineReadable); err != nil {
				return newCommandError(ExitInfrastructure, "OUTPUT_ERROR", err, false)
			}
			if report.State == "INTERRUPTED" {
				return newCommandError(ExitInterrupted, "INTERRUPTED", fmt.Errorf("qualification was interrupted"), true)
			}
			switch report.Classification {
			case "PASS":
				return nil
			case "SEMANTIC_REGRESSION", "SCHEMA_REGRESSION", "EXTERNAL_EFFECT_REGRESSION", "PERFORMANCE_REGRESSION":
				return newCommandError(ExitRegression, report.Classification, fmt.Errorf("qualification found %s", report.Classification), true)
			case "INFRASTRUCTURE_ERROR":
				return newCommandError(ExitInfrastructure, report.Classification, fmt.Errorf("qualification failed: %s", report.Error), true)
			default:
				return newCommandError(ExitUnresolved, report.Classification, fmt.Errorf("qualification ended as %s", report.Classification), true)
			}
		},
	}
	command.Flags().StringVar(&scenarioPath, "scenario", "", "path to the authored Scenario YAML")
	command.Flags().StringVar(&baselinePath, "baseline", "", "path to the baseline Target YAML")
	command.Flags().StringVar(&candidatePath, "candidate", "", "path to the candidate Target YAML")
	command.Flags().StringVar(&outputPath, "out", "", "empty output directory for private run artifacts")
	command.Flags().StringVar(&imageLockPath, "image-lock", "config/images.lock.json", "path to the immutable environment image lock")
	command.Flags().BoolVar(&developmentLocalImages, "development-local-images", false, "allow nonportable local sha256 image IDs")
	command.Flags().BoolVar(&noMinimize, "no-minimize", false, "disable minimization (not available before Milestone 3)")
	command.Flags().BoolVar(&machineReadable, "json", false, "emit machine-readable JSON")
	for _, name := range []string{"scenario", "baseline", "candidate", "out"} {
		_ = command.MarkFlagRequired(name)
	}
	return command
}

func loadViolations(scenarioErr, baselineErr, candidateErr error) []spec.Violation {
	violations := []spec.Violation{}
	for _, item := range []struct {
		document string
		err      error
	}{{"scenario", scenarioErr}, {"baseline", baselineErr}, {"candidate", candidateErr}} {
		if item.err != nil {
			violations = append(violations, spec.Violation{Document: item.document, Pointer: "", Rule: "load", Message: item.err.Error()})
		}
	}
	return violations
}

func writeRunReport(command *cobra.Command, report engine.Report, machineReadable bool) error {
	if machineReadable {
		if err := json.NewEncoder(command.OutOrStdout()).Encode(report); err != nil {
			return fmt.Errorf("write run JSON: %w", err)
		}
		return nil
	}
	_, err := fmt.Fprintf(command.OutOrStdout(), "run %s: %s\n", report.RunID, report.Classification)
	return err
}
