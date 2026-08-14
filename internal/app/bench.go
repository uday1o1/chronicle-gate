package app

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/uday1o1/chronicle-gate/internal/bench"
	"github.com/uday1o1/chronicle-gate/internal/spec"
)

func newBenchCommand(runner func(context.Context, bench.Config) bench.Report) *cobra.Command {
	var workloadPath string
	var baselinePath string
	var candidatePath string
	var outputPath string
	var developmentLocalImages bool
	var dedicatedHost bool
	var machineReadable bool
	command := &cobra.Command{
		Use:   "bench",
		Short: "Run isolated open-loop performance comparison",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			if _, err := os.Lstat(outputPath); err == nil {
				return newCommandError(ExitInvalidInput, "INVALID_OUTPUT", fmt.Errorf("benchmark output %q must not exist", outputPath), false)
			} else if !os.IsNotExist(err) {
				return newCommandError(ExitInvalidInput, "INVALID_OUTPUT", err, false)
			}
			workload, workloadErr := spec.LoadBenchmarkWorkload(workloadPath)
			baseline, baselineErr := spec.LoadTarget(baselinePath)
			candidate, candidateErr := spec.LoadTarget(candidatePath)
			violations := []spec.Violation{}
			for _, loaded := range []struct {
				document string
				err      error
			}{{"workload", workloadErr}, {"baseline", baselineErr}, {"candidate", candidateErr}} {
				if loaded.err != nil {
					violations = append(violations, spec.Violation{Document: loaded.document, Rule: "load", Message: loaded.err.Error()})
				}
			}
			if len(violations) == 0 {
				violations = bench.ValidateInputs(workload, baseline, candidate, developmentLocalImages)
			}
			if len(violations) != 0 {
				report := validationReport{SchemaVersion: validationReportSchema, Status: "invalid", Documents: []string{workloadPath, baselinePath, candidatePath}, Violations: violations}
				if err := writeValidationReport(command, report, machineReadable); err != nil {
					return newCommandError(ExitInfrastructure, "OUTPUT_ERROR", err, false)
				}
				return newCommandError(ExitInvalidInput, "INVALID_INPUT", fmt.Errorf("authored benchmark contracts are invalid"), true)
			}
			report := runner(command.Context(), bench.Config{
				Workload: workload, Baseline: baseline, Candidate: candidate, Output: outputPath,
				DevelopmentLocalImages: developmentLocalImages, DedicatedHost: dedicatedHost,
			})
			if machineReadable {
				if err := json.NewEncoder(command.OutOrStdout()).Encode(report); err != nil {
					return newCommandError(ExitInfrastructure, "OUTPUT_ERROR", err, false)
				}
			} else {
				if _, err := fmt.Fprintf(command.OutOrStdout(), "benchmark %s: %s\n", report.RunID, report.Classification); err != nil {
					return newCommandError(ExitInfrastructure, "OUTPUT_ERROR", err, false)
				}
			}
			if report.State == "INTERRUPTED" {
				return newCommandError(ExitInterrupted, "INTERRUPTED", fmt.Errorf("benchmark was interrupted"), true)
			}
			switch report.Classification {
			case "PASS":
				return nil
			case "PERFORMANCE_REGRESSION":
				return newCommandError(ExitRegression, report.Classification, fmt.Errorf("benchmark confirmed a performance regression"), true)
			case "INFRASTRUCTURE_ERROR":
				return newCommandError(ExitInfrastructure, report.Classification, fmt.Errorf("benchmark infrastructure failed: %s", report.Error), true)
			default:
				return newCommandError(ExitUnresolved, report.Classification, fmt.Errorf("benchmark ended as %s", report.Classification), true)
			}
		},
	}
	command.Flags().StringVar(&workloadPath, "workload", "", "path to the BenchmarkWorkload YAML")
	command.Flags().StringVar(&baselinePath, "baseline", "", "path to the baseline Target YAML")
	command.Flags().StringVar(&candidatePath, "candidate", "", "path to the candidate Target YAML")
	command.Flags().StringVar(&outputPath, "out", "", "new output directory for private benchmark artifacts")
	command.Flags().BoolVar(&developmentLocalImages, "development-local-images", false, "allow nonportable local sha256 image IDs")
	command.Flags().BoolVar(&dedicatedHost, "dedicated-host", false, "attest that publication mode owns an idle dedicated Linux Docker host")
	command.Flags().BoolVar(&machineReadable, "json", false, "emit machine-readable JSON")
	for _, name := range []string{"workload", "baseline", "candidate", "out"} {
		_ = command.MarkFlagRequired(name)
	}
	return command
}
