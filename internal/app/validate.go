package app

import (
	"encoding/json"
	"fmt"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/uday1o1/chronicle-gate/internal/spec"
)

const validationReportSchema = "chronicle.dev/validation/v1alpha1"

type validationReport struct {
	SchemaVersion string           `json:"schemaVersion"`
	Status        string           `json:"status"`
	Documents     []string         `json:"documents"`
	Violations    []spec.Violation `json:"violations"`
}

func newValidateCommand() *cobra.Command {
	var scenarioPath string
	var targetPath string
	var machineReadable bool
	command := &cobra.Command{
		Use:   "validate",
		Short: "Validate authored contracts without starting containers",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			report := validationReport{
				SchemaVersion: validationReportSchema,
				Status:        "valid",
				Documents:     []string{scenarioPath, targetPath},
				Violations:    []spec.Violation{},
			}

			scenario, err := spec.LoadScenario(scenarioPath)
			if err != nil {
				report.Violations = append(report.Violations, spec.Violation{Document: "scenario", Pointer: "", Rule: "load", Message: err.Error()})
			}
			target, targetErr := spec.LoadTarget(targetPath)
			if targetErr != nil {
				report.Violations = append(report.Violations, spec.Violation{Document: "target", Pointer: "", Rule: "load", Message: targetErr.Error()})
			}
			if err == nil && targetErr == nil {
				report.Violations = append(report.Violations, spec.ValidateScenarioAndTarget(scenario, target, filepath.Dir(scenarioPath))...)
			}
			if len(report.Violations) > 0 {
				report.Status = "invalid"
			}
			if err := writeValidationReport(command, report, machineReadable); err != nil {
				return newCommandError(ExitInfrastructure, "OUTPUT_ERROR", err, false)
			}
			if report.Status == "invalid" {
				return newCommandError(ExitInvalidInput, "INVALID_INPUT", fmt.Errorf("authored contracts are invalid"), true)
			}
			return nil
		},
	}
	command.Flags().StringVar(&scenarioPath, "scenario", "", "path to the authored Scenario YAML")
	command.Flags().StringVar(&targetPath, "target", "", "path to the authored Target YAML")
	command.Flags().BoolVar(&machineReadable, "json", false, "emit machine-readable JSON")
	_ = command.MarkFlagRequired("scenario")
	_ = command.MarkFlagRequired("target")
	return command
}

func writeValidationReport(command *cobra.Command, report validationReport, machineReadable bool) error {
	if machineReadable {
		if err := json.NewEncoder(command.OutOrStdout()).Encode(report); err != nil {
			return fmt.Errorf("write validation JSON: %w", err)
		}
		return nil
	}
	if report.Status == "valid" {
		_, err := fmt.Fprintln(command.OutOrStdout(), "valid")
		return err
	}
	for _, violation := range report.Violations {
		if _, err := fmt.Fprintf(command.OutOrStdout(), "%s %s [%s]: %s\n", violation.Document, violation.Pointer, violation.Rule, violation.Message); err != nil {
			return err
		}
	}
	return nil
}
