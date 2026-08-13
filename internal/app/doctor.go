package app

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/uday1o1/chronicle-gate/internal/doctor"
)

func newDoctorCommand(checker *doctor.Checker) *cobra.Command {
	var machineReadable bool
	command := &cobra.Command{
		Use:   "doctor",
		Short: "Verify local ChronicleGate prerequisites",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			report := checker.Run(command.Context())
			if machineReadable {
				if err := json.NewEncoder(command.OutOrStdout()).Encode(report); err != nil {
					return newCommandError(ExitInfrastructure, "OUTPUT_ERROR", fmt.Errorf("write doctor JSON: %w", err), false)
				}
			} else if err := writeDoctorText(command, report); err != nil {
				return newCommandError(ExitInfrastructure, "OUTPUT_ERROR", err, false)
			}

			if report.Status != doctor.StatusPass {
				return newCommandError(ExitInfrastructure, "INFRASTRUCTURE_ERROR", fmt.Errorf("one or more environment checks failed"), true)
			}
			return nil
		},
	}
	command.Flags().BoolVar(&machineReadable, "json", false, "emit machine-readable JSON")
	return command
}

func writeDoctorText(command *cobra.Command, report doctor.Report) error {
	for _, check := range report.Checks {
		if _, err := fmt.Fprintf(command.OutOrStdout(), "%-4s %-30s %s\n", check.Status, check.ID, check.Summary); err != nil {
			return fmt.Errorf("write doctor report: %w", err)
		}
	}
	if _, err := fmt.Fprintf(command.OutOrStdout(), "overall: %s\n", report.Status); err != nil {
		return fmt.Errorf("write doctor summary: %w", err)
	}
	return nil
}
