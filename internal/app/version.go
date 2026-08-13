package app

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/uday1o1/chronicle-gate/internal/buildinfo"
)

func newVersionCommand(info buildinfo.Info) *cobra.Command {
	var machineReadable bool
	command := &cobra.Command{
		Use:   "version",
		Short: "Print ChronicleGate build metadata",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			if machineReadable {
				payload := struct {
					SchemaVersion string `json:"schemaVersion"`
					buildinfo.Info
				}{SchemaVersion: "chronicle.dev/version/v1alpha1", Info: info}
				if err := json.NewEncoder(command.OutOrStdout()).Encode(payload); err != nil {
					return newCommandError(ExitInfrastructure, "OUTPUT_ERROR", fmt.Errorf("write version JSON: %w", err), false)
				}
				return nil
			}
			_, err := fmt.Fprintf(command.OutOrStdout(), "chronicle %s (commit %s, built %s)\n", info.Version, info.Commit, info.BuildDate)
			if err != nil {
				return newCommandError(ExitInfrastructure, "OUTPUT_ERROR", fmt.Errorf("write version: %w", err), false)
			}
			return nil
		},
	}
	command.Flags().BoolVar(&machineReadable, "json", false, "emit machine-readable JSON")
	return command
}
