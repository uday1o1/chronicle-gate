package app

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/uday1o1/chronicle-gate/internal/report"
	"github.com/uday1o1/chronicle-gate/internal/runlog"
)

func newReportCommand() *cobra.Command {
	var resultDirectory string
	var format string
	var machineReadable bool
	command := &cobra.Command{
		Use:   "report",
		Short: "Render a completed run result",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			if machineReadable {
				format = "json"
			}
			events, truncated, err := runlog.Read(filepath.Join(resultDirectory, "events.ndjson"))
			if err != nil {
				return newCommandError(ExitInvalidInput, "INVALID_RESULT", err, false)
			}
			finalState, authoritative := runlog.FinalState(events, truncated)
			document, err := os.ReadFile(filepath.Join(resultDirectory, "result.json"))
			if err != nil {
				return newCommandError(ExitInvalidInput, "INVALID_RESULT", fmt.Errorf("read result: %w", err), false)
			}
			decoded, err := report.Decode(document)
			if err != nil {
				return newCommandError(ExitInvalidInput, "INVALID_RESULT", err, false)
			}
			if !authoritative {
				decoded.State = finalState
				decoded.Classification = "UNRESOLVED"
				decoded.Error = "the run journal has no valid terminal record"
			}
			rendered, err := report.Render(decoded, format)
			if err != nil {
				return newCommandError(ExitInvalidInput, "INVALID_FORMAT", err, false)
			}
			if _, err := command.OutOrStdout().Write(rendered); err != nil {
				return newCommandError(ExitInfrastructure, "OUTPUT_ERROR", err, false)
			}
			if !authoritative {
				return newCommandError(ExitUnresolved, "INTERRUPTED", fmt.Errorf("run journal is incomplete"), true)
			}
			return nil
		},
	}
	command.Flags().StringVar(&resultDirectory, "result", "", "run result directory")
	command.Flags().StringVar(&format, "format", "text", "report format: text, json, junit, or html")
	command.Flags().BoolVar(&machineReadable, "json", false, "emit machine-readable JSON")
	_ = command.MarkFlagRequired("result")
	return command
}
