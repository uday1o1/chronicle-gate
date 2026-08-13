// Package app owns the public ChronicleGate command tree and exit contract.
package app

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"slices"

	"github.com/spf13/cobra"
	"github.com/uday1o1/chronicle-gate/internal/buildinfo"
	"github.com/uday1o1/chronicle-gate/internal/doctor"
)

const errorSchemaVersion = "chronicle.dev/error/v1alpha1"

// Dependencies allows commands to use deterministic substitutes in tests.
type Dependencies struct {
	Doctor *doctor.Checker
	Build  buildinfo.Info
}

// Execute runs ChronicleGate without terminating the process.
func Execute(ctx context.Context, args []string, stdout io.Writer, stderr io.Writer, dependencies Dependencies) ExitCode {
	root := newRootCommand(dependencies)
	root.SetArgs(args)
	root.SetOut(stdout)
	root.SetErr(stderr)
	root.SetContext(ctx)

	err := root.Execute()
	code, kind, reported := classifyError(err)
	if err == nil || reported {
		return code
	}

	if slices.Contains(args, "--json") {
		payload := struct {
			SchemaVersion string `json:"schemaVersion"`
			Error         struct {
				Kind    string `json:"kind"`
				Message string `json:"message"`
			} `json:"error"`
		}{SchemaVersion: errorSchemaVersion}
		payload.Error.Kind = kind
		payload.Error.Message = errorMessage(err)
		if encodeErr := json.NewEncoder(stdout).Encode(payload); encodeErr != nil {
			_, _ = fmt.Fprintf(stderr, "write JSON error: %v\n", encodeErr)
		}
	} else {
		_, _ = fmt.Fprintf(stderr, "Error: %s\n", errorMessage(err))
	}
	return code
}

func newRootCommand(dependencies Dependencies) *cobra.Command {
	if dependencies.Doctor == nil {
		dependencies.Doctor = doctor.New(doctor.Options{})
	}
	if dependencies.Build == (buildinfo.Info{}) {
		dependencies.Build = buildinfo.Current()
	}

	root := &cobra.Command{
		Use:           "chronicle",
		Short:         "Semantic release qualification for stateful Kafka consumers",
		SilenceErrors: true,
		SilenceUsage:  true,
		Args:          cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			return newCommandError(ExitInvalidInput, "INVALID_INPUT", fmt.Errorf("a subcommand is required; run %q", command.CommandPath()+" --help"), false)
		},
	}
	root.CompletionOptions.DisableDefaultCmd = true
	root.AddCommand(newVersionCommand(dependencies.Build))
	root.AddCommand(newDoctorCommand(dependencies.Doctor))
	root.AddCommand(newValidateCommand())
	return root
}
