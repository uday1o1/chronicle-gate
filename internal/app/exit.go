package app

import (
	"errors"
	"fmt"
)

// ExitCode is ChronicleGate's public process status contract.
type ExitCode int

const (
	ExitSuccess        ExitCode = 0
	ExitRegression     ExitCode = 2
	ExitInvalidInput   ExitCode = 3
	ExitInfrastructure ExitCode = 4
	ExitUnresolved     ExitCode = 5
	ExitInterrupted    ExitCode = 130
)

type commandError struct {
	code     ExitCode
	kind     string
	err      error
	reported bool
}

func (err *commandError) Error() string {
	return err.err.Error()
}

func (err *commandError) Unwrap() error {
	return err.err
}

func newCommandError(code ExitCode, kind string, err error, reported bool) error {
	return &commandError{code: code, kind: kind, err: err, reported: reported}
}

func classifyError(err error) (ExitCode, string, bool) {
	if err == nil {
		return ExitSuccess, "", false
	}
	var typed *commandError
	if errors.As(err, &typed) {
		return typed.code, typed.kind, typed.reported
	}
	return ExitInvalidInput, "INVALID_INPUT", false
}

func errorMessage(err error) string {
	var typed *commandError
	if errors.As(err, &typed) {
		return typed.err.Error()
	}
	return fmt.Sprint(err)
}
