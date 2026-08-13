package doctor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
)

const maxCommandOutput = 1 << 20

// CommandRunner executes bounded external diagnostics without a shell.
type CommandRunner interface {
	Run(ctx context.Context, name string, args ...string) (stdout []byte, stderr []byte, err error)
}

// ExecRunner runs commands using os/exec and caps each output stream at one MiB.
type ExecRunner struct{}

func (ExecRunner) Run(ctx context.Context, name string, args ...string) ([]byte, []byte, error) {
	command := exec.CommandContext(ctx, name, args...)
	var stdout limitedBuffer
	var stderr limitedBuffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	err := command.Run()
	if stdout.exceeded || stderr.exceeded {
		err = errors.Join(err, fmt.Errorf("command output exceeded %d bytes", maxCommandOutput))
	}
	return stdout.Bytes(), stderr.Bytes(), err
}

type limitedBuffer struct {
	buffer   bytes.Buffer
	exceeded bool
}

func (buffer *limitedBuffer) Write(data []byte) (int, error) {
	remaining := maxCommandOutput - buffer.buffer.Len()
	if remaining <= 0 {
		buffer.exceeded = true
		return len(data), nil
	}
	if len(data) > remaining {
		buffer.exceeded = true
		_, _ = buffer.buffer.Write(data[:remaining])
		return len(data), nil
	}
	return buffer.buffer.Write(data)
}

func (buffer *limitedBuffer) Bytes() []byte {
	return buffer.buffer.Bytes()
}
