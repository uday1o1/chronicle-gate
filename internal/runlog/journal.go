// Package runlog persists ChronicleGate's crash-readable execution journal.
package runlog

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/uday1o1/chronicle-gate/internal/artifact"
)

const SchemaVersion = "chronicle.dev/run-event/v1alpha1"

type Event struct {
	SchemaVersion string         `json:"schemaVersion"`
	Sequence      uint64         `json:"sequence"`
	Timestamp     string         `json:"timestamp"`
	State         string         `json:"state"`
	Phase         string         `json:"phase"`
	OperationID   string         `json:"operationId"`
	Operation     string         `json:"operation"`
	Status        string         `json:"status,omitempty"`
	Cause         string         `json:"cause,omitempty"`
	Detail        map[string]any `json:"detail,omitempty"`
}

type Journal struct {
	mutex    sync.Mutex
	file     *os.File
	sequence uint64
	secrets  []string
}

func (journal *Journal) SetSecretValues(values []string) {
	journal.mutex.Lock()
	defer journal.mutex.Unlock()
	journal.secrets = append([]string(nil), values...)
}

func Open(path string) (*Journal, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open run journal: %w", err)
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("secure run journal: %w", err)
	}
	events, _, err := Read(path)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	journal := &Journal{file: file}
	if len(events) > 0 {
		journal.sequence = events[len(events)-1].Sequence
	}
	return journal, nil
}

func (journal *Journal) Before(state, operation string, detail map[string]any) (string, error) {
	journal.mutex.Lock()
	defer journal.mutex.Unlock()
	operationID := fmt.Sprintf("op-%06d", journal.sequence+1)
	err := journal.appendLocked(Event{State: state, Phase: "before", OperationID: operationID, Operation: operation, Detail: detail})
	return operationID, err
}

func (journal *Journal) After(state, operationID, operation, status, cause string, detail map[string]any) error {
	journal.mutex.Lock()
	defer journal.mutex.Unlock()
	return journal.appendLocked(Event{State: state, Phase: "after", OperationID: operationID, Operation: operation, Status: status, Cause: cause, Detail: detail})
}

func (journal *Journal) State(state, cause string) error {
	journal.mutex.Lock()
	defer journal.mutex.Unlock()
	return journal.appendLocked(Event{State: state, Phase: "state", OperationID: fmt.Sprintf("state-%06d", journal.sequence+1), Operation: "transition", Cause: cause})
}

func (journal *Journal) appendLocked(event Event) error {
	event.SchemaVersion = SchemaVersion
	event.Sequence = journal.sequence + 1
	event.Timestamp = time.Now().UTC().Format(time.RFC3339Nano)
	document, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode run journal event: %w", err)
	}
	if err := artifact.ValidatePublic(document, journal.secrets); err != nil {
		return fmt.Errorf("redact run journal event: %w", err)
	}
	document = append(document, '\n')
	for len(document) > 0 {
		written, writeErr := journal.file.Write(document)
		if writeErr != nil {
			return fmt.Errorf("append run journal event: %w", writeErr)
		}
		if written == 0 {
			return fmt.Errorf("append run journal event: %w", io.ErrShortWrite)
		}
		document = document[written:]
	}
	if err := journal.file.Sync(); err != nil {
		return fmt.Errorf("sync run journal event: %w", err)
	}
	journal.sequence = event.Sequence
	return nil
}

func (journal *Journal) Close() error {
	journal.mutex.Lock()
	defer journal.mutex.Unlock()
	if journal.file == nil {
		return nil
	}
	err := journal.file.Close()
	journal.file = nil
	if err != nil {
		return fmt.Errorf("close run journal: %w", err)
	}
	return nil
}

// Read accepts only a truncated final line as recoverable interruption evidence.
func Read(path string) ([]Event, bool, error) {
	document, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return []Event{}, false, nil
		}
		return nil, false, fmt.Errorf("read run journal: %w", err)
	}
	truncated := len(document) > 0 && document[len(document)-1] != '\n'
	lines := bytes.Split(document, []byte{'\n'})
	if len(lines) > 0 && len(lines[len(lines)-1]) == 0 {
		lines = lines[:len(lines)-1]
	}
	if truncated && len(lines) > 0 {
		lines = lines[:len(lines)-1]
	}
	events := make([]Event, 0, len(lines))
	for index, line := range lines {
		var event Event
		decoder := json.NewDecoder(bufio.NewReader(bytes.NewReader(line)))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&event); err != nil {
			return nil, truncated, fmt.Errorf("decode journal line %d: %w", index+1, err)
		}
		if event.SchemaVersion != SchemaVersion || event.Sequence != uint64(index+1) {
			return nil, truncated, fmt.Errorf("journal line %d has invalid schema or noncontiguous sequence", index+1)
		}
		events = append(events, event)
	}
	return events, truncated, nil
}

func IsComplete(events []Event, truncated bool) bool {
	return !truncated && len(events) > 0 && events[len(events)-1].Phase == "state" && events[len(events)-1].State == "COMPLETE"
}

func FinalState(events []Event, truncated bool) (string, bool) {
	if truncated || len(events) == 0 || events[len(events)-1].Phase != "state" {
		return "INTERRUPTED", false
	}
	state := events[len(events)-1].State
	switch state {
	case "COMPLETE", "INFRASTRUCTURE_ERROR", "TIMEOUT", "UNRESOLVED", "INTERRUPTED", "FLAKY":
		return state, true
	default:
		return "INTERRUPTED", false
	}
}
