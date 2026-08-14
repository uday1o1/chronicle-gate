package bench

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type streamArtifact struct {
	mutex     sync.Mutex
	file      *os.File
	buffer    *bufio.Writer
	temporary string
	limit     int64
	written   int64
}

func newStreamArtifact(root, name string, limit int64) (*streamArtifact, error) {
	file, err := os.CreateTemp(root, "."+name+"-*.tmp")
	if err != nil {
		return nil, err
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		_ = os.Remove(file.Name())
		return nil, err
	}
	return &streamArtifact{file: file, buffer: bufio.NewWriterSize(file, 64<<10), temporary: file.Name(), limit: limit}, nil
}

func (artifact *streamArtifact) write(value any) error {
	artifact.mutex.Lock()
	defer artifact.mutex.Unlock()
	document, err := json.Marshal(value)
	if err != nil {
		return err
	}
	document = append(document, '\n')
	if int64(len(document)) > artifact.limit-artifact.written {
		return fmt.Errorf("stream artifact exceeds %d bytes", artifact.limit)
	}
	if _, err := artifact.buffer.Write(document); err != nil {
		return err
	}
	artifact.written += int64(len(document))
	return nil
}

func (artifact *streamArtifact) publish(root, name string) error {
	artifact.mutex.Lock()
	defer artifact.mutex.Unlock()
	if artifact.file == nil {
		return nil
	}
	flushErr := artifact.buffer.Flush()
	syncErr := artifact.file.Sync()
	closeErr := artifact.file.Close()
	artifact.file = nil
	if err := errors.Join(flushErr, syncErr, closeErr); err != nil {
		return err
	}
	if err := os.Rename(artifact.temporary, filepath.Join(root, name)); err != nil {
		return err
	}
	return syncDirectory(root)
}

func (artifact *streamArtifact) discard() {
	artifact.mutex.Lock()
	defer artifact.mutex.Unlock()
	if artifact.file != nil {
		_ = artifact.buffer.Flush()
		_ = artifact.file.Close()
		artifact.file = nil
	}
	_ = os.Remove(artifact.temporary)
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	return errors.Join(syncErr, closeErr)
}
