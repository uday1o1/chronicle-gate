package artifact

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// PrepareDirectory creates an empty private output directory without deleting user data.
func PrepareDirectory(path string) error {
	parent := filepath.Dir(path)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return fmt.Errorf("create output parent directory: %w", err)
	}
	if err := os.Mkdir(path, 0o700); err == nil {
		return nil
	} else if !os.IsExist(err) {
		return fmt.Errorf("create output directory: %w", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("inspect output directory: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("output path %q is not a directory", path)
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return fmt.Errorf("read output directory: %w", err)
	}
	if len(entries) != 0 {
		return fmt.Errorf("output directory %q must be empty", path)
	}
	if err := os.Chmod(path, 0o700); err != nil {
		return fmt.Errorf("secure output directory: %w", err)
	}
	return nil
}

// WriteJSON atomically writes private JSON and fsyncs its containing directory.
func WriteJSON(path string, value any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create artifact parent: %w", err)
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".chronicle-*.tmp")
	if err != nil {
		return fmt.Errorf("create artifact temporary file: %w", err)
	}
	temporary := file.Name()
	succeeded := false
	defer func() {
		_ = file.Close()
		if !succeeded {
			_ = os.Remove(temporary)
		}
	}()
	if err := file.Chmod(0o600); err != nil {
		return fmt.Errorf("secure artifact temporary file: %w", err)
	}
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(value); err != nil {
		return fmt.Errorf("encode artifact: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync artifact: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close artifact: %w", err)
	}
	if err := os.Rename(temporary, path); err != nil {
		return fmt.Errorf("publish artifact: %w", err)
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return fmt.Errorf("open artifact directory: %w", err)
	}
	if err := directory.Sync(); err != nil {
		_ = directory.Close()
		return fmt.Errorf("sync artifact directory: %w", err)
	}
	if err := directory.Close(); err != nil {
		return fmt.Errorf("close artifact directory: %w", err)
	}
	succeeded = true
	return nil
}
