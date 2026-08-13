package artifact

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPrepareDirectoryRefusesExistingData(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	if err := os.WriteFile(filepath.Join(directory, "owned.txt"), []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := PrepareDirectory(directory); err == nil {
		t.Fatal("PrepareDirectory() accepted a nonempty directory")
	}
}

func TestWritePublicJSONRejectsResolvedSecret(t *testing.T) {
	path := filepath.Join(t.TempDir(), "result.json")
	if err := WritePublicJSON(path, map[string]string{"message": "canary-secret-value"}, []string{"canary-secret-value"}); err == nil {
		t.Fatal("expected resolved secret rejection")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("secret artifact exists: %v", err)
	}
}

func TestWriteJSONUsesPrivateMode(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "nested", "result.json")
	if err := WriteJSON(path, map[string]string{"status": "ok"}); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("artifact mode = %o", info.Mode().Perm())
	}
}

func TestPrepareDirectoryCreatesPrivateParents(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "new-parent", "run")
	if err := PrepareDirectory(path); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() || info.Mode().Perm() != 0o700 {
		t.Fatalf("output info = mode %v", info.Mode())
	}
}
