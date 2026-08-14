package bundle

import (
	"archive/zip"
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func FuzzArchiveSafety(f *testing.F) {
	seedPath := filepath.Join(f.TempDir(), "seed.zip")
	writeTestBundle(f, seedPath, false)
	seed, err := os.ReadFile(seedPath)
	if err != nil {
		f.Fatal(err)
	}
	f.Add("scenario/scenario.json", seed)
	f.Add("../escape", []byte("not a zip"))
	f.Fuzz(func(t *testing.T, candidatePath string, document []byte) {
		if len(candidatePath) > 4096 || len(document) > 4<<20 {
			t.Skip()
		}
		if normalized, err := safePath(candidatePath); err == nil {
			root := t.TempDir()
			joined := filepath.Join(root, filepath.FromSlash(normalized))
			relative, err := filepath.Rel(root, joined)
			if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
				t.Fatalf("accepted path escapes extraction root: %q", candidatePath)
			}
		}
		reader, err := zip.NewReader(bytes.NewReader(document), int64(len(document)))
		if err != nil {
			return
		}
		archive := &Archive{reader: reader, entries: map[string]*zip.File{}}
		if err := archive.verify(); err != nil {
			return
		}
		destination := filepath.Join(t.TempDir(), "extract")
		if err := archive.Extract(destination); err != nil {
			t.Fatalf("verified archive did not extract: %v", err)
		}
		if err := filepath.Walk(destination, func(path string, _ os.FileInfo, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			relative, err := filepath.Rel(destination, path)
			if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
				t.Fatalf("extracted path escaped root: %q", path)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})
}
