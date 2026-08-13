package bundle

import (
	"archive/zip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/uday1o1/chronicle-gate/internal/spec"
)

func TestArchiveVerifiesAndExtracts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "reproduction.zip")
	writeTestBundle(t, path, false)
	archive, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = archive.Close() }()
	destination := filepath.Join(t.TempDir(), "extracted")
	if err := archive.Extract(destination); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(destination, "scenario", "scenario.json")); err != nil {
		t.Fatal(err)
	}
	if err := archive.Extract(destination); err == nil {
		t.Fatal("expected existing extraction destination rejection")
	}
}

func TestBundleCreationRejectsResolvedSecretBeforeDocker(t *testing.T) {
	root := t.TempDir()
	lock := filepath.Join(root, "images.lock.json")
	if err := os.WriteFile(lock, []byte(`{"schemaVersion":"test","images":[]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	target := spec.Target{APIVersion: spec.APIVersion, Kind: "Target", Spec: spec.TargetSpec{Services: []spec.Service{{Name: "service", Image: "example.invalid/service@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Environment: map[string]string{"CANARY": "canary-secret-value"}}}}}
	err := Create(context.Background(), CreateConfig{
		Path: filepath.Join(root, "bundle.zip"), RunID: "run-1", Scenario: spec.Scenario{}, ScenarioRoot: root,
		Baseline: target, Candidate: target, EnvironmentLock: lock,
		ExpectedSignature: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", SecretValues: []string{"canary-secret-value"},
	})
	if err == nil {
		t.Fatal("expected secret-bearing bundle rejection")
	}
	if _, statErr := os.Stat(filepath.Join(root, "bundle.zip")); !os.IsNotExist(statErr) {
		t.Fatalf("secret-bearing bundle exists: %v", statErr)
	}
}

func TestArchiveRejectsUnsafeEntryNames(t *testing.T) {
	for _, name := range []string{"/absolute", "../escape", "a//b", "a/./b", `a\b`} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "unsafe.zip")
			writeRawZIP(t, path, []rawEntry{{name: name, document: []byte("x"), mode: 0o600}})
			if archive, err := Open(path); err == nil {
				_ = archive.Close()
				t.Fatalf("accepted unsafe entry %q", name)
			}
		})
	}
}

func TestArchiveRejectsDuplicatesCaseCollisionsSymlinksAndCorruption(t *testing.T) {
	tests := []struct {
		name    string
		entries []rawEntry
	}{
		{"duplicate", []rawEntry{{"same", []byte("a"), 0o600}, {"same", []byte("b"), 0o600}}},
		{"case-collision", []rawEntry{{"Same", []byte("a"), 0o600}, {"same", []byte("b"), 0o600}}},
		{"symlink", []rawEntry{{"link", []byte("target"), os.ModeSymlink | 0o777}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "invalid.zip")
			writeRawZIP(t, path, test.entries)
			if archive, err := Open(path); err == nil {
				_ = archive.Close()
				t.Fatal("accepted invalid archive")
			}
		})
	}
	path := filepath.Join(t.TempDir(), "corrupt.zip")
	writeTestBundle(t, path, true)
	if archive, err := Open(path); err == nil {
		_ = archive.Close()
		t.Fatal("accepted checksum corruption")
	}
}

func TestArchiveRejectsFileCountSizeAndMalformedMetadata(t *testing.T) {
	countPath := filepath.Join(t.TempDir(), "many.zip")
	entries := make([]rawEntry, maxBundleFiles+1)
	for index := range entries {
		entries[index] = rawEntry{name: fmt.Sprintf("entry-%04d", index), document: []byte{}, mode: 0o600}
	}
	writeRawZIP(t, countPath, entries)
	if archive, err := Open(countPath); err == nil {
		_ = archive.Close()
		t.Fatal("accepted excessive file count")
	}
	sizePath := filepath.Join(t.TempDir(), "large.zip")
	file, err := os.Create(sizePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(maxBundleBytes + 1); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if archive, err := Open(sizePath); err == nil {
		_ = archive.Close()
		t.Fatal("accepted oversized archive")
	}
	malformedPath := filepath.Join(t.TempDir(), "malformed.zip")
	if err := os.WriteFile(malformedPath, []byte("not a zip"), 0o600); err != nil {
		t.Fatal(err)
	}
	if archive, err := Open(malformedPath); err == nil {
		_ = archive.Close()
		t.Fatal("accepted malformed ZIP metadata")
	}
}

func writeTestBundle(t *testing.T, path string, corrupt bool) {
	t.Helper()
	repository := filepath.Join("..", "..")
	scenario, err := spec.LoadScenario(filepath.Join(repository, "examples", "order-lifecycle", "scenarios", "r1-offset-rewind.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := spec.LoadTarget(filepath.Join(repository, "examples", "order-lifecycle", "targets", "baseline.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := spec.LoadTarget(filepath.Join(repository, "examples", "order-lifecycle", "targets", "candidate.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	entries := map[string][]byte{}
	for name, value := range map[string]any{"scenario/scenario.json": scenario, "targets/baseline.json": baseline, "targets/candidate.json": candidate} {
		document, marshalErr := json.Marshal(value)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		entries[name] = document
	}
	manifest := spec.Bundle{
		APIVersion: spec.APIVersion, Kind: "Bundle", RunID: "run-1", Scenario: "scenario/scenario.json",
		Targets: []string{"targets/baseline.json", "targets/candidate.json"},
		Images: []spec.BundleImage{
			{Name: "baseline/fulfillment-projector", Reference: baseline.Spec.Services[0].Image, Portable: true},
			{Name: "candidate/fulfillment-projector", Reference: candidate.Spec.Services[0].Image, Portable: true},
		},
		Resources:         spec.BundleResources{CPUs: 4, MemoryBytes: 6 << 30, DiskBytes: 10 << 30},
		Safety:            spec.BundleSafety{MaxFiles: 1000, MaxExpandedBytes: 1 << 30, SymlinksAllowed: false},
		ExpectedSignature: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Files:             []spec.BundleFile{},
	}
	for name, document := range entries {
		digest := sha256.Sum256(document)
		value := hex.EncodeToString(digest[:])
		if corrupt && name == "scenario/scenario.json" {
			value = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		}
		manifest.Files = append(manifest.Files, spec.BundleFile{Path: name, SHA256: value, Size: int64(len(document))})
	}
	manifestDocument, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	entries["bundle.json"] = manifestDocument
	if err := writeZIP(path, entries); err != nil {
		t.Fatal(err)
	}
}

type rawEntry struct {
	name     string
	document []byte
	mode     os.FileMode
}

func writeRawZIP(t *testing.T, path string, entries []rawEntry) {
	t.Helper()
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	writer := zip.NewWriter(file)
	for _, entry := range entries {
		header := &zip.FileHeader{Name: entry.name}
		header.SetMode(entry.mode)
		output, createErr := writer.CreateHeader(header)
		if createErr != nil {
			t.Fatal(createErr)
		}
		if _, writeErr := output.Write(entry.document); writeErr != nil {
			t.Fatal(writeErr)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}
