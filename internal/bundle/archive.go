// Package bundle creates and verifies safe ChronicleGate reproduction archives.
package bundle

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/moby/moby/client"
	"github.com/uday1o1/chronicle-gate/internal/artifact"
	"github.com/uday1o1/chronicle-gate/internal/imagelock"
	"github.com/uday1o1/chronicle-gate/internal/spec"
)

const (
	maxBundleFiles = 1000
	maxBundleBytes = int64(1 << 30)
)

type CreateConfig struct {
	Path              string
	RunID             string
	Scenario          spec.Scenario
	ScenarioRoot      string
	Baseline          spec.Target
	Candidate         spec.Target
	EnvironmentLock   string
	ExpectedSignature string
	SecretValues      []string
}

type Archive struct {
	file     *os.File
	reader   *zip.Reader
	entries  map[string]*zip.File
	Manifest spec.Bundle
	SHA256   string
}

func Create(ctx context.Context, config CreateConfig) error {
	entries := map[string][]byte{}
	addJSON := func(name string, value any) error {
		document, err := json.MarshalIndent(value, "", "  ")
		if err != nil {
			return err
		}
		entries[name] = append(document, '\n')
		return nil
	}
	if err := addJSON("scenario/scenario.json", config.Scenario); err != nil {
		return err
	}
	if err := addJSON("targets/baseline.json", config.Baseline); err != nil {
		return err
	}
	if err := addJSON("targets/candidate.json", config.Candidate); err != nil {
		return err
	}
	lock, err := os.ReadFile(config.EnvironmentLock)
	if err != nil {
		return fmt.Errorf("read environment lock for bundle: %w", err)
	}
	entries["environment.lock.json"] = lock
	if err := addScenarioClosure(entries, config.ScenarioRoot); err != nil {
		return err
	}

	manifest := spec.Bundle{
		APIVersion: spec.APIVersion, Kind: "Bundle", RunID: config.RunID,
		Scenario: "scenario/scenario.json", Targets: []string{"targets/baseline.json", "targets/candidate.json"},
		Images: []spec.BundleImage{}, Resources: spec.BundleResources{CPUs: 4, MemoryBytes: 6 << 30, DiskBytes: 10 << 30},
		Files: []spec.BundleFile{}, Safety: spec.BundleSafety{MaxFiles: maxBundleFiles, MaxExpandedBytes: maxBundleBytes, SymlinksAllowed: false},
		ExpectedSignature: config.ExpectedSignature,
	}
	if err := artifact.ValidatePublic(mustJSON(entries), config.SecretValues); err != nil {
		return err
	}

	docker, err := client.New(client.FromEnv)
	if err != nil {
		return fmt.Errorf("create Docker client for bundle: %w", err)
	}
	defer func() { _ = docker.Close() }()
	seenImages := map[string]string{}
	for _, target := range []struct {
		name  string
		value spec.Target
	}{{"baseline", config.Baseline}, {"candidate", config.Candidate}} {
		for _, service := range target.value.Spec.Services {
			image := spec.BundleImage{Name: target.name + "/" + service.Name, Reference: service.Image, Portable: !imagelock.IsLocalImageID(service.Image)}
			if !image.Portable {
				manifest.Nonportable = true
				archivePath, exists := seenImages[service.Image]
				if !exists {
					archivePath = "images/" + strings.TrimPrefix(service.Image, "sha256:") + ".tar"
					document, saveErr := saveCanonicalImage(ctx, docker, service.Image)
					if saveErr != nil {
						return saveErr
					}
					if err := verifyImageArchive(document, service.Image); err != nil {
						return fmt.Errorf("verify canonical image archive: %w", err)
					}
					entries[archivePath] = document
					seenImages[service.Image] = archivePath
				}
				image.Archive = archivePath
			}
			manifest.Images = append(manifest.Images, image)
		}
	}
	for name, document := range entries {
		digest := sha256.Sum256(document)
		manifest.Files = append(manifest.Files, spec.BundleFile{Path: name, SHA256: hex.EncodeToString(digest[:]), Size: int64(len(document))})
	}
	sort.Slice(manifest.Files, func(left, right int) bool { return manifest.Files[left].Path < manifest.Files[right].Path })
	if violations := spec.ValidateBundle(manifest); len(violations) != 0 {
		return fmt.Errorf("generated bundle manifest is invalid: %s", violations[0].Message)
	}
	manifestDocument, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	manifestDocument = append(manifestDocument, '\n')
	if err := artifact.ValidatePublic(manifestDocument, config.SecretValues); err != nil {
		return err
	}
	entries["bundle.json"] = manifestDocument
	return writeZIP(config.Path, entries)
}

func Open(path string) (*Archive, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open reproduction bundle: %w", err)
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > maxBundleBytes {
		_ = file.Close()
		return nil, fmt.Errorf("reproduction bundle is not a bounded regular file")
	}
	reader, err := zip.NewReader(file, info.Size())
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("open reproduction ZIP: %w", err)
	}
	archive := &Archive{file: file, reader: reader, entries: map[string]*zip.File{}}
	if err := archive.verify(); err != nil {
		_ = file.Close()
		return nil, err
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		_ = file.Close()
		return nil, err
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		_ = file.Close()
		return nil, err
	}
	archive.SHA256 = hex.EncodeToString(hash.Sum(nil))
	return archive, nil
}

func (archive *Archive) verify() error {
	if len(archive.reader.File) == 0 || len(archive.reader.File) > maxBundleFiles+1 {
		return fmt.Errorf("bundle file-count limit exceeded")
	}
	casePaths := map[string]string{}
	var expanded int64
	for _, file := range archive.reader.File {
		name, err := safePath(file.Name)
		if err != nil {
			return fmt.Errorf("unsafe bundle entry: %w", err)
		}
		if file.Flags&0x1 != 0 || !file.FileInfo().Mode().IsRegular() {
			return fmt.Errorf("bundle entry %q is encrypted or not a regular file", name)
		}
		if _, exists := archive.entries[name]; exists {
			return fmt.Errorf("duplicate bundle entry %q", name)
		}
		folded := strings.ToLower(name)
		if prior, exists := casePaths[folded]; exists {
			return fmt.Errorf("case-colliding bundle entries %q and %q", prior, name)
		}
		casePaths[folded] = name
		if file.UncompressedSize64 > uint64(maxBundleBytes) || expanded+int64(file.UncompressedSize64) > maxBundleBytes {
			return fmt.Errorf("bundle expanded-size limit exceeded")
		}
		expanded += int64(file.UncompressedSize64)
		archive.entries[name] = file
	}
	manifestDocument, err := archive.readEntry("bundle.json", spec.MaxContractBytes)
	if err != nil {
		return err
	}
	manifest, err := spec.DecodeBundleJSON(manifestDocument)
	if err != nil {
		return err
	}
	if violations := spec.ValidateBundle(manifest); len(violations) != 0 {
		return fmt.Errorf("bundle semantic validation failed: %s", violations[0].Message)
	}
	if manifest.Safety.MaxFiles > maxBundleFiles || manifest.Safety.MaxExpandedBytes > maxBundleBytes || manifest.Safety.SymlinksAllowed {
		return fmt.Errorf("bundle safety limits exceed ChronicleGate limits")
	}
	scenarioDocument, err := archive.readEntry(manifest.Scenario, spec.MaxContractBytes)
	if err != nil {
		return err
	}
	scenario, err := spec.DecodeScenarioJSON(scenarioDocument)
	if err != nil {
		return err
	}
	if len(manifest.Targets) != 2 {
		return fmt.Errorf("bundle must contain exactly two targets")
	}
	targets := make([]spec.Target, 2)
	for index, targetPath := range manifest.Targets {
		targetDocument, readErr := archive.readEntry(targetPath, spec.MaxContractBytes)
		if readErr != nil {
			return readErr
		}
		targets[index], readErr = spec.DecodeTargetJSON(targetDocument)
		if readErr != nil {
			return readErr
		}
	}
	options := spec.ValidationOptions{AllowLocalImageIDs: manifest.Nonportable}
	for _, target := range targets {
		if violations := spec.ValidateTargetWithOptions(target, options); len(violations) != 0 {
			return fmt.Errorf("bundled target is invalid: %s", violations[0].Message)
		}
	}
	if violations := spec.CompareTargets(targets[0], targets[1], scenario.Spec.Comparison.AllowedTargetDifferences); len(violations) != 0 {
		return fmt.Errorf("bundled targets are not comparable: %s", violations[0].Message)
	}
	expected := map[string]spec.BundleFile{}
	for _, file := range manifest.Files {
		expected[file.Path] = file
	}
	if len(archive.entries) != len(expected)+1 {
		return fmt.Errorf("bundle contains files absent from its manifest")
	}
	for path, record := range expected {
		file, exists := archive.entries[path]
		if !exists || int64(file.UncompressedSize64) != record.Size {
			return fmt.Errorf("bundle file %q is absent or has the wrong size", path)
		}
		document, err := archive.readEntry(path, manifest.Safety.MaxExpandedBytes)
		if err != nil {
			return err
		}
		digest := sha256.Sum256(document)
		if hex.EncodeToString(digest[:]) != record.SHA256 {
			return fmt.Errorf("bundle file %q checksum mismatch", path)
		}
	}
	for _, image := range manifest.Images {
		if !image.Portable {
			document, err := archive.readEntry(image.Archive, maxImageArchiveBytes)
			if err != nil {
				return err
			}
			if err := verifyImageArchive(document, image.Reference); err != nil {
				return fmt.Errorf("verify embedded image %q: %w", image.Name, err)
			}
		}
	}
	wantImages := map[string]struct{}{}
	for _, target := range targets {
		for _, service := range target.Spec.Services {
			wantImages[service.Image] = struct{}{}
		}
	}
	for image := range wantImages {
		found := false
		for _, record := range manifest.Images {
			if record.Reference == image {
				found = true
			}
		}
		if !found {
			return fmt.Errorf("target image %s is absent from bundle manifest", image)
		}
	}
	imageNames := map[string]struct{}{}
	for _, record := range manifest.Images {
		if _, exists := wantImages[record.Reference]; !exists {
			return fmt.Errorf("bundle declares unexpected image %s", record.Reference)
		}
		if _, exists := imageNames[record.Name]; exists {
			return fmt.Errorf("bundle declares duplicate image name %q", record.Name)
		}
		imageNames[record.Name] = struct{}{}
	}
	archive.Manifest = manifest
	return nil
}

func (archive *Archive) Extract(destination string) error {
	if info, err := os.Lstat(destination); err == nil || !errors.Is(err, os.ErrNotExist) {
		if err == nil && info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("bundle extraction destination is a symlink")
		}
		return fmt.Errorf("bundle extraction destination must not exist")
	}
	if err := os.Mkdir(destination, 0o700); err != nil {
		return fmt.Errorf("create bundle extraction directory: %w", err)
	}
	root, err := filepath.Abs(destination)
	if err != nil {
		return err
	}
	for _, record := range archive.Manifest.Files {
		destinationPath := filepath.Join(root, filepath.FromSlash(record.Path))
		if !strings.HasPrefix(destinationPath, root+string(filepath.Separator)) {
			return fmt.Errorf("bundle entry escapes extraction root")
		}
		if err := os.MkdirAll(filepath.Dir(destinationPath), 0o700); err != nil {
			return err
		}
		input, err := archive.entries[record.Path].Open()
		if err != nil {
			return err
		}
		output, err := os.OpenFile(destinationPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			_ = input.Close()
			return err
		}
		_, copyErr := io.Copy(output, io.LimitReader(input, record.Size+1))
		syncErr := output.Sync()
		closeOutputErr := output.Close()
		closeInputErr := input.Close()
		if err := errors.Join(copyErr, syncErr, closeOutputErr, closeInputErr); err != nil {
			return err
		}
	}
	return nil
}

func (archive *Archive) LoadImages(ctx context.Context) error {
	docker, err := client.New(client.FromEnv)
	if err != nil {
		return err
	}
	defer func() { _ = docker.Close() }()
	loaded := map[string]struct{}{}
	for _, image := range archive.Manifest.Images {
		if image.Portable {
			continue
		}
		if _, exists := loaded[image.Reference]; exists {
			continue
		}
		if inspected, inspectErr := docker.ImageInspect(ctx, image.Reference); inspectErr == nil && inspected.ID == image.Reference {
			loaded[image.Reference] = struct{}{}
			continue
		}
		input, err := archive.entries[image.Archive].Open()
		if err != nil {
			return err
		}
		response, err := docker.ImageLoad(ctx, input, client.ImageLoadWithQuiet(true))
		_ = input.Close()
		if err != nil {
			return fmt.Errorf("load embedded image %s: %w", image.Reference, err)
		}
		_, readErr := io.Copy(io.Discard, io.LimitReader(response, 1<<20))
		closeErr := response.Close()
		if err := errors.Join(readErr, closeErr); err != nil {
			return fmt.Errorf("read image load response: %w", err)
		}
		inspected, err := docker.ImageInspect(ctx, image.Reference)
		if err != nil || inspected.ID != image.Reference {
			return fmt.Errorf("loaded image identity does not match %s", image.Reference)
		}
		loaded[image.Reference] = struct{}{}
	}
	return nil
}

func (archive *Archive) Close() error {
	if archive.file == nil {
		return nil
	}
	err := archive.file.Close()
	archive.file = nil
	return err
}

func (archive *Archive) readEntry(name string, limit int64) ([]byte, error) {
	file, exists := archive.entries[name]
	if !exists {
		return nil, fmt.Errorf("bundle entry %q is absent", name)
	}
	input, err := file.Open()
	if err != nil {
		return nil, err
	}
	document, readErr := io.ReadAll(io.LimitReader(input, limit+1))
	closeErr := input.Close()
	if readErr != nil || closeErr != nil {
		return nil, errors.Join(readErr, closeErr)
	}
	if int64(len(document)) > limit {
		return nil, fmt.Errorf("bundle entry %q exceeds limit", name)
	}
	return document, nil
}

func addScenarioClosure(entries map[string][]byte, root string) error {
	return filepath.Walk(root, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == root {
			return nil
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("scenario closure contains symlink %q", path)
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if relative == "r1-offset-rewind.yaml" || relative == "r1-offset-rewind-noisy.yaml" {
			return nil
		}
		document, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		entries["scenario/"+filepath.ToSlash(relative)] = document
		return nil
	})
}

func writeZIP(path string, entries map[string][]byte) error {
	names := make([]string, 0, len(entries))
	for name := range entries {
		names = append(names, name)
	}
	sort.Strings(names)
	var output bytes.Buffer
	writer := zip.NewWriter(&output)
	for _, name := range names {
		header := &zip.FileHeader{Name: name, Method: zip.Deflate}
		header.SetMode(0o600)
		header.Modified = time.Unix(0, 0).UTC()
		file, err := writer.CreateHeader(header)
		if err != nil {
			return err
		}
		if _, err := file.Write(entries[name]); err != nil {
			return err
		}
	}
	if err := writer.Close(); err != nil {
		return err
	}
	return artifact.WriteFile(path, output.Bytes())
}

func mustJSON(entries map[string][]byte) []byte {
	value := make(map[string]string, len(entries))
	for name, document := range entries {
		value[name] = string(document)
	}
	document, _ := json.Marshal(value)
	return document
}
