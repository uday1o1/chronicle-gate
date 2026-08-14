// Package bundle creates and verifies safe ChronicleGate reproduction archives.
package bundle

import (
	"archive/zip"
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
	maxBundleFiles           = 1000
	maxBundleBytes           = int64(1 << 30)
	maxBundleEntryBytes      = int64(512 << 20)
	maxCreationRetainedBytes = int64(256 << 20)
	maxCompressionRatio      = int64(200)
	compressionRatioFloor    = int64(1 << 20)
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
	saveImage         func(context.Context, string, int64) ([]byte, error)
}

type Archive struct {
	file     *os.File
	reader   *zip.Reader
	entries  map[string]*zip.File
	Manifest spec.Bundle
	SHA256   string
}

func Create(ctx context.Context, config CreateConfig) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := os.Lstat(config.Path); err == nil {
		return fmt.Errorf("reproduction bundle destination already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect reproduction bundle destination: %w", err)
	}
	entries := map[string][]byte{}
	casePaths := map[string]string{}
	var retained int64
	addEntry := func(name string, document []byte) error {
		name, err := safePath(name)
		if err != nil {
			return fmt.Errorf("unsafe bundle creation entry: %w", err)
		}
		if _, exists := entries[name]; exists {
			return fmt.Errorf("duplicate bundle creation entry %q", name)
		}
		folded := strings.ToLower(name)
		if prior, exists := casePaths[folded]; exists {
			return fmt.Errorf("case-colliding bundle creation entries %q and %q", prior, name)
		}
		if len(entries) >= maxBundleFiles || int64(len(document)) > maxBundleEntryBytes || retained > maxCreationRetainedBytes-int64(len(document)) {
			return fmt.Errorf("bundle creation retained-input limit exceeded")
		}
		if err := artifact.ValidatePublic(document, config.SecretValues); err != nil {
			return err
		}
		entries[name] = document
		casePaths[folded] = name
		retained += int64(len(document))
		return nil
	}
	addJSON := func(name string, value any) error {
		document, err := json.MarshalIndent(value, "", "  ")
		if err != nil {
			return err
		}
		return addEntry(name, append(document, '\n'))
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
	lock, err := readBoundedFile(config.EnvironmentLock, maxCreationRetainedBytes-retained)
	if err != nil {
		return fmt.Errorf("read environment lock for bundle: %w", err)
	}
	if err := addEntry("environment.lock.json", lock); err != nil {
		return err
	}
	if err := addScenarioClosure(config.ScenarioRoot, maxCreationRetainedBytes-retained, addEntry); err != nil {
		return err
	}

	manifest := spec.Bundle{
		APIVersion: spec.APIVersion, Kind: "Bundle", RunID: config.RunID,
		Scenario: "scenario/scenario.json", Targets: []string{"targets/baseline.json", "targets/candidate.json"},
		Images: []spec.BundleImage{}, Resources: spec.BundleResources{CPUs: 4, MemoryBytes: 6 << 30, DiskBytes: 10 << 30},
		Files: []spec.BundleFile{}, Safety: spec.BundleSafety{MaxFiles: maxBundleFiles, MaxExpandedBytes: maxBundleBytes, SymlinksAllowed: false},
		ExpectedSignature: config.ExpectedSignature,
	}
	var docker *client.Client
	defer func() {
		if docker != nil {
			_ = docker.Close()
		}
	}()
	seenImages := map[string]string{}
	for _, target := range []struct {
		name  string
		value spec.Target
	}{{"baseline", config.Baseline}, {"candidate", config.Candidate}} {
		for _, service := range target.value.Spec.Services {
			if err := ctx.Err(); err != nil {
				return err
			}
			image := spec.BundleImage{Name: target.name + "/" + service.Name, Reference: service.Image, Portable: !imagelock.IsLocalImageID(service.Image)}
			if !image.Portable {
				manifest.Nonportable = true
				archivePath, exists := seenImages[service.Image]
				if !exists {
					archivePath = "images/" + strings.TrimPrefix(service.Image, "sha256:") + ".tar"
					saver := config.saveImage
					if saver == nil {
						if docker == nil {
							docker, err = client.New(client.FromEnv)
							if err != nil {
								return fmt.Errorf("create Docker client for bundle: %w", err)
							}
						}
						saver = func(saveContext context.Context, reference string, limit int64) ([]byte, error) {
							return saveCanonicalImage(saveContext, docker, reference, limit)
						}
					}
					document, saveErr := saver(ctx, service.Image, maxCreationRetainedBytes-retained)
					if saveErr != nil {
						return saveErr
					}
					if err := verifyImageArchive(document, service.Image); err != nil {
						return fmt.Errorf("verify canonical image archive: %w", err)
					}
					if err := addEntry(archivePath, document); err != nil {
						return err
					}
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
	if err := addEntry("bundle.json", manifestDocument); err != nil {
		return err
	}
	return writeZIPContext(ctx, config.Path, entries)
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
	entries, err := validatedZIPEntries(archive.reader.File)
	if err != nil {
		return err
	}
	archive.entries = entries
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
		if int64(len(document)) != record.Size || uint64(len(document)) != file.UncompressedSize64 {
			return fmt.Errorf("bundle file %q decompressed size mismatch", path)
		}
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

func validatedZIPEntries(files []*zip.File) (map[string]*zip.File, error) {
	if len(files) == 0 || len(files) > maxBundleFiles+1 {
		return nil, fmt.Errorf("bundle file-count limit exceeded")
	}
	entries := make(map[string]*zip.File, len(files))
	casePaths := map[string]string{}
	var expanded int64
	for _, file := range files {
		name, err := safePath(file.Name)
		if err != nil {
			return nil, fmt.Errorf("unsafe bundle entry: %w", err)
		}
		if file.Flags&0x1 != 0 || !file.FileInfo().Mode().IsRegular() {
			return nil, fmt.Errorf("bundle entry %q is encrypted or not a regular file", name)
		}
		if _, exists := entries[name]; exists {
			return nil, fmt.Errorf("duplicate bundle entry %q", name)
		}
		folded := strings.ToLower(name)
		if prior, exists := casePaths[folded]; exists {
			return nil, fmt.Errorf("case-colliding bundle entries %q and %q", prior, name)
		}
		casePaths[folded] = name
		if file.UncompressedSize64 > uint64(maxBundleEntryBytes) || expanded > maxBundleBytes-int64(file.UncompressedSize64) {
			return nil, fmt.Errorf("bundle expanded-size limit exceeded")
		}
		if compressionRatioExceeded(int64(file.UncompressedSize64), int64(file.CompressedSize64)) {
			return nil, fmt.Errorf("bundle entry %q exceeds compression-ratio limit", name)
		}
		expanded += int64(file.UncompressedSize64)
		entries[name] = file
	}
	return entries, nil
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
		written, copyErr := io.Copy(output, io.LimitReader(input, record.Size+1))
		syncErr := output.Sync()
		closeOutputErr := output.Close()
		closeInputErr := input.Close()
		if err := errors.Join(copyErr, syncErr, closeOutputErr, closeInputErr); err != nil {
			return err
		}
		if written != record.Size {
			return fmt.Errorf("bundle entry %q extracted %d bytes, want %d", record.Path, written, record.Size)
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
	if uint64(len(document)) != file.UncompressedSize64 {
		return nil, fmt.Errorf("bundle entry %q decompressed size differs from its ZIP header", name)
	}
	return document, nil
}

func addScenarioClosure(root string, budget int64, add func(string, []byte) error) error {
	var retained int64
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
		if relative == "scenario.json" || relative == "r1-offset-rewind.yaml" || relative == "r1-offset-rewind-noisy.yaml" {
			return nil
		}
		remaining := budget - retained
		document, err := readBoundedFile(path, remaining)
		if err != nil {
			return err
		}
		if err := add("scenario/"+filepath.ToSlash(relative), document); err != nil {
			return err
		}
		retained += int64(len(document))
		return nil
	})
}

func writeZIP(path string, entries map[string][]byte) error {
	return writeZIPContext(context.Background(), path, entries)
}

func writeZIPContext(ctx context.Context, destination string, entries map[string][]byte) error {
	return writeZIPContextWithHook(ctx, destination, entries, nil)
}

func writeZIPContextWithHook(ctx context.Context, destination string, entries map[string][]byte, afterPublish func()) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := os.Lstat(destination); err == nil {
		return fmt.Errorf("reproduction bundle destination already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect reproduction bundle destination: %w", err)
	}
	names := make([]string, 0, len(entries))
	for name := range entries {
		if _, err := safePath(name); err != nil {
			return err
		}
		names = append(names, name)
	}
	sort.Strings(names)
	directory := filepath.Dir(destination)
	temporary, err := os.CreateTemp(directory, "."+filepath.Base(destination)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create private bundle temporary file: %w", err)
	}
	temporaryPath := temporary.Name()
	defer func() { _ = os.Remove(temporaryPath) }()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return err
	}
	writer := zip.NewWriter(temporary)
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			_ = writer.Close()
			_ = temporary.Close()
			return err
		}
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
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	identity, err := temporary.Stat()
	if err != nil {
		_ = temporary.Close()
		return err
	}
	reader, err := zip.NewReader(temporary, identity.Size())
	if err != nil {
		_ = temporary.Close()
		return fmt.Errorf("verify created reproduction ZIP: %w", err)
	}
	if _, err := validatedZIPEntries(reader.File); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("verify created reproduction ZIP: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.Link(temporaryPath, destination); err != nil {
		return fmt.Errorf("publish reproduction bundle without overwrite: %w", err)
	}
	if afterPublish != nil {
		afterPublish()
	}
	if err := syncDirectory(directory); err != nil {
		return cleanupPublishedBundle(destination, temporaryPath, identity, err)
	}
	if err := ctx.Err(); err != nil {
		return cleanupPublishedBundle(destination, temporaryPath, identity, err)
	}
	if err := os.Remove(temporaryPath); err != nil {
		return fmt.Errorf("remove published bundle temporary link: %w", err)
	}
	if err := syncDirectory(directory); err != nil {
		return err
	}
	return nil
}

func cleanupPublishedBundle(destination, temporary string, identity os.FileInfo, cause error) error {
	destinationInfo, statErr := os.Lstat(destination)
	if statErr != nil {
		return errors.Join(cause, fmt.Errorf("inspect canceled bundle publication: %w", statErr))
	}
	if !os.SameFile(identity, destinationInfo) {
		return errors.Join(cause, fmt.Errorf("canceled bundle destination identity changed; refusing deletion"))
	}
	removeDestinationErr := os.Remove(destination)
	removeTemporaryErr := os.Remove(temporary)
	syncErr := syncDirectory(filepath.Dir(destination))
	return errors.Join(cause, removeDestinationErr, removeTemporaryErr, syncErr)
}

func syncDirectory(directory string) error {
	file, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	return file.Sync()
}

func readBoundedFile(name string, limit int64) ([]byte, error) {
	if limit < 0 {
		return nil, fmt.Errorf("bundle creation retained-input limit exceeded")
	}
	file, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > limit {
		return nil, fmt.Errorf("bundle input %q is not a bounded regular file", name)
	}
	document, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(document)) > limit {
		return nil, fmt.Errorf("bundle input %q exceeds retained-input limit", name)
	}
	return document, nil

}

func compressionRatioExceeded(expanded, compressed int64) bool {
	return expanded >= compressionRatioFloor && (compressed <= 0 || expanded > compressed*maxCompressionRatio)
}
