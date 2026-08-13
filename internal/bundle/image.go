package bundle

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/moby/moby/client"
)

const maxImageArchiveBytes = 512 << 20

type descriptor struct {
	MediaType   string            `json:"mediaType"`
	Digest      string            `json:"digest"`
	Size        int64             `json:"size"`
	Annotations map[string]string `json:"annotations,omitempty"`
}

type imageIndex struct {
	SchemaVersion int          `json:"schemaVersion"`
	MediaType     string       `json:"mediaType"`
	Manifests     []descriptor `json:"manifests"`
}

type imageManifest struct {
	SchemaVersion int          `json:"schemaVersion"`
	MediaType     string       `json:"mediaType"`
	Config        descriptor   `json:"config"`
	Layers        []descriptor `json:"layers"`
}

type dockerManifest struct {
	Config   string   `json:"Config"`
	RepoTags []string `json:"RepoTags"`
	Layers   []string `json:"Layers"`
}

type imageConfig struct {
	RootFS struct {
		Type    string   `json:"type"`
		DiffIDs []string `json:"diff_ids"`
	} `json:"rootfs"`
}

func saveCanonicalImage(ctx context.Context, docker *client.Client, reference string) ([]byte, error) {
	stream, err := docker.ImageSave(ctx, []string{reference})
	if err != nil {
		return nil, fmt.Errorf("save image %s: %w", reference, err)
	}
	document, readErr := io.ReadAll(io.LimitReader(stream, maxImageArchiveBytes+1))
	closeErr := stream.Close()
	if readErr != nil {
		return nil, fmt.Errorf("read saved image %s: %w", reference, readErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close saved image %s: %w", reference, closeErr)
	}
	if len(document) > maxImageArchiveBytes {
		return nil, fmt.Errorf("saved image %s exceeds archive limit", reference)
	}
	entries, err := readImageTar(document)
	if err != nil {
		return nil, err
	}
	if err := validateImageEntries(entries, reference, true); err != nil {
		return nil, err
	}
	var index imageIndex
	_ = json.Unmarshal(entries["index.json"], &index)
	index.Manifests[0].Annotations = nil
	entries["index.json"], _ = json.Marshal(index)
	var compatibility []dockerManifest
	_ = json.Unmarshal(entries["manifest.json"], &compatibility)
	compatibility[0].RepoTags = nil
	entries["manifest.json"], _ = json.Marshal(compatibility)
	return writeImageTar(entries)
}

func verifyImageArchive(document []byte, reference string) error {
	if len(document) > maxImageArchiveBytes {
		return fmt.Errorf("image archive exceeds %d bytes", maxImageArchiveBytes)
	}
	entries, err := readImageTar(document)
	if err != nil {
		return err
	}
	return validateImageEntries(entries, reference, false)
}

func readImageTar(document []byte) (map[string][]byte, error) {
	reader := tar.NewReader(bytes.NewReader(document))
	entries := map[string][]byte{}
	var total int64
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read image tar: %w", err)
		}
		rawName := header.Name
		if header.Typeflag == tar.TypeDir {
			rawName = strings.TrimSuffix(rawName, "/")
		}
		name, err := safePath(rawName)
		if err != nil {
			return nil, fmt.Errorf("unsafe image tar entry: %w", err)
		}
		if header.Typeflag == tar.TypeDir {
			continue
		}
		if name != header.Name {
			return nil, fmt.Errorf("image tar regular entry %q is not normalized", header.Name)
		}
		if header.Typeflag != tar.TypeReg && header.Typeflag != 0 {
			return nil, fmt.Errorf("image tar entry %q is not a regular file", name)
		}
		if _, exists := entries[name]; exists {
			return nil, fmt.Errorf("duplicate image tar entry %q", name)
		}
		if header.Size < 0 || header.Size > maxImageArchiveBytes || total+header.Size > maxImageArchiveBytes {
			return nil, fmt.Errorf("image tar expanded-size limit exceeded")
		}
		value, err := io.ReadAll(io.LimitReader(reader, header.Size+1))
		if err != nil || int64(len(value)) != header.Size {
			return nil, fmt.Errorf("read image tar entry %q", name)
		}
		entries[name] = value
		total += header.Size
		if len(entries) > 256 {
			return nil, fmt.Errorf("image tar file-count limit exceeded")
		}
	}
	return entries, nil
}

func validateImageEntries(entries map[string][]byte, reference string, allowTags bool) error {
	if !strings.HasPrefix(reference, "sha256:") || len(reference) != 71 {
		return fmt.Errorf("local image reference %q is invalid", reference)
	}
	var index imageIndex
	if err := strictJSON(entries["index.json"], &index); err != nil || index.SchemaVersion != 2 || len(index.Manifests) != 1 {
		return fmt.Errorf("image index must contain exactly one manifest: %w", err)
	}
	manifestDescriptor := index.Manifests[0]
	if manifestDescriptor.Digest != reference {
		return fmt.Errorf("image index manifest %s does not match declared image %s", manifestDescriptor.Digest, reference)
	}
	manifestPath, err := digestPath(manifestDescriptor.Digest)
	if err != nil {
		return err
	}
	manifestDocument, exists := entries[manifestPath]
	if !exists || int64(len(manifestDocument)) != manifestDescriptor.Size || contentDigest(manifestDocument) != manifestDescriptor.Digest {
		return fmt.Errorf("image manifest descriptor content is invalid")
	}
	var manifest imageManifest
	if err := strictJSON(manifestDocument, &manifest); err != nil || manifest.SchemaVersion != 2 || len(manifest.Layers) == 0 {
		return fmt.Errorf("decode image manifest: %w", err)
	}
	referenced := map[string]struct{}{"index.json": {}, "manifest.json": {}, "oci-layout": {}, manifestPath: {}}
	configPath, err := validateDescriptor(entries, manifest.Config)
	if err != nil {
		return fmt.Errorf("image config: %w", err)
	}
	referenced[configPath] = struct{}{}
	var config imageConfig
	if err := json.Unmarshal(entries[configPath], &config); err != nil || config.RootFS.Type != "layers" || len(config.RootFS.DiffIDs) != len(manifest.Layers) {
		return fmt.Errorf("image config rootfs is invalid: %w", err)
	}
	layerPaths := make([]string, 0, len(manifest.Layers))
	for index, layer := range manifest.Layers {
		layerPath, err := validateDescriptor(entries, layer)
		if err != nil {
			return fmt.Errorf("image layer %d: %w", index, err)
		}
		if err := validateDiffID(entries[layerPath], layer.MediaType, config.RootFS.DiffIDs[index]); err != nil {
			return fmt.Errorf("image layer %d: %w", index, err)
		}
		referenced[layerPath] = struct{}{}
		layerPaths = append(layerPaths, layerPath)
	}
	var compatibility []dockerManifest
	if err := strictJSON(entries["manifest.json"], &compatibility); err != nil || len(compatibility) != 1 {
		return fmt.Errorf("docker compatibility manifest must contain exactly one image: %w", err)
	}
	if !allowTags && len(compatibility[0].RepoTags) != 0 {
		return fmt.Errorf("embedded image archive contains repository tags")
	}
	if compatibility[0].Config != configPath || !equalStrings(compatibility[0].Layers, layerPaths) {
		return fmt.Errorf("docker compatibility manifest disagrees with OCI manifest")
	}
	var layout struct {
		Version string `json:"imageLayoutVersion"`
	}
	if err := strictJSON(entries["oci-layout"], &layout); err != nil || layout.Version != "1.0.0" {
		return fmt.Errorf("OCI layout is invalid: %w", err)
	}
	if len(entries) != len(referenced) {
		return fmt.Errorf("image archive contains unreferenced files")
	}
	for name := range entries {
		if _, expected := referenced[name]; !expected {
			return fmt.Errorf("unexpected image archive entry %q", name)
		}
	}
	return nil
}

func validateDescriptor(entries map[string][]byte, value descriptor) (string, error) {
	path, err := digestPath(value.Digest)
	if err != nil {
		return "", err
	}
	document, exists := entries[path]
	if !exists || int64(len(document)) != value.Size || contentDigest(document) != value.Digest {
		return "", fmt.Errorf("descriptor %s is absent or corrupt", value.Digest)
	}
	return path, nil
}

func validateDiffID(layer []byte, mediaType, expected string) error {
	var reader io.Reader = bytes.NewReader(layer)
	var closer io.Closer
	if strings.Contains(mediaType, "gzip") {
		gzipReader, err := gzip.NewReader(reader)
		if err != nil {
			return fmt.Errorf("open gzip layer: %w", err)
		}
		reader = gzipReader
		closer = gzipReader
	} else if strings.Contains(mediaType, "zstd") {
		return fmt.Errorf("zstd layers are unsupported in development bundles")
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, io.LimitReader(reader, maxImageArchiveBytes+1)); err != nil {
		return fmt.Errorf("hash expanded layer: %w", err)
	}
	if closer != nil {
		_ = closer.Close()
	}
	actual := "sha256:" + hex.EncodeToString(hash.Sum(nil))
	if actual != expected {
		return fmt.Errorf("diff ID mismatch: got %s want %s", actual, expected)
	}
	return nil
}

func writeImageTar(entries map[string][]byte) ([]byte, error) {
	names := make([]string, 0, len(entries))
	for name := range entries {
		names = append(names, name)
	}
	sort.Strings(names)
	var output bytes.Buffer
	writer := tar.NewWriter(&output)
	for _, name := range names {
		document := entries[name]
		header := &tar.Header{Name: name, Mode: 0o600, Size: int64(len(document)), ModTime: time.Unix(0, 0).UTC(), Typeflag: tar.TypeReg, Format: tar.FormatPAX}
		if err := writer.WriteHeader(header); err != nil {
			return nil, err
		}
		if _, err := writer.Write(document); err != nil {
			return nil, err
		}
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}

func strictJSON(document []byte, destination any) error {
	if len(document) == 0 {
		return fmt.Errorf("required JSON entry is absent")
	}
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return fmt.Errorf("JSON entry has trailing content")
	}
	return nil
}

func digestPath(value string) (string, error) {
	if !strings.HasPrefix(value, "sha256:") || len(value) != 71 {
		return "", fmt.Errorf("unsupported content digest %q", value)
	}
	return "blobs/sha256/" + strings.TrimPrefix(value, "sha256:"), nil
}

func contentDigest(document []byte) string {
	digest := sha256.Sum256(document)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func safePath(name string) (string, error) {
	if name == "" || strings.Contains(name, "\\") || strings.HasPrefix(name, "/") || path.Clean(name) != name || name == "." || strings.HasPrefix(name, "../") {
		return "", fmt.Errorf("path %q is not normalized and relative", name)
	}
	return name, nil
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
