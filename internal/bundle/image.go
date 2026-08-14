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
	"unicode"
	"unicode/utf8"

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

func saveCanonicalImage(ctx context.Context, docker *client.Client, reference string, remainingBudget int64) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	limit := min(maxImageArchiveBytes, remainingBudget)
	if limit <= 0 {
		return nil, fmt.Errorf("saved image %s exceeds bundle creation retained-input limit", reference)
	}
	stream, err := docker.ImageSave(ctx, []string{reference})
	if err != nil {
		return nil, fmt.Errorf("save image %s: %w", reference, err)
	}
	document, readErr := io.ReadAll(io.LimitReader(stream, limit+1))
	closeErr := stream.Close()
	if readErr != nil {
		return nil, fmt.Errorf("read saved image %s: %w", reference, readErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close saved image %s: %w", reference, closeErr)
	}
	if int64(len(document)) > limit {
		return nil, fmt.Errorf("saved image %s exceeds bundle creation retained-input limit", reference)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
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
	var totalExpanded int64
	for index, layer := range manifest.Layers {
		layerPath, err := validateDescriptor(entries, layer)
		if err != nil {
			return fmt.Errorf("image layer %d: %w", index, err)
		}
		expanded, err := validateDiffID(entries[layerPath], layer.MediaType, config.RootFS.DiffIDs[index])
		if err != nil {
			return fmt.Errorf("image layer %d: %w", index, err)
		}
		if totalExpanded > maxImageArchiveBytes-expanded {
			return fmt.Errorf("image layers exceed aggregate expanded-size limit")
		}
		totalExpanded += expanded
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

func validateDiffID(layer []byte, mediaType, expected string) (int64, error) {
	var reader io.Reader = bytes.NewReader(layer)
	var closer io.Closer
	if strings.Contains(mediaType, "gzip") {
		gzipReader, err := gzip.NewReader(reader)
		if err != nil {
			return 0, fmt.Errorf("open gzip layer: %w", err)
		}
		reader = gzipReader
		closer = gzipReader
	} else if strings.Contains(mediaType, "zstd") {
		return 0, fmt.Errorf("zstd layers are unsupported in development bundles")
	}
	hash := sha256.New()
	expanded, err := io.Copy(hash, io.LimitReader(reader, maxImageArchiveBytes+1))
	if err != nil {
		return 0, fmt.Errorf("hash expanded layer: %w", err)
	}
	if expanded > maxImageArchiveBytes {
		return 0, fmt.Errorf("expanded image layer exceeds %d bytes", maxImageArchiveBytes)
	}
	if compressionRatioExceeded(expanded, int64(len(layer))) {
		return 0, fmt.Errorf("expanded image layer exceeds compression-ratio limit")
	}
	if closer != nil {
		_ = closer.Close()
	}
	actual := "sha256:" + hex.EncodeToString(hash.Sum(nil))
	if actual != expected {
		return 0, fmt.Errorf("diff ID mismatch: got %s want %s", actual, expected)
	}
	return expanded, nil
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
	if err := rejectDuplicateJSONKeys(document); err != nil {
		return err
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
	if name == "" || !utf8.ValidString(name) || strings.ContainsAny(name, "\\:") || strings.HasPrefix(name, "/") || path.Clean(name) != name || name == "." || strings.HasPrefix(name, "../") {
		return "", fmt.Errorf("path %q is not normalized and relative", name)
	}
	for _, character := range name {
		if unicode.IsControl(character) {
			return "", fmt.Errorf("path %q contains a control character", name)
		}
	}
	return name, nil
}

func rejectDuplicateJSONKeys(document []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(document))
	type frame struct {
		object    bool
		expectKey bool
		keys      map[string]struct{}
	}
	stack := []frame{}
	for {
		token, err := decoder.Token()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		switch typed := token.(type) {
		case json.Delim:
			switch typed {
			case '{':
				stack = append(stack, frame{object: true, expectKey: true, keys: map[string]struct{}{}})
			case '[':
				stack = append(stack, frame{})
			case '}', ']':
				if len(stack) == 0 {
					return fmt.Errorf("JSON delimiter is unbalanced")
				}
				stack = stack[:len(stack)-1]
				if len(stack) > 0 && stack[len(stack)-1].object {
					stack[len(stack)-1].expectKey = true
				}
			}
		case string:
			if len(stack) > 0 && stack[len(stack)-1].object && stack[len(stack)-1].expectKey {
				current := &stack[len(stack)-1]
				if _, exists := current.keys[typed]; exists {
					return fmt.Errorf("JSON object contains duplicate key %q", typed)
				}
				current.keys[typed] = struct{}{}
				current.expectKey = false
			} else if len(stack) > 0 && stack[len(stack)-1].object {
				stack[len(stack)-1].expectKey = true
			}
		default:
			if len(stack) > 0 && stack[len(stack)-1].object && !stack[len(stack)-1].expectKey {
				stack[len(stack)-1].expectKey = true
			}
		}
	}
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
