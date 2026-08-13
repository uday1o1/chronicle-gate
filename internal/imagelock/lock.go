package imagelock

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
)

const SchemaVersion = "chronicle.dev/images-lock/v1alpha1"

var requiredPlatforms = []string{"linux/amd64", "linux/arm64"}

// Lock is the checked-in immutable OCI image inventory.
type Lock struct {
	Schema        string  `json:"$schema,omitempty"`
	SchemaVersion string  `json:"schemaVersion"`
	ResolvedAt    string  `json:"resolvedAt"`
	Images        []Image `json:"images"`
}

// Image binds a provenance tag to one immutable OCI index and its runtime manifests.
type Image struct {
	Name        string            `json:"name"`
	Role        string            `json:"role"`
	Source      string            `json:"source"`
	Reference   string            `json:"reference"`
	IndexDigest string            `json:"indexDigest"`
	Platforms   map[string]string `json:"platforms"`
	Reason      string            `json:"reason"`
}

// Load reads and strictly decodes an image lock.
func Load(path string) (Lock, error) {
	document, err := os.ReadFile(path)
	if err != nil {
		return Lock{}, fmt.Errorf("open image lock: %w", err)
	}
	return Decode(bytes.NewReader(document))
}

// Decode strictly decodes and validates an image lock.
func Decode(reader io.Reader) (Lock, error) {
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()

	var lock Lock
	if err := decoder.Decode(&lock); err != nil {
		return Lock{}, fmt.Errorf("decode image lock: %w", err)
	}
	if err := ensureEOF(decoder); err != nil {
		return Lock{}, err
	}
	if err := lock.Validate(); err != nil {
		return Lock{}, err
	}

	return lock, nil
}

func ensureEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("decode image lock: multiple JSON values")
		}
		return fmt.Errorf("decode image lock trailing data: %w", err)
	}
	return nil
}

// Validate enforces the publication lock invariants.
func (lock Lock) Validate() error {
	if lock.SchemaVersion != SchemaVersion {
		return fmt.Errorf("unsupported image lock schema version %q", lock.SchemaVersion)
	}
	if lock.ResolvedAt == "" {
		return fmt.Errorf("image lock resolvedAt is required")
	}
	if len(lock.Images) == 0 {
		return fmt.Errorf("image lock must contain at least one image")
	}

	seen := make(map[string]struct{}, len(lock.Images))
	for index, image := range lock.Images {
		if image.Name == "" {
			return fmt.Errorf("image %d has no name", index)
		}
		if _, exists := seen[image.Name]; exists {
			return fmt.Errorf("duplicate image name %q", image.Name)
		}
		seen[image.Name] = struct{}{}

		if image.Role != "runtime" && image.Role != "toolchain" {
			return fmt.Errorf("image %q has unsupported role %q", image.Name, image.Role)
		}
		if image.Source == "" || image.Reason == "" {
			return fmt.Errorf("image %q requires source and reason provenance", image.Name)
		}
		_, digest, err := ParseImmutableReference(image.Reference)
		if err != nil {
			return fmt.Errorf("image %q reference: %w", image.Name, err)
		}
		if digest != image.IndexDigest {
			return fmt.Errorf("image %q reference digest does not match indexDigest", image.Name)
		}

		for _, platform := range requiredPlatforms {
			platformDigest, exists := image.Platforms[platform]
			if !exists {
				return fmt.Errorf("image %q is missing required platform %q", image.Name, platform)
			}
			if !sha256DigestPattern.MatchString(platformDigest) {
				return fmt.Errorf("image %q platform %q has invalid digest", image.Name, platform)
			}
		}
		if len(image.Platforms) != len(requiredPlatforms) {
			platforms := make([]string, 0, len(image.Platforms))
			for platform := range image.Platforms {
				platforms = append(platforms, platform)
			}
			sort.Strings(platforms)
			return fmt.Errorf("image %q has unsupported platform set %v", image.Name, platforms)
		}
	}

	return nil
}

// RequiredPlatforms returns a defensive copy in stable display order.
func RequiredPlatforms() []string {
	return append([]string(nil), requiredPlatforms...)
}
