package imagelock

import (
	"encoding/json"
	"fmt"
)

type manifestIndex struct {
	Manifests []manifestDescriptor `json:"manifests"`
}

type manifestDescriptor struct {
	Digest   string   `json:"digest"`
	Platform platform `json:"platform"`
}

type platform struct {
	OS           string `json:"os"`
	Architecture string `json:"architecture"`
}

// VerifyIndexManifest proves that an inspected immutable index contains the exact locked runtime children.
func VerifyIndexManifest(image Image, document []byte) error {
	var index manifestIndex
	if err := json.Unmarshal(document, &index); err != nil {
		return fmt.Errorf("decode OCI index: %w", err)
	}
	if len(index.Manifests) == 0 {
		return fmt.Errorf("OCI document is not a multi-platform index")
	}

	found := make(map[string]string, len(requiredPlatforms))
	for _, descriptor := range index.Manifests {
		platformName := descriptor.Platform.OS + "/" + normalizeArchitecture(descriptor.Platform.Architecture)
		if _, required := image.Platforms[platformName]; !required {
			continue
		}
		if _, duplicate := found[platformName]; duplicate {
			return fmt.Errorf("OCI index has duplicate runtime descriptor for %s", platformName)
		}
		found[platformName] = descriptor.Digest
	}

	for _, platformName := range requiredPlatforms {
		actual, exists := found[platformName]
		if !exists {
			return fmt.Errorf("OCI index is missing %s", platformName)
		}
		if actual != image.Platforms[platformName] {
			return fmt.Errorf("OCI index child for %s is %s, expected %s", platformName, actual, image.Platforms[platformName])
		}
	}

	return nil
}

func normalizeArchitecture(architecture string) string {
	switch architecture {
	case "aarch64":
		return "arm64"
	case "x86_64":
		return "amd64"
	default:
		return architecture
	}
}
