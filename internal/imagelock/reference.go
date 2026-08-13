package imagelock

import (
	"fmt"
	"regexp"
	"strings"
)

var sha256DigestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// ParseImmutableReference validates an OCI reference used in publication mode.
// A source tag may be retained before the digest, but a digest is mandatory.
func ParseImmutableReference(reference string) (repository string, digest string, err error) {
	if strings.Count(reference, "@") != 1 {
		return "", "", fmt.Errorf("image reference must contain exactly one digest separator")
	}

	repository, digest, _ = strings.Cut(reference, "@")
	if strings.TrimSpace(repository) == "" || strings.ContainsAny(repository, " \t\r\n") {
		return "", "", fmt.Errorf("image repository is invalid")
	}
	if !sha256DigestPattern.MatchString(digest) {
		return "", "", fmt.Errorf("image digest must use sha256 with 64 lowercase hexadecimal characters")
	}

	return repository, digest, nil
}

// ValidatePublicationReference rejects mutable or malformed image references.
func ValidatePublicationReference(reference string) error {
	_, _, err := ParseImmutableReference(reference)
	return err
}
