package imagelock

import (
	"strings"
	"testing"
)

const testDigestA = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
const testDigestB = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
const testDigestC = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"

func validLockJSON() string {
	return `{
  "schemaVersion":"chronicle.dev/images-lock/v1alpha1",
  "resolvedAt":"2026-08-13",
  "images":[{
    "name":"example",
    "role":"runtime",
    "source":"example.invalid/image:v1",
    "reference":"example.invalid/image@` + testDigestA + `",
    "indexDigest":"` + testDigestA + `",
    "platforms":{"linux/amd64":"` + testDigestB + `","linux/arm64":"` + testDigestC + `"},
    "reason":"test"
  }]
}`
}

func TestDecodeLock(t *testing.T) {
	t.Parallel()

	lock, err := Decode(strings.NewReader(validLockJSON()))
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if got, want := len(lock.Images), 1; got != want {
		t.Fatalf("len(images) = %d, want %d", got, want)
	}
}

func TestDecodeRejectsUnknownField(t *testing.T) {
	t.Parallel()

	document := strings.Replace(validLockJSON(), `"reason":"test"`, `"reason":"test","unexpected":true`, 1)
	if _, err := Decode(strings.NewReader(document)); err == nil {
		t.Fatal("Decode() accepted an unknown field")
	}
}

func TestVerifyIndexManifest(t *testing.T) {
	t.Parallel()

	lock, err := Decode(strings.NewReader(validLockJSON()))
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	document := `{"manifests":[
  {"digest":"` + testDigestB + `","platform":{"os":"linux","architecture":"x86_64"}},
  {"digest":"` + testDigestC + `","platform":{"os":"linux","architecture":"aarch64"}},
  {"digest":"sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd","platform":{"os":"unknown","architecture":"unknown"}}
]}`
	if err := VerifyIndexManifest(lock.Images[0], []byte(document)); err != nil {
		t.Fatalf("VerifyIndexManifest() error = %v", err)
	}
}

func TestVerifyIndexManifestRejectsWrongChild(t *testing.T) {
	t.Parallel()

	lock, err := Decode(strings.NewReader(validLockJSON()))
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	document := `{"manifests":[
  {"digest":"` + testDigestC + `","platform":{"os":"linux","architecture":"amd64"}},
  {"digest":"` + testDigestC + `","platform":{"os":"linux","architecture":"arm64"}}
]}`
	if err := VerifyIndexManifest(lock.Images[0], []byte(document)); err == nil {
		t.Fatal("VerifyIndexManifest() accepted a mismatched child digest")
	}
}
