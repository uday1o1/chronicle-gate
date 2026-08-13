package imagelock

import "testing"

func TestValidatePublicationReference(t *testing.T) {
	t.Parallel()

	digest := "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	tests := []struct {
		name      string
		reference string
		wantError bool
	}{
		{name: "digest only", reference: "registry.example/repo@" + digest},
		{name: "tag and digest", reference: "registry.example/repo:v1@" + digest},
		{name: "tag only", reference: "registry.example/repo:v1", wantError: true},
		{name: "implicit latest", reference: "registry.example/repo", wantError: true},
		{name: "malformed digest", reference: "registry.example/repo@sha256:abc", wantError: true},
		{name: "unsupported algorithm", reference: "registry.example/repo@sha512:0123", wantError: true},
		{name: "uppercase digest", reference: "registry.example/repo@sha256:ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789", wantError: true},
		{name: "multiple separators", reference: "registry.example/repo@" + digest + "@" + digest, wantError: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := ValidatePublicationReference(test.reference)
			if (err != nil) != test.wantError {
				t.Fatalf("ValidatePublicationReference() error = %v, wantError %v", err, test.wantError)
			}
		})
	}
}
