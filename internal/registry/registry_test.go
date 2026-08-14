package registry

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBundleSchemaImportsLocalReferencesDeterministically(t *testing.T) {
	root := t.TempDir()
	write := func(name, document string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(root, name), []byte(document), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("shared.json", `{"type":"string","minLength":1}`)
	write("root.json", `{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","properties":{"id":{"$ref":"shared.json"}}}`)
	first, err := BundleSchema(root, "root.json")
	if err != nil {
		t.Fatal(err)
	}
	second, err := BundleSchema(root, "root.json")
	if err != nil {
		t.Fatal(err)
	}
	if first.SHA256 != second.SHA256 || len(first.Mappings) != 2 || strings.Contains(string(first.Document), `shared.json`) {
		t.Fatalf("bundle is incomplete or unstable: %#v %s", first, first.Document)
	}
	var decoded map[string]any
	if err := json.Unmarshal(first.Document, &decoded); err != nil || decoded["$defs"] == nil {
		t.Fatalf("generated schema is invalid: %v", err)
	}
}

func TestBundleSchemaRejectsExternalReferences(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "root.json"), []byte(`{"$ref":"https://example.invalid/schema"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := BundleSchema(root, "root.json"); err == nil {
		t.Fatal("external reference was accepted")
	}
}
