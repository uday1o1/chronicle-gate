// Package registry records reproducible Redpanda Schema Registry history.
package registry

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	jsonschema "github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/uday1o1/chronicle-gate/internal/observe"
	"github.com/uday1o1/chronicle-gate/internal/spec"
)

const maxRegistryResponseBytes = 2 << 20

type Evidence struct {
	LogicalSubject  string          `json:"logicalSubject"`
	PhysicalSubject string          `json:"physicalSubject"`
	Compatibility   string          `json:"compatibility"`
	EffectiveMode   string          `json:"effectiveMode"`
	Endpoint        string          `json:"endpoint"`
	Versions        []Version       `json:"versions"`
	BundleMappings  []BundleMapping `json:"bundleMappings"`
}

type Version struct {
	SourceFile          string `json:"sourceFile"`
	SourceSHA256        string `json:"sourceSha256"`
	BundledSHA256       string `json:"bundledSha256"`
	Version             int    `json:"version"`
	ID                  int    `json:"id"`
	SchemaType          string `json:"schemaType"`
	PredecessorVersions []int  `json:"predecessorVersions"`
	CompatibilityChecks []bool `json:"compatibilityChecks"`
}

type BundleMapping struct {
	SourceFile   string `json:"sourceFile"`
	SourceSHA256 string `json:"sourceSha256"`
	Pointer      string `json:"pointer"`
}

type bundledSchema struct {
	SourceFile   string
	SourceSHA256 string
	Document     []byte
	SHA256       string
	Mappings     []BundleMapping
}

type client struct {
	endpoint string
	http     *http.Client
}

func RegisterEvent(ctx context.Context, endpoint, attemptPrefix, root string, event spec.CloudEvent) (Evidence, error) {
	if event.Registry == nil {
		return Evidence{}, errors.New("event has no Registry declaration")
	}
	declaration := event.Registry
	physical := attemptPrefix + "." + declaration.Subject
	api := &client{endpoint: strings.TrimRight(endpoint, "/"), http: &http.Client{
		Timeout:       5 * time.Second,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse },
	}}
	evidence := Evidence{
		LogicalSubject: declaration.Subject, PhysicalSubject: physical,
		Compatibility: declaration.Compatibility, Endpoint: api.endpoint,
		Versions: []Version{}, BundleMappings: []BundleMapping{},
	}
	if err := api.setCompatibility(ctx, physical, declaration.Compatibility); err != nil {
		return evidence, err
	}
	mode, err := api.compatibility(ctx, physical)
	if err != nil {
		return evidence, err
	}
	evidence.EffectiveMode = mode
	if mode != declaration.Compatibility {
		return evidence, fmt.Errorf("registry compatibility read-back is %q, want %q", mode, declaration.Compatibility)
	}
	paths := append(append([]string(nil), declaration.History...), event.DataSchema)
	for index, path := range paths {
		bundled, err := BundleSchema(root, path)
		if err != nil {
			return evidence, err
		}
		evidence.BundleMappings = append(evidence.BundleMappings, bundled.Mappings...)
		predecessors := []int{}
		checks := []bool{}
		if index > 0 {
			start := index - 1
			if strings.HasSuffix(declaration.Compatibility, "_TRANSITIVE") {
				start = 0
			}
			for prior := start; prior < index; prior++ {
				compatible, checkErr := api.checkCompatibility(ctx, physical, prior+1, bundled.Document)
				if checkErr != nil {
					return evidence, checkErr
				}
				predecessors = append(predecessors, prior+1)
				checks = append(checks, compatible)
				if !compatible {
					return evidence, fmt.Errorf("schema %q is incompatible with %s version %d", path, physical, prior+1)
				}
			}
		}
		id, err := api.register(ctx, physical, bundled.Document)
		if err != nil {
			return evidence, err
		}
		readBack, err := api.readVersion(ctx, physical, index+1)
		if err != nil {
			return evidence, err
		}
		if readBack.ID != id || readBack.Version != index+1 || readBack.SchemaType != "JSON" {
			return evidence, fmt.Errorf("registry version identity mismatch for %s version %d", physical, index+1)
		}
		readCanonical, err := canonicalSchema([]byte(readBack.Schema))
		if err != nil {
			return evidence, fmt.Errorf("canonicalize Registry read-back: %w", err)
		}
		readDigest := sha256.Sum256(readCanonical)
		if hex.EncodeToString(readDigest[:]) != bundled.SHA256 {
			return evidence, fmt.Errorf("registry schema hash mismatch for %s version %d", physical, index+1)
		}
		evidence.Versions = append(evidence.Versions, Version{
			SourceFile: path, SourceSHA256: bundled.SourceSHA256, BundledSHA256: bundled.SHA256,
			Version: index + 1, ID: id, SchemaType: "JSON",
			PredecessorVersions: predecessors, CompatibilityChecks: checks,
		})
	}
	return evidence, nil
}

// BundleSchema produces a deterministic self-contained schema for Registry storage.
func BundleSchema(root, relative string) (bundledSchema, error) {
	rootAbsolute, err := filepath.Abs(root)
	if err != nil {
		return bundledSchema{}, err
	}
	rootResolved, err := filepath.EvalSymlinks(rootAbsolute)
	if err != nil {
		return bundledSchema{}, err
	}
	path, err := confined(rootResolved, relative)
	if err != nil {
		return bundledSchema{}, err
	}
	source, err := os.ReadFile(path)
	if err != nil {
		return bundledSchema{}, err
	}
	sourceDigest := sha256.Sum256(source)
	var rootDocument any
	if err := decodeSchema(source, &rootDocument); err != nil {
		return bundledSchema{}, err
	}
	mappings := []BundleMapping{{SourceFile: relative, SourceSHA256: hex.EncodeToString(sourceDigest[:]), Pointer: "#"}}
	definitions := map[string]any{}
	cache := map[string]string{}
	rewritten, err := rewriteRefs(rootResolved, path, rootDocument, definitions, cache, &mappings)
	if err != nil {
		return bundledSchema{}, err
	}
	object, ok := rewritten.(map[string]any)
	if !ok {
		return bundledSchema{}, errors.New("root JSON Schema must be an object")
	}
	if len(definitions) != 0 {
		object["$defs"] = mergeDefinitions(object["$defs"], definitions)
	}
	document, err := observe.Canonical(object)
	if err != nil {
		return bundledSchema{}, err
	}
	if err := compileSelfContained(document); err != nil {
		return bundledSchema{}, err
	}
	digest := sha256.Sum256(document)
	return bundledSchema{
		SourceFile: relative, SourceSHA256: hex.EncodeToString(sourceDigest[:]),
		Document: document, SHA256: hex.EncodeToString(digest[:]), Mappings: mappings,
	}, nil
}

func rewriteRefs(root, currentPath string, value any, definitions map[string]any, cache map[string]string, mappings *[]BundleMapping) (any, error) {
	switch typed := value.(type) {
	case map[string]any:
		result := make(map[string]any, len(typed))
		for key, child := range typed {
			if key == "$ref" {
				reference, ok := child.(string)
				if !ok {
					return nil, errors.New("json schema $ref must be a string")
				}
				base, fragment, _ := strings.Cut(reference, "#")
				if base == "" {
					result[key] = child
					continue
				}
				parsed, err := url.Parse(base)
				if err != nil || parsed.Scheme != "" || parsed.Host != "" || filepath.IsAbs(base) {
					return nil, fmt.Errorf("external schema reference %q is forbidden", reference)
				}
				nestedRelative, err := filepath.Rel(root, filepath.Join(filepath.Dir(currentPath), filepath.FromSlash(base)))
				if err != nil {
					return nil, err
				}
				nestedPath, err := confined(root, nestedRelative)
				if err != nil {
					return nil, err
				}
				name, exists := cache[nestedPath]
				if !exists {
					document, err := os.ReadFile(nestedPath)
					if err != nil {
						return nil, err
					}
					digest := sha256.Sum256(document)
					name = "schema_" + hex.EncodeToString(digest[:8])
					cache[nestedPath] = name
					var nested any
					if err := decodeSchema(document, &nested); err != nil {
						return nil, err
					}
					rewritten, err := rewriteRefs(root, nestedPath, nested, definitions, cache, mappings)
					if err != nil {
						return nil, err
					}
					definitions[name] = rewritten
					relative, _ := filepath.Rel(root, nestedPath)
					*mappings = append(*mappings, BundleMapping{SourceFile: filepath.ToSlash(relative), SourceSHA256: hex.EncodeToString(digest[:]), Pointer: "#/$defs/" + name})
				}
				result[key] = "#/$defs/" + name
				if fragment != "" {
					result[key] = result[key].(string) + "/" + strings.TrimPrefix(fragment, "/")
				}
				continue
			}
			rewritten, err := rewriteRefs(root, currentPath, child, definitions, cache, mappings)
			if err != nil {
				return nil, err
			}
			result[key] = rewritten
		}
		return result, nil
	case []any:
		result := make([]any, len(typed))
		for index, child := range typed {
			rewritten, err := rewriteRefs(root, currentPath, child, definitions, cache, mappings)
			if err != nil {
				return nil, err
			}
			result[index] = rewritten
		}
		return result, nil
	default:
		return value, nil
	}
}

func mergeDefinitions(existing any, imported map[string]any) map[string]any {
	result := map[string]any{}
	if current, ok := existing.(map[string]any); ok {
		for key, value := range current {
			result[key] = value
		}
	}
	for key, value := range imported {
		result[key] = value
	}
	return result
}

type registryVersion struct {
	Subject    string `json:"subject"`
	Version    int    `json:"version"`
	ID         int    `json:"id"`
	SchemaType string `json:"schemaType"`
	Schema     string `json:"schema"`
	References []any  `json:"references,omitempty"`
}

func (api *client) setCompatibility(ctx context.Context, subject, mode string) error {
	return api.request(ctx, http.MethodPut, "/config/"+url.PathEscape(subject), map[string]any{"compatibility": mode}, nil)
}

func (api *client) compatibility(ctx context.Context, subject string) (string, error) {
	var response struct {
		Compatibility string `json:"compatibilityLevel"`
	}
	err := api.request(ctx, http.MethodGet, "/config/"+url.PathEscape(subject), nil, &response)
	return response.Compatibility, err
}

func (api *client) checkCompatibility(ctx context.Context, subject string, version int, schema []byte) (bool, error) {
	response := map[string]any{}
	err := api.request(ctx, http.MethodPost, fmt.Sprintf("/compatibility/subjects/%s/versions/%d", url.PathEscape(subject), version), schemaRequest(schema), &response)
	if err != nil {
		return false, err
	}
	compatible, ok := response["is_compatible"].(bool)
	if !ok {
		return false, errors.New("schema registry compatibility response omitted is_compatible")
	}
	return compatible, nil
}

func (api *client) register(ctx context.Context, subject string, schema []byte) (int, error) {
	response := map[string]any{}
	err := api.request(ctx, http.MethodPost, "/subjects/"+url.PathEscape(subject)+"/versions", schemaRequest(schema), &response)
	if err != nil {
		return 0, err
	}
	idNumber, ok := response["id"].(float64)
	if !ok {
		if number, numberOK := response["id"].(json.Number); numberOK {
			value, convertErr := number.Int64()
			return int(value), convertErr
		}
		return 0, errors.New("schema registry registration response omitted id")
	}
	return int(idNumber), nil
}

func (api *client) readVersion(ctx context.Context, subject string, version int) (registryVersion, error) {
	response := map[string]any{}
	err := api.request(ctx, http.MethodGet, fmt.Sprintf("/subjects/%s/versions/%d", url.PathEscape(subject), version), nil, &response)
	if err != nil {
		return registryVersion{}, err
	}
	readString := func(name string) (string, error) {
		value, ok := response[name].(string)
		if !ok {
			return "", fmt.Errorf("schema registry version response omitted %s", name)
		}
		return value, nil
	}
	readInt := func(name string) (int, error) {
		value, ok := response[name].(float64)
		if !ok || value != float64(int(value)) {
			return 0, fmt.Errorf("schema registry version response omitted integer %s", name)
		}
		return int(value), nil
	}
	readSubject, subjectErr := readString("subject")
	readVersion, versionErr := readInt("version")
	readID, idErr := readInt("id")
	readSchema, schemaErr := readString("schema")
	readType, typeErr := readString("schemaType")
	if err := errors.Join(subjectErr, versionErr, idErr, schemaErr, typeErr); err != nil {
		return registryVersion{}, err
	}
	return registryVersion{Subject: readSubject, Version: readVersion, ID: readID, Schema: readSchema, SchemaType: readType}, nil
}

func schemaRequest(schema []byte) map[string]any {
	return map[string]any{"schema": string(schema), "schemaType": "JSON"}
}

func (api *client) request(ctx context.Context, method, path string, body, destination any) error {
	var reader io.Reader
	if body != nil {
		document, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(document)
	}
	request, err := http.NewRequestWithContext(ctx, method, api.endpoint+path, reader)
	if err != nil {
		return err
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	}
	response, err := api.http.Do(request)
	if err != nil {
		return fmt.Errorf("schema registry %s %s: %w", method, path, err)
	}
	defer func() { _ = response.Body.Close() }()
	document, err := io.ReadAll(io.LimitReader(response.Body, maxRegistryResponseBytes+1))
	if err != nil {
		return err
	}
	if len(document) > maxRegistryResponseBytes {
		return errors.New("schema registry response exceeds limit")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("schema registry %s %s returned %d: %s", method, path, response.StatusCode, strings.TrimSpace(string(document)))
	}
	if destination != nil {
		if _, err := observe.DecodeStrictJSON(document); err != nil {
			return fmt.Errorf("decode Schema Registry response: %w", err)
		}
		decoder := json.NewDecoder(bytes.NewReader(document))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(destination); err != nil {
			return fmt.Errorf("decode Schema Registry response contract: %w", err)
		}
	}
	return nil
}

func compileSelfContained(document []byte) error {
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	compiler.AssertFormat()
	compiler.UseLoader(denyLoader{})
	var value any
	if err := json.Unmarshal(document, &value); err != nil {
		return err
	}
	if err := compiler.AddResource("https://chronicle.dev/generated/registry.json", value); err != nil {
		return err
	}
	if _, err := compiler.Compile("https://chronicle.dev/generated/registry.json"); err != nil {
		return fmt.Errorf("compile self-contained Registry schema: %w", err)
	}
	return nil
}

type denyLoader struct{}

func (denyLoader) Load(resource string) (any, error) {
	return nil, fmt.Errorf("generated Registry schema retained external resource %q", resource)
}

func canonicalSchema(document []byte) ([]byte, error) {
	var value any
	if err := decodeSchema(document, &value); err != nil {
		return nil, err
	}
	return observe.Canonical(value)
}

func decodeSchema(document []byte, destination *any) error {
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.UseNumber()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("schema contains trailing JSON")
	}
	return nil
}

func confined(root, relative string) (string, error) {
	if relative == "" || filepath.IsAbs(relative) || strings.Contains(relative, "://") {
		return "", errors.New("schema path must be a local relative path")
	}
	candidate := filepath.Join(root, filepath.Clean(relative))
	resolved, err := filepath.EvalSymlinks(candidate)
	if err != nil {
		return "", err
	}
	within, err := filepath.Rel(root, resolved)
	if err != nil || within == ".." || strings.HasPrefix(within, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("schema path %q escapes the root", relative)
	}
	return resolved, nil
}
