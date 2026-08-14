package spec

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"sync"

	jsonschema "github.com/santhosh-tekuri/jsonschema/v6"
)

//go:embed schemaassets/*.json
var embeddedSchemas embed.FS

var (
	schemasOnce   sync.Once
	schemasByKind map[string]*jsonschema.Schema
	schemasError  error
)

var schemaFiles = map[string]string{
	"Scenario":          "scenario.schema.json",
	"Target":            "target.schema.json",
	"Workload":          "workload.schema.json",
	"Result":            "result.schema.json",
	"Bundle":            "bundle.schema.json",
	"BenchmarkWorkload": "benchmark-workload.schema.json",
	"BenchmarkResult":   "benchmark-result.schema.json",
}

type denyLoader struct{}

func (denyLoader) Load(url string) (any, error) {
	return nil, fmt.Errorf("external schema resource %q is forbidden", url)
}

func compileSchemas() {
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	compiler.AssertFormat()
	compiler.UseLoader(denyLoader{})

	for _, name := range schemaFiles {
		document, err := fs.ReadFile(embeddedSchemas, "schemaassets/"+name)
		if err != nil {
			schemasError = fmt.Errorf("read embedded schema %s: %w", name, err)
			return
		}
		var value any
		if err := json.Unmarshal(document, &value); err != nil {
			schemasError = fmt.Errorf("decode embedded schema %s: %w", name, err)
			return
		}
		if err := compiler.AddResource(schemaURL(name), value); err != nil {
			schemasError = fmt.Errorf("add embedded schema %s: %w", name, err)
			return
		}
	}

	schemasByKind = make(map[string]*jsonschema.Schema, len(schemaFiles))
	for kind, name := range schemaFiles {
		schema, err := compiler.Compile(schemaURL(name))
		if err != nil {
			schemasError = fmt.Errorf("compile %s schema: %w", kind, err)
			return
		}
		schemasByKind[kind] = schema
	}
}

func schemaURL(name string) string {
	return "https://chronicle.dev/schemas/" + name
}

func validateSchema(kind string, value any) error {
	schemasOnce.Do(compileSchemas)
	if schemasError != nil {
		return schemasError
	}
	schema, exists := schemasByKind[kind]
	if !exists {
		return fmt.Errorf("no schema is registered for kind %q", kind)
	}
	if err := schema.Validate(value); err != nil {
		return fmt.Errorf("%s JSON Schema validation failed: %w", kind, err)
	}
	return nil
}

// EmbeddedSchema returns a defensive copy for parity tests and tooling.
func EmbeddedSchema(name string) ([]byte, error) {
	document, err := fs.ReadFile(embeddedSchemas, "schemaassets/"+name)
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), document...), nil
}
