package evidence

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	jsonschema "github.com/santhosh-tekuri/jsonschema/v6"
)

const canonicalCorpusSHA256 = "16751a3b097bd9b1849d658f350eeb4579be250f84d906a8232c4003ca5d3e5b"

var forbiddenTrackedExtensions = map[string]struct{}{
	".db": {}, ".gz": {}, ".key": {}, ".log": {}, ".ndjson": {}, ".pem": {}, ".sqlite": {}, ".tar": {}, ".tgz": {}, ".trace": {}, ".zip": {},
}

func CheckReleaseRepository(repository string) error {
	repository, err := filepath.Abs(repository)
	if err != nil {
		return err
	}
	corpusPath := filepath.Join(repository, "evidence", "corpus.json")
	corpus, err := LoadCorpus(corpusPath)
	if err != nil {
		return err
	}
	if digest, err := HashFile(corpusPath); err != nil || digest != canonicalCorpusSHA256 {
		return fmt.Errorf("canonical evidence corpus changed without a contract version")
	}
	if err := validateSchemaFile(repository, "schemas/evidence-corpus.schema.json", corpusPath); err != nil {
		return err
	}
	if err := checkTrackedArtifacts(repository); err != nil {
		return err
	}
	if err := checkWorkflowHistory(repository); err != nil {
		return err
	}
	if err := checkE2EAlias(repository); err != nil {
		return err
	}
	resultsRoot := filepath.Join(repository, "evidence", "results")
	if _, err := os.Stat(resultsRoot); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	return checkPublicEvidence(repository, corpus, resultsRoot)
}

func checkTrackedArtifacts(repository string) error {
	command := exec.Command("git", "ls-files", "-z", "--stage")
	command.Dir = repository
	document, err := command.Output()
	if err != nil {
		return fmt.Errorf("list tracked files: %w", err)
	}
	entries := bytes.Split(document, []byte{0})
	for _, entry := range entries {
		if len(entry) == 0 {
			continue
		}
		parts := bytes.SplitN(entry, []byte{'\t'}, 2)
		if len(parts) != 2 {
			return fmt.Errorf("malformed git index entry")
		}
		metadata := strings.Fields(string(parts[0]))
		path := filepath.ToSlash(string(parts[1]))
		if len(metadata) != 3 || metadata[0] != "100644" {
			return fmt.Errorf("tracked file %s has unexpected mode %s", path, metadata[0])
		}
		lower := strings.ToLower(path)
		for _, prefix := range []string{"bin/", "dist/", "run/", ".cache/", "examples/order-lifecycle/targets/generated/"} {
			if strings.HasPrefix(lower, prefix) {
				return fmt.Errorf("generated or private artifact is tracked: %s", path)
			}
		}
		if _, forbidden := forbiddenTrackedExtensions[strings.ToLower(filepath.Ext(path))]; forbidden {
			return fmt.Errorf("raw or restricted artifact is tracked: %s", path)
		}
		for _, name := range []string{"capture.json", ".env", "credentials", "secret", "token"} {
			if strings.Contains(lower, name) {
				return fmt.Errorf("private evidence or credential-like file is tracked: %s", path)
			}
		}
	}
	return nil
}

func checkWorkflowHistory(repository string) error {
	paths, err := filepath.Glob(filepath.Join(repository, ".github", "workflows", "*.y*ml"))
	if err != nil {
		return err
	}
	for _, path := range paths {
		document, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		text := string(document)
		checkoutCount := strings.Count(text, "uses: actions/checkout@")
		if checkoutCount == 0 || strings.Count(text, "fetch-depth: 0") != checkoutCount {
			return fmt.Errorf("workflow %s does not retain full source history for every checkout", filepath.Base(path))
		}
	}
	return nil
}

func checkE2EAlias(repository string) error {
	document, err := os.ReadFile(filepath.Join(repository, "Makefile"))
	if err != nil {
		return err
	}
	if !bytes.Contains(document, []byte("test-e2e: test-integration\n")) {
		return fmt.Errorf("test-e2e is no longer the exact test-integration alias")
	}
	return nil
}

func checkPublicEvidence(repository string, corpus Corpus, resultsRoot string) error {
	entries, err := os.ReadDir(resultsRoot)
	if err != nil {
		return err
	}
	want := map[string]SemanticCase{}
	for _, item := range corpus.SemanticCases {
		want[item.ID+".json"] = item
	}
	seen := map[string]struct{}{}
	for _, entry := range entries {
		if entry.IsDir() {
			return fmt.Errorf("public evidence contains unexpected directory %s", entry.Name())
		}
		if entry.Name() == "benchmark.json" {
			if err := checkPublicBenchmarkFile(repository, filepath.Join(resultsRoot, entry.Name())); err != nil {
				return err
			}
			seen[entry.Name()] = struct{}{}
			continue
		}
		item, exists := want[entry.Name()]
		if !exists {
			return fmt.Errorf("public evidence contains unexpected file %s", entry.Name())
		}
		if err := checkPublicCaseFile(repository, filepath.Join(resultsRoot, entry.Name()), item); err != nil {
			return err
		}
		seen[entry.Name()] = struct{}{}
	}
	if len(seen) != 15 {
		return fmt.Errorf("public evidence has %d records, want 15", len(seen))
	}
	for name := range want {
		if _, exists := seen[name]; !exists {
			return fmt.Errorf("public evidence omits %s", name)
		}
	}
	if _, exists := seen["benchmark.json"]; !exists {
		return fmt.Errorf("public evidence omits benchmark.json")
	}
	return nil
}

func checkPublicCaseFile(repository, path string, item SemanticCase) error {
	if err := validateSchemaFile(repository, "schemas/public-case-evidence.schema.json", path); err != nil {
		return err
	}
	value, err := loadStrict[PublicCaseEvidence](path)
	if err != nil {
		return err
	}
	if err := validatePublicCase(value, item); err != nil {
		return err
	}
	return checkHistoricalSource(repository, value.Source)
}

func checkPublicBenchmarkFile(repository, path string) error {
	if err := validateSchemaFile(repository, "schemas/public-benchmark-evidence.schema.json", path); err != nil {
		return err
	}
	value, err := loadStrict[PublicBenchmarkEvidence](path)
	if err != nil {
		return err
	}
	if err := validatePublicBenchmark(value); err != nil {
		return err
	}
	for _, comparison := range value.Comparisons {
		if err := checkHistoricalSource(repository, comparison.Source); err != nil {
			return err
		}
	}
	return nil
}

func checkHistoricalSource(repository string, source Source) error {
	if len(source.BinaryCommit) < 7 || !strings.HasPrefix(source.Commit, source.BinaryCommit) {
		return fmt.Errorf("public evidence binary commit does not identify its source commit")
	}
	tree, err := gitOutput(repository, "show", "-s", "--format=%T", source.Commit)
	if err != nil || strings.TrimSpace(tree) != source.Tree {
		return fmt.Errorf("public evidence source commit %s is absent or has the wrong tree", source.Commit)
	}
	for _, input := range source.Inputs {
		if strings.Contains(input.Path, "/generated/") {
			continue
		}
		command := exec.Command("git", "show", source.Commit+":"+input.Path)
		command.Dir = repository
		document, err := command.Output()
		if err != nil || hashBytes(document) != input.SHA256 {
			return fmt.Errorf("historical evidence input %s does not match %s", input.Path, source.Commit)
		}
	}
	return nil
}

func validateSchemaFile(repository, schemaPath, documentPath string) error {
	document, err := os.ReadFile(documentPath)
	if err != nil {
		return err
	}
	return validateSchemaDocument(repository, filepath.Base(schemaPath), document)
}

func validateSchemaDocument(repository, schemaName string, document []byte) error {
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	compiler.AssertFormat()
	compiler.UseLoader(denyEvidenceSchemaLoader{})
	for _, name := range []string{"evidence-corpus.schema.json", "evidence-capture.schema.json", "public-case-evidence.schema.json", "public-benchmark-evidence.schema.json"} {
		schemaDocument, err := os.ReadFile(filepath.Join(repository, "schemas", name))
		if err != nil {
			return err
		}
		var raw any
		if err := json.Unmarshal(schemaDocument, &raw); err != nil {
			return fmt.Errorf("decode schema %s: %w", name, err)
		}
		if err := compiler.AddResource("https://chronicle.dev/schemas/"+name, raw); err != nil {
			return err
		}
	}
	schema, err := compiler.Compile("https://chronicle.dev/schemas/" + schemaName)
	if err != nil {
		return err
	}
	var raw any
	decoder := json.NewDecoder(bytes.NewReader(document))
	if err := decoder.Decode(&raw); err != nil {
		return err
	}
	if err := requireJSONEOF(decoder); err != nil {
		return err
	}
	if err := schema.Validate(raw); err != nil {
		return fmt.Errorf("%s: %w", schemaName, err)
	}
	return nil
}

type denyEvidenceSchemaLoader struct{}

func (denyEvidenceSchemaLoader) Load(url string) (any, error) {
	return nil, fmt.Errorf("external evidence schema resource %q is forbidden", url)
}
