package evidence

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/uday1o1/chronicle-gate/internal/runlog"
)

func TestCanonicalCorpusAndSchemas(t *testing.T) {
	repository := repositoryRoot(t)
	corpusPath := filepath.Join(repository, "evidence", "corpus.json")
	corpus, err := LoadCorpus(corpusPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(corpus.SemanticCases) != 14 || len(corpus.BenchmarkCases) != 2 {
		t.Fatalf("corpus inventory = %d semantic, %d benchmark", len(corpus.SemanticCases), len(corpus.BenchmarkCases))
	}
	if err := validateSchemaFile(repository, "schemas/evidence-corpus.schema.json", corpusPath); err != nil {
		t.Fatal(err)
	}
	if err := CheckReleaseRepository(repository); err != nil {
		t.Fatal(err)
	}
}

func TestCorpusRejectsMissingDuplicateAndExtraCases(t *testing.T) {
	corpus, err := LoadCorpus(filepath.Join(repositoryRoot(t), "evidence", "corpus.json"))
	if err != nil {
		t.Fatal(err)
	}
	missing := corpus
	missing.SemanticCases = append([]SemanticCase(nil), corpus.SemanticCases[:13]...)
	if err := ValidateCorpus(missing); err == nil {
		t.Fatal("missing semantic case was accepted")
	}
	duplicate := corpus
	duplicate.SemanticCases = append([]SemanticCase(nil), corpus.SemanticCases...)
	duplicate.SemanticCases[1] = duplicate.SemanticCases[0]
	if err := ValidateCorpus(duplicate); err == nil {
		t.Fatal("duplicate semantic case was accepted")
	}
	extra := corpus
	extra.BenchmarkCases = append(append([]BenchmarkCase(nil), corpus.BenchmarkCases...), corpus.BenchmarkCases[0])
	if err := ValidateCorpus(extra); err == nil {
		t.Fatal("extra benchmark case was accepted")
	}
}

func TestStrictLoaderRejectsUnknownFieldAndOversize(t *testing.T) {
	directory := t.TempDir()
	unknown := filepath.Join(directory, "unknown.json")
	if err := os.WriteFile(unknown, []byte(`{"schemaVersion":"chronicle.dev/evidence-corpus/v1alpha1","semanticCases":[],"benchmarkCases":[],"extra":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadCorpus(unknown); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("unknown field error = %v", err)
	}
	oversize := filepath.Join(directory, "oversize.json")
	if err := os.WriteFile(oversize, make([]byte, maxEvidenceDocumentBytes+1), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadCorpus(oversize); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("oversize error = %v", err)
	}
}

func TestCaptureContractSeparatesSemanticAndBenchmarkArtifacts(t *testing.T) {
	semantic := validCapture("SemanticCapture")
	if err := ValidateCapture(semantic); err != nil {
		t.Fatal(err)
	}
	benchmark := validCapture("BenchmarkCapture")
	benchmark.Artifacts.JournalSHA256 = nil
	if err := ValidateCapture(benchmark); err != nil {
		t.Fatal(err)
	}
	benchmark.Artifacts.JournalSHA256 = stringPointer(strings.Repeat("b", 64))
	if err := ValidateCapture(benchmark); err == nil {
		t.Fatal("benchmark capture accepted a semantic journal")
	}
	semantic.Artifacts.JournalSHA256 = nil
	if err := ValidateCapture(semantic); err == nil {
		t.Fatal("semantic capture omitted its journal")
	}
}

func TestCaptureAndPublicSchemasCompileAndValidate(t *testing.T) {
	repository := repositoryRoot(t)
	semantic := validCapture("SemanticCapture")
	document, err := json.Marshal(semantic)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateSchemaDocument(repository, "evidence-capture.schema.json", document); err != nil {
		t.Fatal(err)
	}
	control := PublicCaseEvidence{
		SchemaVersion: PublicCaseSchemaVersion, Kind: "PublicCaseEvidence", CaseID: "r1-single-delivery-control", ClaimID: "R1-control", Role: "control",
		CapturedAt: semantic.CapturedAt, Source: semantic.Source,
		Outcome: PublicOutcome{
			ExitCode: 0, State: "COMPLETE", Classification: "PASS", FailureSignature: json.RawMessage("null"),
			BaselineAttempts: 1, CandidateAttempts: 1, Observations: []ObservationSummary{},
		},
		Reduction: PublicReduction{Status: "skipped", Minimality: "not_applicable"},
		Artifacts: CaptureArtifacts{ResultSHA256: strings.Repeat("1", 64), ChecksumsSHA256: strings.Repeat("2", 64), JournalSHA256: stringPointer(strings.Repeat("3", 64))},
		Boundary:  evidenceBoundary,
	}
	document, err = json.Marshal(control)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateSchemaDocument(repository, "public-case-evidence.schema.json", document); err != nil {
		t.Fatal(err)
	}
	benchmarkSource := semantic.Source
	benchmarkArtifacts := CaptureArtifacts{ResultSHA256: strings.Repeat("1", 64), ChecksumsSHA256: strings.Repeat("2", 64)}
	benchmark := PublicBenchmarkEvidence{
		SchemaVersion: PublicBenchmarkSchemaVersion, Kind: "PublicBenchmarkEvidence", Boundary: evidenceBoundary,
		Comparisons: []PublicBenchmarkEntry{
			{CaseID: "benchmark-aa", CapturedAt: semantic.CapturedAt, Source: benchmarkSource, Artifacts: benchmarkArtifacts, Outcome: BenchmarkOutcome{ExitCode: 0, State: "COMPLETE", Classification: "PASS", Rounds: 4, BaselineRequests: 1, CandidateRequests: 1, BaselineP95Nanos: 1, CandidateP95Nanos: 1}},
			{CaseID: "benchmark-slowdown", CapturedAt: semantic.CapturedAt, Source: benchmarkSource, Artifacts: benchmarkArtifacts, Outcome: BenchmarkOutcome{ExitCode: 2, State: "COMPLETE", Classification: "PERFORMANCE_REGRESSION", Rounds: 4, BaselineRequests: 1, CandidateRequests: 1, BaselineP95Nanos: 1, CandidateP95Nanos: 2, Regression: true}},
		},
	}
	document, err = json.Marshal(benchmark)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateSchemaDocument(repository, "public-benchmark-evidence.schema.json", document); err != nil {
		t.Fatal(err)
	}
}

func TestCaptureRejectsUnsortedInputsAndNonzeroCleanup(t *testing.T) {
	capture := validCapture("SemanticCapture")
	capture.Source.Inputs = []InputFile{{Path: "z", SHA256: strings.Repeat("a", 64)}, {Path: "a", SHA256: strings.Repeat("b", 64)}}
	if err := ValidateCapture(capture); err == nil {
		t.Fatal("capture accepted unsorted inputs")
	}
	capture = validCapture("SemanticCapture")
	capture.Cleanup.Containers = 1
	if err := ValidateCapture(capture); err == nil {
		t.Fatal("capture accepted leaked resources")
	}
}

func TestDirtySourceAndSourceIdentityFailClosed(t *testing.T) {
	repository := initializeRepository(t)
	if err := os.MkdirAll(filepath.Join(repository, "bin"), 0o700); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(repository, "bin", "chronicle")
	commit := strings.TrimSpace(runGit(t, repository, "rev-parse", "HEAD"))
	tree := strings.TrimSpace(runGit(t, repository, "rev-parse", "HEAD^{tree}"))
	binaryDocument := "#!/bin/sh\nprintf '%s\\n' '{\"schemaVersion\":\"chronicle.dev/version/v1alpha1\",\"version\":\"dev\",\"commit\":\"" + commit[:7] + "\",\"buildDate\":\"2026-08-14T00:00:00Z\"}'\n"
	if err := os.WriteFile(binary, []byte(binaryDocument), 0o700); err != nil {
		t.Fatal(err)
	}
	binaryHash, err := HashFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	arguments := []string{"run", "--out", "run/capture/raw"}
	capture := validCapture("SemanticCapture")
	capture.Source.Commit = commit
	capture.Source.Tree = tree
	capture.Source.BinarySHA256 = binaryHash
	capture.Source.BinaryCommit = commit[:7]
	capture.Source.Argv = append([]string{"bin/chronicle"}, arguments...)
	if err := verifySource(repository, "run/capture", capture, arguments); err != nil {
		t.Fatal(err)
	}
	capture.Source.Tree = strings.Repeat("f", len(tree))
	if err := verifySource(repository, "run/capture", capture, arguments); err == nil {
		t.Fatal("source tree mismatch was accepted")
	}
	if err := os.WriteFile(filepath.Join(repository, "untracked.go"), []byte("package untracked\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := requireCleanSource(repository); err == nil {
		t.Fatal("dirty source tree was accepted")
	}
}

func TestJournalRunOwnershipRequiresOneConsistentRunID(t *testing.T) {
	events := []runlog.Event{{Detail: map[string]any{"runId": "run-1"}}, {Detail: map[string]any{}}}
	if !journalOwnsRun(events, "run-1") {
		t.Fatal("consistent run ownership was rejected")
	}
	events = append(events, runlog.Event{Detail: map[string]any{"runId": "run-2"}})
	if journalOwnsRun(events, "run-1") {
		t.Fatal("mixed run ownership was accepted")
	}
	if journalOwnsRun([]runlog.Event{{Detail: map[string]any{}}}, "run-1") {
		t.Fatal("journal without a run ID was accepted")
	}
}

func TestPublicEvidenceControlAndRegressionContracts(t *testing.T) {
	corpus, err := LoadCorpus(filepath.Join(repositoryRoot(t), "evidence", "corpus.json"))
	if err != nil {
		t.Fatal(err)
	}
	control, _ := FindSemanticCase(corpus, "r1-single-delivery-control")
	public := PublicCaseEvidence{
		SchemaVersion: PublicCaseSchemaVersion, Kind: "PublicCaseEvidence", CaseID: control.ID, ClaimID: control.ClaimID, Role: control.Role,
		Outcome: PublicOutcome{ExitCode: 0, State: "COMPLETE", Classification: "PASS", FailureSignature: json.RawMessage("null")},
	}
	if err := validatePublicCase(public, control); err != nil {
		t.Fatal(err)
	}
	public.Artifacts.BundleSHA256 = stringPointer(strings.Repeat("a", 64))
	if err := validatePublicCase(public, control); err == nil {
		t.Fatal("control accepted a bundle claim")
	}
	regression, _ := FindSemanticCase(corpus, "r1-offset-rewind")
	public = PublicCaseEvidence{
		SchemaVersion: PublicCaseSchemaVersion, Kind: "PublicCaseEvidence", CaseID: regression.ID, ClaimID: regression.ClaimID, Role: regression.Role,
		Outcome:   PublicOutcome{ExitCode: 2, State: "COMPLETE", Classification: regression.ExpectedClassification, FailureSignature: json.RawMessage(`{"digest":"x"}`), Confirmations: 2},
		Artifacts: CaptureArtifacts{BundleSHA256: stringPointer(strings.Repeat("a", 64))},
	}
	if err := validatePublicCase(public, regression); err != nil {
		t.Fatal(err)
	}
	public.Outcome.Confirmations = 1
	if err := validatePublicCase(public, regression); err == nil {
		t.Fatal("regression accepted one confirmation")
	}
}

func TestSanitizedPublisherRejectsSecretsPrivatePathsAndOverwrite(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "public.json")
	if err := writeSanitizedNewJSON(path, map[string]string{"value": "postgres://user:password@host/db"}); err == nil {
		t.Fatal("publisher accepted a database credential")
	}
	if err := writeSanitizedNewJSON(path, map[string]string{"value": "/Users/example/private"}); err == nil {
		t.Fatal("publisher accepted a private host path")
	}
	value := map[string]string{"value": "safe"}
	if err := writeSanitizedNewJSON(path, value); err != nil {
		t.Fatal(err)
	}
	if err := writeSanitizedNewJSON(path, value); err == nil {
		t.Fatal("publisher overwrote public evidence")
	}
}

func TestInputInventoryIsDeterministic(t *testing.T) {
	repository := t.TempDir()
	for name, value := range map[string]string{"z.json": "z", "a.json": "a"} {
		if err := os.WriteFile(filepath.Join(repository, name), []byte(value), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	first, err := inputInventory(repository, []string{"z.json", "a.json", "z.json"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := inputInventory(repository, []string{"a.json", "z.json"})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) || first[0].Path != "a.json" {
		t.Fatalf("input inventory is not deterministic: %#v %#v", first, second)
	}
}

func TestJSONEquivalenceIgnoresFormattingButNotValues(t *testing.T) {
	compact := []byte(`{"value":1,"nested":{"ok":true}}`)
	indented := []byte("{\n  \"nested\": {\"ok\": true},\n  \"value\": 1\n}\n")
	if !jsonEquivalent(compact, indented) {
		t.Fatal("equivalent JSON formatting was rejected")
	}
	if jsonEquivalent(compact, []byte(`{"value":2,"nested":{"ok":true}}`)) {
		t.Fatal("different JSON values were accepted")
	}
}

func validCapture(kind string) Capture {
	journal := strings.Repeat("b", 64)
	return Capture{
		SchemaVersion: CaptureSchemaVersion, Kind: kind, CaseID: "case-1", CapturedAt: "2026-08-14T00:00:00Z",
		Source: Source{
			Commit: strings.Repeat("a", 40), Tree: strings.Repeat("b", 40), BinarySHA256: strings.Repeat("c", 64), BinaryCommit: strings.Repeat("a", 7),
			Argv: []string{"bin/chronicle", "run"}, Inputs: []InputFile{{Path: "input.json", SHA256: strings.Repeat("d", 64)}},
		},
		RawDirectory: "raw",
		Execution: Execution{
			ExitCode: 2, StdoutSHA256: strings.Repeat("e", 64), StderrSHA256: strings.Repeat("f", 64),
			StdoutBytes: 10, StderrBytes: 0, State: "COMPLETE", Classification: "SEMANTIC_REGRESSION", RunID: "run-1", Signature: json.RawMessage(`{"digest":"x"}`),
		},
		Artifacts: CaptureArtifacts{ResultSHA256: strings.Repeat("1", 64), ChecksumsSHA256: strings.Repeat("2", 64), JournalSHA256: &journal},
	}
}

func initializeRepository(t *testing.T) string {
	t.Helper()
	repository := t.TempDir()
	runGit(t, repository, "init")
	runGit(t, repository, "config", "user.name", "ChronicleGate Tests")
	runGit(t, repository, "config", "user.email", "tests@chronicle.invalid")
	if err := os.WriteFile(filepath.Join(repository, "tracked.txt"), []byte("tracked\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, repository, "add", "tracked.txt")
	runGit(t, repository, "commit", "-m", "test fixture")
	return repository
}

func runGit(t *testing.T, repository string, arguments ...string) string {
	t.Helper()
	command := exec.Command("git", arguments...)
	command.Dir = repository
	document, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v: %s", arguments, err, document)
	}
	return string(document)
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func stringPointer(value string) *string {
	return &value
}
