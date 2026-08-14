package evidence

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/uday1o1/chronicle-gate/internal/artifact"
	"github.com/uday1o1/chronicle-gate/internal/bench"
	"github.com/uday1o1/chronicle-gate/internal/bundle"
	"github.com/uday1o1/chronicle-gate/internal/runlog"
	"github.com/uday1o1/chronicle-gate/internal/spec"
)

const maxCommandOutputBytes = 8 << 20

type CaptureOptions struct {
	Repository string
	Binary     string
	Corpus     string
	CaseID     string
	Output     string
}

type boundedBuffer struct {
	buffer bytes.Buffer
	limit  int
	total  int
}

func (writer *boundedBuffer) Write(document []byte) (int, error) {
	written := len(document)
	writer.total += written
	remaining := writer.limit - writer.buffer.Len()
	if remaining > 0 {
		if len(document) > remaining {
			document = document[:remaining]
		}
		_, _ = writer.buffer.Write(document)
	}
	if writer.total > writer.limit {
		return written, fmt.Errorf("command output exceeds %d bytes", writer.limit)
	}
	return written, nil
}

func CaptureSemantic(ctx context.Context, options CaptureOptions) (Capture, error) {
	repository, binary, corpusPath, output, err := prepareCapture(options)
	if err != nil {
		return Capture{}, err
	}
	corpus, err := LoadCorpus(filepath.Join(repository, filepath.FromSlash(corpusPath)))
	if err != nil {
		return Capture{}, err
	}
	if err := validateSchemaFile(repository, "schemas/evidence-corpus.schema.json", filepath.Join(repository, filepath.FromSlash(corpusPath))); err != nil {
		return Capture{}, err
	}
	item, err := FindSemanticCase(corpus, options.CaseID)
	if err != nil {
		return Capture{}, err
	}
	inputs, err := SemanticInputs(repository, corpusPath, item)
	if err != nil {
		return Capture{}, err
	}
	arguments := []string{"run", "--scenario", item.Scenario, "--baseline", item.Baseline, "--candidate", item.Candidate, "--out", filepath.ToSlash(filepath.Join(output, "raw")), "--development-local-images", "--json"}
	if !item.Minimize {
		arguments = append(arguments, "--no-minimize")
	}
	capture, stdout, stderr, err := executeCapture(ctx, repository, binary, output, item.ID, "SemanticCapture", inputs, arguments)
	if err != nil {
		return Capture{}, err
	}
	resultPath := filepath.Join(repository, filepath.FromSlash(output), "raw", "result.json")
	result, err := spec.LoadResult(resultPath)
	if err != nil {
		return Capture{}, err
	}
	if violations := spec.ValidateResult(result); len(violations) != 0 {
		return Capture{}, fmt.Errorf("result contract violations: %#v", violations)
	}
	if _, err := spec.DecodeResultJSON(stdout); err != nil {
		return Capture{}, fmt.Errorf("validate command stdout: %w", err)
	}
	resultDocument, err := os.ReadFile(resultPath)
	if err != nil {
		return Capture{}, err
	}
	if !jsonEquivalent(resultDocument, stdout) {
		return Capture{}, fmt.Errorf("command stdout differs from result.json")
	}
	if len(bytes.TrimSpace(stderr)) != 0 {
		return Capture{}, fmt.Errorf("machine-readable command wrote stderr")
	}
	if capture.Execution.ExitCode != item.ExpectedExit || result.State != "COMPLETE" || result.Classification != item.ExpectedClassification {
		return Capture{}, fmt.Errorf("case %s exit/state/classification = %d/%s/%s", item.ID, capture.Execution.ExitCode, result.State, result.Classification)
	}
	expectedSignature, err := expectedSignature(repository, item.ExpectedSignature)
	if err != nil {
		return Capture{}, err
	}
	if !reflect.DeepEqual(result.FailureSignature, expectedSignature) {
		return Capture{}, fmt.Errorf("case %s failure signature changed", item.ID)
	}
	signature, err := json.Marshal(result.FailureSignature)
	if err != nil {
		return Capture{}, err
	}
	capture.Execution.State = result.State
	capture.Execution.Classification = result.Classification
	capture.Execution.RunID = result.RunID
	capture.Execution.Signature = signature
	rawRoot := filepath.Dir(resultPath)
	if err := artifact.VerifyChecksums(rawRoot); err != nil {
		return Capture{}, err
	}
	events, truncated, err := runlog.Read(filepath.Join(rawRoot, "events.ndjson"))
	if err != nil {
		return Capture{}, err
	}
	state, terminal := runlog.FinalState(events, truncated)
	if !terminal || state != result.State || !journalOwnsRun(events, result.RunID) {
		return Capture{}, fmt.Errorf("journal does not authoritatively complete run %s", result.RunID)
	}
	journalHash, err := HashFile(filepath.Join(rawRoot, "events.ndjson"))
	if err != nil {
		return Capture{}, err
	}
	capture.Artifacts.JournalSHA256 = &journalHash
	if item.ExpectedSignature != nil {
		archive, err := bundle.Open(filepath.Join(rawRoot, "reproduction.zip"))
		if err != nil {
			return Capture{}, err
		}
		bundleHash := archive.SHA256
		if err := archive.Close(); err != nil {
			return Capture{}, err
		}
		capture.Artifacts.BundleSHA256 = &bundleHash
	} else if result.Bundle != "" {
		return Capture{}, fmt.Errorf("control %s unexpectedly produced a reproduction bundle", item.ID)
	}
	if err := finishCapture(repository, output, &capture, "result.json"); err != nil {
		return Capture{}, err
	}
	return capture, nil
}

func CaptureBenchmark(ctx context.Context, options CaptureOptions) (Capture, error) {
	repository, binary, corpusPath, output, err := prepareCapture(options)
	if err != nil {
		return Capture{}, err
	}
	corpus, err := LoadCorpus(filepath.Join(repository, filepath.FromSlash(corpusPath)))
	if err != nil {
		return Capture{}, err
	}
	if err := validateSchemaFile(repository, "schemas/evidence-corpus.schema.json", filepath.Join(repository, filepath.FromSlash(corpusPath))); err != nil {
		return Capture{}, err
	}
	item, err := FindBenchmarkCase(corpus, options.CaseID)
	if err != nil {
		return Capture{}, err
	}
	inputs, err := BenchmarkInputs(repository, corpusPath, item)
	if err != nil {
		return Capture{}, err
	}
	arguments := []string{"bench", "--workload", item.Workload, "--baseline", item.Baseline, "--candidate", item.Candidate, "--out", filepath.ToSlash(filepath.Join(output, "raw")), "--development-local-images", "--json"}
	capture, stdout, stderr, err := executeCapture(ctx, repository, binary, output, item.ID, "BenchmarkCapture", inputs, arguments)
	if err != nil {
		return Capture{}, err
	}
	if len(bytes.TrimSpace(stderr)) != 0 {
		return Capture{}, fmt.Errorf("machine-readable benchmark wrote stderr")
	}
	var report bench.Report
	decoder := json.NewDecoder(bytes.NewReader(stdout))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&report); err != nil {
		return Capture{}, fmt.Errorf("decode benchmark stdout: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return Capture{}, err
	}
	resultPath := filepath.Join(repository, filepath.FromSlash(output), "raw", "benchmark.json")
	document, err := os.ReadFile(resultPath)
	if err != nil {
		return Capture{}, err
	}
	if err := spec.ValidateBenchmarkResultJSON(document); err != nil {
		return Capture{}, err
	}
	var stored bench.Report
	decoder = json.NewDecoder(bytes.NewReader(document))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&stored); err != nil {
		return Capture{}, err
	}
	if !reflect.DeepEqual(report, stored) {
		return Capture{}, fmt.Errorf("benchmark stdout differs from benchmark.json")
	}
	if capture.Execution.ExitCode != item.ExpectedExit || report.State != "COMPLETE" || report.Classification != item.ExpectedClassification {
		return Capture{}, fmt.Errorf("benchmark %s exit/state/classification = %d/%s/%s", item.ID, capture.Execution.ExitCode, report.State, report.Classification)
	}
	capture.Execution.State = report.State
	capture.Execution.Classification = report.Classification
	capture.Execution.RunID = report.RunID
	capture.Execution.Signature = json.RawMessage("null")
	if err := artifact.VerifyChecksums(filepath.Dir(resultPath)); err != nil {
		return Capture{}, err
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(resultPath), "events.ndjson")); !os.IsNotExist(err) {
		return Capture{}, fmt.Errorf("benchmark output contains a semantic journal")
	}
	if err := finishCapture(repository, output, &capture, "benchmark.json"); err != nil {
		return Capture{}, err
	}
	return capture, nil
}

func prepareCapture(options CaptureOptions) (string, string, string, string, error) {
	repository, err := filepath.Abs(options.Repository)
	if err != nil {
		return "", "", "", "", err
	}
	for label, path := range map[string]string{"binary": options.Binary, "corpus": options.Corpus, "output": options.Output} {
		if !safeRepositoryPath(filepath.ToSlash(path)) {
			return "", "", "", "", fmt.Errorf("%s path %q must be repository-relative", label, path)
		}
	}
	if filepath.ToSlash(filepath.Clean(options.Binary)) != "bin/chronicle" || filepath.ToSlash(filepath.Clean(options.Corpus)) != "evidence/corpus.json" {
		return "", "", "", "", fmt.Errorf("capture requires the canonical binary and corpus paths")
	}
	if err := requireCleanSource(repository); err != nil {
		return "", "", "", "", err
	}
	output := filepath.ToSlash(filepath.Clean(options.Output))
	if !strings.HasPrefix(output, "run/") {
		return "", "", "", "", fmt.Errorf("private capture output must be under run/")
	}
	if err := artifact.PrepareDirectory(filepath.Join(repository, filepath.FromSlash(output))); err != nil {
		return "", "", "", "", err
	}
	return repository, filepath.ToSlash(filepath.Clean(options.Binary)), filepath.ToSlash(filepath.Clean(options.Corpus)), output, nil
}

func executeCapture(ctx context.Context, repository, binary, output, caseID, kind string, inputs []InputFile, arguments []string) (Capture, []byte, []byte, error) {
	commit, err := gitOutput(repository, "rev-parse", "HEAD")
	if err != nil {
		return Capture{}, nil, nil, err
	}
	tree, err := gitOutput(repository, "rev-parse", "HEAD^{tree}")
	if err != nil {
		return Capture{}, nil, nil, err
	}
	binaryHash, err := HashFile(filepath.Join(repository, filepath.FromSlash(binary)))
	if err != nil {
		return Capture{}, nil, nil, fmt.Errorf("hash capture binary: %w", err)
	}
	binaryCommit, err := binaryVersionCommit(repository, binary)
	if err != nil {
		return Capture{}, nil, nil, err
	}
	if len(binaryCommit) < 7 || !strings.HasPrefix(strings.TrimSpace(commit), binaryCommit) {
		return Capture{}, nil, nil, fmt.Errorf("capture binary commit %q does not identify source commit %s", binaryCommit, strings.TrimSpace(commit))
	}
	stdout := &boundedBuffer{limit: maxCommandOutputBytes}
	stderr := &boundedBuffer{limit: maxCommandOutputBytes}
	command := exec.CommandContext(ctx, filepath.Join(repository, filepath.FromSlash(binary)), arguments...)
	command.Dir = repository
	command.Stdout = stdout
	command.Stderr = stderr
	runErr := command.Run()
	exitCode := 0
	if runErr != nil {
		var exitError *exec.ExitError
		if !errors.As(runErr, &exitError) {
			return Capture{}, nil, nil, fmt.Errorf("execute ChronicleGate: %w", runErr)
		}
		exitCode = exitError.ExitCode()
	}
	if stdout.total > maxCommandOutputBytes || stderr.total > maxCommandOutputBytes {
		return Capture{}, nil, nil, fmt.Errorf("ChronicleGate output exceeded the capture limit")
	}
	stdoutDocument := append([]byte(nil), stdout.buffer.Bytes()...)
	stderrDocument := append([]byte(nil), stderr.buffer.Bytes()...)
	return Capture{
		SchemaVersion: CaptureSchemaVersion, Kind: kind, CaseID: caseID, CapturedAt: time.Now().UTC().Format(time.RFC3339),
		Source:       Source{Commit: strings.TrimSpace(commit), Tree: strings.TrimSpace(tree), BinarySHA256: binaryHash, BinaryCommit: binaryCommit, Argv: append([]string{binary}, arguments...), Inputs: inputs},
		RawDirectory: "raw",
		Execution:    Execution{ExitCode: exitCode, StdoutSHA256: hashBytes(stdoutDocument), StderrSHA256: hashBytes(stderrDocument), StdoutBytes: len(stdoutDocument), StderrBytes: len(stderrDocument)},
	}, stdoutDocument, stderrDocument, nil
}

func finishCapture(repository, output string, capture *Capture, resultName string) error {
	rawRoot := filepath.Join(repository, filepath.FromSlash(output), "raw")
	resultHash, err := HashFile(filepath.Join(rawRoot, resultName))
	if err != nil {
		return err
	}
	checksumsHash, err := HashFile(filepath.Join(rawRoot, "checksums.sha256"))
	if err != nil {
		return err
	}
	capture.Artifacts.ResultSHA256 = resultHash
	capture.Artifacts.ChecksumsSHA256 = checksumsHash
	cleanup, err := dockerCleanupEvidence(capture.Execution.RunID)
	if err != nil {
		return err
	}
	capture.Cleanup = cleanup
	if err := ValidateCapture(*capture); err != nil {
		return err
	}
	document, err := json.Marshal(capture)
	if err != nil {
		return err
	}
	if err := validateSchemaDocument(repository, "evidence-capture.schema.json", document); err != nil {
		return err
	}
	return artifact.WriteNewJSON(filepath.Join(repository, filepath.FromSlash(output), "capture.json"), capture)
}

func expectedSignature(repository string, path *string) (*spec.ResultSignature, error) {
	if path == nil {
		return nil, nil
	}
	document, err := os.ReadFile(filepath.Join(repository, filepath.FromSlash(*path)))
	if err != nil {
		return nil, err
	}
	var signature spec.ResultSignature
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&signature); err != nil {
		return nil, err
	}
	if err := requireJSONEOF(decoder); err != nil {
		return nil, err
	}
	return &signature, nil
}

func journalOwnsRun(events []runlog.Event, runID string) bool {
	found := false
	for _, event := range events {
		if value, exists := event.Detail["runId"]; exists {
			text, ok := value.(string)
			if !ok || text != runID {
				return false
			}
			found = true
		}
	}
	return found
}

func dockerCleanupEvidence(runID string) (CleanupEvidence, error) {
	if runID == "" {
		return CleanupEvidence{}, fmt.Errorf("run ID is empty")
	}
	values := []*int{}
	evidence := CleanupEvidence{}
	values = append(values, &evidence.Containers, &evidence.Networks, &evidence.Volumes)
	commands := [][]string{
		{"ps", "-a", "--filter", "label=dev.chronicle.run=" + runID, "--format", "{{.ID}}"},
		{"network", "ls", "--filter", "label=dev.chronicle.run=" + runID, "--format", "{{.ID}}"},
		{"volume", "ls", "--filter", "label=dev.chronicle.run=" + runID, "--format", "{{.Name}}"},
	}
	for index, arguments := range commands {
		document, err := exec.Command("docker", arguments...).CombinedOutput()
		if err != nil {
			return CleanupEvidence{}, fmt.Errorf("inspect Docker cleanup %v: %w: %s", arguments, err, document)
		}
		count := 0
		for _, line := range strings.Split(strings.TrimSpace(string(document)), "\n") {
			if strings.TrimSpace(line) != "" {
				count++
			}
		}
		*values[index] = count
	}
	if evidence != (CleanupEvidence{}) {
		return evidence, fmt.Errorf("run %s leaked Docker resources: %s", runID, strconv.Itoa(evidence.Containers+evidence.Networks+evidence.Volumes))
	}
	return evidence, nil
}

func requireCleanSource(repository string) error {
	status, err := gitOutput(repository, "status", "--porcelain", "--untracked-files=normal")
	if err != nil {
		return err
	}
	if strings.TrimSpace(status) != "" {
		return fmt.Errorf("evidence capture requires a clean committed source tree")
	}
	return nil
}

func gitOutput(repository string, arguments ...string) (string, error) {
	command := exec.Command("git", arguments...)
	command.Dir = repository
	document, err := command.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("git %s: %w: %s", strings.Join(arguments, " "), err, document)
	}
	return string(document), nil
}

func binaryVersionCommit(repository, binary string) (string, error) {
	command := exec.Command(filepath.Join(repository, filepath.FromSlash(binary)), "version", "--json")
	command.Dir = repository
	document, err := command.Output()
	if err != nil {
		return "", fmt.Errorf("read capture binary version: %w", err)
	}
	if len(document) > 4096 {
		return "", fmt.Errorf("capture binary version exceeds 4096 bytes")
	}
	var version struct {
		SchemaVersion string `json:"schemaVersion"`
		Version       string `json:"version"`
		Commit        string `json:"commit"`
		BuildDate     string `json:"buildDate"`
	}
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&version); err != nil {
		return "", fmt.Errorf("decode capture binary version: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return "", err
	}
	if version.SchemaVersion != "chronicle.dev/version/v1alpha1" {
		return "", fmt.Errorf("capture binary has unsupported version schema %q", version.SchemaVersion)
	}
	return version.Commit, nil
}

func hashBytes(document []byte) string {
	digest := sha256.Sum256(document)
	return fmt.Sprintf("%x", digest)
}

func jsonEquivalent(left, right []byte) bool {
	decode := func(document []byte) (any, error) {
		decoder := json.NewDecoder(bytes.NewReader(document))
		decoder.UseNumber()
		var value any
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		if err := requireJSONEOF(decoder); err != nil {
			return nil, err
		}
		return value, nil
	}
	leftValue, leftErr := decode(left)
	rightValue, rightErr := decode(right)
	return leftErr == nil && rightErr == nil && reflect.DeepEqual(leftValue, rightValue)
}
