package evidence

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

const maxEvidenceDocumentBytes = 8 << 20

var identifierPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,63}$`)

func LoadCorpus(path string) (Corpus, error) {
	corpus, err := loadStrict[Corpus](path)
	if err != nil {
		return Corpus{}, err
	}
	return corpus, ValidateCorpus(corpus)
}

func LoadCapture(path string) (Capture, error) {
	capture, err := loadStrict[Capture](path)
	if err != nil {
		return Capture{}, err
	}
	return capture, ValidateCapture(capture)
}

func loadStrict[T any](path string) (T, error) {
	var zero T
	file, err := os.Open(path)
	if err != nil {
		return zero, fmt.Errorf("open %s: %w", path, err)
	}
	document, readErr := io.ReadAll(io.LimitReader(file, maxEvidenceDocumentBytes+1))
	closeErr := file.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return zero, fmt.Errorf("read %s: %w", path, err)
	}
	if len(document) > maxEvidenceDocumentBytes {
		return zero, fmt.Errorf("%s exceeds %d bytes", path, maxEvidenceDocumentBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.DisallowUnknownFields()
	var value T
	if err := decoder.Decode(&value); err != nil {
		return zero, fmt.Errorf("decode %s: %w", path, err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return zero, fmt.Errorf("decode %s: %w", path, err)
	}
	return value, nil
}

func requireJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("multiple JSON values are forbidden")
		}
		return err
	}
	return nil
}

func ValidateCorpus(corpus Corpus) error {
	if corpus.SchemaVersion != CorpusSchemaVersion {
		return fmt.Errorf("unsupported corpus schema %q", corpus.SchemaVersion)
	}
	if len(corpus.SemanticCases) != 14 {
		return fmt.Errorf("semantic corpus has %d cases, want exactly 14", len(corpus.SemanticCases))
	}
	if len(corpus.BenchmarkCases) != 2 {
		return fmt.Errorf("benchmark corpus has %d cases, want exactly 2", len(corpus.BenchmarkCases))
	}
	seen := map[string]struct{}{}
	claims := map[string]int{}
	for index, item := range corpus.SemanticCases {
		if err := validateIDAndRole(item.ID, item.Role, seen); err != nil {
			return fmt.Errorf("semantic case %d: %w", index, err)
		}
		claims[item.ClaimID]++
		for label, path := range map[string]string{"scenario": item.Scenario, "baseline": item.Baseline, "candidate": item.Candidate} {
			if !safeRepositoryPath(path) {
				return fmt.Errorf("semantic case %s has unsafe %s path %q", item.ID, label, path)
			}
		}
		if item.Role == "control" {
			if item.ExpectedExit != 0 || item.ExpectedClassification != "PASS" || item.ExpectedSignature != nil || item.Minimize {
				return fmt.Errorf("control %s has a non-PASS expectation", item.ID)
			}
		} else if item.ExpectedExit != 2 || item.ExpectedSignature == nil || !safeRepositoryPath(*item.ExpectedSignature) || item.ExpectedClassification != "SEMANTIC_REGRESSION" && item.ExpectedClassification != "SCHEMA_REGRESSION" && item.ExpectedClassification != "EXTERNAL_EFFECT_REGRESSION" {
			return fmt.Errorf("regression %s has an invalid expected outcome", item.ID)
		}
	}
	for defect := 1; defect <= 7; defect++ {
		if claims[fmt.Sprintf("R%d", defect)] != 1 || claims[fmt.Sprintf("R%d-control", defect)] != 1 {
			return fmt.Errorf("claim R%d does not have exactly one regression and one control", defect)
		}
	}
	for index, item := range corpus.BenchmarkCases {
		if err := validateIDAndRole(item.ID, item.Role, seen); err != nil {
			return fmt.Errorf("benchmark case %d: %w", index, err)
		}
		for label, path := range map[string]string{"workload": item.Workload, "baseline": item.Baseline, "candidate": item.Candidate} {
			if !safeRepositoryPath(path) {
				return fmt.Errorf("benchmark case %s has unsafe %s path %q", item.ID, label, path)
			}
		}
		if item.Role == "control" && (item.ExpectedExit != 0 || item.ExpectedClassification != "PASS") || item.Role == "regression" && (item.ExpectedExit != 2 || item.ExpectedClassification != "PERFORMANCE_REGRESSION") {
			return fmt.Errorf("benchmark case %s has an invalid expected outcome", item.ID)
		}
	}
	return nil
}

func validateIDAndRole(id, role string, seen map[string]struct{}) error {
	if !identifierPattern.MatchString(id) {
		return fmt.Errorf("invalid ID %q", id)
	}
	if role != "regression" && role != "control" {
		return fmt.Errorf("invalid role %q", role)
	}
	if _, exists := seen[id]; exists {
		return fmt.Errorf("duplicate ID %q", id)
	}
	seen[id] = struct{}{}
	return nil
}

func safeRepositoryPath(path string) bool {
	return path != "" && !strings.Contains(path, "\\") && !filepath.IsAbs(path) && filepath.ToSlash(filepath.Clean(path)) == path && path != "." && !strings.HasPrefix(path, "../")
}

func ValidateCapture(capture Capture) error {
	if capture.SchemaVersion != CaptureSchemaVersion || capture.Kind != "SemanticCapture" && capture.Kind != "BenchmarkCapture" {
		return fmt.Errorf("unsupported capture contract")
	}
	if !identifierPattern.MatchString(capture.CaseID) || capture.RawDirectory != "raw" {
		return fmt.Errorf("capture identity or raw directory is invalid")
	}
	if !validGitObject(capture.Source.Commit) || !validGitObject(capture.Source.Tree) || !validSHA256(capture.Source.BinarySHA256) {
		return fmt.Errorf("capture source identity is invalid")
	}
	if len(capture.Source.BinaryCommit) < 7 || !strings.HasPrefix(capture.Source.Commit, capture.Source.BinaryCommit) {
		return fmt.Errorf("capture binary commit does not identify the source commit")
	}
	if len(capture.Source.Argv) < 2 || capture.Source.Argv[0] != "bin/chronicle" || len(capture.Source.Inputs) == 0 {
		return fmt.Errorf("capture command provenance is incomplete")
	}
	paths := map[string]struct{}{}
	previous := ""
	for _, input := range capture.Source.Inputs {
		if !safeRepositoryPath(input.Path) || !validSHA256(input.SHA256) || input.Path <= previous {
			return fmt.Errorf("capture input inventory is unsafe or unsorted")
		}
		if _, exists := paths[input.Path]; exists {
			return fmt.Errorf("capture duplicates input %q", input.Path)
		}
		paths[input.Path] = struct{}{}
		previous = input.Path
	}
	if !validSHA256(capture.Execution.StdoutSHA256) || !validSHA256(capture.Execution.StderrSHA256) || capture.Execution.StdoutBytes < 0 || capture.Execution.StderrBytes < 0 || capture.Execution.RunID == "" {
		return fmt.Errorf("capture execution evidence is incomplete")
	}
	if !validSHA256(capture.Artifacts.ResultSHA256) || !validSHA256(capture.Artifacts.ChecksumsSHA256) {
		return fmt.Errorf("capture artifact hashes are incomplete")
	}
	if capture.Kind == "SemanticCapture" && (capture.Artifacts.JournalSHA256 == nil || !validSHA256(*capture.Artifacts.JournalSHA256)) {
		return fmt.Errorf("semantic capture omits its journal hash")
	}
	if capture.Kind == "BenchmarkCapture" && (capture.Artifacts.JournalSHA256 != nil || capture.Artifacts.BundleSHA256 != nil) {
		return fmt.Errorf("benchmark capture cannot claim semantic journal or bundle evidence")
	}
	if capture.Artifacts.BundleSHA256 != nil && !validSHA256(*capture.Artifacts.BundleSHA256) {
		return fmt.Errorf("capture bundle hash is invalid")
	}
	if capture.Cleanup != (CleanupEvidence{}) {
		return fmt.Errorf("capture cleanup inventory is nonzero")
	}
	return nil
}

func validGitObject(value string) bool {
	if len(value) != 40 && len(value) != 64 {
		return false
	}
	for _, character := range value {
		if character < '0' || character > '9' && character < 'a' || character > 'f' {
			return false
		}
	}
	return true
}

func validSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil && strings.ToLower(value) == value
}

func HashFile(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	digest := sha256.New()
	_, copyErr := io.Copy(digest, file)
	closeErr := file.Close()
	if err := errors.Join(copyErr, closeErr); err != nil {
		return "", err
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}
