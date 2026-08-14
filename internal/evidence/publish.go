package evidence

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/uday1o1/chronicle-gate/internal/artifact"
	"github.com/uday1o1/chronicle-gate/internal/bench"
	"github.com/uday1o1/chronicle-gate/internal/bundle"
	"github.com/uday1o1/chronicle-gate/internal/engine"
	"github.com/uday1o1/chronicle-gate/internal/runlog"
	"github.com/uday1o1/chronicle-gate/internal/spec"
)

const evidenceBoundary = "Synthetic local comparative evidence demonstrates only the declared ChronicleGate scenarios and does not establish production reliability or capacity."

func PublishSemantic(repository, corpusPath, captureRoot, output string) (PublicCaseEvidence, error) {
	if filepath.ToSlash(filepath.Clean(corpusPath)) != "evidence/corpus.json" {
		return PublicCaseEvidence{}, fmt.Errorf("semantic publication requires the canonical corpus path")
	}
	repository, err := filepath.Abs(repository)
	if err != nil {
		return PublicCaseEvidence{}, err
	}
	corpus, err := LoadCorpus(filepath.Join(repository, filepath.FromSlash(corpusPath)))
	if err != nil {
		return PublicCaseEvidence{}, err
	}
	if err := validateSchemaFile(repository, "schemas/evidence-corpus.schema.json", filepath.Join(repository, filepath.FromSlash(corpusPath))); err != nil {
		return PublicCaseEvidence{}, err
	}
	capture, item, rawRoot, err := verifySemanticCapture(repository, corpusPath, captureRoot, corpus)
	if err != nil {
		return PublicCaseEvidence{}, err
	}
	if filepath.ToSlash(filepath.Clean(output)) != "evidence/results/"+item.ID+".json" {
		return PublicCaseEvidence{}, fmt.Errorf("semantic publication requires the canonical case output path")
	}
	document, err := os.ReadFile(filepath.Join(rawRoot, "result.json"))
	if err != nil {
		return PublicCaseEvidence{}, err
	}
	var report engine.Report
	if err := decodeStrict(document, &report); err != nil {
		return PublicCaseEvidence{}, err
	}
	summaries := []ObservationSummary{}
	if report.Baseline != nil {
		summaries = appendObservationSummaries(summaries, "baseline", *report.Baseline)
	}
	for index, attempt := range report.Candidate {
		summaries = appendObservationSummaries(summaries, fmt.Sprintf("candidate-%d", index+1), attempt)
	}
	public := PublicCaseEvidence{
		SchemaVersion: PublicCaseSchemaVersion,
		Kind:          "PublicCaseEvidence",
		CaseID:        item.ID,
		ClaimID:       item.ClaimID,
		Role:          item.Role,
		CapturedAt:    capture.CapturedAt,
		Source:        capture.Source,
		Outcome: PublicOutcome{
			ExitCode:          capture.Execution.ExitCode,
			State:             report.State,
			Classification:    report.Classification,
			FailureSignature:  capture.Execution.Signature,
			Confirmations:     report.Confirmations,
			BaselineAttempts:  boolInt(report.Baseline != nil),
			CandidateAttempts: len(report.Candidate),
			Observations:      summaries,
		},
		Reduction: PublicReduction{
			Status: report.Minimization.Status, Minimality: report.Minimization.Minimality,
			OriginalEvents: report.Minimization.OriginalEvents, FinalEvents: report.Minimization.FinalEvents,
			OriginalActions: report.Minimization.OriginalActions, FinalActions: report.Minimization.FinalActions,
			Trials: report.Minimization.Trials,
		},
		Artifacts: capture.Artifacts,
		Boundary:  evidenceBoundary,
	}
	if err := validatePublicCase(public, item); err != nil {
		return PublicCaseEvidence{}, err
	}
	publicDocument, err := json.Marshal(public)
	if err != nil {
		return PublicCaseEvidence{}, err
	}
	if err := validateSchemaDocument(repository, "public-case-evidence.schema.json", publicDocument); err != nil {
		return PublicCaseEvidence{}, err
	}
	if err := writeSanitizedNewJSON(filepath.Join(repository, filepath.FromSlash(output)), public); err != nil {
		return PublicCaseEvidence{}, err
	}
	return public, nil
}

func PublishBenchmark(repository, corpusPath, aaCaptureRoot, slowdownCaptureRoot, output string) (PublicBenchmarkEvidence, error) {
	if filepath.ToSlash(filepath.Clean(corpusPath)) != "evidence/corpus.json" || filepath.ToSlash(filepath.Clean(output)) != "evidence/results/benchmark.json" {
		return PublicBenchmarkEvidence{}, fmt.Errorf("benchmark publication requires the canonical corpus and output path")
	}
	repository, err := filepath.Abs(repository)
	if err != nil {
		return PublicBenchmarkEvidence{}, err
	}
	corpus, err := LoadCorpus(filepath.Join(repository, filepath.FromSlash(corpusPath)))
	if err != nil {
		return PublicBenchmarkEvidence{}, err
	}
	if err := validateSchemaFile(repository, "schemas/evidence-corpus.schema.json", filepath.Join(repository, filepath.FromSlash(corpusPath))); err != nil {
		return PublicBenchmarkEvidence{}, err
	}
	entries := make([]PublicBenchmarkEntry, 0, 2)
	for _, root := range []string{aaCaptureRoot, slowdownCaptureRoot} {
		capture, item, rawRoot, err := verifyBenchmarkCapture(repository, corpusPath, root, corpus)
		if err != nil {
			return PublicBenchmarkEvidence{}, err
		}
		document, err := os.ReadFile(filepath.Join(rawRoot, "benchmark.json"))
		if err != nil {
			return PublicBenchmarkEvidence{}, err
		}
		var report bench.Report
		if err := decodeStrict(document, &report); err != nil {
			return PublicBenchmarkEvidence{}, err
		}
		if len(report.Aggregates) != 2 {
			return PublicBenchmarkEvidence{}, fmt.Errorf("benchmark %s has %d aggregates", item.ID, len(report.Aggregates))
		}
		baseline, candidate := report.Aggregates[0], report.Aggregates[1]
		if baseline.Target != "baseline" || candidate.Target != "candidate" {
			return PublicBenchmarkEvidence{}, fmt.Errorf("benchmark aggregate order is invalid")
		}
		pairs, err := bench.PairP95Trials(report.Trials, report.Plan.Rounds)
		if err != nil {
			return PublicBenchmarkEvidence{}, err
		}
		if err := bench.ValidateAnalysis(pairs, report.Analysis); err != nil {
			return PublicBenchmarkEvidence{}, err
		}
		entries = append(entries, PublicBenchmarkEntry{
			CaseID: item.ID, CapturedAt: capture.CapturedAt, Source: capture.Source, Artifacts: capture.Artifacts,
			Outcome: BenchmarkOutcome{
				ExitCode: capture.Execution.ExitCode, State: report.State, Classification: report.Classification,
				Rounds: report.Plan.Rounds, BaselineRequests: baseline.Requests, CandidateRequests: candidate.Requests,
				PooledBaselineP95Nanos: baseline.P95Nanos, PooledCandidateP95Nanos: candidate.P95Nanos,
				PairedP95: publicP95Pairs(pairs), Analysis: publicBenchmarkAnalysis(report.Analysis),
			},
		})
	}
	public := PublicBenchmarkEvidence{SchemaVersion: PublicBenchmarkSchemaVersion, Kind: "PublicBenchmarkEvidence", Boundary: evidenceBoundary, Comparisons: entries}
	if err := validatePublicBenchmark(public); err != nil {
		return PublicBenchmarkEvidence{}, err
	}
	publicDocument, err := json.Marshal(public)
	if err != nil {
		return PublicBenchmarkEvidence{}, err
	}
	if err := validateSchemaDocument(repository, "public-benchmark-evidence.schema.json", publicDocument); err != nil {
		return PublicBenchmarkEvidence{}, err
	}
	if err := writeSanitizedNewJSON(filepath.Join(repository, filepath.FromSlash(output)), public); err != nil {
		return PublicBenchmarkEvidence{}, err
	}
	return public, nil
}

func verifySemanticCapture(repository, corpusPath, captureRoot string, corpus Corpus) (Capture, SemanticCase, string, error) {
	capturePath, rawRoot, err := capturePaths(repository, captureRoot)
	if err != nil {
		return Capture{}, SemanticCase{}, "", err
	}
	capture, err := LoadCapture(capturePath)
	if err != nil {
		return Capture{}, SemanticCase{}, "", err
	}
	if err := validateSchemaFile(repository, "schemas/evidence-capture.schema.json", capturePath); err != nil {
		return Capture{}, SemanticCase{}, "", err
	}
	if capture.Kind != "SemanticCapture" {
		return Capture{}, SemanticCase{}, "", fmt.Errorf("capture %s is not semantic", capture.CaseID)
	}
	item, err := FindSemanticCase(corpus, capture.CaseID)
	if err != nil {
		return Capture{}, SemanticCase{}, "", err
	}
	inputs, err := SemanticInputs(repository, corpusPath, item)
	if err != nil || !reflect.DeepEqual(inputs, capture.Source.Inputs) {
		return Capture{}, SemanticCase{}, "", fmt.Errorf("capture %s input closure changed", capture.CaseID)
	}
	if err := verifySource(repository, captureRoot, capture, semanticArguments(item, filepath.ToSlash(filepath.Join(captureRoot, "raw")))); err != nil {
		return Capture{}, SemanticCase{}, "", err
	}
	result, err := spec.LoadResult(filepath.Join(rawRoot, "result.json"))
	if err != nil {
		return Capture{}, SemanticCase{}, "", err
	}
	if violations := spec.ValidateResult(result); len(violations) != 0 {
		return Capture{}, SemanticCase{}, "", fmt.Errorf("result contract violations: %#v", violations)
	}
	if result.RunID != capture.Execution.RunID || result.State != capture.Execution.State || result.Classification != capture.Execution.Classification || capture.Execution.ExitCode != item.ExpectedExit {
		return Capture{}, SemanticCase{}, "", fmt.Errorf("capture outcome differs from result.json")
	}
	expected, err := expectedSignature(repository, item.ExpectedSignature)
	if err != nil || !reflect.DeepEqual(result.FailureSignature, expected) {
		return Capture{}, SemanticCase{}, "", fmt.Errorf("capture signature differs from the declared signature")
	}
	encoded, _ := json.Marshal(result.FailureSignature)
	if !jsonEquivalent(encoded, capture.Execution.Signature) {
		return Capture{}, SemanticCase{}, "", fmt.Errorf("capture signature encoding changed")
	}
	if err := artifact.VerifyChecksums(rawRoot); err != nil {
		return Capture{}, SemanticCase{}, "", err
	}
	if err := verifyArtifactHashes(rawRoot, "result.json", capture); err != nil {
		return Capture{}, SemanticCase{}, "", err
	}
	events, truncated, err := runlog.Read(filepath.Join(rawRoot, "events.ndjson"))
	if err != nil {
		return Capture{}, SemanticCase{}, "", err
	}
	state, terminal := runlog.FinalState(events, truncated)
	if !terminal || state != result.State || !journalOwnsRun(events, result.RunID) {
		return Capture{}, SemanticCase{}, "", fmt.Errorf("journal is not authoritative for %s", result.RunID)
	}
	journalHash, err := HashFile(filepath.Join(rawRoot, "events.ndjson"))
	if err != nil || capture.Artifacts.JournalSHA256 == nil || journalHash != *capture.Artifacts.JournalSHA256 {
		return Capture{}, SemanticCase{}, "", fmt.Errorf("journal hash changed")
	}
	if _, err := dockerCleanupEvidence(result.RunID); err != nil {
		return Capture{}, SemanticCase{}, "", err
	}
	return capture, item, rawRoot, nil
}

func verifyBenchmarkCapture(repository, corpusPath, captureRoot string, corpus Corpus) (Capture, BenchmarkCase, string, error) {
	capturePath, rawRoot, err := capturePaths(repository, captureRoot)
	if err != nil {
		return Capture{}, BenchmarkCase{}, "", err
	}
	capture, err := LoadCapture(capturePath)
	if err != nil {
		return Capture{}, BenchmarkCase{}, "", err
	}
	if err := validateSchemaFile(repository, "schemas/evidence-capture.schema.json", capturePath); err != nil {
		return Capture{}, BenchmarkCase{}, "", err
	}
	if capture.Kind != "BenchmarkCapture" {
		return Capture{}, BenchmarkCase{}, "", fmt.Errorf("capture %s is not a benchmark", capture.CaseID)
	}
	item, err := FindBenchmarkCase(corpus, capture.CaseID)
	if err != nil {
		return Capture{}, BenchmarkCase{}, "", err
	}
	inputs, err := BenchmarkInputs(repository, corpusPath, item)
	if err != nil || !reflect.DeepEqual(inputs, capture.Source.Inputs) {
		return Capture{}, BenchmarkCase{}, "", fmt.Errorf("capture %s input closure changed", capture.CaseID)
	}
	if err := verifySource(repository, captureRoot, capture, benchmarkArguments(item, filepath.ToSlash(filepath.Join(captureRoot, "raw")))); err != nil {
		return Capture{}, BenchmarkCase{}, "", err
	}
	document, err := os.ReadFile(filepath.Join(rawRoot, "benchmark.json"))
	if err != nil {
		return Capture{}, BenchmarkCase{}, "", err
	}
	if err := spec.ValidateBenchmarkResultJSON(document); err != nil {
		return Capture{}, BenchmarkCase{}, "", err
	}
	var report bench.Report
	if err := decodeStrict(document, &report); err != nil {
		return Capture{}, BenchmarkCase{}, "", err
	}
	if report.RunID != capture.Execution.RunID || report.State != capture.Execution.State || report.Classification != capture.Execution.Classification || capture.Execution.ExitCode != item.ExpectedExit {
		return Capture{}, BenchmarkCase{}, "", fmt.Errorf("benchmark capture outcome differs from benchmark.json")
	}
	if err := artifact.VerifyChecksums(rawRoot); err != nil {
		return Capture{}, BenchmarkCase{}, "", err
	}
	if err := verifyArtifactHashes(rawRoot, "benchmark.json", capture); err != nil {
		return Capture{}, BenchmarkCase{}, "", err
	}
	if _, err := os.Stat(filepath.Join(rawRoot, "events.ndjson")); !os.IsNotExist(err) {
		return Capture{}, BenchmarkCase{}, "", fmt.Errorf("benchmark capture contains a journal")
	}
	if _, err := dockerCleanupEvidence(report.RunID); err != nil {
		return Capture{}, BenchmarkCase{}, "", err
	}
	return capture, item, rawRoot, nil
}

func capturePaths(repository, captureRoot string) (string, string, error) {
	if !safeRepositoryPath(filepath.ToSlash(captureRoot)) {
		return "", "", fmt.Errorf("capture root must be repository-relative")
	}
	root := filepath.Join(repository, filepath.FromSlash(captureRoot))
	info, err := os.Lstat(root)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", "", fmt.Errorf("capture root is not a real directory")
	}
	rawInfo, err := os.Lstat(filepath.Join(root, "raw"))
	if err != nil || !rawInfo.IsDir() || rawInfo.Mode()&os.ModeSymlink != 0 {
		return "", "", fmt.Errorf("capture raw root is not a real directory")
	}
	return filepath.Join(root, "capture.json"), filepath.Join(root, "raw"), nil
}

func verifySource(repository, captureRoot string, capture Capture, arguments []string) error {
	commit, err := gitOutput(repository, "rev-parse", "HEAD")
	if err != nil || strings.TrimSpace(commit) != capture.Source.Commit {
		return fmt.Errorf("capture source commit differs from HEAD")
	}
	tree, err := gitOutput(repository, "rev-parse", "HEAD^{tree}")
	if err != nil || strings.TrimSpace(tree) != capture.Source.Tree {
		return fmt.Errorf("capture source tree differs from HEAD")
	}
	binaryHash, err := HashFile(filepath.Join(repository, "bin", "chronicle"))
	if err != nil || binaryHash != capture.Source.BinarySHA256 {
		return fmt.Errorf("capture binary identity changed")
	}
	binaryCommit, err := binaryVersionCommit(repository, "bin/chronicle")
	if err != nil || binaryCommit != capture.Source.BinaryCommit || !strings.HasPrefix(capture.Source.Commit, binaryCommit) {
		return fmt.Errorf("capture binary source identity changed")
	}
	want := append([]string{"bin/chronicle"}, arguments...)
	if !reflect.DeepEqual(want, capture.Source.Argv) || filepath.ToSlash(filepath.Dir(filepath.Join(captureRoot, "raw"))) != filepath.ToSlash(captureRoot) {
		return fmt.Errorf("capture argv changed")
	}
	return nil
}

func semanticArguments(item SemanticCase, raw string) []string {
	arguments := []string{"run", "--scenario", item.Scenario, "--baseline", item.Baseline, "--candidate", item.Candidate, "--out", raw, "--development-local-images", "--json"}
	if !item.Minimize {
		arguments = append(arguments, "--no-minimize")
	}
	return arguments
}

func benchmarkArguments(item BenchmarkCase, raw string) []string {
	return []string{"bench", "--workload", item.Workload, "--baseline", item.Baseline, "--candidate", item.Candidate, "--out", raw, "--development-local-images", "--json"}
}

func verifyArtifactHashes(rawRoot, resultName string, capture Capture) error {
	for path, want := range map[string]string{resultName: capture.Artifacts.ResultSHA256, "checksums.sha256": capture.Artifacts.ChecksumsSHA256} {
		got, err := HashFile(filepath.Join(rawRoot, path))
		if err != nil || got != want {
			return fmt.Errorf("captured artifact %s changed", path)
		}
	}
	if capture.Artifacts.BundleSHA256 == nil {
		if _, err := os.Stat(filepath.Join(rawRoot, "reproduction.zip")); !os.IsNotExist(err) {
			return fmt.Errorf("capture has an undeclared reproduction bundle")
		}
	} else {
		archive, err := bundle.Open(filepath.Join(rawRoot, "reproduction.zip"))
		if err != nil {
			return fmt.Errorf("verify captured reproduction bundle: %w", err)
		}
		got := archive.SHA256
		closeErr := archive.Close()
		if closeErr != nil || got != *capture.Artifacts.BundleSHA256 {
			return fmt.Errorf("captured reproduction bundle changed")
		}
	}
	return nil
}

func appendObservationSummaries(output []ObservationSummary, role string, attempt engine.AttemptEvidence) []ObservationSummary {
	for _, observation := range attempt.Observations {
		output = append(output, ObservationSummary{Role: role, ObserverID: observation.Identity.ObserverID, Type: observation.Type, Count: observation.Count, RawSchemaValid: observation.RawSchemaValid})
	}
	return output
}

func validatePublicCase(public PublicCaseEvidence, item SemanticCase) error {
	if public.SchemaVersion != PublicCaseSchemaVersion || public.Kind != "PublicCaseEvidence" || public.CaseID != item.ID || public.ClaimID != item.ClaimID || public.Role != item.Role || public.Outcome.ExitCode != item.ExpectedExit || public.Outcome.Classification != item.ExpectedClassification || public.Outcome.State != "COMPLETE" {
		return fmt.Errorf("public case evidence does not match the corpus")
	}
	if item.Role == "control" && (string(public.Outcome.FailureSignature) != "null" || public.Artifacts.BundleSHA256 != nil) {
		return fmt.Errorf("control public evidence claims a failure or bundle")
	}
	if item.Role == "regression" && (len(public.Outcome.FailureSignature) == 0 || string(public.Outcome.FailureSignature) == "null" || public.Artifacts.BundleSHA256 == nil || public.Outcome.Confirmations != 2) {
		return fmt.Errorf("regression public evidence is incomplete")
	}
	return nil
}

func validatePublicBenchmark(public PublicBenchmarkEvidence) error {
	if public.SchemaVersion != PublicBenchmarkSchemaVersion || public.Kind != "PublicBenchmarkEvidence" || len(public.Comparisons) != 2 {
		return fmt.Errorf("public benchmark evidence must contain exactly two comparisons")
	}
	if public.Comparisons[0].CaseID != "benchmark-aa" || public.Comparisons[0].Outcome.Classification != "PASS" || public.Comparisons[0].Outcome.ExitCode != 0 || public.Comparisons[0].Outcome.Analysis.Regression {
		return fmt.Errorf("public benchmark A/A evidence is invalid")
	}
	if public.Comparisons[1].CaseID != "benchmark-slowdown" || public.Comparisons[1].Outcome.Classification != "PERFORMANCE_REGRESSION" || public.Comparisons[1].Outcome.ExitCode != 2 || !public.Comparisons[1].Outcome.Analysis.Regression {
		return fmt.Errorf("public benchmark slowdown evidence is invalid")
	}
	for _, comparison := range public.Comparisons {
		if err := validatePublicBenchmarkOutcome(comparison.Outcome); err != nil {
			return fmt.Errorf("public benchmark %s: %w", comparison.CaseID, err)
		}
	}
	return nil
}

func publicP95Pairs(pairs []bench.P95Pair) []BenchmarkP95Pair {
	public := make([]BenchmarkP95Pair, len(pairs))
	for index, pair := range pairs {
		public[index] = BenchmarkP95Pair{Round: pair.Round, BaselineP95Nanos: pair.BaselineP95Nanos, CandidateP95Nanos: pair.CandidateP95Nanos}
	}
	return public
}

func publicBenchmarkAnalysis(analysis bench.Analysis) PublicBenchmarkAnalysis {
	return PublicBenchmarkAnalysis{
		Algorithm: analysis.Algorithm, BootstrapSeed: analysis.BootstrapSeed,
		BootstrapResamples: analysis.BootstrapResamples, Confidence: analysis.Confidence, BlockSize: analysis.BlockSize,
		AbsoluteP95DeltaUnit: analysis.AbsoluteP95DeltaUnit, RelativeP95DeltaUnit: analysis.RelativeP95DeltaUnit,
		MeanAbsoluteP95DeltaNanos: analysis.MeanAbsoluteP95DeltaNanos, MeanRelativeP95Delta: analysis.MeanRelativeP95Delta,
		LowerRelativeCI: analysis.LowerRelativeCI, UpperRelativeCI: analysis.UpperRelativeCI,
		LowerIndex: analysis.LowerIndex, UpperIndex: analysis.UpperIndex,
		AbsoluteThresholdNanos: analysis.AbsoluteThresholdNanos, RelativeThreshold: analysis.RelativeThreshold,
		Regression: analysis.Regression,
	}
}

func validatePublicBenchmarkOutcome(outcome BenchmarkOutcome) error {
	if outcome.State != "COMPLETE" || outcome.Rounds <= 0 || outcome.BaselineRequests <= 0 || outcome.CandidateRequests <= 0 || outcome.PooledBaselineP95Nanos <= 0 || outcome.PooledCandidateP95Nanos <= 0 || len(outcome.PairedP95) != outcome.Rounds {
		return fmt.Errorf("outcome inventory is invalid")
	}
	pairs := make([]bench.P95Pair, len(outcome.PairedP95))
	for index, pair := range outcome.PairedP95 {
		pairs[index] = bench.P95Pair{Round: pair.Round, BaselineP95Nanos: pair.BaselineP95Nanos, CandidateP95Nanos: pair.CandidateP95Nanos}
	}
	configuration := bench.Analysis{
		Algorithm: outcome.Analysis.Algorithm, BootstrapSeed: outcome.Analysis.BootstrapSeed,
		BootstrapResamples: outcome.Analysis.BootstrapResamples, Confidence: outcome.Analysis.Confidence,
		BlockSize: outcome.Analysis.BlockSize, AbsoluteP95DeltaUnit: outcome.Analysis.AbsoluteP95DeltaUnit,
		RelativeP95DeltaUnit:   outcome.Analysis.RelativeP95DeltaUnit,
		AbsoluteThresholdNanos: outcome.Analysis.AbsoluteThresholdNanos, RelativeThreshold: outcome.Analysis.RelativeThreshold,
	}
	recomputed, err := bench.RecomputeAnalysis(pairs, configuration)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(publicBenchmarkAnalysis(recomputed), outcome.Analysis) {
		return fmt.Errorf("published analysis does not match deterministic recomputation")
	}
	return nil
}

func writeSanitizedNewJSON(path string, value any) error {
	document, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	if err := artifact.ValidatePublic(document, nil); err != nil {
		return err
	}
	lower := bytes.ToLower(document)
	for _, forbidden := range [][]byte{[]byte("/users/"), []byte("/home/"), []byte("docker.sock"), []byte("chronicle_probe_token"), []byte("chronicle_database_dsn"), []byte("authorization\"")} {
		if bytes.Contains(lower, forbidden) {
			return fmt.Errorf("public evidence contains forbidden private material %q", forbidden)
		}
	}
	return artifact.WriteNewFile(path, append(document, '\n'))
}

func decodeStrict(document []byte, value any) error {
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	return requireJSONEOF(decoder)
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
