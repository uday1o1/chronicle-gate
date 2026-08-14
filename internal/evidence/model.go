// Package evidence captures and publishes bounded, source-authenticated release evidence.
package evidence

import "encoding/json"

const (
	CorpusSchemaVersion          = "chronicle.dev/evidence-corpus/v1alpha1"
	CaptureSchemaVersion         = "chronicle.dev/evidence-capture/v1alpha1"
	PublicCaseSchemaVersion      = "chronicle.dev/public-case-evidence/v1alpha1"
	PublicBenchmarkSchemaVersion = "chronicle.dev/public-benchmark-evidence/v1alpha2"
)

type Corpus struct {
	SchemaVersion  string          `json:"schemaVersion"`
	SemanticCases  []SemanticCase  `json:"semanticCases"`
	BenchmarkCases []BenchmarkCase `json:"benchmarkCases"`
}

type SemanticCase struct {
	ID                     string  `json:"id"`
	ClaimID                string  `json:"claimId"`
	Role                   string  `json:"role"`
	Scenario               string  `json:"scenario"`
	Baseline               string  `json:"baseline"`
	Candidate              string  `json:"candidate"`
	ExpectedExit           int     `json:"expectedExit"`
	ExpectedClassification string  `json:"expectedClassification"`
	ExpectedSignature      *string `json:"expectedSignature"`
	Minimize               bool    `json:"minimize"`
}

type BenchmarkCase struct {
	ID                     string `json:"id"`
	Role                   string `json:"role"`
	Workload               string `json:"workload"`
	Baseline               string `json:"baseline"`
	Candidate              string `json:"candidate"`
	ExpectedExit           int    `json:"expectedExit"`
	ExpectedClassification string `json:"expectedClassification"`
}

type Source struct {
	Commit       string      `json:"commit"`
	Tree         string      `json:"tree"`
	BinarySHA256 string      `json:"binarySha256"`
	BinaryCommit string      `json:"binaryCommit"`
	Argv         []string    `json:"argv"`
	Inputs       []InputFile `json:"inputs"`
}

type InputFile struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

type Capture struct {
	SchemaVersion string           `json:"schemaVersion"`
	Kind          string           `json:"kind"`
	CaseID        string           `json:"caseId"`
	CapturedAt    string           `json:"capturedAt"`
	Source        Source           `json:"source"`
	RawDirectory  string           `json:"rawDirectory"`
	Execution     Execution        `json:"execution"`
	Artifacts     CaptureArtifacts `json:"artifacts"`
	Cleanup       CleanupEvidence  `json:"cleanup"`
}

type Execution struct {
	ExitCode       int             `json:"exitCode"`
	StdoutSHA256   string          `json:"stdoutSha256"`
	StderrSHA256   string          `json:"stderrSha256"`
	StdoutBytes    int             `json:"stdoutBytes"`
	StderrBytes    int             `json:"stderrBytes"`
	State          string          `json:"state"`
	Classification string          `json:"classification"`
	RunID          string          `json:"runId"`
	Signature      json.RawMessage `json:"signature"`
}

type CaptureArtifacts struct {
	ResultSHA256    string  `json:"resultSha256"`
	ChecksumsSHA256 string  `json:"checksumsSha256"`
	JournalSHA256   *string `json:"journalSha256"`
	BundleSHA256    *string `json:"bundleSha256"`
}

type CleanupEvidence struct {
	Containers int `json:"containers"`
	Networks   int `json:"networks"`
	Volumes    int `json:"volumes"`
}

type PublicCaseEvidence struct {
	SchemaVersion string           `json:"schemaVersion"`
	Kind          string           `json:"kind"`
	CaseID        string           `json:"caseId"`
	ClaimID       string           `json:"claimId"`
	Role          string           `json:"role"`
	CapturedAt    string           `json:"capturedAt"`
	Source        Source           `json:"source"`
	Outcome       PublicOutcome    `json:"outcome"`
	Reduction     PublicReduction  `json:"reduction"`
	Artifacts     CaptureArtifacts `json:"artifacts"`
	Boundary      string           `json:"boundary"`
}

type PublicOutcome struct {
	ExitCode          int                  `json:"exitCode"`
	State             string               `json:"state"`
	Classification    string               `json:"classification"`
	FailureSignature  json.RawMessage      `json:"failureSignature"`
	Confirmations     int                  `json:"confirmations"`
	BaselineAttempts  int                  `json:"baselineAttempts"`
	CandidateAttempts int                  `json:"candidateAttempts"`
	Observations      []ObservationSummary `json:"observations"`
}

type ObservationSummary struct {
	Role           string `json:"role"`
	ObserverID     string `json:"observerId"`
	Type           string `json:"type"`
	Count          int    `json:"count"`
	RawSchemaValid bool   `json:"rawSchemaValid"`
}

type PublicReduction struct {
	Status          string `json:"status"`
	Minimality      string `json:"minimality"`
	OriginalEvents  int    `json:"originalEvents"`
	FinalEvents     int    `json:"finalEvents"`
	OriginalActions int    `json:"originalActions"`
	FinalActions    int    `json:"finalActions"`
	Trials          int    `json:"trials"`
}

type PublicBenchmarkEvidence struct {
	SchemaVersion string                 `json:"schemaVersion"`
	Kind          string                 `json:"kind"`
	Boundary      string                 `json:"boundary"`
	Comparisons   []PublicBenchmarkEntry `json:"comparisons"`
}

type PublicBenchmarkEntry struct {
	CaseID     string           `json:"caseId"`
	CapturedAt string           `json:"capturedAt"`
	Source     Source           `json:"source"`
	Outcome    BenchmarkOutcome `json:"outcome"`
	Artifacts  CaptureArtifacts `json:"artifacts"`
}

type BenchmarkOutcome struct {
	ExitCode                int                     `json:"exitCode"`
	State                   string                  `json:"state"`
	Classification          string                  `json:"classification"`
	Rounds                  int                     `json:"rounds"`
	BaselineRequests        int                     `json:"baselineRequests"`
	CandidateRequests       int                     `json:"candidateRequests"`
	PooledBaselineP95Nanos  int64                   `json:"pooledBaselineP95Nanos"`
	PooledCandidateP95Nanos int64                   `json:"pooledCandidateP95Nanos"`
	PairedP95               []BenchmarkP95Pair      `json:"pairedP95"`
	Analysis                PublicBenchmarkAnalysis `json:"analysis"`
}

type BenchmarkP95Pair struct {
	Round             int   `json:"round"`
	BaselineP95Nanos  int64 `json:"baselineP95Nanos"`
	CandidateP95Nanos int64 `json:"candidateP95Nanos"`
}

type PublicBenchmarkAnalysis struct {
	Algorithm                 string  `json:"algorithm"`
	BootstrapSeed             int64   `json:"bootstrapSeed"`
	BootstrapResamples        int     `json:"bootstrapResamples"`
	Confidence                float64 `json:"confidence"`
	BlockSize                 int     `json:"blockSize"`
	AbsoluteP95DeltaUnit      string  `json:"absoluteP95DeltaUnit"`
	RelativeP95DeltaUnit      string  `json:"relativeP95DeltaUnit"`
	MeanAbsoluteP95DeltaNanos float64 `json:"meanAbsoluteP95DeltaNanos"`
	MeanRelativeP95Delta      float64 `json:"meanRelativeP95Delta"`
	LowerRelativeCI           float64 `json:"lowerRelativeCI"`
	UpperRelativeCI           float64 `json:"upperRelativeCI"`
	LowerIndex                int     `json:"lowerIndex"`
	UpperIndex                int     `json:"upperIndex"`
	AbsoluteThresholdNanos    int64   `json:"absoluteThresholdNanos"`
	RelativeThreshold         float64 `json:"relativeThreshold"`
	Regression                bool    `json:"regression"`
}
