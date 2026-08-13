package spec

import "encoding/json"

type Result struct {
	APIVersion        string             `json:"apiVersion"`
	Kind              string             `json:"kind"`
	RunID             string             `json:"runId"`
	State             string             `json:"state"`
	Classification    string             `json:"classification"`
	StartedAt         string             `json:"startedAt"`
	CompletedAt       string             `json:"completedAt"`
	NonportableImages bool               `json:"nonportableImages"`
	Environment       json.RawMessage    `json:"environment"`
	Baseline          json.RawMessage    `json:"baseline,omitempty"`
	Candidate         []json.RawMessage  `json:"candidate"`
	FailureSignature  *ResultSignature   `json:"failureSignature,omitempty"`
	Violations        []ResultViolation  `json:"violations"`
	Confirmations     int                `json:"confirmations"`
	Error             string             `json:"error,omitempty"`
	Minimization      ResultMinimization `json:"minimization"`
	Bundle            string             `json:"bundle,omitempty"`
	Replay            *ResultReplay      `json:"replay,omitempty"`
}

type ResultSignature struct {
	InvariantID    string `json:"invariantId"`
	Classification string `json:"classification"`
	ObservationID  string `json:"observationId"`
	RowKey         string `json:"rowKey"`
	Pointer        string `json:"pointer"`
	Expected       any    `json:"expected"`
	Actual         any    `json:"actual"`
	Digest         string `json:"digest"`
}

type ResultViolation struct {
	Classification string `json:"classification"`
	ObservationID  string `json:"observationId"`
	Pointer        string `json:"pointer"`
	RowKey         string `json:"rowKey"`
	Expected       any    `json:"expected"`
	Actual         any    `json:"actual"`
	ExpectedHash   string `json:"expectedHash"`
	ActualHash     string `json:"actualHash"`
	Message        string `json:"message"`
}

type ResultMinimization struct {
	Status             string                 `json:"status"`
	Minimality         string                 `json:"minimality"`
	OriginalEvents     int                    `json:"originalEvents"`
	FinalEvents        int                    `json:"finalEvents"`
	OriginalActions    int                    `json:"originalActions"`
	FinalActions       int                    `json:"finalActions"`
	Trials             int                    `json:"trials"`
	CacheHits          int                    `json:"cacheHits"`
	AcceptedTransforms []string               `json:"acceptedTransforms"`
	Rejections         []ResultTransformTrial `json:"rejections"`
}

type ResultTransformTrial struct {
	Transform string `json:"transform"`
	Outcome   string `json:"outcome"`
	Reason    string `json:"reason"`
}

type ResultReplay struct {
	SourceBundleSHA256 string `json:"sourceBundleSha256"`
	ExpectedSignature  string `json:"expectedSignature"`
}

type Violation struct {
	Document       string `json:"document"`
	Pointer        string `json:"pointer"`
	Rule           string `json:"rule"`
	Message        string `json:"message"`
	Classification string `json:"classification,omitempty"`
	Expected       any    `json:"expected,omitempty"`
	Actual         any    `json:"actual,omitempty"`
	SignatureHash  string `json:"signatureHash,omitempty"`
}

var classifications = map[string]struct{}{
	"PASS": {}, "SEMANTIC_REGRESSION": {}, "SCHEMA_REGRESSION": {},
	"EXTERNAL_EFFECT_REGRESSION": {}, "PERFORMANCE_REGRESSION": {},
	"INFRASTRUCTURE_ERROR": {}, "TIMEOUT": {}, "FLAKY": {}, "UNRESOLVED": {},
}
