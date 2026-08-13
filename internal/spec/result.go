package spec

type Result struct {
	APIVersion     string      `json:"apiVersion"`
	Kind           string      `json:"kind"`
	RunID          string      `json:"runId"`
	State          string      `json:"state"`
	Classification string      `json:"classification"`
	Violations     []Violation `json:"violations"`
	StartedAt      string      `json:"startedAt"`
	CompletedAt    string      `json:"completedAt,omitempty"`
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
