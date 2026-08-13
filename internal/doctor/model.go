package doctor

const ReportSchemaVersion = "chronicle.dev/doctor/v1alpha1"

// Status is a stable machine-readable diagnostic state.
type Status string

const (
	StatusPass Status = "pass"
	StatusFail Status = "fail"
	StatusSkip Status = "skip"
)

// Report is emitted by chronicle doctor.
type Report struct {
	SchemaVersion string  `json:"schemaVersion"`
	Status        Status  `json:"status"`
	Checks        []Check `json:"checks"`
}

// Check is one ordered diagnostic result.
type Check struct {
	ID      string         `json:"id"`
	Scope   string         `json:"scope"`
	Status  Status         `json:"status"`
	Summary string         `json:"summary"`
	Details map[string]any `json:"details,omitempty"`
}

func newReport(checks []Check) Report {
	status := StatusPass
	for _, check := range checks {
		if check.Status == StatusFail {
			status = StatusFail
			break
		}
	}
	return Report{SchemaVersion: ReportSchemaVersion, Status: status, Checks: checks}
}
