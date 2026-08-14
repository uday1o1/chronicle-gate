// Package report renders one result into ChronicleGate's public report formats.
package report

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"html/template"
	"sort"
	"strings"
)

type Signature struct {
	InvariantID    string `json:"invariantId"`
	Classification string `json:"classification"`
	ObservationID  string `json:"observationId"`
	RowKey         string `json:"rowKey"`
	Pointer        string `json:"pointer"`
	Expected       any    `json:"expected"`
	Actual         any    `json:"actual"`
	Digest         string `json:"digest"`
}

type Minimization struct {
	Status             string      `json:"status"`
	Minimality         string      `json:"minimality"`
	OriginalEvents     int         `json:"originalEvents"`
	FinalEvents        int         `json:"finalEvents"`
	OriginalActions    int         `json:"originalActions"`
	FinalActions       int         `json:"finalActions"`
	Trials             int         `json:"trials"`
	CacheHits          int         `json:"cacheHits"`
	AcceptedTransforms []string    `json:"acceptedTransforms"`
	Rejections         []Rejection `json:"rejections"`
}

type Rejection struct {
	Transform string `json:"transform"`
	Outcome   string `json:"outcome"`
	Reason    string `json:"reason"`
}

type Document struct {
	APIVersion        string            `json:"apiVersion"`
	Kind              string            `json:"kind"`
	RunID             string            `json:"runId"`
	State             string            `json:"state"`
	Classification    string            `json:"classification"`
	StartedAt         string            `json:"startedAt"`
	CompletedAt       string            `json:"completedAt"`
	FailureSignature  *Signature        `json:"failureSignature,omitempty"`
	Violations        []Violation       `json:"violations"`
	Error             string            `json:"error,omitempty"`
	Minimization      Minimization      `json:"minimization"`
	NonportableImages bool              `json:"nonportableImages"`
	Environment       json.RawMessage   `json:"environment"`
	Baseline          json.RawMessage   `json:"baseline,omitempty"`
	Candidate         []json.RawMessage `json:"candidate"`
	Confirmations     int               `json:"confirmations"`
	Bundle            string            `json:"bundle,omitempty"`
	Replay            json.RawMessage   `json:"replay,omitempty"`
	Metadata          map[string]any    `json:"metadata,omitempty"`
	Normalizations    []Normalization   `json:"normalizations,omitempty"`
}

type Normalization struct {
	AttemptID     string `json:"attemptId"`
	StepID        string `json:"stepId"`
	ObservationID string `json:"observationId"`
	Occurrence    int    `json:"occurrence"`
	RuleID        string `json:"ruleId"`
	Type          string `json:"type"`
	Pointer       string `json:"pointer"`
	AffectedCount int    `json:"affectedCount"`
}

type Violation struct {
	Classification string `json:"classification"`
	ObservationID  string `json:"observationId"`
	Pointer        string `json:"pointer"`
	RowKey         string `json:"rowKey"`
	ExpectedHash   string `json:"expectedHash"`
	ActualHash     string `json:"actualHash"`
	Expected       any    `json:"expected"`
	Actual         any    `json:"actual"`
	Message        string `json:"message"`
}

func Decode(document []byte) (Document, error) {
	var value Document
	decoder := json.NewDecoder(bytes.NewReader(document))
	if err := decoder.Decode(&value); err != nil {
		return Document{}, fmt.Errorf("decode result: %w", err)
	}
	if value.RunID == "" || value.Classification == "" {
		return Document{}, fmt.Errorf("result is missing runId or classification")
	}
	if err := collectNormalizations(&value); err != nil {
		return Document{}, err
	}
	return value, nil
}

func collectNormalizations(document *Document) error {
	document.Normalizations = nil
	type applied struct {
		RuleID          string `json:"ruleId"`
		Type            string `json:"type"`
		AuthoredPointer string `json:"authoredPointer"`
		AffectedCount   int    `json:"affectedCount"`
	}
	type observation struct {
		Identity struct {
			StepID     string `json:"stepId"`
			ObserverID string `json:"observerId"`
			Occurrence int    `json:"occurrence"`
		} `json:"identity"`
		Applied []applied `json:"appliedNormalization"`
	}
	type attempt struct {
		AttemptID    string        `json:"attemptId"`
		Observations []observation `json:"observations"`
	}
	values := append([]json.RawMessage{document.Baseline}, document.Candidate...)
	for _, raw := range values {
		if len(raw) == 0 || string(raw) == "null" {
			continue
		}
		var decoded attempt
		if err := json.Unmarshal(raw, &decoded); err != nil {
			return fmt.Errorf("decode attempt normalization evidence: %w", err)
		}
		for _, observed := range decoded.Observations {
			for _, rule := range observed.Applied {
				document.Normalizations = append(document.Normalizations, Normalization{
					AttemptID: decoded.AttemptID, StepID: observed.Identity.StepID,
					ObservationID: observed.Identity.ObserverID, Occurrence: observed.Identity.Occurrence,
					RuleID: rule.RuleID, Type: rule.Type, Pointer: rule.AuthoredPointer, AffectedCount: rule.AffectedCount,
				})
			}
		}
	}
	sort.Slice(document.Normalizations, func(left, right int) bool {
		a, b := document.Normalizations[left], document.Normalizations[right]
		return a.AttemptID+a.StepID+a.ObservationID+fmt.Sprint(a.Occurrence)+a.RuleID < b.AttemptID+b.StepID+b.ObservationID+fmt.Sprint(b.Occurrence)+b.RuleID
	})
	return nil
}

func Render(value any, format string) ([]byte, error) {
	document, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("encode report input: %w", err)
	}
	decoded, err := Decode(document)
	if err != nil {
		return nil, err
	}
	sort.SliceStable(decoded.Violations, func(left, right int) bool {
		a, b := decoded.Violations[left], decoded.Violations[right]
		return a.Classification+a.ObservationID+a.Pointer+a.RowKey+a.ExpectedHash+a.ActualHash < b.Classification+b.ObservationID+b.Pointer+b.RowKey+b.ExpectedHash+b.ActualHash
	})
	switch format {
	case "json":
		pretty, err := json.MarshalIndent(decoded, "", "  ")
		return append(pretty, '\n'), err
	case "text":
		return renderText(decoded), nil
	case "junit":
		return renderJUnit(decoded)
	case "html":
		return renderHTML(decoded)
	default:
		return nil, fmt.Errorf("unsupported report format %q", format)
	}
}

func renderText(document Document) []byte {
	var output strings.Builder
	fmt.Fprintf(&output, "ChronicleGate run %s\n", document.RunID)
	fmt.Fprintf(&output, "classification: %s\n", document.Classification)
	fmt.Fprintf(&output, "state: %s\n", document.State)
	if document.FailureSignature != nil {
		fmt.Fprintf(&output, "signature: %s\n", document.FailureSignature.Digest)
		fmt.Fprintf(&output, "violation: %s %s %s\n", document.FailureSignature.Classification, document.FailureSignature.ObservationID, document.FailureSignature.Pointer)
	}
	if document.Minimization.Status != "" {
		fmt.Fprintf(&output, "minimization: %s (%s), events %d -> %d, actions %d -> %d\n", document.Minimization.Status, document.Minimization.Minimality, document.Minimization.OriginalEvents, document.Minimization.FinalEvents, document.Minimization.OriginalActions, document.Minimization.FinalActions)
	}
	if document.Error != "" {
		fmt.Fprintf(&output, "error: %s\n", document.Error)
	}
	for _, rule := range document.Normalizations {
		fmt.Fprintf(&output, "normalization: %s %s/%s/%d %s %s %s affected=%d\n", rule.AttemptID, rule.StepID, rule.ObservationID, rule.Occurrence, rule.RuleID, rule.Type, rule.Pointer, rule.AffectedCount)
	}
	return []byte(output.String())
}

type testsuite struct {
	XMLName   xml.Name   `xml:"testsuite"`
	Name      string     `xml:"name,attr"`
	Tests     int        `xml:"tests,attr"`
	Failures  int        `xml:"failures,attr"`
	Errors    int        `xml:"errors,attr"`
	Testcase  []testcase `xml:"testcase"`
	SystemOut string     `xml:"system-out,omitempty"`
}

type testcase struct {
	Name    string       `xml:"name,attr"`
	Failure *junitDetail `xml:"failure,omitempty"`
	Error   *junitDetail `xml:"error,omitempty"`
}

type junitDetail struct {
	Message string `xml:"message,attr"`
	Body    string `xml:",chardata"`
}

func renderJUnit(document Document) ([]byte, error) {
	item := testcase{Name: document.RunID}
	suite := testsuite{Name: "ChronicleGate", Tests: 1}
	detail := &junitDetail{Message: document.Classification, Body: document.Error}
	switch document.Classification {
	case "SEMANTIC_REGRESSION", "SCHEMA_REGRESSION", "EXTERNAL_EFFECT_REGRESSION", "PERFORMANCE_REGRESSION":
		suite.Failures = 1
		item.Failure = detail
	case "INFRASTRUCTURE_ERROR", "TIMEOUT", "UNRESOLVED", "FLAKY":
		suite.Errors = 1
		item.Error = detail
	}
	suite.Testcase = []testcase{item}
	if len(document.Normalizations) != 0 {
		summary, err := json.Marshal(document.Normalizations)
		if err != nil {
			return nil, fmt.Errorf("encode JUnit normalization evidence: %w", err)
		}
		suite.SystemOut = string(summary)
	}
	documentXML, err := xml.MarshalIndent(suite, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("render JUnit: %w", err)
	}
	return append([]byte(xml.Header), append(documentXML, '\n')...), nil
}

var page = template.Must(template.New("report").Parse(`<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>ChronicleGate {{.RunID}}</title>
<style>body{font:16px system-ui,sans-serif;max-width:900px;margin:3rem auto;padding:0 1rem;color:#17202a}code{background:#f3f4f6;padding:.15rem .3rem}.result{border-left:.4rem solid #555;padding:1rem;background:#f8fafc}dt{font-weight:700}dd{margin-bottom:.75rem}</style></head>
<body><h1>ChronicleGate result</h1><section class="result"><dl><dt>Run</dt><dd><code>{{.RunID}}</code></dd><dt>Classification</dt><dd>{{.Classification}}</dd><dt>State</dt><dd>{{.State}}</dd>{{if .FailureSignature}}<dt>Signature</dt><dd><code>{{.FailureSignature.Digest}}</code></dd>{{end}}{{if .Error}}<dt>Error</dt><dd>{{.Error}}</dd>{{end}}</dl></section>
<h2>Minimization</h2><p>{{.Minimization.Status}} ({{.Minimization.Minimality}}), events {{.Minimization.OriginalEvents}} to {{.Minimization.FinalEvents}}, actions {{.Minimization.OriginalActions}} to {{.Minimization.FinalActions}}.</p>
{{if .Normalizations}}<h2>Applied normalizations</h2><table><thead><tr><th>Attempt</th><th>Observation</th><th>Rule</th><th>Pointer</th><th>Affected</th></tr></thead><tbody>{{range .Normalizations}}<tr><td><code>{{.AttemptID}}</code></td><td><code>{{.StepID}}/{{.ObservationID}}/{{.Occurrence}}</code></td><td>{{.RuleID}} ({{.Type}})</td><td><code>{{.Pointer}}</code></td><td>{{.AffectedCount}}</td></tr>{{end}}</tbody></table>{{end}}</body></html>
`))

func renderHTML(document Document) ([]byte, error) {
	var output bytes.Buffer
	if err := page.Execute(&output, document); err != nil {
		return nil, fmt.Errorf("render HTML: %w", err)
	}
	return output.Bytes(), nil
}
