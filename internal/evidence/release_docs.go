package evidence

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"unicode"

	"github.com/uday1o1/chronicle-gate/internal/artifact"
)

var (
	markdownLinkPattern = regexp.MustCompile(`\[[^\]]*\]\(([^)]+)\)`)
	measuredPattern     = regexp.MustCompile(`<!-- measured: (evidence/results/[A-Za-z0-9._-]+\.json)#(/[^ ]*) -->`)
	secondSentence      = regexp.MustCompile(`[.!?][ \t]+[A-Z0-9` + "`" + `]`)
)

var requiredPublicDocuments = []string{
	"README.md",
	"docs/architecture.md",
	"docs/benchmarking.md",
	"docs/controlled-schedules.md",
	"docs/limitations.md",
	"docs/methodology.md",
	"docs/observer-model.md",
	"docs/outbox-qualification.md",
	"docs/portfolio-core.md",
	"docs/reproduction.md",
	"docs/results.md",
	"docs/resume.md",
	"docs/security-model.md",
	"demo/r1-transcript.md",
}

const (
	benchmarkResultsStart = "<!-- benchmark-results:start -->"
	benchmarkResultsEnd   = "<!-- benchmark-results:end -->"
	benchmarkResumeStart  = "<!-- benchmark-resume:start -->"
	benchmarkResumeEnd    = "<!-- benchmark-resume:end -->"
)

func checkPublicDocumentation(repository string) error {
	for _, path := range requiredPublicDocuments {
		if err := checkPublicMarkdown(repository, path); err != nil {
			return err
		}
	}
	if err := checkREADMEOrder(repository); err != nil {
		return err
	}
	if err := checkMeasuredEvidenceReferences(repository); err != nil {
		return err
	}
	if err := checkBenchmarkDocumentation(repository); err != nil {
		return err
	}
	return checkDemo(repository)
}

func checkBenchmarkDocumentation(repository string) error {
	evidencePath := filepath.Join(repository, "evidence", "results", "benchmark.json")
	public, err := loadStrict[PublicBenchmarkEvidence](evidencePath)
	if err != nil {
		return err
	}
	if err := validatePublicBenchmark(public); err != nil {
		return err
	}
	resultsBlock, err := renderBenchmarkResultsBlock(public)
	if err != nil {
		return err
	}
	resumeBlock, err := renderBenchmarkResumeBlock(public)
	if err != nil {
		return err
	}
	for _, check := range []struct {
		path, start, end, expected string
	}{
		{"docs/results.md", benchmarkResultsStart, benchmarkResultsEnd, resultsBlock},
		{"docs/resume.md", benchmarkResumeStart, benchmarkResumeEnd, resumeBlock},
	} {
		document, err := os.ReadFile(filepath.Join(repository, filepath.FromSlash(check.path)))
		if err != nil {
			return err
		}
		if err := requireExactMarkedBlock(string(document), check.start, check.end, check.expected); err != nil {
			return fmt.Errorf("public document %s: %w", check.path, err)
		}
	}
	return nil
}

func renderBenchmarkResultsBlock(public PublicBenchmarkEvidence) (string, error) {
	if err := validatePublicBenchmark(public); err != nil {
		return "", err
	}
	var output strings.Builder
	output.WriteString(benchmarkResultsStart + "\n")
	output.WriteString("| Comparison | Rounds | Requests per target | Pooled descriptive baseline p95 | Pooled descriptive candidate p95 | Mean paired absolute p95 delta | Mean paired relative p95 delta and confidence interval | Result |\n")
	output.WriteString("| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |\n")
	for _, comparison := range public.Comparisons {
		outcome := comparison.Outcome
		analysis := outcome.Analysis
		label := "A/A"
		if comparison.CaseID == "benchmark-slowdown" {
			label = "Seeded slowdown"
		}
		_, _ = fmt.Fprintf(&output,
			"| %s | %d | %d | %s ns | %s ns | %s ns | %s%% (%s%% CI: %s%% to %s%%) | `%s` |\n",
			label,
			outcome.Rounds,
			outcome.BaselineRequests,
			formatGroupedInteger(outcome.PooledBaselineP95Nanos),
			formatGroupedInteger(outcome.PooledCandidateP95Nanos),
			formatGroupedFloat(analysis.MeanAbsoluteP95DeltaNanos, 2),
			formatRatioPercent(analysis.MeanRelativeP95Delta),
			formatRatioPercent(analysis.Confidence),
			formatRatioPercent(analysis.LowerRelativeCI),
			formatRatioPercent(analysis.UpperRelativeCI),
			outcome.Classification,
		)
	}
	output.WriteString(benchmarkResultsEnd)
	return output.String(), nil
}

func renderBenchmarkResumeBlock(public PublicBenchmarkEvidence) (string, error) {
	if err := validatePublicBenchmark(public); err != nil {
		return "", err
	}
	slowdown := public.Comparisons[1].Outcome.Analysis
	return fmt.Sprintf(
		"%s\n- Built an isolated paired open-loop benchmark gate whose A/A control passed and whose seeded slowdown produced a mean paired relative p95 increase of %s%% with a %s%% paired-bootstrap confidence interval from %s%% to %s%%, plus a separately measured mean paired absolute p95 increase of %s ns.\n%s",
		benchmarkResumeStart,
		formatRatioPercent(slowdown.MeanRelativeP95Delta),
		formatRatioPercent(slowdown.Confidence),
		formatRatioPercent(slowdown.LowerRelativeCI),
		formatRatioPercent(slowdown.UpperRelativeCI),
		formatGroupedFloat(slowdown.MeanAbsoluteP95DeltaNanos, 2),
		benchmarkResumeEnd,
	), nil
}

func requireExactMarkedBlock(document, start, end, expected string) error {
	if strings.Count(document, start) != 1 || strings.Count(document, end) != 1 {
		return fmt.Errorf("benchmark evidence block markers are missing or duplicated")
	}
	startIndex := strings.Index(document, start)
	endIndex := strings.Index(document, end)
	if endIndex < startIndex {
		return fmt.Errorf("benchmark evidence block markers are out of order")
	}
	actual := document[startIndex : endIndex+len(end)]
	if actual != expected {
		return fmt.Errorf("benchmark evidence block differs from canonical public evidence")
	}
	return nil
}

func formatRatioPercent(value float64) string {
	return strconv.FormatFloat(value*100, 'f', 6, 64)
}

func formatGroupedFloat(value float64, precision int) string {
	formatted := strconv.FormatFloat(value, 'f', precision, 64)
	parts := strings.SplitN(formatted, ".", 2)
	grouped := formatGroupedDecimal(parts[0])
	if len(parts) == 2 {
		return grouped + "." + parts[1]
	}
	return grouped
}

func formatGroupedInteger(value int64) string {
	return formatGroupedDecimal(strconv.FormatInt(value, 10))
}

func formatGroupedDecimal(value string) string {
	sign := ""
	if strings.HasPrefix(value, "-") {
		sign = "-"
		value = strings.TrimPrefix(value, "-")
	}
	for index := len(value) - 3; index > 0; index -= 3 {
		value = value[:index] + "," + value[index:]
	}
	return sign + value
}

func checkPublicMarkdown(repository, relative string) error {
	path := filepath.Join(repository, filepath.FromSlash(relative))
	document, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read public document %s: %w", relative, err)
	}
	if err := artifact.ValidatePublic(document, nil); err != nil {
		return fmt.Errorf("public document %s failed secret screening: %w", relative, err)
	}
	lower := strings.ToLower(string(document))
	for _, forbidden := range []string{"\u2014", "todo", "fixme", "tbd", "/users/", "/home/", "/private/tmp/", "placeholder"} {
		if strings.Contains(lower, forbidden) {
			return fmt.Errorf("public document %s contains forbidden private or unfinished text %q", relative, forbidden)
		}
	}
	if err := checkSentenceLines(relative, document); err != nil {
		return err
	}
	return checkMarkdownLinks(repository, relative, document)
}

func checkSentenceLines(relative string, document []byte) error {
	scanner := bufio.NewScanner(bytes.NewReader(document))
	lineNumber := 0
	inFence := false
	for scanner.Scan() {
		lineNumber++
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "```") {
			inFence = !inFence
			continue
		}
		if inFence || line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "|") || strings.HasPrefix(line, "<!--") {
			continue
		}
		if secondSentence.MatchString(line) {
			return fmt.Errorf("public document %s line %d contains multiple physical-line sentences", relative, lineNumber)
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if inFence {
		return fmt.Errorf("public document %s has an unterminated code fence", relative)
	}
	return nil
}

func checkMarkdownLinks(repository, relative string, document []byte) error {
	for _, match := range markdownLinkPattern.FindAllSubmatch(document, -1) {
		destination := strings.TrimSpace(string(match[1]))
		if fields := strings.Fields(destination); len(fields) > 0 {
			destination = strings.Trim(fields[0], "<>")
		}
		if destination == "" || strings.HasPrefix(destination, "#") || strings.HasPrefix(destination, "http://") || strings.HasPrefix(destination, "https://") || strings.HasPrefix(destination, "mailto:") {
			continue
		}
		parts := strings.SplitN(destination, "#", 2)
		target := filepath.Clean(filepath.Join(repository, filepath.Dir(filepath.FromSlash(relative)), filepath.FromSlash(parts[0])))
		inside, err := filepath.Rel(repository, target)
		if err != nil || inside == ".." || strings.HasPrefix(inside, ".."+string(filepath.Separator)) {
			return fmt.Errorf("public document %s link escapes the repository: %s", relative, destination)
		}
		info, err := os.Stat(target)
		if err != nil {
			return fmt.Errorf("public document %s has broken link %s", relative, destination)
		}
		if len(parts) == 2 && parts[1] != "" {
			if info.IsDir() {
				return fmt.Errorf("public document %s links a fragment on a directory: %s", relative, destination)
			}
			if err := checkMarkdownAnchor(target, parts[1]); err != nil {
				return fmt.Errorf("public document %s has broken anchor %s: %w", relative, destination, err)
			}
		}
	}
	return nil
}

func checkMarkdownAnchor(path, want string) error {
	document, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	anchors := map[string]int{}
	for _, line := range strings.Split(string(document), "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, "#") {
			continue
		}
		title := strings.TrimSpace(strings.TrimLeft(trimmed, "#"))
		anchor := githubAnchor(title)
		occurrence := anchors[anchor]
		anchors[anchor]++
		if occurrence > 0 {
			anchor += "-" + strconv.Itoa(occurrence)
		}
		if anchor == want {
			return nil
		}
	}
	return fmt.Errorf("anchor not found")
}

func githubAnchor(value string) string {
	var output strings.Builder
	for _, character := range strings.ToLower(value) {
		switch {
		case unicode.IsLetter(character), unicode.IsDigit(character), character == '-', character == '_':
			output.WriteRune(character)
		case unicode.IsSpace(character):
			output.WriteByte('-')
		}
	}
	return output.String()
}

func checkREADMEOrder(repository string) error {
	document, err := os.ReadFile(filepath.Join(repository, "README.md"))
	if err != nil {
		return err
	}
	text := string(document)
	headings := []string{"## Problem", "## Boundary", "## Architecture", "## Quick demo", "## Measured results", "## Limitations", "## Build and internals"}
	previous := -1
	for _, heading := range headings {
		index := strings.Index(text, heading)
		if index < 0 || index <= previous {
			return fmt.Errorf("README public section order is missing or invalid at %s", heading)
		}
		previous = index
	}
	if !strings.Contains(text, "Extended V1 is complete through Milestone 10") {
		return fmt.Errorf("README does not label the completed extended V1 gate")
	}
	return nil
}

func checkMeasuredEvidenceReferences(repository string) error {
	requiredCounts := map[string]int{"README.md": 3, "docs/results.md": 14, "docs/resume.md": 3, "demo/r1-transcript.md": 5}
	for relative, minimum := range requiredCounts {
		document, err := os.ReadFile(filepath.Join(repository, filepath.FromSlash(relative)))
		if err != nil {
			return err
		}
		matches := measuredPattern.FindAllSubmatch(document, -1)
		if bytes.Count(document, []byte("<!-- measured:")) != len(matches) {
			return fmt.Errorf("public document %s contains a malformed measured reference", relative)
		}
		if len(matches) < minimum {
			return fmt.Errorf("public document %s has %d measured references, want at least %d", relative, len(matches), minimum)
		}
		for _, match := range matches {
			if err := resolveMeasuredPointer(repository, string(match[1]), string(match[2])); err != nil {
				return fmt.Errorf("public document %s has invalid measured reference: %w", relative, err)
			}
		}
	}
	return nil
}

func resolveMeasuredPointer(repository, relative, pointer string) error {
	path := filepath.Join(repository, filepath.FromSlash(relative))
	document, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var value any
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return err
	}
	for _, token := range strings.Split(strings.TrimPrefix(pointer, "/"), "/") {
		token = strings.ReplaceAll(strings.ReplaceAll(token, "~1", "/"), "~0", "~")
		switch typed := value.(type) {
		case map[string]any:
			var exists bool
			value, exists = typed[token]
			if !exists {
				return fmt.Errorf("JSON pointer %s is absent from %s", pointer, relative)
			}
		case []any:
			index, err := strconv.Atoi(token)
			if err != nil || index < 0 || index >= len(typed) {
				return fmt.Errorf("JSON pointer %s has an invalid array index in %s", pointer, relative)
			}
			value = typed[index]
		default:
			return fmt.Errorf("JSON pointer %s traverses a scalar in %s", pointer, relative)
		}
	}
	if value == nil {
		return fmt.Errorf("JSON pointer %s resolves to null in %s", pointer, relative)
	}
	return nil
}

func checkDemo(repository string) error {
	regression, err := loadStrict[PublicCaseEvidence](filepath.Join(repository, "evidence", "results", "r1-offset-rewind.json"))
	if err != nil {
		return err
	}
	control, err := loadStrict[PublicCaseEvidence](filepath.Join(repository, "evidence", "results", "r1-single-delivery-control.json"))
	if err != nil {
		return err
	}
	expected, err := expectedDemoOutput(regression, control)
	if err != nil {
		return err
	}
	cast, err := readCastOutput(filepath.Join(repository, "demo", "r1.cast"))
	if err != nil {
		return err
	}
	if cast != expected {
		return fmt.Errorf("demo cast does not match the verified public R1 evidence")
	}
	transcript, err := readDemoTranscript(filepath.Join(repository, "demo", "r1-transcript.md"))
	if err != nil {
		return err
	}
	if transcript != expected {
		return fmt.Errorf("demo transcript and cast output differ")
	}
	return nil
}

func expectedDemoOutput(regression, control PublicCaseEvidence) (string, error) {
	var signature map[string]any
	if err := json.Unmarshal(regression.Outcome.FailureSignature, &signature); err != nil {
		return "", err
	}
	digest, ok := signature["digest"].(string)
	if !ok || digest == "" {
		return "", fmt.Errorf("R1 public signature has no digest")
	}
	lines := []string{
		"ChronicleGate R1 verified demo",
		"$ " + strings.Join(regression.Source.Argv, " "),
		fmt.Sprintf("exit: %d", regression.Outcome.ExitCode),
		"classification: " + regression.Outcome.Classification,
		"signature: " + digest,
		fmt.Sprintf("confirmations: %d", regression.Outcome.Confirmations),
		fmt.Sprintf("reduction: events %d -> %d, actions %d -> %d, trials %d, minimality %s", regression.Reduction.OriginalEvents, regression.Reduction.FinalEvents, regression.Reduction.OriginalActions, regression.Reduction.FinalActions, regression.Reduction.Trials, regression.Reduction.Minimality),
		"$ " + strings.Join(control.Source.Argv, " "),
		fmt.Sprintf("exit: %d", control.Outcome.ExitCode),
		"classification: " + control.Outcome.Classification,
		"artifacts: checksummed reports, authoritative journal, verified regression bundle",
		"boundary: trusted synthetic workload, locked local Docker environment, development-local images",
	}
	return strings.Join(lines, "\r\n") + "\r\n", nil
}

func readCastOutput(path string) (string, error) {
	document, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	if err := artifact.ValidatePublic(document, nil); err != nil {
		return "", fmt.Errorf("demo cast failed secret screening: %w", err)
	}
	lines := bytes.Split(bytes.TrimSpace(document), []byte{'\n'})
	if len(lines) < 2 {
		return "", fmt.Errorf("demo cast is incomplete")
	}
	var header struct {
		Version int               `json:"version"`
		Width   int               `json:"width"`
		Height  int               `json:"height"`
		Env     map[string]string `json:"env"`
	}
	decoder := json.NewDecoder(bytes.NewReader(lines[0]))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&header); err != nil || requireJSONEOF(decoder) != nil || header.Version != 2 || header.Width != 100 || header.Height != 28 || len(header.Env) != 2 || header.Env["SHELL"] != "/bin/sh" || header.Env["TERM"] != "xterm-256color" {
		return "", fmt.Errorf("demo cast header is invalid")
	}
	var output strings.Builder
	lastTime := -1.0
	for _, line := range lines[1:] {
		var event []json.RawMessage
		if err := json.Unmarshal(line, &event); err != nil || len(event) != 3 {
			return "", fmt.Errorf("demo cast event is invalid")
		}
		var timestamp float64
		var kind, data string
		if json.Unmarshal(event[0], &timestamp) != nil || json.Unmarshal(event[1], &kind) != nil || json.Unmarshal(event[2], &data) != nil || !isFinite(timestamp) || timestamp <= lastTime || timestamp > 30 || kind != "o" || strings.ContainsRune(data, '\x1b') {
			return "", fmt.Errorf("demo cast event contract is invalid")
		}
		lastTime = timestamp
		output.WriteString(data)
	}
	return output.String(), nil
}

func isFinite(value float64) bool {
	return !math.IsInf(value, 0) && !math.IsNaN(value)
}

func readDemoTranscript(path string) (string, error) {
	document, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	startMarker := []byte("<!-- demo-output:start -->\n```text\n")
	endMarker := []byte("```\n<!-- demo-output:end -->")
	start := bytes.Index(document, startMarker)
	if start < 0 {
		return "", fmt.Errorf("demo transcript start marker is absent")
	}
	start += len(startMarker)
	end := bytes.Index(document[start:], endMarker)
	if end < 0 {
		return "", fmt.Errorf("demo transcript end marker is absent")
	}
	text := string(document[start : start+end])
	text = strings.ReplaceAll(text, "\n", "\r\n")
	return text, nil
}
