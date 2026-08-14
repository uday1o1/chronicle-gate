package spec

import (
	"fmt"
	"math"
	"net/http"
	"strings"
	"time"
)

const (
	BenchmarkScheduleAlgorithm  = "chronicle-schedule-v1"
	BenchmarkBootstrapAlgorithm = "paired-percentile-bootstrap-v1"
	maxBenchmarkOperations      = 32
	maxBenchmarkRate            = 500
	maxBenchmarkRounds          = 20
	maxBenchmarkRequests        = 1_000_000
)

// BenchmarkWorkload is the independently configured performance contract.
type BenchmarkWorkload struct {
	APIVersion string                `json:"apiVersion" yaml:"apiVersion"`
	Kind       string                `json:"kind" yaml:"kind"`
	Metadata   Metadata              `json:"metadata" yaml:"metadata"`
	Spec       BenchmarkWorkloadSpec `json:"spec" yaml:"spec"`
}

type BenchmarkWorkloadSpec struct {
	Service                string               `json:"service" yaml:"service"`
	EvidenceScope          string               `json:"evidenceScope" yaml:"evidenceScope"`
	Operations             []BenchmarkOperation `json:"operations" yaml:"operations"`
	Schedule               BenchmarkSchedule    `json:"schedule" yaml:"schedule"`
	Analysis               BenchmarkAnalysis    `json:"analysis" yaml:"analysis"`
	Validity               BenchmarkValidity    `json:"validity" yaml:"validity"`
	ResourceSampleInterval Duration             `json:"resourceSampleInterval" yaml:"resourceSampleInterval"`
}

type BenchmarkOperation struct {
	ID               string            `json:"id" yaml:"id"`
	Weight           int               `json:"weight" yaml:"weight"`
	Method           string            `json:"method" yaml:"method"`
	Path             string            `json:"path" yaml:"path"`
	Headers          map[string]string `json:"headers" yaml:"headers"`
	Body             string            `json:"body" yaml:"body"`
	ExpectedStatuses []int             `json:"expectedStatuses" yaml:"expectedStatuses"`
}

type BenchmarkSchedule struct {
	Algorithm      string   `json:"algorithm" yaml:"algorithm"`
	RatePerSecond  int      `json:"ratePerSecond" yaml:"ratePerSecond"`
	Warmup         Duration `json:"warmup" yaml:"warmup"`
	Measurement    Duration `json:"measurement" yaml:"measurement"`
	RequestTimeout Duration `json:"requestTimeout" yaml:"requestTimeout"`
	MaxInFlight    int      `json:"maxInFlight" yaml:"maxInFlight"`
	MaxScheduleLag Duration `json:"maxScheduleLag" yaml:"maxScheduleLag"`
	Rounds         int      `json:"rounds" yaml:"rounds"`
	OrderSeed      int64    `json:"orderSeed" yaml:"orderSeed"`
	RequestSeed    int64    `json:"requestSeed" yaml:"requestSeed"`
}

type BenchmarkAnalysis struct {
	Algorithm           string   `json:"algorithm" yaml:"algorithm"`
	BootstrapSeed       int64    `json:"bootstrapSeed" yaml:"bootstrapSeed"`
	BootstrapResamples  int      `json:"bootstrapResamples" yaml:"bootstrapResamples"`
	Confidence          float64  `json:"confidence" yaml:"confidence"`
	BlockSize           int      `json:"blockSize" yaml:"blockSize"`
	MinRelativeP95Delta float64  `json:"minRelativeP95Delta" yaml:"minRelativeP95Delta"`
	MinAbsoluteP95Delta Duration `json:"minAbsoluteP95Delta" yaml:"minAbsoluteP95Delta"`
}

type BenchmarkValidity struct {
	MaxResponseHeaderBytes int64 `json:"maxResponseHeaderBytes" yaml:"maxResponseHeaderBytes"`
	MaxResponseBodyBytes   int64 `json:"maxResponseBodyBytes" yaml:"maxResponseBodyBytes"`
	MaxRawTimingBytes      int64 `json:"maxRawTimingBytes" yaml:"maxRawTimingBytes"`
	MaxResourceBytes       int64 `json:"maxResourceBytes" yaml:"maxResourceBytes"`
	MaxOutputBytes         int64 `json:"maxOutputBytes" yaml:"maxOutputBytes"`
}

// ValidateBenchmarkWorkload validates all derived bounds before Docker access.
func ValidateBenchmarkWorkload(workload BenchmarkWorkload) []Violation {
	validator := validation{document: "workload"}
	if workload.APIVersion != APIVersion {
		validator.add("/apiVersion", "api_version", "apiVersion must be chronicle.dev/v1alpha1")
	}
	if workload.Kind != "BenchmarkWorkload" {
		validator.add("/kind", "kind", "kind must be BenchmarkWorkload")
	}
	specification := workload.Spec
	if specification.Service == "" {
		validator.add("/spec/service", "service", "benchmark service is required")
	}
	if specification.EvidenceScope != "local-development" && specification.EvidenceScope != "publication" {
		validator.add("/spec/evidenceScope", "evidence_scope", "evidenceScope must be local-development or publication")
	}
	if len(specification.Operations) == 0 || len(specification.Operations) > maxBenchmarkOperations {
		validator.add("/spec/operations", "operation_count", fmt.Sprintf("operations must contain 1 through %d entries", maxBenchmarkOperations))
	}
	seen := map[string]struct{}{}
	totalWeight := int64(0)
	for index, operation := range specification.Operations {
		pointer := fmt.Sprintf("/spec/operations/%d", index)
		if !validBenchmarkIdentifier(operation.ID) {
			validator.add(pointer+"/id", "operation_id", "benchmark operation ID must use 1 through 64 lowercase letters, digits, or hyphens")
		}
		if _, exists := seen[operation.ID]; exists {
			validator.add(pointer+"/id", "unique_operation", "benchmark operation IDs must be unique")
		}
		seen[operation.ID] = struct{}{}
		if operation.Weight <= 0 {
			validator.add(pointer+"/weight", "operation_weight", "operation weight must be positive")
		} else if totalWeight > math.MaxInt64-int64(operation.Weight) {
			validator.add(pointer+"/weight", "integer_overflow", "operation weight total overflows")
		} else {
			totalWeight += int64(operation.Weight)
		}
		if operation.Method != http.MethodGet && operation.Method != http.MethodPost {
			validator.add(pointer+"/method", "http_method", "benchmark method must be GET or POST")
		}
		if err := validateBenchmarkPath(operation.Path); err != nil {
			validator.add(pointer+"/path", "loopback_path", err.Error())
		}
		if len(operation.Body) > 64<<10 {
			validator.add(pointer+"/body", "request_body_limit", "benchmark request body exceeds 65536 bytes")
		}
		headerBytes := 0
		if len(operation.Headers) > 64 {
			validator.add(pointer+"/headers", "request_header_count", "benchmark operation exceeds 64 headers")
		}
		for name, value := range operation.Headers {
			headerBytes += len(name) + len(value)
			if reservedBenchmarkHeader(name) || !validBenchmarkHeaderName(name) || !validBenchmarkHeaderValue(value) {
				validator.add(pointer+"/headers", "request_header", fmt.Sprintf("header %q is reserved or unsafe", name))
			}
		}
		if headerBytes > 8<<10 {
			validator.add(pointer+"/headers", "request_header_bytes", "benchmark request headers exceed 8192 bytes")
		}
		if len(operation.ExpectedStatuses) == 0 {
			validator.add(pointer+"/expectedStatuses", "expected_status", "at least one expected status is required")
		}
		statuses := map[int]struct{}{}
		for _, status := range operation.ExpectedStatuses {
			if status < 100 || status > 599 {
				validator.add(pointer+"/expectedStatuses", "expected_status", "expected statuses must be valid three-digit HTTP status codes")
			}
			if _, duplicate := statuses[status]; duplicate {
				validator.add(pointer+"/expectedStatuses", "expected_status", "expected statuses must be unique")
			}
			statuses[status] = struct{}{}
		}
	}
	schedule := specification.Schedule
	if schedule.Algorithm != BenchmarkScheduleAlgorithm {
		validator.add("/spec/schedule/algorithm", "schedule_algorithm", "schedule algorithm must be chronicle-schedule-v1")
	}
	if schedule.RatePerSecond < 1 || schedule.RatePerSecond > maxBenchmarkRate {
		validator.add("/spec/schedule/ratePerSecond", "rate_bound", "ratePerSecond must be between 1 and 500")
	}
	if schedule.Rounds < 4 || schedule.Rounds > maxBenchmarkRounds || schedule.Rounds%2 != 0 {
		validator.add("/spec/schedule/rounds", "round_bound", "rounds must be even and between 4 and 20")
	}
	if schedule.MaxInFlight < 1 || schedule.MaxInFlight > 512 {
		validator.add("/spec/schedule/maxInFlight", "in_flight_bound", "maxInFlight must be between 1 and 512")
	}
	validateBenchmarkDuration(&validator, "/spec/schedule/warmup", schedule.Warmup.Duration, 100*time.Millisecond, time.Minute)
	validateBenchmarkDuration(&validator, "/spec/schedule/measurement", schedule.Measurement.Duration, time.Second, 5*time.Minute)
	validateBenchmarkDuration(&validator, "/spec/schedule/requestTimeout", schedule.RequestTimeout.Duration, time.Millisecond, 30*time.Second)
	validateBenchmarkDuration(&validator, "/spec/schedule/maxScheduleLag", schedule.MaxScheduleLag.Duration, time.Millisecond, time.Second)
	validateBenchmarkDuration(&validator, "/spec/resourceSampleInterval", specification.ResourceSampleInterval.Duration, 10*time.Millisecond, time.Second)
	analysis := specification.Analysis
	if analysis.Algorithm != BenchmarkBootstrapAlgorithm || analysis.BlockSize != 1 {
		validator.add("/spec/analysis", "analysis_algorithm", "analysis must use paired-percentile-bootstrap-v1 with blockSize 1")
	}
	if analysis.BootstrapResamples < 1_000 || analysis.BootstrapResamples > 100_000 {
		validator.add("/spec/analysis/bootstrapResamples", "bootstrap_bound", "bootstrapResamples must be between 1000 and 100000")
	}
	if analysis.Confidence < 0.8 || analysis.Confidence > 0.999 {
		validator.add("/spec/analysis/confidence", "confidence_bound", "confidence must be between 0.8 and 0.999")
	}
	if analysis.MinRelativeP95Delta < 0 || analysis.MinRelativeP95Delta > 10 {
		validator.add("/spec/analysis/minRelativeP95Delta", "threshold_bound", "relative p95 threshold must be between 0 and 10")
	}
	validateBenchmarkDuration(&validator, "/spec/analysis/minAbsoluteP95Delta", analysis.MinAbsoluteP95Delta.Duration, time.Nanosecond, time.Minute)
	validity := specification.Validity
	if validity.MaxResponseHeaderBytes < 1024 || validity.MaxResponseHeaderBytes > 32<<10 {
		validator.add("/spec/validity/maxResponseHeaderBytes", "response_header_bound", "response header limit must be between 1024 and 32768 bytes")
	}
	if validity.MaxResponseBodyBytes < 1 || validity.MaxResponseBodyBytes > 1<<20 {
		validator.add("/spec/validity/maxResponseBodyBytes", "response_body_bound", "response body limit must be between 1 and 1048576 bytes")
	}
	if validity.MaxRawTimingBytes < 1 || validity.MaxRawTimingBytes > 1<<30 || validity.MaxResourceBytes < 1 || validity.MaxResourceBytes > 256<<20 || validity.MaxOutputBytes < 1 || validity.MaxOutputBytes > 1536<<20 {
		validator.add("/spec/validity", "artifact_bounds", "benchmark artifact limits exceed the V1 bounds")
	}
	if validity.MaxRawTimingBytes+validity.MaxResourceBytes > validity.MaxOutputBytes {
		validator.add("/spec/validity/maxOutputBytes", "artifact_aggregate", "output limit is smaller than the raw timing and resource budgets")
	}
	if schedule.RatePerSecond > 0 && schedule.Rounds > 0 {
		perTrial := derivedRequestCount(schedule.RatePerSecond, schedule.Warmup.Duration) + derivedRequestCount(schedule.RatePerSecond, schedule.Measurement.Duration)
		if perTrial < 0 || perTrial > maxBenchmarkRequests || int64(schedule.Rounds) > math.MaxInt64/(2*maxInt64(1, perTrial)) || 2*int64(schedule.Rounds)*perTrial > maxBenchmarkRequests {
			validator.add("/spec/schedule", "request_inventory", "derived request inventory exceeds 1000000")
		}
		perTrialDuration, durationOK := checkedDurationSum(schedule.Warmup.Duration, schedule.Measurement.Duration, schedule.RequestTimeout.Duration)
		if !durationOK || perTrialDuration <= 0 || int64(schedule.Rounds) > math.MaxInt64/(2*int64(perTrialDuration)) || 2*time.Duration(schedule.Rounds)*perTrialDuration > 2*time.Hour {
			validator.add("/spec/schedule", "run_bound", "derived benchmark duration exceeds two hours")
		}
	}
	return sortedViolations(validator.violations)
}

func validBenchmarkIdentifier(value string) bool {
	if len(value) < 1 || len(value) > 64 || value[0] == '-' || value[len(value)-1] == '-' {
		return false
	}
	for _, character := range value {
		if character != '-' && (character < 'a' || character > 'z') && (character < '0' || character > '9') {
			return false
		}
	}
	return true
}

func validBenchmarkHeaderName(value string) bool {
	if value == "" {
		return false
	}
	for _, character := range []byte(value) {
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || strings.ContainsRune("!#$%&'*+-.^_`|~", rune(character)) {
			continue
		}
		return false
	}
	return true
}

func validBenchmarkHeaderValue(value string) bool {
	for _, character := range []byte(value) {
		if character == '\t' || character >= 0x20 && character != 0x7f {
			continue
		}
		return false
	}
	return true
}

func reservedBenchmarkHeader(name string) bool {
	for _, reserved := range []string{
		"Accept-Encoding", "Connection", "Content-Length", "Host", "Keep-Alive", "Proxy-Authenticate",
		"Proxy-Authorization", "TE", "Trailer", "Transfer-Encoding", "Upgrade",
	} {
		if strings.EqualFold(name, reserved) {
			return true
		}
	}
	return false
}

func validateBenchmarkDuration(validator *validation, pointer string, value, minimum, maximum time.Duration) {
	if value < minimum || value > maximum {
		validator.add(pointer, "duration_bound", fmt.Sprintf("duration must be between %s and %s", minimum, maximum))
	}
}

func validateBenchmarkPath(value string) error {
	if value == "" || value[0] != '/' || strings.HasPrefix(value, "//") || strings.ContainsAny(value, "\\\r\n\x00#") || strings.Contains(value, "://") {
		return fmt.Errorf("path must be a local absolute-path reference")
	}
	for _, part := range strings.Split(value, "/") {
		lower := strings.ToLower(part)
		if part == "." || part == ".." || strings.Contains(lower, "%2f") || strings.Contains(lower, "%5c") || strings.Contains(lower, "%2e") {
			return fmt.Errorf("path contains an unsafe segment")
		}
	}
	return nil
}

func derivedRequestCount(rate int, duration time.Duration) int64 {
	if rate <= 0 || duration <= 0 || int64(rate) > math.MaxInt64/int64(duration) {
		return -1
	}
	return int64(duration) * int64(rate) / int64(time.Second)
}

func maxInt64(left, right int64) int64 {
	if left > right {
		return left
	}
	return right
}

func checkedDurationSum(values ...time.Duration) (time.Duration, bool) {
	total := time.Duration(0)
	for _, value := range values {
		if value > 0 && total > time.Duration(math.MaxInt64)-value || value < 0 && total < time.Duration(math.MinInt64)-value {
			return 0, false
		}
		total += value
	}
	return total, true
}
