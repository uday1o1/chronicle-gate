package bench

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/uday1o1/chronicle-gate/internal/imagelock"
	"github.com/uday1o1/chronicle-gate/internal/spec"
)

func ValidateInputs(workload spec.BenchmarkWorkload, baseline, candidate spec.Target, allowLocalImages bool) []spec.Violation {
	violations := append([]spec.Violation{}, spec.ValidateBenchmarkWorkload(workload)...)
	options := spec.ValidationOptions{AllowLocalImageIDs: allowLocalImages}
	for _, item := range []struct {
		role   string
		target spec.Target
	}{{"baseline", baseline}, {"candidate", candidate}} {
		role, target := item.role, item.target
		for _, violation := range spec.ValidateTargetWithOptions(target, options) {
			violation.Document = role
			violations = append(violations, violation)
		}
		violations = append(violations, validateBenchmarkTarget(role, workload, target)...)
	}
	if baseline.Spec.DatabaseSchemaVersion != candidate.Spec.DatabaseSchemaVersion {
		violations = append(violations, spec.Violation{Document: "targets", Pointer: "/spec/databaseSchemaVersion", Rule: "equivalent_target", Message: "benchmark targets must declare the same database schema version"})
	}
	if len(baseline.Spec.Services) == 1 && len(candidate.Spec.Services) == 1 {
		left, right := baseline.Spec.Services[0], candidate.Spec.Services[0]
		left.Image, right.Image = "", ""
		if !reflect.DeepEqual(left, right) {
			leftJSON, _ := json.Marshal(left)
			rightJSON, _ := json.Marshal(right)
			violations = append(violations, spec.Violation{Document: "targets", Pointer: "/spec/services/0", Rule: "equivalent_target", Message: fmt.Sprintf("benchmark services may differ only by image identity: baseline=%s candidate=%s", leftJSON, rightJSON)})
		}
	}
	return violations
}

func validateBenchmarkTarget(role string, workload spec.BenchmarkWorkload, target spec.Target) []spec.Violation {
	violations := []spec.Violation{}
	add := func(pointer, rule, message string) {
		violations = append(violations, spec.Violation{Document: role, Pointer: pointer, Rule: rule, Message: message})
	}
	if len(target.Spec.Services) != 1 {
		add("/spec/services", "single_service", "benchmark targets must declare exactly one service")
		return violations
	}
	service := target.Spec.Services[0]
	if workload.Spec.EvidenceScope == "publication" && imagelock.IsLocalImageID(service.Image) {
		add("/spec/services/0/image", "publication_image", "publication benchmarks require a named immutable OCI digest reference")
	}
	if service.Name != workload.Spec.Service {
		add("/spec/services/0/name", "benchmark_service", "target service does not match workload service")
	}
	if service.Health.Type != "http" || service.Health.Path == "" || service.Health.Port == 0 {
		add("/spec/services/0/health", "http_health", "benchmark service requires local HTTP health")
	}
	if service.Probe.Enabled || service.Probe.ProtocolVersion != "" || service.Probe.CommitMode != "" || service.Probe.MaxControlledInFlight != 0 || len(service.Probe.Checkpoints) != 0 || service.Probe.LogicalClock {
		add("/spec/services/0/probe", "instrumentation_absence", "benchmark target probe declaration must be completely disabled")
	}
	if len(service.SecretEnvironment) != 0 || len(service.Dependencies) != 0 {
		add("/spec/services/0", "isolated_target", "benchmark target cannot declare secrets or service dependencies")
	}
	names := make([]string, 0, len(service.Environment))
	for name := range service.Environment {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		upper := strings.ToUpper(name)
		if strings.HasPrefix(upper, "CHRONICLE_") || strings.HasPrefix(upper, "OTEL_") || strings.Contains(upper, "DEBUG") || strings.Contains(upper, "PROFILE") || strings.Contains(upper, "DELAY") {
			add("/spec/services/0/environment/"+name, "instrumentation_absence", "benchmark environment contains a reserved instrumentation or behavior override")
		}
	}
	return violations
}
