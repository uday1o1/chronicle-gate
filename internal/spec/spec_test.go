package spec

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"
)

func repositoryPath(parts ...string) string {
	return filepath.Join(append([]string{"..", ".."}, parts...)...)
}

func TestSchemaAssetsMatchPublicSchemas(t *testing.T) {
	t.Parallel()

	for _, name := range schemaFiles {
		name := name
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			embedded, err := EmbeddedSchema(name)
			if err != nil {
				t.Fatalf("EmbeddedSchema() error = %v", err)
			}
			public, err := os.ReadFile(repositoryPath("schemas", name))
			if err != nil {
				t.Fatalf("ReadFile() error = %v", err)
			}
			if !bytes.Equal(embedded, public) {
				t.Fatalf("embedded and public %s differ", name)
			}
		})
	}
}

func TestExamplesValidateAndRoundTrip(t *testing.T) {
	t.Parallel()

	scenarioPath := repositoryPath("examples", "order-lifecycle", "scenarios", "r1-offset-rewind.yaml")
	targetPath := repositoryPath("examples", "order-lifecycle", "targets", "baseline.yaml")
	candidatePath := repositoryPath("examples", "order-lifecycle", "targets", "candidate.yaml")
	workloadPath := repositoryPath("examples", "order-lifecycle", "workload.yaml")

	scenario, err := LoadScenario(scenarioPath)
	if err != nil {
		t.Fatalf("LoadScenario() error = %v", err)
	}
	target, err := LoadTarget(targetPath)
	if err != nil {
		t.Fatalf("LoadTarget() error = %v", err)
	}
	candidate, err := LoadTarget(candidatePath)
	if err != nil {
		t.Fatalf("LoadTarget(candidate) error = %v", err)
	}
	if violations := ValidateScenarioAndTarget(scenario, target, filepath.Dir(scenarioPath)); len(violations) != 0 {
		t.Fatalf("scenario violations = %#v", violations)
	}
	if violations := CompareTargets(target, candidate, nil); len(violations) != 0 {
		t.Fatalf("target comparison violations = %#v", violations)
	}

	workload, err := LoadWorkload(workloadPath)
	if err != nil {
		t.Fatalf("LoadWorkload() error = %v", err)
	}
	if violations := ValidateWorkload(workload, filepath.Dir(workloadPath)); len(violations) != 0 {
		t.Fatalf("workload violations = %#v", violations)
	}

	roundTripYAML(t, scenario, LoadScenario)
	roundTripYAML(t, target, LoadTarget)
	roundTripYAML(t, workload, LoadWorkload)

	result, err := LoadResult(repositoryPath("examples", "order-lifecycle", "expected", "result.json"))
	if err != nil {
		t.Fatalf("LoadResult() error = %v", err)
	}
	if violations := ValidateResult(result); len(violations) != 0 {
		t.Fatalf("result violations = %#v", violations)
	}
	bundle, err := LoadBundle(repositoryPath("examples", "order-lifecycle", "expected", "bundle.json"))
	if err != nil {
		t.Fatalf("LoadBundle() error = %v", err)
	}
	if violations := ValidateBundle(bundle); len(violations) != 0 {
		t.Fatalf("bundle violations = %#v", violations)
	}
	roundTripJSON(t, result, LoadResult)
	roundTripJSON(t, bundle, LoadBundle)
}

func roundTripYAML[T any](t *testing.T, value T, load func(string) (T, error)) {
	t.Helper()
	document, err := yaml.Marshal(value)
	if err != nil {
		t.Fatalf("yaml.Marshal() error = %v", err)
	}
	path := filepath.Join(t.TempDir(), "roundtrip.yaml")
	if err := os.WriteFile(path, document, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	decoded, err := load(path)
	if err != nil {
		t.Fatalf("round-trip load error = %v\n%s", err, document)
	}
	if !reflect.DeepEqual(value, decoded) {
		t.Fatalf("YAML round trip changed value\nwant: %#v\ngot: %#v", value, decoded)
	}
}

func roundTripJSON[T any](t *testing.T, value T, load func(string) (T, error)) {
	t.Helper()
	document, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	path := filepath.Join(t.TempDir(), "roundtrip.json")
	if err := os.WriteFile(path, document, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	decoded, err := load(path)
	if err != nil {
		t.Fatalf("round-trip load error = %v\n%s", err, document)
	}
	if !reflect.DeepEqual(value, decoded) {
		t.Fatalf("JSON round trip changed value\nwant: %#v\ngot: %#v", value, decoded)
	}
}

func TestInvalidGoldenFixtures(t *testing.T) {
	t.Parallel()

	if _, err := LoadScenario(repositoryPath("tests", "fixtures", "validation", "invalid", "unknown-field.scenario.yaml")); err == nil {
		t.Fatal("unknown field fixture was accepted")
	}
	cycle, err := LoadScenario(repositoryPath("tests", "fixtures", "validation", "invalid", "cycle.scenario.yaml"))
	if err != nil {
		t.Fatalf("LoadScenario(cycle) error = %v", err)
	}
	target, err := LoadTarget(repositoryPath("examples", "order-lifecycle", "targets", "baseline.yaml"))
	if err != nil {
		t.Fatalf("LoadTarget() error = %v", err)
	}
	assertRule(t, ValidateScenarioAndTarget(cycle, target, filepath.Dir(repositoryPath("tests", "fixtures", "validation", "invalid", "cycle.scenario.yaml"))), "step_cycle")
	if _, err := LoadTarget(repositoryPath("tests", "fixtures", "validation", "invalid", "mutable.target.yaml")); err == nil {
		t.Fatal("mutable target fixture was accepted")
	}
}

func TestTargetSemanticRules(t *testing.T) {
	t.Parallel()

	target, err := LoadTarget(repositoryPath("examples", "order-lifecycle", "targets", "baseline.yaml"))
	if err != nil {
		t.Fatalf("LoadTarget() error = %v", err)
	}
	tests := []struct {
		name string
		rule string
		edit func(*Target)
	}{
		{name: "duplicate service", rule: "unique_service", edit: func(value *Target) { value.Spec.Services = append(value.Spec.Services, value.Spec.Services[0]) }},
		{name: "undeclared dependency", rule: "service_dependency", edit: func(value *Target) { value.Spec.Services[0].Dependencies = []string{"missing"} }},
		{name: "self dependency", rule: "service_self_dependency", edit: func(value *Target) { value.Spec.Services[0].Dependencies = []string{value.Spec.Services[0].Name} }},
		{name: "reserved env", rule: "reserved_environment", edit: func(value *Target) { value.Spec.Services[0].Environment["CHRONICLE_RUN_ID"] = "bad" }},
		{name: "literal secret overlap", rule: "secret_literal_overlap", edit: func(value *Target) {
			value.Spec.Services[0].Environment["TOKEN"] = "literal"
			value.Spec.Services[0].SecretEnvironment["TOKEN"] = "token-ref"
		}},
		{name: "resource bounds", rule: "resource_bounds", edit: func(value *Target) { value.Spec.Services[0].Resources.CPUs = 0 }},
		{name: "probe declaration", rule: "probe_declaration", edit: func(value *Target) { value.Spec.Services[0].Probe.Enabled = true }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := cloneJSON(t, target)
			test.edit(&value)
			assertRule(t, ValidateTarget(value), test.rule)
		})
	}
}

func TestScenarioSemanticRules(t *testing.T) {
	t.Parallel()

	scenarioPath := repositoryPath("examples", "order-lifecycle", "scenarios", "r1-offset-rewind.yaml")
	scenario, err := LoadScenario(scenarioPath)
	if err != nil {
		t.Fatalf("LoadScenario() error = %v", err)
	}
	target, err := LoadTarget(repositoryPath("examples", "order-lifecycle", "targets", "baseline.yaml"))
	if err != nil {
		t.Fatalf("LoadTarget() error = %v", err)
	}
	tests := []struct {
		name string
		rule string
		edit func(*Scenario)
	}{
		{name: "duplicate step", rule: "unique_step", edit: func(value *Scenario) { value.Spec.Steps = append(value.Spec.Steps, value.Spec.Steps[0]) }},
		{name: "unresolved dependency", rule: "step_dependency", edit: func(value *Scenario) { value.Spec.Steps[0].DependsOn = []string{"missing"} }},
		{name: "one action", rule: "one_action", edit: func(value *Scenario) {
			value.Spec.Steps[0].Observe = &ObserveAction{Observation: "inventory-reservations"}
		}},
		{name: "undeclared service", rule: "declared_service", edit: func(value *Scenario) { value.Spec.Steps[2].Stop.Service = "missing" }},
		{name: "undeclared event", rule: "declared_event", edit: func(value *Scenario) { value.Spec.Steps[0].Publish.Event = "missing" }},
		{name: "undeclared observation", rule: "declared_observation", edit: func(value *Scenario) { value.Spec.Steps[1].Observe.Observation = "missing" }},
		{name: "rewind ordering", rule: "rewind_order", edit: func(value *Scenario) { value.Spec.Steps[3].DependsOn = []string{"publish-inventory"} }},
		{name: "normalization observer", rule: "normalization_observation", edit: func(value *Scenario) { value.Spec.Normalization[0].Observation = "missing" }},
		{name: "bounded limits", rule: "bounded_limits", edit: func(value *Scenario) { value.Spec.Limits.MaxSteps = 0 }},
		{name: "quiescence bounds", rule: "quiescence_bounds", edit: func(value *Scenario) { value.Spec.Quiescence.StabilityWindow = value.Spec.Quiescence.Timeout }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := cloneJSON(t, scenario)
			test.edit(&value)
			assertRule(t, ValidateScenarioAndTarget(value, target, filepath.Dir(scenarioPath)), test.rule)
		})
	}
}

func TestCheckpointCrashRestartSequenceIsLegal(t *testing.T) {
	t.Parallel()

	scenarioPath := repositoryPath("examples", "order-lifecycle", "scenarios", "r1-offset-rewind.yaml")
	scenario, err := LoadScenario(scenarioPath)
	if err != nil {
		t.Fatalf("LoadScenario() error = %v", err)
	}
	target, err := LoadTarget(repositoryPath("examples", "order-lifecycle", "targets", "baseline.yaml"))
	if err != nil {
		t.Fatalf("LoadTarget() error = %v", err)
	}
	target.Spec.Services[0].Probe = ProbeDeclaration{
		Enabled:               true,
		ProtocolVersion:       "v1",
		CommitMode:            "manual_sync",
		MaxControlledInFlight: 1,
		Checkpoints:           []string{"after_external_effect"},
		LogicalClock:          true,
	}
	selector := CheckpointSelector{
		Service:    "fulfillment-projector",
		Name:       "after_external_effect",
		EventID:    "inventory-requested-1",
		StepID:     "publish-inventory",
		Occurrence: 1,
	}
	scenario.Spec.Steps = []Step{
		{ID: "arm-effect", ArmCheckpoint: &CheckpointAction{CheckpointSelector: selector}},
		{ID: "publish-inventory", DependsOn: []string{"arm-effect"}, Publish: &PublishAction{Event: "inventory-requested", Topic: "inventory", Partition: 0, Key: "order-1"}},
		{ID: "await-effect", DependsOn: []string{"publish-inventory"}, Timeout: mustDuration(t, "10s"), Await: &AwaitAction{Checkpoint: &selector}},
		{ID: "kill-projector", DependsOn: []string{"await-effect"}, Stop: &ServiceAction{Service: "fulfillment-projector"}},
		{ID: "restart-projector", DependsOn: []string{"kill-projector"}, Restart: &ServiceAction{Service: "fulfillment-projector"}},
	}
	if violations := ValidateScenarioAndTarget(scenario, target, filepath.Dir(scenarioPath)); len(violations) != 0 {
		t.Fatalf("canonical crash/restart sequence rejected: %#v", violations)
	}
}

func TestCheckpointPublishOrderIsRequired(t *testing.T) {
	t.Parallel()

	scenarioPath := repositoryPath("examples", "order-lifecycle", "scenarios", "r1-offset-rewind.yaml")
	scenario, err := LoadScenario(scenarioPath)
	if err != nil {
		t.Fatalf("LoadScenario() error = %v", err)
	}
	target, err := LoadTarget(repositoryPath("examples", "order-lifecycle", "targets", "baseline.yaml"))
	if err != nil {
		t.Fatalf("LoadTarget() error = %v", err)
	}
	target.Spec.Services[0].Probe = ProbeDeclaration{Enabled: true, ProtocolVersion: "v1", CommitMode: "manual_sync", MaxControlledInFlight: 1, Checkpoints: []string{"before_offset_commit"}}
	selector := CheckpointSelector{Service: "fulfillment-projector", Name: "before_offset_commit", EventID: "inventory-requested-1", StepID: "publish-inventory", Occurrence: 1}
	scenario.Spec.Steps = []Step{
		{ID: "arm", ArmCheckpoint: &CheckpointAction{CheckpointSelector: selector}},
		{ID: "publish-inventory", Publish: &PublishAction{Event: "inventory-requested", Topic: "inventory", Partition: 0, Key: "order-1"}},
		{ID: "await", DependsOn: []string{"arm"}, Await: &AwaitAction{Checkpoint: &selector}},
		{ID: "stop", DependsOn: []string{"await"}, Stop: &ServiceAction{Service: "fulfillment-projector"}},
	}
	assertRule(t, ValidateScenarioAndTarget(scenario, target, filepath.Dir(scenarioPath)), "checkpoint_publish_order")
}

func TestCompareTargets(t *testing.T) {
	t.Parallel()

	baseline, err := LoadTarget(repositoryPath("examples", "order-lifecycle", "targets", "baseline.yaml"))
	if err != nil {
		t.Fatalf("LoadTarget() error = %v", err)
	}
	candidate := cloneJSON(t, baseline)
	candidate.Spec.Services[0].Image = "ghcr.io/example/fulfillment-projector@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if violations := CompareTargets(baseline, candidate, nil); len(violations) != 0 {
		t.Fatalf("digest-only difference rejected: %#v", violations)
	}
	candidate.Spec.Services[0].Args = []string{"--new"}
	assertRule(t, CompareTargets(baseline, candidate, nil), "unlisted_target_difference")
	allowed := []AllowedTargetDifference{{Pointer: "/spec/services/fulfillment-projector/args/0", Rationale: "exercise an intended flag"}}
	if violations := CompareTargets(baseline, candidate, allowed); len(violations) != 0 {
		t.Fatalf("allowlisted difference rejected: %#v", violations)
	}
	candidate.Spec.DatabaseSchemaVersion = "v2"
	assertRule(t, CompareTargets(baseline, candidate, allowed), "database_schema_match")
}

func TestLocalImageIDsRequireExplicitDevelopmentMode(t *testing.T) {
	t.Parallel()
	target, err := LoadTarget(repositoryPath("examples", "order-lifecycle", "targets", "baseline.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	target.Spec.Services[0].Image = "sha256:" + strings.Repeat("a", 64)
	assertRule(t, ValidateTarget(target), "immutable_image")
	if violations := ValidateTargetWithOptions(target, ValidationOptions{AllowLocalImageIDs: true}); len(violations) != 0 {
		t.Fatalf("development-local image ID rejected: %#v", violations)
	}
	candidate := cloneJSON(t, target)
	candidate.Spec.Services[0].Image = "sha256:" + strings.Repeat("b", 64)
	if violations := CompareTargets(target, candidate, nil); len(violations) != 0 {
		t.Fatalf("local digest-only target difference rejected: %#v", violations)
	}
}

func TestPayloadSchemaRejectsNestedExternalReference(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	mainPath := filepath.Join(root, "main.json")
	nestedPath := filepath.Join(root, "nested.json")
	if err := os.WriteFile(mainPath, []byte(`{"$schema":"https://json-schema.org/draft/2020-12/schema","$ref":"nested.json"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(nestedPath, []byte(`{"$schema":"https://json-schema.org/draft/2020-12/schema","$ref":"https://example.com/remote.json"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	err := validatePayloadSchema(root, "main.json", map[string]any{})
	if err == nil || !strings.Contains(err.Error(), "external schema reference") {
		t.Fatalf("validatePayloadSchema() error = %v", err)
	}
}

func TestDurationEncodingRequiresUnits(t *testing.T) {
	t.Parallel()

	for _, document := range []string{`0`, `"0"`, `1`} {
		var duration Duration
		if err := json.Unmarshal([]byte(document), &duration); err == nil {
			t.Errorf("json.Unmarshal(%s) accepted unitless duration", document)
		}
	}
	parsed, err := ParseDuration("250ms")
	if err != nil {
		t.Fatalf("ParseDuration() error = %v", err)
	}
	document, err := json.Marshal(parsed)
	if err != nil || string(document) != `"250ms"` {
		t.Fatalf("json.Marshal() = %s, %v", document, err)
	}
}

func TestStrictYAMLBoundaries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		document string
	}{
		{name: "multiple documents", document: "apiVersion: chronicle.dev/v1alpha1\n---\napiVersion: chronicle.dev/v1alpha1\n"},
		{name: "non string key", document: "1: value\n"},
		{name: "alias", document: "apiVersion: &version chronicle.dev/v1alpha1\nkind: *version\n"},
		{name: "duplicate", document: "apiVersion: chronicle.dev/v1alpha1\napiVersion: chronicle.dev/v1alpha1\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "invalid.yaml")
			if err := os.WriteFile(path, []byte(test.document), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadScenario(path); err == nil {
				t.Fatal("invalid YAML boundary was accepted")
			}
		})
	}

	path := filepath.Join(t.TempDir(), "oversized.yaml")
	if err := os.WriteFile(path, bytes.Repeat([]byte("x"), MaxContractBytes+1), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadScenario(path); err == nil || !strings.Contains(err.Error(), "contract limit") {
		t.Fatalf("oversized document error = %v", err)
	}
}

func cloneJSON[T any](t *testing.T, value T) T {
	t.Helper()
	document, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	var result T
	if err := json.Unmarshal(document, &result); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	return result
}

func mustDuration(t *testing.T, value string) Duration {
	t.Helper()
	duration, err := ParseDuration(value)
	if err != nil {
		t.Fatalf("ParseDuration() error = %v", err)
	}
	return duration
}

func assertRule(t *testing.T, violations []Violation, rule string) {
	t.Helper()
	for _, violation := range violations {
		if violation.Rule == rule {
			return
		}
	}
	t.Fatalf("rule %q not found in violations: %#v", rule, violations)
}
