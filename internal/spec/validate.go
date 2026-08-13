package spec

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"

	jsonschema "github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/uday1o1/chronicle-gate/internal/imagelock"
)

var checkpointNames = map[string]struct{}{
	"before_handler": {}, "after_state_load": {}, "after_external_effect": {},
	"after_db_commit": {}, "before_offset_commit": {}, "after_offset_commit": {},
}

var reservedEnvironment = map[string]struct{}{
	"CHRONICLE_BROKERS": {}, "CHRONICLE_TOPIC_PREFIX": {}, "CHRONICLE_GROUP_PREFIX": {},
	"CHRONICLE_RUN_ID": {}, "CHRONICLE_ATTEMPT_ID": {}, "CHRONICLE_LOGICAL_CLOCK_SEED": {},
	"CHRONICLE_DATABASE_DSN_FILE": {}, "CHRONICLE_PROBE_TOKEN_FILE": {},
}

type validation struct {
	document   string
	violations []Violation
}

func (validator *validation) add(pointer string, rule string, message string) {
	validator.violations = append(validator.violations, Violation{
		Document: validator.document,
		Pointer:  pointer,
		Rule:     rule,
		Message:  message,
	})
}

func sortedViolations(violations []Violation) []Violation {
	sort.Slice(violations, func(left, right int) bool {
		a, b := violations[left], violations[right]
		if a.Document != b.Document {
			return a.Document < b.Document
		}
		if a.Pointer != b.Pointer {
			return a.Pointer < b.Pointer
		}
		if a.Rule != b.Rule {
			return a.Rule < b.Rule
		}
		return a.Message < b.Message
	})
	return violations
}

// ValidateTarget validates one authored target without contacting Docker.
func ValidateTarget(target Target) []Violation {
	validator := validation{document: "target"}
	if target.APIVersion != APIVersion {
		validator.add("/apiVersion", "api_version", "apiVersion must be chronicle.dev/v1alpha1")
	}
	if target.Kind != "Target" {
		validator.add("/kind", "kind", "kind must be Target")
	}
	if target.Spec.DatabaseSchemaVersion == "" {
		validator.add("/spec/databaseSchemaVersion", "database_schema", "database schema version is required")
	}

	services := make(map[string]Service, len(target.Spec.Services))
	for index, service := range target.Spec.Services {
		pointer := fmt.Sprintf("/spec/services/%d", index)
		if _, exists := services[service.Name]; exists {
			validator.add(pointer+"/name", "unique_service", fmt.Sprintf("service %q is duplicated", service.Name))
		}
		services[service.Name] = service
		if err := imagelock.ValidatePublicationReference(service.Image); err != nil {
			validator.add(pointer+"/image", "immutable_image", err.Error())
		}
		for key := range service.Environment {
			if _, reserved := reservedEnvironment[key]; reserved {
				validator.add(pointer+"/environment/"+escapePointer(key), "reserved_environment", fmt.Sprintf("%s is reserved for runtime injection", key))
			}
			if _, secret := service.SecretEnvironment[key]; secret {
				validator.add(pointer+"/environment/"+escapePointer(key), "secret_literal_overlap", fmt.Sprintf("%s cannot be both literal and secret", key))
			}
		}
		for key := range service.SecretEnvironment {
			if _, reserved := reservedEnvironment[key]; reserved {
				validator.add(pointer+"/secretEnvironment/"+escapePointer(key), "reserved_environment", fmt.Sprintf("%s is reserved for runtime injection", key))
			}
		}
		if service.Resources.CPUs <= 0 || service.Resources.MemoryBytes < 64<<20 || service.Resources.PIDs < 16 {
			validator.add(pointer+"/resources", "resource_bounds", "CPU, memory, and PID limits must satisfy the minimum bounds")
		}
		if service.Health.Timeout.Duration <= 0 || service.Health.Interval.Duration <= 0 {
			validator.add(pointer+"/health", "health_bounds", "health timeout and interval must be positive")
		}
		if service.Probe.Enabled {
			if service.Probe.ProtocolVersion == "" {
				validator.add(pointer+"/probe/protocolVersion", "probe_declaration", "enabled probes must declare a protocol version")
			}
			seen := map[string]struct{}{}
			for checkpointIndex, checkpoint := range service.Probe.Checkpoints {
				if _, supported := checkpointNames[checkpoint]; !supported {
					validator.add(fmt.Sprintf("%s/probe/checkpoints/%d", pointer, checkpointIndex), "checkpoint_name", fmt.Sprintf("checkpoint %q is unsupported", checkpoint))
				}
				if _, duplicate := seen[checkpoint]; duplicate {
					validator.add(fmt.Sprintf("%s/probe/checkpoints/%d", pointer, checkpointIndex), "unique_checkpoint", fmt.Sprintf("checkpoint %q is duplicated", checkpoint))
				}
				seen[checkpoint] = struct{}{}
			}
		}
	}

	for index, service := range target.Spec.Services {
		for dependencyIndex, dependency := range service.Dependencies {
			if dependency == service.Name {
				validator.add(fmt.Sprintf("/spec/services/%d/dependencies/%d", index, dependencyIndex), "service_self_dependency", "a service cannot depend on itself")
			} else if _, exists := services[dependency]; !exists {
				validator.add(fmt.Sprintf("/spec/services/%d/dependencies/%d", index, dependencyIndex), "service_dependency", fmt.Sprintf("service dependency %q is undeclared", dependency))
			}
		}
	}
	validateServiceCycles(target.Spec.Services, &validator)
	return sortedViolations(validator.violations)
}

func validateServiceCycles(services []Service, validator *validation) {
	byName := make(map[string]Service, len(services))
	for _, service := range services {
		byName[service.Name] = service
	}
	state := map[string]int{}
	var visit func(string) bool
	visit = func(name string) bool {
		if state[name] == 1 {
			return true
		}
		if state[name] == 2 {
			return false
		}
		state[name] = 1
		for _, dependency := range byName[name].Dependencies {
			if _, exists := byName[dependency]; exists && visit(dependency) {
				return true
			}
		}
		state[name] = 2
		return false
	}
	for index, service := range services {
		if visit(service.Name) {
			validator.add(fmt.Sprintf("/spec/services/%d/dependencies", index), "service_cycle", "service dependency graph contains a cycle")
			return
		}
	}
}

// ValidateScenarioAndTarget validates the complete authored execution contract without external processes.
func ValidateScenarioAndTarget(scenario Scenario, target Target, root string) []Violation {
	violations := append([]Violation{}, ValidateTarget(target)...)
	validator := validation{document: "scenario"}
	if scenario.APIVersion != APIVersion {
		validator.add("/apiVersion", "api_version", "apiVersion must be chronicle.dev/v1alpha1")
	}
	if scenario.Kind != "Scenario" {
		validator.add("/kind", "kind", "kind must be Scenario")
	}

	services := make(map[string]Service, len(target.Spec.Services))
	for _, service := range target.Spec.Services {
		services[service.Name] = service
	}
	observations := validateCorrectnessContract(scenario.Spec.Events, scenario.Spec.Observations, scenario.Spec.Invariants, scenario.Spec.Normalization, scenario.Spec.Quiescence, root, &validator)
	validateLimits(scenario.Spec.Limits, &validator)
	conditions := make(map[string]struct{}, len(scenario.Spec.Quiescence.Conditions))
	for index, condition := range scenario.Spec.Quiescence.Conditions {
		conditions[condition.ID] = struct{}{}
		if condition.Service != "" {
			if _, exists := services[condition.Service]; !exists {
				validator.add(fmt.Sprintf("/spec/quiescence/conditions/%d/service", index), "declared_service", fmt.Sprintf("service %q is undeclared", condition.Service))
			}
		}
	}

	steps := make(map[string]Step, len(scenario.Spec.Steps))
	stepIndexes := make(map[string]int, len(scenario.Spec.Steps))
	for index, step := range scenario.Spec.Steps {
		pointer := fmt.Sprintf("/spec/steps/%d", index)
		if previous, exists := stepIndexes[step.ID]; exists {
			validator.add(pointer+"/id", "unique_step", fmt.Sprintf("step %q duplicates step at index %d", step.ID, previous))
		}
		stepIndexes[step.ID] = index
		steps[step.ID] = step
		if step.ActionCount() != 1 {
			validator.add(pointer, "one_action", "each step must declare exactly one action")
		}
		if step.Timeout.Duration < 0 {
			validator.add(pointer+"/timeout", "duration_bounds", "step timeout cannot be negative")
		}
		if (step.Await != nil || step.Observe != nil) && step.Timeout.Duration <= 0 {
			validator.add(pointer+"/timeout", "wait_timeout", "await and observe steps require a positive timeout")
		}
		validateStepReferences(step, pointer, scenario, services, observations, conditions, &validator)
	}

	for index, step := range scenario.Spec.Steps {
		for dependencyIndex, dependency := range step.DependsOn {
			if dependency == step.ID {
				validator.add(fmt.Sprintf("/spec/steps/%d/dependsOn/%d", index, dependencyIndex), "step_self_dependency", "a step cannot depend on itself")
			} else if _, exists := steps[dependency]; !exists {
				validator.add(fmt.Sprintf("/spec/steps/%d/dependsOn/%d", index, dependencyIndex), "step_dependency", fmt.Sprintf("step dependency %q is unresolved", dependency))
			}
		}
	}
	ancestors, cyclic := dependencyAncestors(steps)
	if cyclic != "" {
		validator.add("/spec/steps", "step_cycle", fmt.Sprintf("step dependency graph contains a cycle at %q", cyclic))
	} else {
		validateFaultOrdering(scenario.Spec.Steps, steps, ancestors, services, scenario.Spec.Events, &validator)
	}
	for index, step := range scenario.Spec.Steps {
		if step.AdvanceClock == nil {
			continue
		}
		for _, service := range target.Spec.Services {
			if service.Probe.Enabled && !service.Probe.LogicalClock {
				validator.add(fmt.Sprintf("/spec/steps/%d/advanceClock", index), "logical_clock_capability", fmt.Sprintf("instrumented service %q does not declare logical-clock support", service.Name))
			}
		}
	}

	violations = append(violations, validator.violations...)
	return sortedViolations(violations)
}

func validateStepReferences(step Step, pointer string, scenario Scenario, services map[string]Service, observations map[string]struct{}, conditions map[string]struct{}, validator *validation) {
	serviceName := ""
	switch {
	case step.Stop != nil:
		serviceName = step.Stop.Service
	case step.Restart != nil:
		serviceName = step.Restart.Service
	case step.RewindOffset != nil:
		serviceName = step.RewindOffset.Service
	case step.ArmCheckpoint != nil:
		serviceName = step.ArmCheckpoint.Service
	case step.ReleaseCheckpoint != nil:
		serviceName = step.ReleaseCheckpoint.Service
	}
	if serviceName != "" {
		if _, exists := services[serviceName]; !exists {
			validator.add(pointer, "declared_service", fmt.Sprintf("service %q is undeclared", serviceName))
		}
	}
	if step.Publish != nil {
		if _, exists := scenario.Spec.Events[step.Publish.Event]; !exists {
			validator.add(pointer+"/publish/event", "declared_event", fmt.Sprintf("event %q is undeclared", step.Publish.Event))
		}
	}
	if step.Observe != nil {
		if _, exists := observations[step.Observe.Observation]; !exists {
			validator.add(pointer+"/observe/observation", "declared_observation", fmt.Sprintf("observation %q is undeclared", step.Observe.Observation))
		}
	}
	if step.Await != nil {
		if (step.Await.Condition == "") == (step.Await.Checkpoint == nil) {
			validator.add(pointer+"/await", "await_selector", "await must declare exactly one condition or checkpoint")
		}
		if step.Await.Condition != "" {
			if _, exists := conditions[step.Await.Condition]; !exists {
				validator.add(pointer+"/await/condition", "quiescence_condition", fmt.Sprintf("condition %q is undeclared", step.Await.Condition))
			}
		}
	}
	for actionName, checkpoint := range map[string]*CheckpointAction{"armCheckpoint": step.ArmCheckpoint, "releaseCheckpoint": step.ReleaseCheckpoint} {
		if checkpoint == nil {
			continue
		}
		if _, supported := checkpointNames[checkpoint.Name]; !supported {
			validator.add(pointer+"/"+actionName+"/name", "checkpoint_name", fmt.Sprintf("checkpoint %q is unsupported", checkpoint.Name))
		}
		if checkpoint.Occurrence < 1 {
			validator.add(pointer+"/"+actionName+"/occurrence", "checkpoint_occurrence", "checkpoint occurrence must be positive")
		}
	}
}

func dependencyAncestors(steps map[string]Step) (map[string]map[string]struct{}, string) {
	ancestors := make(map[string]map[string]struct{}, len(steps))
	visiting := map[string]bool{}
	var visit func(string) (map[string]struct{}, bool)
	visit = func(id string) (map[string]struct{}, bool) {
		if cached, exists := ancestors[id]; exists {
			return cached, false
		}
		if visiting[id] {
			return nil, true
		}
		visiting[id] = true
		set := map[string]struct{}{}
		for _, dependency := range steps[id].DependsOn {
			if _, exists := steps[dependency]; !exists {
				continue
			}
			set[dependency] = struct{}{}
			parents, cycle := visit(dependency)
			if cycle {
				return nil, true
			}
			for parent := range parents {
				set[parent] = struct{}{}
			}
		}
		visiting[id] = false
		ancestors[id] = set
		return set, false
	}
	for id := range steps {
		if _, cycle := visit(id); cycle {
			return nil, id
		}
	}
	return ancestors, ""
}

func validateFaultOrdering(ordered []Step, steps map[string]Step, ancestors map[string]map[string]struct{}, services map[string]Service, events map[string]CloudEvent, validator *validation) {
	arms := map[CheckpointSelector]string{}
	for index, step := range ordered {
		if step.ArmCheckpoint == nil {
			continue
		}
		selector := step.ArmCheckpoint.CheckpointSelector
		if previous, exists := arms[selector]; exists {
			validator.add(fmt.Sprintf("/spec/steps/%d/armCheckpoint", index), "checkpoint_arm", fmt.Sprintf("the exact checkpoint tuple is already armed by step %q", previous))
		}
		arms[selector] = step.ID
	}
	for index, step := range ordered {
		pointer := fmt.Sprintf("/spec/steps/%d", index)
		if step.RewindOffset != nil {
			if !hasAncestor(ancestors[step.ID], steps, func(candidate Step) bool {
				return candidate.Stop != nil && candidate.Stop.Service == step.RewindOffset.Service
			}) {
				validator.add(pointer+"/rewindOffset", "rewind_order", "rewindOffset must transitively depend on stopping the same service")
			}
			if !hasDescendant(step.ID, ordered, ancestors, func(candidate Step) bool {
				return candidate.Restart != nil && candidate.Restart.Service == step.RewindOffset.Service
			}) {
				validator.add(pointer+"/rewindOffset", "rewind_order", "rewindOffset must be followed by a restart that transitively depends on it")
			}
		}
		if step.ReleaseCheckpoint != nil {
			if !hasMatchingCheckpointAncestor(ancestors[step.ID], steps, step.ReleaseCheckpoint.CheckpointSelector, true) {
				validator.add(pointer+"/releaseCheckpoint", "checkpoint_order", "releaseCheckpoint must transitively depend on the exact armed and blocked checkpoint tuple")
			}
		}
		if step.ArmCheckpoint != nil {
			selector := step.ArmCheckpoint.CheckpointSelector
			if hasMatchingCheckpointAncestor(ancestors[step.ID], steps, selector, false) {
				validator.add(pointer+"/armCheckpoint", "checkpoint_arm", "the exact checkpoint tuple is already armed")
			}
			awaitID := matchingAwaitDescendant(step.ID, ordered, ancestors, selector)
			if awaitID == "" {
				validator.add(pointer+"/armCheckpoint", "checkpoint_order", "armed checkpoint must be followed by an exact blocked-checkpoint await")
			} else {
				if !hasMatchingPublish(step.ID, awaitID, ordered, ancestors, selector, events) {
					validator.add(pointer+"/armCheckpoint", "checkpoint_publish_order", "the exact event publication step must occur after arm and before blocked-checkpoint await")
				}
				if !hasCheckpointTerminal(awaitID, ordered, ancestors, selector) {
					validator.add(pointer+"/armCheckpoint", "checkpoint_terminal", "blocked checkpoint must be released or terminated by stopping its service")
				}
			}
			validateProbeDeclaration(pointer, selector, services[selector.Service], validator)
		}
	}
}

func hasMatchingPublish(armID string, awaitID string, ordered []Step, ancestors map[string]map[string]struct{}, selector CheckpointSelector, events map[string]CloudEvent) bool {
	for _, candidate := range ordered {
		if candidate.ID != selector.StepID || candidate.Publish == nil {
			continue
		}
		if _, afterArm := ancestors[candidate.ID][armID]; !afterArm {
			continue
		}
		if _, beforeAwait := ancestors[awaitID][candidate.ID]; !beforeAwait {
			continue
		}
		if event, exists := events[candidate.Publish.Event]; exists && event.ID == selector.EventID {
			return true
		}
	}
	return false
}

func hasAncestor(ids map[string]struct{}, steps map[string]Step, predicate func(Step) bool) bool {
	for id := range ids {
		if predicate(steps[id]) {
			return true
		}
	}
	return false
}

func hasDescendant(id string, ordered []Step, ancestors map[string]map[string]struct{}, predicate func(Step) bool) bool {
	for _, candidate := range ordered {
		if _, depends := ancestors[candidate.ID][id]; depends && predicate(candidate) {
			return true
		}
	}
	return false
}

func checkpointEqual(left, right CheckpointSelector) bool {
	return left == right
}

func hasMatchingCheckpointAncestor(ids map[string]struct{}, steps map[string]Step, selector CheckpointSelector, requireAwait bool) bool {
	armed := false
	awaited := !requireAwait
	for id := range ids {
		candidate := steps[id]
		if candidate.ArmCheckpoint != nil && checkpointEqual(candidate.ArmCheckpoint.CheckpointSelector, selector) {
			armed = true
		}
		if candidate.Await != nil && candidate.Await.Checkpoint != nil && checkpointEqual(*candidate.Await.Checkpoint, selector) {
			awaited = true
		}
	}
	return armed && awaited
}

func matchingAwaitDescendant(armID string, ordered []Step, ancestors map[string]map[string]struct{}, selector CheckpointSelector) string {
	for _, candidate := range ordered {
		if _, depends := ancestors[candidate.ID][armID]; !depends || candidate.Await == nil || candidate.Await.Checkpoint == nil {
			continue
		}
		if checkpointEqual(*candidate.Await.Checkpoint, selector) {
			return candidate.ID
		}
	}
	return ""
}

func hasCheckpointTerminal(awaitID string, ordered []Step, ancestors map[string]map[string]struct{}, selector CheckpointSelector) bool {
	for _, candidate := range ordered {
		if _, depends := ancestors[candidate.ID][awaitID]; !depends {
			continue
		}
		if candidate.ReleaseCheckpoint != nil && checkpointEqual(candidate.ReleaseCheckpoint.CheckpointSelector, selector) {
			return true
		}
		if candidate.Stop != nil && candidate.Stop.Service == selector.Service {
			return true
		}
	}
	return false
}

func validateProbeDeclaration(pointer string, selector CheckpointSelector, service Service, validator *validation) {
	if !service.Probe.Enabled {
		validator.add(pointer+"/armCheckpoint", "probe_capability_declaration", "target does not declare an enabled probe")
		return
	}
	found := false
	for _, checkpoint := range service.Probe.Checkpoints {
		if checkpoint == selector.Name {
			found = true
			break
		}
	}
	if !found {
		validator.add(pointer+"/armCheckpoint/name", "probe_capability_declaration", fmt.Sprintf("target does not declare checkpoint %q", selector.Name))
	}
	if selector.Name == "before_offset_commit" || selector.Name == "after_offset_commit" {
		if service.Probe.CommitMode != "manual_sync" || service.Probe.MaxControlledInFlight != 1 {
			validator.add(pointer+"/armCheckpoint", "precise_commit_capability", "precise commit checkpoints require authored manual_sync and maxControlledInFlight 1 declarations; runtime proof is still required")
		}
	}
}

func validateLimits(limits Limits, validator *validation) {
	if limits.MaxSteps <= 0 || limits.MaxEvents <= 0 || limits.MaxRunDuration.Duration <= 0 || limits.ConfirmationAttempts < 2 || limits.MinimizationTrials <= 0 || limits.MinimizationDuration.Duration <= 0 {
		validator.add("/spec/limits", "bounded_limits", "all limits must be finite and positive, with at least two confirmation attempts")
	}
}

func validateCorrectnessContract(events map[string]CloudEvent, observationList []Observation, invariants []Invariant, normalizations []Normalization, quiescence Quiescence, root string, validator *validation) map[string]struct{} {
	for name, event := range events {
		if event.SpecVersion != "1.0" || event.ID == "" || event.Source == "" || event.Type == "" || event.Subject == "" || event.Time == "" || event.DataContentType != "application/json" || event.Data == nil {
			validator.add("/spec/events/"+escapePointer(name), "cloudevent_required", "CloudEvent requires specversion 1.0, id, source, type, subject, time, application/json content type, and data")
		}
		if _, err := time.Parse(time.RFC3339Nano, event.Time); err != nil {
			validator.add("/spec/events/"+escapePointer(name)+"/time", "cloudevent_time", "CloudEvent time must be RFC3339")
		}
		if event.DataSchema != "" {
			if err := validatePayloadSchema(root, event.DataSchema, event.Data); err != nil {
				validator.add("/spec/events/"+escapePointer(name)+"/dataschema", "payload_schema", err.Error())
			}
		}
	}

	observations := map[string]struct{}{}
	for index, observation := range observationList {
		pointer := fmt.Sprintf("/spec/observations/%d", index)
		if _, exists := observations[observation.ID]; exists {
			validator.add(pointer+"/id", "unique_observation", fmt.Sprintf("observation %q is duplicated", observation.ID))
		}
		observations[observation.ID] = struct{}{}
		if observation.TypeCount() != 1 {
			validator.add(pointer, "one_observer", "each observation must declare exactly one observer")
		}
		if observation.SQL != nil {
			validateLocalFile(root, observation.SQL.QueryFile, pointer+"/sql/queryFile", "sql_query", validator)
			if len(observation.SQL.OrderBy) == 0 {
				validator.add(pointer+"/sql/orderBy", "stable_sql_order", "SQL snapshots require explicit stable ordering keys")
			}
		}
		if observation.Kafka != nil {
			if observation.Kafka.EndOffset < observation.Kafka.StartOffset {
				validator.add(pointer+"/kafka/endOffset", "bounded_kafka_range", "Kafka endOffset must not precede startOffset")
			}
			if observation.Kafka.Mode == "keyed" && observation.Kafka.KeyPointer == "" {
				validator.add(pointer+"/kafka/keyPointer", "keyed_comparison", "keyed Kafka comparison requires an exact keyPointer")
			}
		}
	}

	invariantIDs := map[string]struct{}{}
	for index, invariant := range invariants {
		pointer := fmt.Sprintf("/spec/invariants/%d", index)
		if _, exists := invariantIDs[invariant.ID]; exists {
			validator.add(pointer+"/id", "unique_invariant", fmt.Sprintf("invariant %q is duplicated", invariant.ID))
		}
		invariantIDs[invariant.ID] = struct{}{}
		validateLocalFile(root, invariant.QueryFile, pointer+"/queryFile", "invariant_query", validator)
	}

	normalizationIDs := map[string]struct{}{}
	for index, normalization := range normalizations {
		pointer := fmt.Sprintf("/spec/normalization/%d", index)
		if _, exists := normalizationIDs[normalization.ID]; exists {
			validator.add(pointer+"/id", "unique_normalization", fmt.Sprintf("normalization %q is duplicated", normalization.ID))
		}
		normalizationIDs[normalization.ID] = struct{}{}
		if _, exists := observations[normalization.Observation]; !exists {
			validator.add(pointer+"/observation", "normalization_observation", fmt.Sprintf("observation %q is undeclared", normalization.Observation))
		}
		if !validJSONPointer(normalization.Pointer) {
			validator.add(pointer+"/pointer", "json_pointer", "normalization pointer is not a canonical JSON Pointer")
		}
		switch normalization.Type {
		case "remove", "timestamp":
			if normalization.Token != "" || len(normalization.Keys) > 0 || normalization.Tolerance != nil {
				validator.add(pointer, "normalization_options", normalization.Type+" normalization accepts only an exact pointer")
			}
		case "replace":
			if normalization.Token == "" {
				validator.add(pointer+"/token", "normalization_options", "replace normalization requires a fixed token")
			}
			if len(normalization.Keys) > 0 || normalization.Tolerance != nil {
				validator.add(pointer, "normalization_options", "replace normalization does not accept keys or tolerance")
			}
		case "stableOrder":
			if len(normalization.Keys) == 0 {
				validator.add(pointer+"/keys", "normalization_options", "stableOrder normalization requires declared keys")
			}
			if normalization.Token != "" || normalization.Tolerance != nil {
				validator.add(pointer, "normalization_options", "stableOrder normalization does not accept token or tolerance")
			}
		case "numericTolerance":
			if normalization.Tolerance == nil || *normalization.Tolerance < 0 {
				validator.add(pointer+"/tolerance", "normalization_options", "numericTolerance requires a nonnegative tolerance")
			}
			if normalization.Token != "" || len(normalization.Keys) > 0 {
				validator.add(pointer, "normalization_options", "numericTolerance normalization does not accept token or keys")
			}
		}
	}

	if quiescence.Timeout.Duration <= 0 || quiescence.StabilityWindow.Duration <= 0 || quiescence.StabilityWindow.Duration >= quiescence.Timeout.Duration {
		validator.add("/spec/quiescence", "quiescence_bounds", "quiescence timeout and stability window must be positive, and the window must be shorter than the timeout")
	}
	conditionIDs := map[string]struct{}{}
	for index, condition := range quiescence.Conditions {
		if _, exists := conditionIDs[condition.ID]; exists {
			validator.add(fmt.Sprintf("/spec/quiescence/conditions/%d/id", index), "unique_quiescence", fmt.Sprintf("quiescence condition %q is duplicated", condition.ID))
		}
		conditionIDs[condition.ID] = struct{}{}
	}
	return observations
}

func validJSONPointer(pointer string) bool {
	if pointer == "" {
		return true
	}
	if !strings.HasPrefix(pointer, "/") {
		return false
	}
	for index := 0; index < len(pointer); index++ {
		if pointer[index] == '~' && (index+1 >= len(pointer) || (pointer[index+1] != '0' && pointer[index+1] != '1')) {
			return false
		}
	}
	return true
}

func validateLocalFile(root, relative, pointer, rule string, validator *validation) {
	path, err := confinedPath(root, relative)
	if err != nil {
		validator.add(pointer, rule, err.Error())
		return
	}
	if rule == "sql_query" || rule == "invariant_query" {
		document, readErr := readBounded(path)
		if readErr != nil {
			validator.add(pointer, rule, readErr.Error())
			return
		}
		upper := strings.ToUpper(strings.TrimSpace(string(document)))
		for _, forbidden := range []string{"INSERT ", "UPDATE ", "DELETE ", "MERGE ", "CREATE ", "ALTER ", "DROP ", "TRUNCATE ", "GRANT ", "REVOKE ", "COPY "} {
			if strings.Contains(upper, forbidden) {
				validator.add(pointer, "read_only_sql", fmt.Sprintf("query contains forbidden statement keyword %s", strings.TrimSpace(forbidden)))
				break
			}
		}
		if !strings.HasPrefix(upper, "SELECT ") && !strings.HasPrefix(upper, "WITH ") {
			validator.add(pointer, "read_only_sql", "query must begin with SELECT or WITH")
		}
	}
}

func confinedPath(root, relative string) (string, error) {
	if relative == "" || filepath.IsAbs(relative) {
		return "", fmt.Errorf("path must be nonempty and relative")
	}
	if strings.Contains(relative, "://") {
		return "", fmt.Errorf("URI schemes are forbidden")
	}
	clean := filepath.Clean(relative)
	if clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path traversal is forbidden")
	}
	rootAbsolute, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	rootResolved, err := filepath.EvalSymlinks(rootAbsolute)
	if err != nil {
		return "", fmt.Errorf("resolve contract root: %w", err)
	}
	candidate := filepath.Join(rootResolved, clean)
	resolved, err := filepath.EvalSymlinks(candidate)
	if err != nil {
		return "", fmt.Errorf("resolve local path %q: %w", relative, err)
	}
	within, err := filepath.Rel(rootResolved, resolved)
	if err != nil || within == ".." || strings.HasPrefix(within, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path %q escapes the contract root", relative)
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return "", fmt.Errorf("inspect local path %q: %w", relative, err)
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("local path %q is not a regular file", relative)
	}
	return resolved, nil
}

func validatePayloadSchema(root, relative string, data map[string]any) error {
	path, err := confinedPath(root, relative)
	if err != nil {
		return err
	}
	root, err = filepath.Abs(root)
	if err != nil {
		return fmt.Errorf("resolve payload schema root: %w", err)
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		return fmt.Errorf("resolve payload schema root: %w", err)
	}
	documents := map[string]any{}
	if err := collectSchemaDocuments(root, path, documents); err != nil {
		return err
	}
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	compiler.AssertFormat()
	compiler.UseLoader(denyLoader{})
	for resourcePath, document := range documents {
		if err := compiler.AddResource(fileURL(resourcePath), document); err != nil {
			return fmt.Errorf("add local payload schema: %w", err)
		}
	}
	schema, err := compiler.Compile(fileURL(path))
	if err != nil {
		return fmt.Errorf("compile local payload schema: %w", err)
	}
	if err := schema.Validate(data); err != nil {
		return fmt.Errorf("event data failed local payload schema: %w", err)
	}
	return nil
}

func collectSchemaDocuments(root, path string, documents map[string]any) error {
	if _, seen := documents[path]; seen {
		return nil
	}
	documentBytes, err := readBounded(path)
	if err != nil {
		return err
	}
	var document any
	if err := json.Unmarshal(documentBytes, &document); err != nil {
		return fmt.Errorf("decode local payload schema %q: %w", path, err)
	}
	documents[path] = document
	refs := []string{}
	collectRefs(document, &refs)
	for _, reference := range refs {
		base, _, _ := strings.Cut(reference, "#")
		if base == "" {
			continue
		}
		parsed, err := url.Parse(base)
		if err != nil || parsed.Scheme != "" || parsed.Host != "" || filepath.IsAbs(base) {
			return fmt.Errorf("external schema reference %q is forbidden", reference)
		}
		nestedRelative, err := filepath.Rel(root, filepath.Join(filepath.Dir(path), filepath.FromSlash(base)))
		if err != nil {
			return err
		}
		nestedPath, err := confinedPath(root, nestedRelative)
		if err != nil {
			return fmt.Errorf("schema reference %q: %w", reference, err)
		}
		if err := collectSchemaDocuments(root, nestedPath, documents); err != nil {
			return err
		}
	}
	return nil
}

func collectRefs(value any, refs *[]string) {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			if key == "$ref" {
				if reference, ok := child.(string); ok {
					*refs = append(*refs, reference)
				}
			}
			collectRefs(child, refs)
		}
	case []any:
		for _, child := range typed {
			collectRefs(child, refs)
		}
	}
}

func fileURL(path string) string {
	return (&url.URL{Scheme: "file", Path: filepath.ToSlash(path)}).String()
}

func escapePointer(value string) string {
	value = strings.ReplaceAll(value, "~", "~0")
	return strings.ReplaceAll(value, "/", "~1")
}

// CompareTargets enforces the V1 baseline/candidate compatibility contract.
func CompareTargets(baseline, candidate Target, allowed []AllowedTargetDifference) []Violation {
	validator := validation{document: "targetComparison"}
	if baseline.Spec.DatabaseSchemaVersion != candidate.Spec.DatabaseSchemaVersion {
		validator.add("/spec/databaseSchemaVersion", "database_schema_match", "baseline and candidate database schema versions must match")
	}
	allow := map[string]struct{}{}
	for index, difference := range allowed {
		if !validJSONPointer(difference.Pointer) || difference.Pointer == "" {
			validator.add(fmt.Sprintf("/comparison/allowedTargetDifferences/%d/pointer", index), "allowed_difference_pointer", "allowed difference requires a nonempty canonical JSON Pointer")
		}
		if strings.TrimSpace(difference.Rationale) == "" {
			validator.add(fmt.Sprintf("/comparison/allowedTargetDifferences/%d/rationale", index), "allowed_difference_rationale", "allowed difference requires a rationale")
		}
		if _, duplicate := allow[difference.Pointer]; duplicate {
			validator.add(fmt.Sprintf("/comparison/allowedTargetDifferences/%d/pointer", index), "allowed_difference_unique", "allowed difference pointer is duplicated")
		}
		allow[difference.Pointer] = struct{}{}
	}

	if !reflect.DeepEqual(baseline.Metadata, candidate.Metadata) {
		recordDifference("/metadata", allow, &validator)
	}
	baseServices := map[string]Service{}
	candidateServices := map[string]Service{}
	for _, service := range baseline.Spec.Services {
		baseServices[service.Name] = service
	}
	for _, service := range candidate.Spec.Services {
		candidateServices[service.Name] = service
	}
	names := map[string]struct{}{}
	for name := range baseServices {
		names[name] = struct{}{}
	}
	for name := range candidateServices {
		names[name] = struct{}{}
	}
	orderedNames := make([]string, 0, len(names))
	for name := range names {
		orderedNames = append(orderedNames, name)
	}
	sort.Strings(orderedNames)
	for _, name := range orderedNames {
		base, baseExists := baseServices[name]
		candidateService, candidateExists := candidateServices[name]
		servicePointer := "/spec/services/" + escapePointer(name)
		if !baseExists || !candidateExists {
			recordDifference(servicePointer, allow, &validator)
			continue
		}
		baseRepository, _, baseImageErr := imagelock.ParseImmutableReference(base.Image)
		candidateRepository, _, candidateImageErr := imagelock.ParseImmutableReference(candidateService.Image)
		if baseImageErr != nil || candidateImageErr != nil || baseRepository != candidateRepository {
			recordDifference(servicePointer+"/image", allow, &validator)
		}
		base.Image = ""
		candidateService.Image = ""
		baseJSON, _ := json.Marshal(base)
		candidateJSON, _ := json.Marshal(candidateService)
		var left, right any
		_ = json.Unmarshal(baseJSON, &left)
		_ = json.Unmarshal(candidateJSON, &right)
		compareJSON(left, right, servicePointer, allow, &validator)
	}
	return sortedViolations(validator.violations)
}

func compareJSON(left, right any, pointer string, allowed map[string]struct{}, validator *validation) {
	if reflect.DeepEqual(left, right) {
		return
	}
	leftMap, leftOK := left.(map[string]any)
	rightMap, rightOK := right.(map[string]any)
	if leftOK && rightOK {
		keys := map[string]struct{}{}
		for key := range leftMap {
			keys[key] = struct{}{}
		}
		for key := range rightMap {
			keys[key] = struct{}{}
		}
		ordered := make([]string, 0, len(keys))
		for key := range keys {
			ordered = append(ordered, key)
		}
		sort.Strings(ordered)
		for _, key := range ordered {
			compareJSON(leftMap[key], rightMap[key], pointer+"/"+escapePointer(key), allowed, validator)
		}
		return
	}
	leftSlice, leftOK := left.([]any)
	rightSlice, rightOK := right.([]any)
	if leftOK && rightOK {
		maximum := len(leftSlice)
		if len(rightSlice) > maximum {
			maximum = len(rightSlice)
		}
		for index := 0; index < maximum; index++ {
			if index >= len(leftSlice) || index >= len(rightSlice) {
				recordDifference(fmt.Sprintf("%s/%d", pointer, index), allowed, validator)
				continue
			}
			compareJSON(leftSlice[index], rightSlice[index], fmt.Sprintf("%s/%d", pointer, index), allowed, validator)
		}
		return
	}
	recordDifference(pointer, allowed, validator)
}

func recordDifference(pointer string, allowed map[string]struct{}, validator *validation) {
	if _, ok := allowed[pointer]; ok {
		return
	}
	validator.add(pointer, "unlisted_target_difference", "baseline and candidate differ outside the exact allowlist")
}

// ValidateWorkload validates a standalone workload correctness contract.
func ValidateWorkload(workload Workload, root string) []Violation {
	validator := validation{document: "workload"}
	if workload.APIVersion != APIVersion {
		validator.add("/apiVersion", "api_version", "apiVersion must be chronicle.dev/v1alpha1")
	}
	if workload.Kind != "Workload" {
		validator.add("/kind", "kind", "kind must be Workload")
	}
	validateCorrectnessContract(workload.Spec.Events, workload.Spec.Observations, workload.Spec.Invariants, workload.Spec.Normalization, workload.Spec.Quiescence, root, &validator)
	return sortedViolations(validator.violations)
}

// ValidateResult validates semantic constraints beyond JSON Schema.
func ValidateResult(result Result) []Violation {
	validator := validation{document: "result"}
	if result.APIVersion != APIVersion {
		validator.add("/apiVersion", "api_version", "apiVersion must be chronicle.dev/v1alpha1")
	}
	if result.Kind != "Result" {
		validator.add("/kind", "kind", "kind must be Result")
	}
	if _, ok := classifications[result.Classification]; !ok {
		validator.add("/classification", "classification", "classification is unsupported")
	}
	return sortedViolations(validator.violations)
}

// ValidateBundle validates safety constraints beyond JSON Schema.
func ValidateBundle(bundle Bundle) []Violation {
	validator := validation{document: "bundle"}
	if bundle.APIVersion != APIVersion {
		validator.add("/apiVersion", "api_version", "apiVersion must be chronicle.dev/v1alpha1")
	}
	if bundle.Kind != "Bundle" {
		validator.add("/kind", "kind", "kind must be Bundle")
	}
	if bundle.Safety.SymlinksAllowed {
		validator.add("/safety/symlinksAllowed", "bundle_symlinks", "reproduction bundles must forbid symlinks")
	}
	seen := map[string]struct{}{}
	for index, file := range bundle.Files {
		clean := filepath.Clean(file.Path)
		if filepath.IsAbs(file.Path) || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
			validator.add(fmt.Sprintf("/files/%d/path", index), "bundle_path", "bundle path is unsafe")
		}
		if _, duplicate := seen[clean]; duplicate {
			validator.add(fmt.Sprintf("/files/%d/path", index), "bundle_path_unique", "bundle path is duplicated")
		}
		seen[clean] = struct{}{}
	}
	return sortedViolations(validator.violations)
}
