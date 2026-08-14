package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/uday1o1/chronicle-gate/internal/artifact"
	"github.com/uday1o1/chronicle-gate/internal/broker"
	"github.com/uday1o1/chronicle-gate/internal/database"
	"github.com/uday1o1/chronicle-gate/internal/observe"
	"github.com/uday1o1/chronicle-gate/internal/registry"
	"github.com/uday1o1/chronicle-gate/internal/runlog"
	cruntime "github.com/uday1o1/chronicle-gate/internal/runtime"
	"github.com/uday1o1/chronicle-gate/internal/spec"
)

type plannedObservation struct {
	Identity    observe.Identity
	Declaration spec.Observation
	Timeout     time.Duration
	Rules       []spec.Normalization
}

type generalPlan struct {
	service      spec.Service
	publishes    []plannedPublish
	observations []plannedObservation
	invariants   []spec.Invariant
}

func isGeneralObserverScenario(scenario spec.Scenario) bool {
	hasObserve := false
	for _, step := range scenario.Spec.Steps {
		if step.Observe != nil {
			hasObserve = true
		}
		if step.RewindOffset != nil || step.ArmCheckpoint != nil || step.ReleaseCheckpoint != nil || step.Stop != nil || step.Restart != nil || step.AdvanceClock != nil {
			return false
		}
	}
	return hasObserve
}

func buildGeneralPlan(scenario spec.Scenario, target spec.Target) (generalPlan, error) {
	if len(target.Spec.Services) != 1 {
		return generalPlan{}, errors.New("observer scenarios currently require exactly one target service")
	}
	plan := generalPlan{service: target.Spec.Services[0], invariants: append([]spec.Invariant(nil), scenario.Spec.Invariants...)}
	declarations := map[string]spec.Observation{}
	for _, declaration := range scenario.Spec.Observations {
		declarations[declaration.ID] = declaration
	}
	ordered, err := topologicalSteps(scenario.Spec.Steps)
	if err != nil {
		return generalPlan{}, err
	}
	occurrences := map[string]int{}
	for _, step := range ordered {
		switch {
		case step.Publish != nil:
			event, exists := scenario.Spec.Events[step.Publish.Event]
			if !exists {
				return generalPlan{}, fmt.Errorf("published event %q is undeclared", step.Publish.Event)
			}
			if step.Publish.Key == "" || step.Publish.Key != event.AggregateID {
				return generalPlan{}, errors.New("kafka record key must equal CloudEvent aggregateid")
			}
			plan.publishes = append(plan.publishes, plannedPublish{StepID: step.ID, Action: *step.Publish, Event: event})
		case step.Observe != nil:
			declaration, exists := declarations[step.Observe.Observation]
			if !exists {
				return generalPlan{}, fmt.Errorf("observe step %q references an undeclared observer", step.ID)
			}
			occurrences[declaration.ID]++
			rules := []spec.Normalization{}
			for _, rule := range scenario.Spec.Normalization {
				if rule.Observation == declaration.ID {
					if declaration.Kafka != nil && declaration.Kafka.Mode == "ordered" && rule.Type == "stableOrder" && rule.Pointer == "" {
						return generalPlan{}, errors.New("ordered Kafka observations cannot sort their top-level record array")
					}
					rules = append(rules, rule)
				}
			}
			plan.observations = append(plan.observations, plannedObservation{
				Identity:    observe.Identity{StepID: step.ID, ObserverID: declaration.ID, Occurrence: occurrences[declaration.ID]},
				Declaration: declaration, Timeout: step.Timeout.Duration, Rules: rules,
			})
		case step.Await != nil:
		default:
			return generalPlan{}, fmt.Errorf("observer scenario step %q uses an unsupported action", step.ID)
		}
	}
	if len(plan.publishes) == 0 || len(plan.observations) == 0 {
		return generalPlan{}, errors.New("observer scenarios require at least one publish and one observe step")
	}
	return plan, nil
}

type generalAttemptConfig struct {
	RunID        string
	Index        int
	Role         string
	Scenario     spec.Scenario
	ScenarioRoot string
	Output       string
	Plan         generalPlan
	Target       spec.Target
	Environment  *cruntime.Environment
	Database     *database.Manager
	Broker       *broker.Admin
	Journal      *runlog.Journal
	SecretValues []string
	Baseline     *AttemptEvidence
}

func executeGeneralAttempt(ctx context.Context, config generalAttemptConfig) (evidence AttemptEvidence, resultErr error) {
	attemptID := fmt.Sprintf("%s-%d", config.Role, config.Index)
	databaseName := fmt.Sprintf("cg_%s_%s_%d", config.RunID, config.Role, config.Index)
	prefix := "cg." + config.RunID + "." + config.Role + "." + fmt.Sprint(config.Index)
	group := prefix + "." + config.Plan.service.Name
	evidence = AttemptEvidence{
		AttemptID: attemptID, Role: config.Role, Status: "INCOMPLETE", Database: databaseName, Group: group,
		AuthoredImage: config.Plan.service.Image, Publications: []broker.RecordIdentity{}, Deliveries: []database.Delivery{},
		Observations: []observe.Evidence{}, Registry: []registry.Evidence{}, ObservationRows: []map[string]any{}, InvariantRows: []map[string]any{},
	}
	var attempt database.Attempt
	var service *cruntime.ServiceRuntime
	createdTopics := []string{}
	databaseCreated := false
	defer func() {
		if resultErr != nil {
			evidence.Error = resultErr.Error()
		}
		if writeErr := journaled(config.Journal, executionState(config.Role, "OBSERVING"), "persist_attempt_evidence", map[string]any{"attemptId": attemptID}, func() error {
			return artifact.WritePublicJSON(filepath.Join(config.Output, "attempts", attemptID+".json"), evidence, config.SecretValues)
		}); writeErr != nil {
			resultErr = joinInfrastructure(resultErr, writeErr)
		}
		cleanupContext, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if service != nil {
			resultErr = joinInfrastructure(resultErr, journaled(config.Journal, executionState(config.Role, "STOPPING"), "terminate_service", map[string]any{"attemptId": attemptID}, func() error { return service.Terminate(cleanupContext) }))
		}
		for index := len(createdTopics) - 1; index >= 0; index-- {
			topic := createdTopics[index]
			resultErr = joinInfrastructure(resultErr, journaled(config.Journal, executionState(config.Role, "STOPPING"), "delete_topic", map[string]any{"attemptId": attemptID, "topic": topic}, func() error { return config.Broker.DeleteTopic(cleanupContext, topic) }))
		}
		if databaseCreated {
			resultErr = joinInfrastructure(resultErr, journaled(config.Journal, executionState(config.Role, "STOPPING"), "drop_attempt_database", map[string]any{"attemptId": attemptID, "database": databaseName}, func() error { return config.Database.DropAttempt(cleanupContext, databaseName) }))
		}
	}()

	var err error
	err = journaled(config.Journal, executionState(config.Role, "STARTING"), "clone_attempt_database", map[string]any{"attemptId": attemptID}, func() error {
		attempt, err = config.Database.Clone(ctx, databaseName)
		return err
	})
	if err != nil {
		return evidence, err
	}
	databaseCreated = true
	if err := database.AssertObserverReadOnly(ctx, attempt.ObserverDSN); err != nil {
		return evidence, err
	}
	logicalTopics := map[string]struct{}{}
	for _, publication := range config.Plan.publishes {
		logicalTopics[publication.Action.Topic] = struct{}{}
	}
	for _, observation := range config.Plan.observations {
		if observation.Declaration.Kafka != nil {
			logicalTopics[observation.Declaration.Kafka.Topic] = struct{}{}
		}
	}
	orderedTopics := make([]string, 0, len(logicalTopics))
	for topic := range logicalTopics {
		orderedTopics = append(orderedTopics, topic)
	}
	sort.Strings(orderedTopics)
	for _, logical := range orderedTopics {
		physical := prefix + "." + logical
		if err := config.Broker.CreateTopic(ctx, physical); err != nil {
			return evidence, err
		}
		createdTopics = append(createdTopics, physical)
	}
	for _, publication := range config.Plan.publishes {
		if publication.Event.Registry == nil {
			continue
		}
		registered, err := registry.RegisterEvent(ctx, config.Environment.HostSchemaRegistry, prefix, config.ScenarioRoot, publication.Event)
		if err != nil {
			return evidence, fmt.Errorf("register event schema: %w", err)
		}
		evidence.Registry = append(evidence.Registry, registered)
	}
	exposed := []int{}
	for _, observation := range config.Plan.observations {
		if observation.Declaration.HTTP != nil {
			exposed = append(exposed, observation.Declaration.HTTP.Port)
		}
	}
	service, err = cruntime.StartService(ctx, cruntime.ServiceConfig{
		RunID: config.RunID, AttemptID: attemptID, Service: config.Plan.service, Network: config.Environment.Network,
		DatabaseDSN: attempt.ServiceDSN, InternalBroker: config.Environment.InternalBroker, TopicPrefix: prefix, GroupPrefix: prefix,
		SecretDirectory: filepath.Join(config.Output, ".secrets"), ExposedPorts: exposed,
	})
	if err != nil {
		return evidence, err
	}
	evidence.ExecutedImageID = service.ImageID
	if err := config.Journal.State(executionState(config.Role, "EXECUTING"), ""); err != nil {
		return evidence, err
	}
	evidence.SchemaAfterHealth, err = config.Database.FingerprintAttempt(ctx, attempt)
	if err != nil || evidence.SchemaAfterHealth != config.Database.TemplateFingerprint() {
		if err == nil {
			err = errors.New("attempt schema fingerprint differs after health")
		}
		return evidence, err
	}
	for _, publication := range config.Plan.publishes {
		document, marshalErr := observe.Canonical(publication.Event)
		if marshalErr != nil {
			return evidence, marshalErr
		}
		digest := valueHash(publication.Event)
		physical := prefix + "." + publication.Action.Topic
		record, publishErr := config.Broker.Publish(ctx, physical, int32(publication.Action.Partition), []byte(publication.Action.Key), document, digest)
		if publishErr != nil {
			return evidence, publishErr
		}
		evidence.Publications = append(evidence.Publications, record)
		if len(evidence.Publications) == 1 {
			evidence.Published, evidence.Topic = record, record.Topic
		}
	}
	if err := waitGeneralQuiescence(ctx, config, group, evidence.Publications); err != nil {
		return evidence, fmt.Errorf("%w; service=%s; logs=%q", err, service.Diagnostics(context.Background()), service.RecentLogs(context.Background()))
	}
	if err := config.Journal.State(executionState(config.Role, "OBSERVING"), ""); err != nil {
		return evidence, err
	}
	for _, planned := range config.Plan.observations {
		collectionContext, cancel := context.WithTimeout(ctx, planned.Timeout)
		var observation observe.Evidence
		switch declaration := planned.Declaration; {
		case declaration.SQL != nil:
			observation, err = observe.CollectSQL(collectionContext, planned.Identity, config.ScenarioRoot, attempt.ObserverDSN, declaration, planned.Rules)
		case declaration.Kafka != nil:
			physical := prefix + "." + declaration.Kafka.Topic
			bounds, boundsErr := config.Broker.Bounds(collectionContext, physical, int32(declaration.Kafka.Partition))
			if boundsErr != nil {
				err = boundsErr
			} else {
				observation, err = observe.CollectKafka(collectionContext, observe.KafkaConfig{Brokers: []string{config.Environment.HostBroker}, Identity: planned.Identity, Root: config.ScenarioRoot, PhysicalTopic: physical, Declaration: *declaration.Kafka, Rules: planned.Rules, Bounds: bounds})
			}
		case declaration.HTTP != nil:
			if declaration.HTTP.Service != config.Plan.service.Name {
				err = fmt.Errorf("http observer service %q is not the target service", declaration.HTTP.Service)
				break
			}
			endpoint, endpointErr := service.PortEndpoint(collectionContext, declaration.HTTP.Port)
			if endpointErr != nil {
				err = endpointErr
			} else {
				source := observe.HTTPSource{Service: declaration.HTTP.Service, Port: declaration.HTTP.Port, Path: declaration.HTTP.Path, Endpoint: endpoint}
				observation, err = observe.CollectHTTP(collectionContext, planned.Identity, "http", declaration.HTTP.Mode, source, planned.Rules, "")
			}
		default:
			err = fmt.Errorf("observer %q is not supported by the no-fault executor", declaration.ID)
		}
		cancel()
		if err != nil {
			var payloadFailure *observe.PayloadValidationError
			if errors.As(err, &payloadFailure) {
				evidence.Observations = append(evidence.Observations, observation)
				if writeErr := writeGeneralObservation(config, attemptID, planned, observation); writeErr != nil {
					return evidence, writeErr
				}
				if config.Baseline == nil {
					return evidence, &unresolvedFailure{err: fmt.Errorf("baseline runtime payload is invalid: %w", payloadFailure)}
				}
				if evidence.Signature == nil {
					evidence.Signature, err = newObservedSignature("SCHEMA_REGRESSION", planned.Identity.ObserverID, observe.Difference{
						Pointer: fmt.Sprintf("/%d/event/data", payloadFailure.RecordIndex), RowKey: payloadFailure.SchemaFile,
						Expected: true, Actual: false, Message: "candidate runtime payload violates the shared local schema",
					})
					if err != nil {
						return evidence, err
					}
				}
				err = nil
				continue
			}
			if errors.Is(err, observe.ErrRangeExpired) {
				return evidence, &unresolvedFailure{err: err}
			}
			return evidence, err
		}
		evidence.Observations = append(evidence.Observations, observation)
		if err := writeGeneralObservation(config, attemptID, planned, observation); err != nil {
			return evidence, err
		}
	}
	for _, invariant := range config.Plan.invariants {
		document, err := os.ReadFile(filepath.Join(config.ScenarioRoot, invariant.QueryFile))
		if err != nil {
			return evidence, err
		}
		rows, err := database.Query(ctx, attempt.ObserverDSN, string(document))
		if err != nil {
			return evidence, err
		}
		evidence.InvariantRows = append(evidence.InvariantRows, rows...)
		if len(rows) != 0 {
			evidence.Signature, err = NewFailureSignature(invariant, rows)
			if err != nil {
				return evidence, err
			}
		}
	}
	evidence.SchemaAfterObservation, err = config.Database.FingerprintAttempt(ctx, attempt)
	if err != nil {
		return evidence, err
	}
	if evidence.SchemaAfterObservation != evidence.SchemaAfterHealth || evidence.SchemaAfterObservation != config.Database.TemplateFingerprint() {
		return evidence, errors.New("attempt schema fingerprint changed during observation")
	}
	if config.Baseline != nil && evidence.Signature == nil {
		evidence.Signature, err = compareGeneralAttempts(*config.Baseline, evidence, config.Plan)
		if err != nil {
			return evidence, err
		}
	}
	evidence.Status = "COMPLETE"
	return evidence, nil
}

func writeGeneralObservation(config generalAttemptConfig, attemptID string, planned plannedObservation, observation observe.Evidence) error {
	artifactName := fmt.Sprintf("%s--%s--%d.json", planned.Identity.StepID, planned.Identity.ObserverID, planned.Identity.Occurrence)
	return artifact.WritePublicJSON(filepath.Join(config.Output, "observations", attemptID, artifactName), observation, config.SecretValues)
}

func waitGeneralQuiescence(ctx context.Context, config generalAttemptConfig, group string, publications []broker.RecordIdentity) error {
	deadline := time.Now().Add(config.Scenario.Spec.Quiescence.Timeout.Duration)
	stableSince := time.Time{}
	for time.Now().Before(deadline) {
		allCommitted := true
		for _, publication := range publications {
			position, err := config.Broker.CommittedOffset(ctx, group, publication.Topic, publication.Partition)
			if err != nil || position != publication.Offset+1 {
				allCommitted = false
				break
			}
		}
		if allCommitted {
			if stableSince.IsZero() {
				stableSince = time.Now()
			}
			if time.Since(stableSince) >= config.Scenario.Spec.Quiescence.StabilityWindow.Duration {
				return nil
			}
		} else {
			stableSince = time.Time{}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	return &unresolvedFailure{err: errors.New("declared quiescence was not stable before its deadline")}
}

func compareGeneralAttempts(baseline, candidate AttemptEvidence, plan generalPlan) (*FailureSignature, error) {
	if len(baseline.Observations) != len(plan.observations) || len(candidate.Observations) != len(plan.observations) {
		return nil, errors.New("baseline and candidate did not complete the declared observation inventory")
	}
	if difference := compareRegistryEvidence(baseline.Registry, candidate.Registry); difference != nil {
		return newObservedSignature("SCHEMA_REGRESSION", "schema-registry", *difference)
	}
	for index, planned := range plan.observations {
		left, right := baseline.Observations[index], candidate.Observations[index]
		if left.Identity != planned.Identity || right.Identity != planned.Identity {
			return nil, fmt.Errorf("observation inventory identity mismatch at %d", index)
		}
		keyPointer := ""
		switch declaration := planned.Declaration; {
		case declaration.SQL != nil:
			keyPointer = declaration.SQL.KeyPointer
		case declaration.Kafka != nil:
			keyPointer = declaration.Kafka.KeyPointer
		case declaration.HTTP != nil:
			keyPointer = declaration.HTTP.KeyPointer
		}
		differences, err := observe.Compare(left, right, planned.Rules, keyPointer)
		if err != nil {
			return nil, err
		}
		if len(differences) != 0 {
			sort.Slice(differences, func(a, b int) bool {
				return differences[a].Pointer+differences[a].RowKey < differences[b].Pointer+differences[b].RowKey
			})
			return newObservedSignature("SEMANTIC_REGRESSION", planned.Identity.ObserverID, differences[0])
		}
	}
	return nil, nil
}

func compareRegistryEvidence(left, right []registry.Evidence) *observe.Difference {
	if len(left) != len(right) {
		return &observe.Difference{Pointer: "/registry", Expected: len(left), Actual: len(right), Message: "Registry evidence inventory differs"}
	}
	for index := range left {
		a, b := left[index], right[index]
		if a.LogicalSubject != b.LogicalSubject || a.Compatibility != b.Compatibility || a.EffectiveMode != b.EffectiveMode || len(a.Versions) != len(b.Versions) {
			return &observe.Difference{Pointer: fmt.Sprintf("/registry/%d", index), Expected: a.LogicalSubject + ":" + a.EffectiveMode, Actual: b.LogicalSubject + ":" + b.EffectiveMode, Message: "Registry contract differs"}
		}
		for version := range a.Versions {
			if a.Versions[version].BundledSHA256 != b.Versions[version].BundledSHA256 || a.Versions[version].SchemaType != b.Versions[version].SchemaType {
				return &observe.Difference{Pointer: fmt.Sprintf("/registry/%d/versions/%d", index, version), Expected: a.Versions[version].BundledSHA256, Actual: b.Versions[version].BundledSHA256, Message: "Registry schema differs"}
			}
		}
	}
	return nil
}

func newObservedSignature(classification, observationID string, difference observe.Difference) (*FailureSignature, error) {
	signature := &FailureSignature{
		InvariantID: observationID, Classification: classification, ObservationID: observationID,
		RowKey: difference.RowKey, Pointer: difference.Pointer, Expected: difference.Expected, Actual: difference.Actual,
	}
	digest := valueHash(*signature)
	signature.Digest = digest
	return signature, nil
}

func topologicalSteps(steps []spec.Step) ([]spec.Step, error) {
	byID := map[string]spec.Step{}
	remaining := map[string]int{}
	dependents := map[string][]string{}
	for _, step := range steps {
		byID[step.ID] = step
		remaining[step.ID] = len(step.DependsOn)
		for _, dependency := range step.DependsOn {
			dependents[dependency] = append(dependents[dependency], step.ID)
		}
	}
	ready := []string{}
	for id, count := range remaining {
		if count == 0 {
			ready = append(ready, id)
		}
	}
	sort.Strings(ready)
	ordered := make([]spec.Step, 0, len(steps))
	for len(ready) != 0 {
		id := ready[0]
		ready = ready[1:]
		ordered = append(ordered, byID[id])
		for _, dependent := range dependents[id] {
			remaining[dependent]--
			if remaining[dependent] == 0 {
				ready = append(ready, dependent)
				sort.Strings(ready)
			}
		}
	}
	if len(ordered) != len(steps) {
		return nil, errors.New("scenario step graph is cyclic")
	}
	return ordered, nil
}

func observationRules(scenario spec.Scenario, id string) []spec.Normalization {
	rules := []spec.Normalization{}
	for _, rule := range scenario.Spec.Normalization {
		if strings.EqualFold(rule.Observation, id) {
			rules = append(rules, rule)
		}
	}
	return rules
}
