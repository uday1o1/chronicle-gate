package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/uday1o1/chronicle-gate/internal/artifact"
	"github.com/uday1o1/chronicle-gate/internal/broker"
	"github.com/uday1o1/chronicle-gate/internal/database"
	"github.com/uday1o1/chronicle-gate/internal/effects"
	"github.com/uday1o1/chronicle-gate/internal/observe"
	"github.com/uday1o1/chronicle-gate/internal/probeclient"
	"github.com/uday1o1/chronicle-gate/internal/runlog"
	cruntime "github.com/uday1o1/chronicle-gate/internal/runtime"
	"github.com/uday1o1/chronicle-gate/internal/spec"
	"github.com/uday1o1/chronicle-gate/pkg/probe"
)

const outboxDatabaseSchemaVersion = "order-lifecycle-v3"

type outboxPlan struct {
	mode         string
	orderAPI     spec.Service
	relay        spec.Service
	workflow     spec.Service
	projector    spec.Service
	sink         spec.Service
	trigger      plannedPublish
	checkpoint   *spec.CheckpointSelector
	observations []plannedObservation
	invariants   []spec.Invariant
}

func isOutboxScenario(scenario spec.Scenario) bool {
	return scenario.Spec.Control != nil && (scenario.Spec.Control.Mode == "outbox-crash" || scenario.Spec.Control.Mode == "outbox-control")
}

func buildOutboxPlan(scenario spec.Scenario, target spec.Target) (outboxPlan, error) {
	if !isOutboxScenario(scenario) {
		return outboxPlan{}, errors.New("outbox executor requires outbox-crash or outbox-control")
	}
	if target.Spec.DatabaseSchemaVersion != outboxDatabaseSchemaVersion {
		return outboxPlan{}, fmt.Errorf("outbox target requires database schema %q", outboxDatabaseSchemaVersion)
	}
	if len(target.Spec.Services) != 5 {
		return outboxPlan{}, errors.New("outbox scenarios require exactly five connected services")
	}
	services := map[string]spec.Service{}
	for _, service := range target.Spec.Services {
		if _, exists := services[service.Name]; exists {
			return outboxPlan{}, fmt.Errorf("duplicate outbox service %q", service.Name)
		}
		services[service.Name] = service
	}
	for _, name := range []string{"order-api", "outbox-relay", "order-workflow", "fulfillment-projector", "effect-sink"} {
		if _, exists := services[name]; !exists {
			return outboxPlan{}, fmt.Errorf("outbox target omits required service %q", name)
		}
	}
	relay := services["outbox-relay"]
	if !relay.Probe.Enabled || relay.Probe.ProtocolVersion != probe.ProtocolVersion || relay.Probe.CommitMode != "manual_sync" || relay.Probe.MaxControlledInFlight != 1 || !relay.Probe.LogicalClock || !containsString(relay.Probe.Checkpoints, "after_outbox_publish") {
		return outboxPlan{}, errors.New("outbox relay requires the live manual-sync single-record probe with after_outbox_publish and logical time")
	}
	if !containsString(services["order-workflow"].Dependencies, "effect-sink") {
		return outboxPlan{}, errors.New("order-workflow must declare its effect-sink dependency")
	}

	ordered, err := topologicalSteps(scenario.Spec.Steps)
	if err != nil {
		return outboxPlan{}, err
	}
	declarations := map[string]spec.Observation{}
	for _, declaration := range scenario.Spec.Observations {
		declarations[declaration.ID] = declaration
	}
	plan := outboxPlan{
		mode: scenario.Spec.Control.Mode, orderAPI: services["order-api"], relay: relay,
		workflow: services["order-workflow"], projector: services["fulfillment-projector"], sink: services["effect-sink"],
		invariants: append([]spec.Invariant(nil), scenario.Spec.Invariants...),
	}
	occurrences := map[string]int{}
	var armStep, publishStep, awaitStep, stopStep, restartStep string
	for _, step := range ordered {
		switch {
		case step.Publish != nil:
			if publishStep != "" {
				return outboxPlan{}, errors.New("outbox scenarios require exactly one qualification publication")
			}
			event, exists := scenario.Spec.Events[step.Publish.Event]
			if !exists || step.Publish.Topic != "outbox-relay-trigger" || step.Publish.Partition != 0 || step.Publish.Key == "" || step.Publish.Key != event.AggregateID {
				return outboxPlan{}, fmt.Errorf("outbox publish %q must be one keyed partition-0 outbox-relay-trigger CloudEvent", step.ID)
			}
			publishStep = step.ID
			plan.trigger = plannedPublish{StepID: step.ID, Action: *step.Publish, Event: event}
		case step.ArmCheckpoint != nil:
			if armStep != "" {
				return outboxPlan{}, errors.New("outbox crash supports exactly one checkpoint arm")
			}
			selector := step.ArmCheckpoint.CheckpointSelector
			armStep = step.ID
			plan.checkpoint = &selector
		case step.Await != nil:
			if step.Await.Checkpoint == nil || awaitStep != "" {
				return outboxPlan{}, fmt.Errorf("outbox await %q must identify the one relay checkpoint", step.ID)
			}
			awaitStep = step.ID
		case step.Stop != nil:
			if stopStep != "" || step.Stop.Service != "outbox-relay" {
				return outboxPlan{}, errors.New("outbox crash must stop only the outbox-relay")
			}
			stopStep = step.ID
		case step.Restart != nil:
			if restartStep != "" || step.Restart.Service != "outbox-relay" {
				return outboxPlan{}, errors.New("outbox crash must restart only the outbox-relay")
			}
			restartStep = step.ID
		case step.Observe != nil:
			declaration, exists := declarations[step.Observe.Observation]
			if !exists {
				return outboxPlan{}, fmt.Errorf("outbox observe step %q references an undeclared observer", step.ID)
			}
			if declaration.Kafka != nil {
				return outboxPlan{}, errors.New("outbox observations use effects, HTTP, or SQL evidence; Kafka is retained as physical runtime evidence")
			}
			occurrences[declaration.ID]++
			plan.observations = append(plan.observations, plannedObservation{
				Identity:    observe.Identity{StepID: step.ID, ObserverID: declaration.ID, Occurrence: occurrences[declaration.ID]},
				Declaration: declaration, Timeout: step.Timeout.Duration, Rules: observationRules(scenario, declaration.ID),
			})
		default:
			return outboxPlan{}, fmt.Errorf("outbox step %q uses an unsupported action", step.ID)
		}
	}
	if publishStep == "" || len(plan.observations) == 0 || len(plan.observations) != len(scenario.Spec.Observations) || len(plan.invariants) != 1 {
		return outboxPlan{}, errors.New("outbox scenarios require one trigger, every observation exactly once, and one invariant")
	}
	ancestors := engineAncestors(scenario.Spec.Steps)
	switch plan.mode {
	case "outbox-crash":
		if len(scenario.Spec.Seed.Orders) != 1 || armStep == "" || awaitStep == "" || stopStep == "" || restartStep == "" || plan.checkpoint == nil {
			return outboxPlan{}, errors.New("outbox-crash requires one seeded order and one complete arm-publish-await-stop-restart path")
		}
		expected := spec.CheckpointSelector{
			Service: "outbox-relay", Name: "after_outbox_publish", EventID: plan.trigger.Event.ID,
			StepID: plan.trigger.StepID, Occurrence: 1,
		}
		if *plan.checkpoint != expected {
			return outboxPlan{}, errors.New("outbox-crash checkpoint does not identify the exact qualification delivery")
		}
		awaitSelector := findAwaitSelector(scenario.Spec.Steps, awaitStep)
		if awaitSelector == nil || *awaitSelector != expected || !isAncestor(ancestors, armStep, publishStep) || !isAncestor(ancestors, publishStep, awaitStep) || !isAncestor(ancestors, awaitStep, stopStep) || !isAncestor(ancestors, stopStep, restartStep) {
			return outboxPlan{}, errors.New("outbox-crash lacks the exact arm-publish-await-stop-restart dependency chain")
		}
		for _, observation := range plan.observations {
			if !isAncestor(ancestors, restartStep, observation.Identity.StepID) {
				return outboxPlan{}, errors.New("every outbox-crash observation must follow relay restart")
			}
		}
	case "outbox-control":
		if len(scenario.Spec.Seed.Orders) != 2 || armStep != "" || awaitStep != "" || stopStep != "" || restartStep != "" {
			return outboxPlan{}, errors.New("outbox-control requires two seeded orders and no fault actions")
		}
		for _, observation := range plan.observations {
			if !isAncestor(ancestors, publishStep, observation.Identity.StepID) {
				return outboxPlan{}, errors.New("every outbox-control observation must follow its trigger")
			}
		}
	}
	return plan, nil
}

func findAwaitSelector(steps []spec.Step, id string) *spec.CheckpointSelector {
	for _, step := range steps {
		if step.ID == id && step.Await != nil {
			return step.Await.Checkpoint
		}
	}
	return nil
}

func containsString(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

type outboxAttemptConfig struct {
	RunID        string
	Index        int
	Role         string
	Scenario     spec.Scenario
	ScenarioRoot string
	Output       string
	Plan         outboxPlan
	Target       spec.Target
	Environment  *cruntime.Environment
	Database     *database.Manager
	Broker       *broker.Admin
	Journal      *runlog.Journal
	SecretValues []string
	Baseline     *AttemptEvidence
}

type outboxTopology struct {
	topics map[string]string
	groups map[string]string
}

func newOutboxTopology(prefix string) outboxTopology {
	topics := map[string]string{}
	for _, logical := range []string{
		"outbox-relay-trigger", "orders-created", "payment-requests", "inventory-requests",
		"payment-outcomes", "inventory-outcomes", "fulfillment", "fulfillment-results",
	} {
		topics[logical] = prefix + "." + logical
	}
	groups := map[string]string{
		"outbox-relay-trigger": prefix + ".outbox-relay-trigger",
		"orders-created":       prefix + ".order-workflow",
		"payment-requests":     prefix + ".payment-responder",
		"inventory-requests":   prefix + ".inventory-responder",
		"payment-outcomes":     prefix + ".payment-outcome",
		"inventory-outcomes":   prefix + ".inventory-outcome",
		"fulfillment":          prefix + ".fulfillment-projector",
	}
	return outboxTopology{topics: topics, groups: groups}
}

func executeOutboxAttempt(ctx context.Context, config outboxAttemptConfig) (evidence AttemptEvidence, resultErr error) {
	attemptID := fmt.Sprintf("%s-%d", config.Role, config.Index)
	databaseName := fmt.Sprintf("cg_%s_%s_%d", config.RunID, config.Role, config.Index)
	prefix := "cg." + config.RunID + "." + config.Role + "." + fmt.Sprint(config.Index)
	topology := newOutboxTopology(prefix)
	evidence = AttemptEvidence{
		AttemptID: attemptID, Role: config.Role, Status: "INCOMPLETE", Database: databaseName,
		Topic: topology.topics["orders-created"], Group: topology.groups["orders-created"],
		AuthoredImage: config.Plan.relay.Image, Publications: []broker.RecordIdentity{}, Deliveries: []database.Delivery{},
		ServiceImages: []ServiceImageEvidence{}, ProbeCapabilities: []probe.Capabilities{}, ProbeDeliveries: []probe.DeliveryReceipt{},
		CheckpointMode: config.Plan.mode, ControlMode: config.Plan.mode, GroupInitializations: map[string]broker.InitializationEvidence{},
		Observations: []observe.Evidence{}, ObservationRows: []map[string]any{}, InvariantRows: []map[string]any{},
		TopicBounds: map[string]broker.OffsetBounds{}, GroupOffsets: map[string]int64{}, EffectProjection: []effects.SemanticEntry{},
	}
	var attempt database.Attempt
	var sinkService, apiService, workflowService, projectorService, relayService *cruntime.ServiceRuntime
	var relayClient *probeclient.Client
	createdTopics := []string{}
	databaseCreated := false
	writerToken, err := randomRuntimeToken()
	if err != nil {
		return evidence, err
	}
	observerToken, err := randomRuntimeToken()
	if err != nil {
		return evidence, err
	}
	probeToken, err := randomRuntimeToken()
	if err != nil {
		return evidence, err
	}
	config.SecretValues = append(config.SecretValues, writerToken, observerToken, probeToken)
	config.Journal.SetSecretValues(config.SecretValues)
	defer func() {
		if resultErr != nil {
			evidence.Error = resultErr.Error()
		}
		if writeErr := journaled(config.Journal, executionState(config.Role, "OBSERVING"), "persist_attempt_evidence", map[string]any{"attemptId": attemptID}, func() error {
			return artifact.WritePublicJSON(filepath.Join(config.Output, "attempts", attemptID+".json"), evidence, config.SecretValues)
		}); writeErr != nil {
			resultErr = joinInfrastructure(resultErr, writeErr)
		}
		cleanupContext, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		if relayClient != nil {
			relayClient.Close()
		}
		services := []struct {
			name    string
			service **cruntime.ServiceRuntime
		}{
			{name: "outbox-relay", service: &relayService},
			{name: "fulfillment-projector", service: &projectorService},
			{name: "order-workflow", service: &workflowService},
			{name: "order-api", service: &apiService},
			{name: "effect-sink", service: &sinkService},
		}
		for _, item := range services {
			if *item.service != nil {
				current := *item.service
				resultErr = joinInfrastructure(resultErr, journaled(config.Journal, executionState(config.Role, "STOPPING"), "terminate_"+strings.ReplaceAll(item.name, "-", "_"), map[string]any{"attemptId": attemptID}, func() error { return current.Terminate(cleanupContext) }))
			}
		}
		for index := len(createdTopics) - 1; index >= 0; index-- {
			topic := createdTopics[index]
			resultErr = joinInfrastructure(resultErr, journaled(config.Journal, executionState(config.Role, "STOPPING"), "delete_topic", map[string]any{"attemptId": attemptID, "topic": topic}, func() error { return config.Broker.DeleteTopic(cleanupContext, topic) }))
		}
		if databaseCreated {
			resultErr = joinInfrastructure(resultErr, journaled(config.Journal, executionState(config.Role, "STOPPING"), "drop_attempt_database", map[string]any{"attemptId": attemptID, "database": databaseName}, func() error { return config.Database.DropAttempt(cleanupContext, databaseName) }))
		}
	}()

	if err := journaled(config.Journal, executionState(config.Role, "STARTING"), "clone_attempt_database", map[string]any{"attemptId": attemptID}, func() error {
		var cloneErr error
		attempt, cloneErr = config.Database.Clone(ctx, databaseName)
		return cloneErr
	}); err != nil {
		return evidence, err
	}
	databaseCreated = true
	if err := database.AssertObserverReadOnly(ctx, attempt.ObserverDSN); err != nil {
		return evidence, err
	}
	if err := config.Database.AssertWorkloadRoles(ctx, attempt); err != nil {
		return evidence, err
	}
	logicalTopics := make([]string, 0, len(topology.topics))
	for logical := range topology.topics {
		logicalTopics = append(logicalTopics, logical)
	}
	sort.Strings(logicalTopics)
	for _, logical := range logicalTopics {
		topic := topology.topics[logical]
		if err := config.Broker.CreateTopic(ctx, topic); err != nil {
			return evidence, err
		}
		createdTopics = append(createdTopics, topic)
	}
	groupTopics := make([]string, 0, len(topology.groups))
	for logical := range topology.groups {
		groupTopics = append(groupTopics, logical)
	}
	sort.Strings(groupTopics)
	for _, logical := range groupTopics {
		initialized, err := config.Broker.InitializeEmptyGroupAtZero(ctx, topology.groups[logical], topology.topics[logical], 0)
		if err != nil {
			return evidence, err
		}
		evidence.GroupInitializations[logical] = initialized
	}

	startService := func(name string, declaration spec.Service, dsn string, environment map[string]string, secrets []cruntime.SecretMount, exposed []int) (*cruntime.ServiceRuntime, error) {
		service, startErr := cruntime.StartService(ctx, cruntime.ServiceConfig{
			RunID: config.RunID, AttemptID: attemptID, Service: declaration, Network: config.Environment.Network,
			DatabaseDSN: dsn, InternalBroker: config.Environment.InternalBroker, TopicPrefix: prefix, GroupPrefix: prefix,
			SecretDirectory: filepath.Join(config.Output, ".secrets", attemptID, name), Environment: environment, Secrets: secrets, ExposedPorts: exposed,
		})
		if startErr != nil {
			return nil, startErr
		}
		if hardeningErr := service.AssertHardened(ctx, append([]int{declaration.Health.Port}, exposed...)...); hardeningErr != nil {
			_ = service.Terminate(context.Background())
			return nil, &infrastructureFailure{err: hardeningErr}
		}
		evidence.ServiceImages = append(evidence.ServiceImages, ServiceImageEvidence{Service: name, AuthoredImage: declaration.Image, ExecutedImageID: service.ImageID})
		return service, nil
	}

	sinkService, err = startService("effect-sink", config.Plan.sink, attempt.ServiceDSN, nil, []cruntime.SecretMount{
		{Environment: "CHRONICLE_SINK_WRITER_TOKEN_FILE", Filename: "sink-writer-token", Value: writerToken},
		{Environment: "CHRONICLE_SINK_OBSERVER_TOKEN_FILE", Filename: "sink-observer-token", Value: observerToken},
	}, nil)
	if err != nil {
		return evidence, err
	}
	sinkEndpoint, err := sinkService.PortEndpoint(ctx, 8080)
	if err != nil {
		return evidence, err
	}
	effectClient, err := effects.New(sinkEndpoint, observerToken)
	if err != nil {
		return evidence, err
	}
	apiService, err = startService("order-api", config.Plan.orderAPI, attempt.OrderAPIDSN, nil, nil, nil)
	if err != nil {
		return evidence, err
	}
	apiEndpoint, err := apiService.PortEndpoint(ctx, 8080)
	if err != nil {
		return evidence, err
	}
	workflowService, err = startService("order-workflow", config.Plan.workflow, attempt.ServiceDSN, map[string]string{
		"CHRONICLE_EFFECT_SINK_URL": "http://effect-sink:8080", "CHRONICLE_OUTCOME_ORDER": "payment-first",
	}, []cruntime.SecretMount{{Environment: "CHRONICLE_SINK_WRITER_TOKEN_FILE", Filename: "sink-writer-token", Value: writerToken}}, nil)
	if err != nil {
		return evidence, err
	}
	projectorService, err = startService("fulfillment-projector", config.Plan.projector, attempt.ServiceDSN, nil, nil, nil)
	if err != nil {
		return evidence, err
	}
	projectorEndpoint, err := projectorService.PortEndpoint(ctx, 8080)
	if err != nil {
		return evidence, err
	}
	clockStart, err := time.Parse(time.RFC3339Nano, config.Scenario.Spec.Clock.Start)
	if err != nil {
		return evidence, fmt.Errorf("parse outbox logical clock: %w", err)
	}
	relayService, err = startService("outbox-relay", config.Plan.relay, attempt.OutboxDSN, map[string]string{
		"CHRONICLE_QUALIFICATION_RELAY_LATCH": "enabled", "CHRONICLE_CONTROLLED_STEP_ID": config.Plan.trigger.StepID,
		"CHRONICLE_LOGICAL_CLOCK_CURRENT": clockStart.UTC().Format(time.RFC3339Nano),
	}, []cruntime.SecretMount{{Environment: "CHRONICLE_PROBE_TOKEN_FILE", Filename: "probe-token", Value: probeToken}}, []int{9090})
	if err != nil {
		return evidence, err
	}
	evidence.ExecutedImageID = relayService.ImageID
	probeEndpoint, err := relayService.PortEndpoint(ctx, 9090)
	if err != nil {
		return evidence, err
	}
	relayClient, err = probeclient.New(probeEndpoint, probeToken)
	if err != nil {
		return evidence, &infrastructureFailure{err: err}
	}
	capabilities, err := waitProbeCapabilities(ctx, relayClient, 5*time.Second)
	if err != nil {
		return evidence, &infrastructureFailure{err: err}
	}
	if err := requirePreciseCapabilities(capabilities, config.Plan.relay.Probe, clockStart); err != nil || capabilities.Service != "outbox-relay" {
		if err == nil {
			err = fmt.Errorf("live probe service is %q, want outbox-relay", capabilities.Service)
		}
		return evidence, &infrastructureFailure{err: &probeclient.CapabilityError{Err: err}}
	}
	evidence.ProbeCapabilities = append(evidence.ProbeCapabilities, capabilities)
	if err := waitOutboxAssignments(ctx, config, topology, attemptID, relayService); err != nil {
		return evidence, err
	}
	for _, order := range config.Scenario.Spec.Seed.Orders {
		if err := createOrderThroughAPI(ctx, apiEndpoint, order); err != nil {
			return evidence, err
		}
	}
	evidence.SchemaAfterHealth, err = config.Database.FingerprintAttempt(ctx, attempt)
	if err != nil || evidence.SchemaAfterHealth != config.Database.TemplateFingerprint() {
		if err == nil {
			err = errors.New("outbox attempt schema differs after service health")
		}
		return evidence, err
	}
	if err := config.Journal.State(executionState(config.Role, "EXECUTING"), ""); err != nil {
		return evidence, err
	}

	var armed probe.CheckpointState
	if config.Plan.mode == "outbox-crash" {
		checkpoint := checkpointFromSelector(*config.Plan.checkpoint)
		armed, err = relayClient.Arm(ctx, capabilities.InstanceID, checkpoint)
		if err != nil {
			return evidence, &infrastructureFailure{err: err}
		}
	}
	document, err := observe.Canonical(config.Plan.trigger.Event)
	if err != nil {
		return evidence, err
	}
	digest := sha256.Sum256(document)
	evidence.Published, err = config.Broker.Publish(ctx, topology.topics["outbox-relay-trigger"], 0, []byte(config.Plan.trigger.Action.Key), document, hex.EncodeToString(digest[:]))
	if err != nil {
		return evidence, err
	}
	evidence.Publications = append(evidence.Publications, evidence.Published)

	if config.Plan.mode == "outbox-crash" {
		if _, err := waitBlocked(ctx, relayClient, armed, 30*time.Second); err != nil {
			return evidence, err
		}
		if err := waitOutboxPreCrash(ctx, config, topology, attempt, effectClient); err != nil {
			return evidence, err
		}
		receipt, err := waitProbeDelivery(ctx, relayClient, 10*time.Second)
		if err != nil {
			return evidence, err
		}
		if err := requireProbeRecord(evidence.Published, receipt); err != nil {
			return evidence, err
		}
		evidence.ProbeDeliveries = append(evidence.ProbeDeliveries, receipt)
		if err := relayService.Kill(ctx); err != nil {
			return evidence, err
		}
		emptyContext, cancel := context.WithTimeout(ctx, 20*time.Second)
		err = config.Broker.WaitGroupEmpty(emptyContext, topology.groups["outbox-relay-trigger"])
		cancel()
		if err != nil {
			return evidence, err
		}
		relayClient.Close()
		previousInstance := capabilities.InstanceID
		if err := relayService.Start(ctx); err != nil {
			return evidence, err
		}
		probeEndpoint, err = relayService.PortEndpoint(ctx, 9090)
		if err != nil {
			return evidence, err
		}
		relayClient, err = probeclient.New(probeEndpoint, probeToken)
		if err != nil {
			return evidence, err
		}
		capabilities, err = waitProbeCapabilities(ctx, relayClient, 5*time.Second)
		if err != nil {
			return evidence, err
		}
		if capabilities.InstanceID == previousInstance {
			return evidence, errors.New("restarted relay reused its probe instance identity")
		}
		if err := requirePreciseCapabilities(capabilities, config.Plan.relay.Probe, clockStart); err != nil {
			return evidence, &infrastructureFailure{err: &probeclient.CapabilityError{Err: err}}
		}
		evidence.ProbeCapabilities = append(evidence.ProbeCapabilities, capabilities)
		restartedReceipts, err := relayClient.Deliveries(ctx)
		if err != nil || len(restartedReceipts) != 0 {
			if err == nil {
				err = errors.New("restarted relay inherited stale delivery receipts")
			}
			return evidence, err
		}
	}

	quiescence, effectObservation, err := waitOutboxQuiescence(ctx, config, topology, attempt, relayClient, effectClient)
	if err != nil {
		return evidence, err
	}
	evidence.Quiescence = &quiescence
	evidence.Effects = &effectObservation
	evidence.EffectProjection = effects.Project(effectObservation)
	for logical, offset := range quiescence.CommittedOffsets {
		evidence.GroupOffsets[logical] = offset
	}
	for logical, topic := range topology.topics {
		bounds, boundsErr := config.Broker.Bounds(ctx, topic, 0)
		if boundsErr != nil {
			return evidence, fmt.Errorf("capture %s topic bounds: %w", logical, boundsErr)
		}
		evidence.TopicBounds[logical] = bounds
	}
	evidence.Outbox, err = database.OutboxStates(ctx, attempt.ObserverDSN)
	if err != nil {
		return evidence, err
	}
	evidence.OutboxPublishes, err = database.OutboxPublishes(ctx, attempt.ObserverDSN)
	if err != nil {
		return evidence, err
	}
	evidence.Deliveries, err = database.WaitDeliveries(ctx, attempt.ObserverDSN, 1)
	if err != nil {
		return evidence, err
	}
	if err := validateOutboxSchedule(config, topology, evidence); err != nil {
		return evidence, &unresolvedFailure{err: err}
	}
	if err := collectOutboxObservations(ctx, config, attemptID, attempt, projectorEndpoint, sinkEndpoint, &evidence); err != nil {
		return evidence, err
	}
	for _, invariant := range config.Plan.invariants {
		query, err := os.ReadFile(filepath.Join(config.ScenarioRoot, invariant.QueryFile))
		if err != nil {
			return evidence, err
		}
		rows, err := database.Query(ctx, attempt.ObserverDSN, string(query))
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
		return evidence, errors.New("outbox workload changed the declared database schema")
	}
	if config.Baseline != nil && evidence.Signature == nil {
		evidence.Signature, err = compareOutboxAttempts(*config.Baseline, evidence, config.Plan)
		if err != nil {
			return evidence, err
		}
	}
	evidence.Status = "COMPLETE"
	return evidence, nil
}

func waitOutboxAssignments(ctx context.Context, config outboxAttemptConfig, topology outboxTopology, attemptID string, relay *cruntime.ServiceRuntime) error {
	assignments := []struct {
		logical string
		client  string
	}{
		{logical: "outbox-relay-trigger", client: "chronicle-outbox-trigger-" + attemptID},
		{logical: "orders-created", client: "chronicle-lifecycle-created-" + attemptID},
		{logical: "payment-requests", client: "chronicle-lifecycle-payment-responder-" + attemptID},
		{logical: "inventory-requests", client: "chronicle-lifecycle-inventory-responder-" + attemptID},
		{logical: "payment-outcomes", client: "chronicle-lifecycle-payment-outcome-" + attemptID},
		{logical: "inventory-outcomes", client: "chronicle-lifecycle-inventory-outcome-" + attemptID},
		{logical: "fulfillment", client: "chronicle-" + attemptID},
	}
	for _, assignment := range assignments {
		waitContext, cancel := context.WithTimeout(ctx, 20*time.Second)
		err := config.Broker.WaitGroupAssignment(waitContext, topology.groups[assignment.logical], assignment.client, topology.topics[assignment.logical], 0)
		cancel()
		if err != nil {
			return fmt.Errorf("wait for %s assignment: %w; relay=%s", assignment.logical, err, relay.Diagnostics(context.Background()))
		}
	}
	return nil
}

func createOrderThroughAPI(ctx context.Context, endpoint string, order spec.OrderSeed) error {
	document, err := json.Marshal(order)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(endpoint, "/")+"/orders", bytes.NewReader(document))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 5 * time.Second, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("create order through public API: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	payload, err := io.ReadAll(io.LimitReader(response.Body, 32<<10))
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusCreated {
		return fmt.Errorf("order API returned %d: %s", response.StatusCode, strings.TrimSpace(string(payload)))
	}
	value, err := observe.DecodeStrictJSON(payload)
	if err != nil {
		return fmt.Errorf("decode order API response: %w", err)
	}
	object, ok := value.(map[string]any)
	if !ok || object["requestId"] != order.RequestID || object["orderId"] != order.OrderID || object["eventId"] != "order-created-"+order.RequestID || object["status"] != "pending" {
		return fmt.Errorf("order API response does not identify the created transaction: %#v", value)
	}
	return nil
}

func waitOutboxPreCrash(ctx context.Context, config outboxAttemptConfig, topology outboxTopology, attempt database.Attempt, effectClient *effects.Client) error {
	deadline, cancel := context.WithTimeout(ctx, config.Scenario.Spec.Quiescence.Timeout.Duration)
	defer cancel()
	stableSince := time.Time{}
	for {
		states, stateErr := database.OutboxStates(deadline, attempt.ObserverDSN)
		publishes, publishErr := database.OutboxPublishes(deadline, attempt.ObserverDSN)
		effectState, effectErr := effectClient.Observe(deadline)
		terminal, terminalErr := readOutboxTerminalCounts(deadline, attempt.ObserverDSN)
		complete := stateErr == nil && publishErr == nil && effectErr == nil && terminalErr == nil &&
			len(states) == 1 && states[0].PublishAttempts == 1 && !states[0].Published &&
			len(publishes) == 1 && publishes[0].Attempt == 1 && publishes[0].Offset == 0 && publishes[0].EmittedEventID == publishes[0].LogicalEventID &&
			len(effectState.Entries) == 1 && effectState.Pending == 0 && terminal.readyOrders == 1 && terminal.payments == 1 && terminal.reservations == 1 && terminal.projections == 1
		if complete {
			for _, logical := range []string{"orders-created", "payment-requests", "inventory-requests", "payment-outcomes", "inventory-outcomes", "fulfillment"} {
				bounds, boundsErr := config.Broker.Bounds(deadline, topology.topics[logical], 0)
				position, positionErr := config.Broker.CommittedOffset(deadline, topology.groups[logical], topology.topics[logical], 0)
				if boundsErr != nil || positionErr != nil || bounds.End != 1 || position != 1 {
					complete = false
					break
				}
			}
			resultBounds, boundsErr := config.Broker.Bounds(deadline, topology.topics["fulfillment-results"], 0)
			complete = complete && boundsErr == nil && resultBounds.End == 1
		}
		if complete {
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
		case <-deadline.Done():
			return &unresolvedFailure{err: fmt.Errorf("connected workflow did not complete before relay crash: %w", deadline.Err())}
		case <-time.After(100 * time.Millisecond):
		}
	}
}

type outboxTerminalCounts struct {
	readyOrders  int64
	payments     int64
	reservations int64
	projections  int64
}

func readOutboxTerminalCounts(ctx context.Context, dsn string) (outboxTerminalCounts, error) {
	rows, err := database.Query(ctx, dsn, `
SELECT (SELECT count(*) FROM orders WHERE status = 'ready') AS ready_orders,
       (SELECT count(*) FROM payments WHERE status = 'confirmed') AS payments,
       (SELECT count(*) FROM inventory_reservations) AS reservations,
       (SELECT count(*) FROM fulfillment_projection WHERE status = 'ready') AS projections`)
	if err != nil || len(rows) != 1 {
		return outboxTerminalCounts{}, fmt.Errorf("query connected terminal counts: %w", err)
	}
	result := outboxTerminalCounts{}
	values := []*int64{&result.readyOrders, &result.payments, &result.reservations, &result.projections}
	keys := []string{"ready_orders", "payments", "reservations", "projections"}
	for index, key := range keys {
		value, ok := rows[0][key].(int64)
		if !ok {
			return outboxTerminalCounts{}, fmt.Errorf("terminal count %q has type %T", key, rows[0][key])
		}
		*values[index] = value
	}
	return result, nil
}

func waitOutboxQuiescence(ctx context.Context, config outboxAttemptConfig, topology outboxTopology, attempt database.Attempt, relayClient *probeclient.Client, effectClient *effects.Client) (QuiescenceEvidence, effects.Observation, error) {
	deadline, cancel := context.WithTimeout(ctx, config.Scenario.Spec.Quiescence.Timeout.Duration)
	defer cancel()
	stableSince := time.Time{}
	samples := 0
	expectedPublishes := len(config.Scenario.Spec.Seed.Orders)
	if config.Plan.mode == "outbox-crash" {
		expectedPublishes++
	}
	for {
		samples++
		states, stateErr := database.OutboxStates(deadline, attempt.ObserverDSN)
		publishes, publishErr := database.OutboxPublishes(deadline, attempt.ObserverDSN)
		terminal, terminalErr := readOutboxTerminalCounts(deadline, attempt.ObserverDSN)
		probeState, probeErr := relayClient.Quiescence(deadline)
		effectState, effectErr := effectClient.Observe(deadline)
		unpublished, outboxErr := database.CountUnpublishedOutbox(deadline, attempt.ObserverDSN)
		conditions := map[string]bool{
			"outboxEmpty":      stateErr == nil && publishErr == nil && outboxErr == nil && unpublished == 0 && len(states) == len(config.Scenario.Spec.Seed.Orders) && len(publishes) == expectedPublishes,
			"probeIdle":        probeErr == nil && probeState.InFlight == 0,
			"checkpointsClear": probeErr == nil && probeState.Armed == 0 && probeState.Blocked == 0,
			"effectsIdle":      effectErr == nil && effectState.Pending == 0 && len(effectState.Entries) >= len(config.Scenario.Spec.Seed.Orders),
			"terminalState":    terminalErr == nil && terminal.readyOrders == int64(len(config.Scenario.Spec.Seed.Orders)) && terminal.payments >= int64(len(config.Scenario.Spec.Seed.Orders)) && terminal.reservations >= int64(len(config.Scenario.Spec.Seed.Orders)) && terminal.projections == int64(len(config.Scenario.Spec.Seed.Orders)),
			"groupLagZero":     true,
		}
		boundsEvidence := map[string]broker.OffsetBounds{}
		offsetsEvidence := map[string]int64{}
		for logical, group := range topology.groups {
			bounds, boundsErr := config.Broker.Bounds(deadline, topology.topics[logical], 0)
			position, positionErr := config.Broker.CommittedOffset(deadline, group, topology.topics[logical], 0)
			if boundsErr != nil || positionErr != nil || position != bounds.End {
				conditions["groupLagZero"] = false
			}
			boundsEvidence[logical] = bounds
			offsetsEvidence[logical] = position
		}
		resultBounds, resultErr := config.Broker.Bounds(deadline, topology.topics["fulfillment-results"], 0)
		fulfillmentBounds := boundsEvidence["fulfillment"]
		if resultErr != nil || resultBounds.End != fulfillmentBounds.End {
			conditions["groupLagZero"] = false
		}
		boundsEvidence["fulfillment-results"] = resultBounds
		triggerBounds := boundsEvidence["outbox-relay-trigger"]
		ordersBounds := boundsEvidence["orders-created"]
		if triggerBounds.End != 1 || offsetsEvidence["outbox-relay-trigger"] != 1 || ordersBounds.End != int64(expectedPublishes) {
			conditions["groupLagZero"] = false
		}
		all := true
		for _, condition := range config.Scenario.Spec.Quiescence.Conditions {
			if !conditions[condition.Type] {
				all = false
			}
		}
		if all {
			if stableSince.IsZero() {
				stableSince = time.Now()
			}
			if time.Since(stableSince) >= config.Scenario.Spec.Quiescence.StabilityWindow.Duration {
				return QuiescenceEvidence{
					StableForMilliseconds: time.Since(stableSince).Milliseconds(), Samples: samples, Conditions: conditions,
					Probe: probeState, EffectPending: effectState.Pending, ProcessedEvents: int64(len(states)),
					CommittedOffset: offsetsEvidence["orders-created"], CommittedOffsets: offsetsEvidence,
				}, effectState, nil
			}
		} else {
			stableSince = time.Time{}
		}
		select {
		case <-deadline.Done():
			return QuiescenceEvidence{}, effects.Observation{}, &unresolvedFailure{err: fmt.Errorf("connected outbox quiescence was not stable: %w", deadline.Err())}
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func validateOutboxSchedule(config outboxAttemptConfig, topology outboxTopology, evidence AttemptEvidence) error {
	expectedPublishes := len(config.Scenario.Spec.Seed.Orders)
	if config.Plan.mode == "outbox-crash" {
		expectedPublishes++
	}
	if len(evidence.OutboxPublishes) != expectedPublishes || len(evidence.Outbox) != len(config.Scenario.Spec.Seed.Orders) {
		return errors.New("outbox publication inventory is incomplete")
	}
	statesByID := make(map[int64]database.OutboxState, len(evidence.Outbox))
	for _, state := range evidence.Outbox {
		statesByID[state.ID] = state
	}
	publicationsByRow := map[int64][]database.OutboxPublish{}
	for index, publication := range evidence.OutboxPublishes {
		state, exists := statesByID[publication.OutboxID]
		if !exists {
			return fmt.Errorf("outbox publication %d references unknown row %d", index, publication.OutboxID)
		}
		if publication.Sequence != int64(index+1) || publication.Topic != topology.topics["orders-created"] || publication.Partition != 0 || publication.Offset != int64(index) || publication.Attempt < 1 || publication.LogicalEventID != state.EventID {
			return fmt.Errorf("outbox publication %d has an unexpected broker identity", index)
		}
		publicationsByRow[publication.OutboxID] = append(publicationsByRow[publication.OutboxID], publication)
	}
	for _, state := range evidence.Outbox {
		if !state.Published {
			return fmt.Errorf("outbox row %d was not marked published", state.ID)
		}
		if config.Plan.mode == "outbox-crash" && state.PublishAttempts != 2 {
			return fmt.Errorf("crashed outbox row has %d attempts, want 2", state.PublishAttempts)
		}
		if config.Plan.mode == "outbox-control" && state.PublishAttempts != 1 {
			return fmt.Errorf("control outbox row has %d attempts, want 1", state.PublishAttempts)
		}
		published := publicationsByRow[state.ID]
		if len(published) != state.PublishAttempts {
			return fmt.Errorf("outbox row %d has %d durable publications, want %d", state.ID, len(published), state.PublishAttempts)
		}
		for index, publication := range published {
			if publication.Attempt != index+1 {
				return fmt.Errorf("outbox row %d publication attempt sequence is not contiguous", state.ID)
			}
			if config.Role == "baseline" || config.Plan.mode == "outbox-control" || index == 0 {
				if publication.EmittedEventID != state.EventID {
					return fmt.Errorf("outbox row %d changed stable event identity on attempt %d", state.ID, publication.Attempt)
				}
			}
		}
	}
	if len(evidence.TopicBounds) != len(topology.topics) || len(evidence.GroupOffsets) != len(topology.groups) {
		return errors.New("outbox offset evidence inventory is incomplete")
	}
	for logical, group := range topology.groups {
		bounds, exists := evidence.TopicBounds[logical]
		position, offsetExists := evidence.GroupOffsets[logical]
		if !exists || !offsetExists || position != bounds.End {
			return fmt.Errorf("outbox group %s (%s) did not finish at the retained topic end", logical, group)
		}
	}
	return nil
}

func collectOutboxObservations(ctx context.Context, config outboxAttemptConfig, attemptID string, attempt database.Attempt, projectorEndpoint, sinkEndpoint string, evidence *AttemptEvidence) error {
	for _, planned := range config.Plan.observations {
		collectionContext, cancel := context.WithTimeout(ctx, planned.Timeout)
		var observation observe.Evidence
		var err error
		switch declaration := planned.Declaration; {
		case declaration.Effects != nil:
			values := make([]any, len(evidence.EffectProjection))
			for index, entry := range evidence.EffectProjection {
				values[index] = entry
			}
			normalized, applied, normalizeErr := observe.Normalize(values, planned.Rules)
			if normalizeErr != nil {
				err = normalizeErr
			} else {
				observation, err = observe.NewEvidence(planned.Identity, "effects", declaration.Effects.Mode, observe.Source{Effects: &observe.HTTPSource{
					Service: declaration.Effects.Service, Port: declaration.Effects.Port, Path: declaration.Effects.Path, Endpoint: sinkEndpoint,
				}}, normalized, applied, nil)
			}
		case declaration.HTTP != nil:
			if declaration.HTTP.Service != "fulfillment-projector" {
				err = fmt.Errorf("outbox HTTP observer service %q is not fulfillment-projector", declaration.HTTP.Service)
			} else {
				source := observe.HTTPSource{Service: declaration.HTTP.Service, Port: declaration.HTTP.Port, Path: declaration.HTTP.Path, Endpoint: projectorEndpoint}
				observation, err = observe.CollectHTTP(collectionContext, planned.Identity, "http", declaration.HTTP.Mode, source, planned.Rules, "")
			}
		case declaration.SQL != nil:
			observation, err = observe.CollectSQL(collectionContext, planned.Identity, config.ScenarioRoot, attempt.ObserverDSN, declaration, planned.Rules)
		default:
			err = fmt.Errorf("unsupported outbox observer %q", declaration.ID)
		}
		cancel()
		if err != nil {
			return err
		}
		evidence.Observations = append(evidence.Observations, observation)
		artifactName := fmt.Sprintf("%s--%s--%d.json", planned.Identity.StepID, planned.Identity.ObserverID, planned.Identity.Occurrence)
		if err := artifact.WritePublicJSON(filepath.Join(config.Output, "observations", attemptID, artifactName), observation, config.SecretValues); err != nil {
			return err
		}
	}
	return nil
}

func compareOutboxAttempts(baseline, candidate AttemptEvidence, plan outboxPlan) (*FailureSignature, error) {
	if !reflect.DeepEqual(baseline.EffectProjection, candidate.EffectProjection) {
		difference := observe.Difference{
			Pointer: "/entries", RowKey: "effect-ledger", Expected: baseline.EffectProjection, Actual: candidate.EffectProjection,
			Message: "candidate changed the workload-owned external effect projection",
		}
		if len(baseline.EffectProjection) != len(candidate.EffectProjection) {
			difference.Pointer = "/entries/count"
			difference.Expected = len(baseline.EffectProjection)
			difference.Actual = len(candidate.EffectProjection)
		}
		return newObservedSignature("EXTERNAL_EFFECT_REGRESSION", "capture-effects", difference)
	}
	if len(baseline.Observations) != len(plan.observations) || len(candidate.Observations) != len(plan.observations) {
		return nil, errors.New("outbox observation inventory is incomplete")
	}
	for index, planned := range plan.observations {
		left, right := baseline.Observations[index], candidate.Observations[index]
		if left.Identity != planned.Identity || right.Identity != planned.Identity {
			return nil, fmt.Errorf("outbox observation identity mismatch at %d", index)
		}
		keyPointer := ""
		switch declaration := planned.Declaration; {
		case declaration.Effects != nil:
			continue
		case declaration.HTTP != nil:
			keyPointer = declaration.HTTP.KeyPointer
		case declaration.SQL != nil:
			keyPointer = declaration.SQL.KeyPointer
		}
		differences, err := observe.Compare(left, right, planned.Rules, keyPointer)
		if err != nil {
			return nil, err
		}
		if len(differences) != 0 {
			return newObservedSignature("SEMANTIC_REGRESSION", planned.Identity.ObserverID, differences[0])
		}
	}
	return nil, nil
}
