package engine

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
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

type precisePlan struct {
	workflow    spec.Service
	sink        spec.Service
	event       spec.CloudEvent
	publish     plannedPublish
	checkpoints []spec.CheckpointSelector
	mode        string
	observation spec.Observation
	identity    observe.Identity
	invariant   spec.Invariant
}

func isPreciseScenario(scenario spec.Scenario) bool {
	for _, step := range scenario.Spec.Steps {
		if step.ArmCheckpoint != nil {
			return true
		}
	}
	return false
}

func buildPrecisePlan(scenario spec.Scenario, target spec.Target) (precisePlan, error) {
	plan := precisePlan{}
	for _, service := range target.Spec.Services {
		switch service.Name {
		case "order-workflow":
			plan.workflow = service
		case "effect-sink":
			plan.sink = service
		}
	}
	if plan.workflow.Name == "" || plan.sink.Name == "" || len(target.Spec.Services) != 2 {
		return precisePlan{}, errors.New("precise R2 scenarios require exactly order-workflow and effect-sink services")
	}
	stops, restarts, releases := 0, 0, 0
	observationID := ""
	for _, step := range scenario.Spec.Steps {
		switch {
		case step.Publish != nil:
			if plan.publish.StepID != "" {
				return precisePlan{}, errors.New("precise R2 scenarios require exactly one publication")
			}
			event, exists := scenario.Spec.Events[step.Publish.Event]
			if !exists {
				return precisePlan{}, fmt.Errorf("published event %q is undeclared", step.Publish.Event)
			}
			plan.event = event
			plan.publish = plannedPublish{StepID: step.ID, Action: *step.Publish, Event: event}
		case step.ArmCheckpoint != nil:
			plan.checkpoints = append(plan.checkpoints, step.ArmCheckpoint.CheckpointSelector)
		case step.Stop != nil:
			stops++
		case step.Restart != nil:
			restarts++
		case step.ReleaseCheckpoint != nil:
			releases++
		case step.Observe != nil:
			observationID = step.Observe.Observation
			plan.identity = observe.Identity{StepID: step.ID, ObserverID: observationID, Occurrence: 1}
		}
	}
	if plan.publish.StepID == "" || len(plan.checkpoints) == 0 || observationID == "" {
		return precisePlan{}, errors.New("precise R2 scenario requires publish, armCheckpoint, and observe actions")
	}
	if plan.publish.Action.Key != plan.event.AggregateID {
		return precisePlan{}, errors.New("precise R2 Kafka key must equal the CloudEvent aggregateid")
	}
	for _, checkpoint := range plan.checkpoints {
		if checkpoint.Service != plan.workflow.Name || checkpoint.EventID != plan.event.ID || checkpoint.StepID != plan.publish.StepID || checkpoint.Occurrence != 1 {
			return precisePlan{}, errors.New("precise checkpoint must identify the first published workflow delivery exactly")
		}
	}
	if stops == 1 && restarts == 1 && releases == 0 && len(plan.checkpoints) == 1 && plan.checkpoints[0].Name == "after_external_effect" {
		plan.mode = "precise-crash"
	} else if stops == 0 && restarts == 0 && releases >= 2 && hasCheckpoint(plan.checkpoints, "before_offset_commit") && hasCheckpoint(plan.checkpoints, "after_offset_commit") {
		plan.mode = "manual-commit-control"
	} else {
		return precisePlan{}, errors.New("unsupported precise fault lifecycle")
	}
	for _, observation := range scenario.Spec.Observations {
		if observation.ID == observationID {
			plan.observation = observation
		}
	}
	if plan.observation.Effects == nil {
		return precisePlan{}, errors.New("precise R2 scenario requires an effect-ledger observation")
	}
	if len(scenario.Spec.Invariants) != 1 {
		return precisePlan{}, errors.New("precise R2 scenario requires one processed-event invariant")
	}
	plan.invariant = scenario.Spec.Invariants[0]
	return plan, nil
}

func hasCheckpoint(checkpoints []spec.CheckpointSelector, name string) bool {
	return slices.ContainsFunc(checkpoints, func(checkpoint spec.CheckpointSelector) bool { return checkpoint.Name == name })
}

type preciseAttemptConfig struct {
	RunID        string
	Index        int
	Role         string
	Scenario     spec.Scenario
	ScenarioRoot string
	Output       string
	Plan         precisePlan
	Target       spec.Target
	Environment  *cruntime.Environment
	Database     *database.Manager
	Broker       *broker.Admin
	Journal      *runlog.Journal
	SecretValues []string
}

func executePreciseAttempt(ctx context.Context, config preciseAttemptConfig) (evidence AttemptEvidence, resultErr error) {
	attemptID := fmt.Sprintf("%s-%d", config.Role, config.Index)
	databaseName := fmt.Sprintf("cg_%s_%s_%d", config.RunID, config.Role, config.Index)
	prefix := "cg." + config.RunID + "." + config.Role + "." + fmt.Sprint(config.Index)
	topic := prefix + "." + config.Plan.publish.Action.Topic
	group := prefix + ".order-workflow"
	evidence = AttemptEvidence{
		AttemptID: attemptID, Role: config.Role, Status: "INCOMPLETE", Database: databaseName, Topic: topic, Group: group,
		AuthoredImage: config.Plan.workflow.Image, Publications: []broker.RecordIdentity{}, Deliveries: []database.Delivery{},
		ProbeCapabilities: []probe.Capabilities{}, ProbeDeliveries: []probe.DeliveryReceipt{}, CheckpointMode: config.Plan.mode,
		ObservationRows: []map[string]any{}, InvariantRows: []map[string]any{},
	}
	var attempt database.Attempt
	var workflowService, sinkService *cruntime.ServiceRuntime
	var workflowClient *probeclient.Client
	createdTopic, databaseCreated := false, false
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
		cleanupContext, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if workflowClient != nil {
			workflowClient.Close()
		}
		if workflowService != nil {
			resultErr = joinInfrastructure(resultErr, journaled(config.Journal, executionState(config.Role, "STOPPING"), "terminate_workflow", map[string]any{"attemptId": attemptID}, func() error { return workflowService.Terminate(cleanupContext) }))
		}
		if sinkService != nil {
			resultErr = joinInfrastructure(resultErr, journaled(config.Journal, executionState(config.Role, "STOPPING"), "terminate_effect_sink", map[string]any{"attemptId": attemptID}, func() error { return sinkService.Terminate(cleanupContext) }))
		}
		if createdTopic {
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
	if err := journaled(config.Journal, executionState(config.Role, "STARTING"), "create_topic", map[string]any{"attemptId": attemptID, "topic": topic}, func() error { return config.Broker.CreateTopic(ctx, topic) }); err != nil {
		return evidence, err
	}
	createdTopic = true
	if err := journaled(config.Journal, executionState(config.Role, "STARTING"), "initialize_empty_group", map[string]any{"attemptId": attemptID, "group": group}, func() error {
		var initializeErr error
		evidence.GroupInitialization, initializeErr = config.Broker.InitializeEmptyGroupAtZero(ctx, group, topic, int32(config.Plan.publish.Action.Partition))
		return initializeErr
	}); err != nil {
		return evidence, err
	}

	sinkSecretDirectory := filepath.Join(config.Output, ".secrets", attemptID, "effect-sink")
	if err := journaled(config.Journal, executionState(config.Role, "STARTING"), "start_effect_sink", map[string]any{"attemptId": attemptID}, func() error {
		var startErr error
		sinkService, startErr = cruntime.StartService(ctx, cruntime.ServiceConfig{
			RunID: config.RunID, AttemptID: attemptID + "-sink", Service: config.Plan.sink, Network: config.Environment.Network,
			DatabaseDSN: attempt.ServiceDSN, InternalBroker: config.Environment.InternalBroker, TopicPrefix: prefix, GroupPrefix: prefix,
			SecretDirectory: sinkSecretDirectory,
			Secrets: []cruntime.SecretMount{
				{Environment: "CHRONICLE_SINK_WRITER_TOKEN_FILE", Filename: "sink-writer-token", Value: writerToken},
				{Environment: "CHRONICLE_SINK_OBSERVER_TOKEN_FILE", Filename: "sink-observer-token", Value: observerToken},
			},
		})
		return startErr
	}); err != nil {
		return evidence, err
	}
	if err := sinkService.AssertHardened(ctx, 8080); err != nil {
		return evidence, &infrastructureFailure{err: err}
	}
	sinkEndpoint, err := sinkService.PortEndpoint(ctx, 8080)
	if err != nil {
		return evidence, err
	}
	effectClient, err := effects.New(sinkEndpoint, observerToken)
	if err != nil {
		return evidence, err
	}
	clockStart, err := time.Parse(time.RFC3339Nano, config.Scenario.Spec.Clock.Start)
	if err != nil {
		return evidence, fmt.Errorf("parse scenario clock: %w", err)
	}
	workflowSecretDirectory := filepath.Join(config.Output, ".secrets", attemptID, "order-workflow")
	if err := journaled(config.Journal, executionState(config.Role, "STARTING"), "start_workflow", map[string]any{"attemptId": attemptID}, func() error {
		var startErr error
		workflowService, startErr = cruntime.StartService(ctx, cruntime.ServiceConfig{
			RunID: config.RunID, AttemptID: attemptID, Service: config.Plan.workflow, Network: config.Environment.Network,
			DatabaseDSN: attempt.ServiceDSN, InternalBroker: config.Environment.InternalBroker, TopicPrefix: prefix, GroupPrefix: prefix,
			SecretDirectory: workflowSecretDirectory, ExposedPorts: []int{9090},
			Environment: map[string]string{
				"CHRONICLE_EFFECT_SINK_URL": "http://effect-sink:8080", "CHRONICLE_CONTROLLED_STEP_ID": config.Plan.publish.StepID,
				"CHRONICLE_LOGICAL_CLOCK_CURRENT": clockStart.Format(time.RFC3339Nano),
			},
			Secrets: []cruntime.SecretMount{
				{Environment: "CHRONICLE_PROBE_TOKEN_FILE", Filename: "probe-token", Value: probeToken},
				{Environment: "CHRONICLE_SINK_WRITER_TOKEN_FILE", Filename: "sink-writer-token", Value: writerToken},
			},
		})
		return startErr
	}); err != nil {
		return evidence, err
	}
	if err := workflowService.AssertHardened(ctx, 8080, 9090); err != nil {
		return evidence, &infrastructureFailure{err: err}
	}
	evidence.ExecutedImageID = workflowService.ImageID
	probeEndpoint, err := workflowService.PortEndpoint(ctx, 9090)
	if err != nil {
		return evidence, err
	}
	workflowClient, err = probeclient.New(probeEndpoint, probeToken)
	if err != nil {
		return evidence, &infrastructureFailure{err: err}
	}
	capabilities, err := waitProbeCapabilities(ctx, workflowClient, 5*time.Second)
	if err != nil {
		return evidence, &infrastructureFailure{err: err}
	}
	if err := requirePreciseCapabilities(capabilities, config.Plan.workflow.Probe, clockStart); err != nil {
		return evidence, &infrastructureFailure{err: &probeclient.CapabilityError{Err: err}}
	}
	evidence.ProbeCapabilities = append(evidence.ProbeCapabilities, capabilities)
	armed := map[string]probe.CheckpointState{}
	for _, selector := range config.Plan.checkpoints {
		checkpoint := checkpointFromSelector(selector)
		state, armErr := workflowClient.Arm(ctx, capabilities.InstanceID, checkpoint)
		if armErr != nil {
			return evidence, &infrastructureFailure{err: armErr}
		}
		armed[checkpoint.Name] = state
	}

	eventDocument, err := json.Marshal(config.Plan.event)
	if err != nil {
		return evidence, fmt.Errorf("encode CloudEvent: %w", err)
	}
	eventDigest := sha256.Sum256(eventDocument)
	if err := journaled(config.Journal, executionState(config.Role, "EXECUTING"), "publish_record", map[string]any{"attemptId": attemptID, "topic": topic}, func() error {
		var publishErr error
		evidence.Published, publishErr = config.Broker.Publish(ctx, topic, int32(config.Plan.publish.Action.Partition), []byte(config.Plan.publish.Action.Key), eventDocument, hex.EncodeToString(eventDigest[:]))
		if publishErr == nil {
			evidence.Publications = append(evidence.Publications, evidence.Published)
		}
		return publishErr
	}); err != nil {
		return evidence, err
	}

	switch config.Plan.mode {
	case "precise-crash":
		if _, err := waitBlocked(ctx, workflowClient, armed["after_external_effect"], 20*time.Second); err != nil {
			return evidence, err
		}
		first, err := waitProbeDelivery(ctx, workflowClient, 20*time.Second)
		if err != nil {
			return evidence, err
		}
		if err := requireProbeRecord(evidence.Published, first); err != nil {
			return evidence, err
		}
		evidence.ProbeDeliveries = append(evidence.ProbeDeliveries, first)
		initialEffects, err := waitEffects(ctx, effectClient, 1, 20*time.Second)
		if err != nil {
			return evidence, err
		}
		if err := requireCanonicalEffect(initialEffects.Entries[0], evidence.Published, config.Plan.event); err != nil {
			return evidence, err
		}
		committed, err := config.Broker.CommittedOffset(ctx, group, topic, evidence.Published.Partition)
		if err != nil || committed != evidence.Published.Offset {
			return evidence, fmt.Errorf("committed offset moved while blocked: got %d: %w", committed, err)
		}
		evidence.CommittedWhileBlocked = &committed
		if err := config.Broker.RequireGroupMember(ctx, group, workflowService.ClientID); err != nil {
			return evidence, err
		}
		if err := journaled(config.Journal, executionState(config.Role, "EXECUTING"), "sigkill_workflow", map[string]any{"attemptId": attemptID}, func() error { return workflowService.Kill(ctx) }); err != nil {
			return evidence, err
		}
		emptyContext, cancelEmpty := context.WithTimeout(ctx, 20*time.Second)
		err = config.Broker.WaitGroupEmpty(emptyContext, group)
		cancelEmpty()
		if err != nil {
			return evidence, err
		}
		workflowClient.Close()
		if err := journaled(config.Journal, executionState(config.Role, "EXECUTING"), "restart_workflow", map[string]any{"attemptId": attemptID}, func() error { return workflowService.Start(ctx) }); err != nil {
			return evidence, err
		}
		probeEndpoint, err = workflowService.PortEndpoint(ctx, 9090)
		if err != nil {
			return evidence, err
		}
		workflowClient, err = probeclient.New(probeEndpoint, probeToken)
		if err != nil {
			return evidence, err
		}
		restartedCapabilities, err := waitProbeCapabilities(ctx, workflowClient, 5*time.Second)
		if err != nil {
			return evidence, &infrastructureFailure{err: fmt.Errorf("%w; workflow %s; logs: %s", err, workflowService.Diagnostics(context.Background()), workflowService.RecentLogs(context.Background()))}
		}
		if restartedCapabilities.InstanceID == capabilities.InstanceID {
			return evidence, errors.New("restarted probe reused its process instance ID")
		}
		if err := requirePreciseCapabilities(restartedCapabilities, config.Plan.workflow.Probe, clockStart); err != nil {
			return evidence, &infrastructureFailure{err: &probeclient.CapabilityError{Err: err}}
		}
		evidence.ProbeCapabilities = append(evidence.ProbeCapabilities, restartedCapabilities)
		replayed, err := waitProbeDelivery(ctx, workflowClient, 20*time.Second)
		if err != nil {
			return evidence, err
		}
		if err := requireProbeRecord(evidence.Published, replayed); err != nil {
			return evidence, err
		}
		evidence.ProbeDeliveries = append(evidence.ProbeDeliveries, replayed)
	case "manual-commit-control":
		before := armed["before_offset_commit"]
		if _, err := waitBlocked(ctx, workflowClient, before, 20*time.Second); err != nil {
			return evidence, err
		}
		receipt, err := waitProbeDelivery(ctx, workflowClient, 20*time.Second)
		if err != nil {
			return evidence, err
		}
		if err := requireProbeRecord(evidence.Published, receipt); err != nil {
			return evidence, err
		}
		evidence.ProbeDeliveries = append(evidence.ProbeDeliveries, receipt)
		committed, err := config.Broker.CommittedOffset(ctx, group, topic, evidence.Published.Partition)
		if err != nil || committed != evidence.Published.Offset {
			return evidence, fmt.Errorf("committed offset moved before explicit commit: got %d: %w", committed, err)
		}
		evidence.CommittedWhileBlocked = &committed
		if _, err := workflowClient.Release(ctx, before); err != nil {
			return evidence, err
		}
		after := armed["after_offset_commit"]
		if _, err := waitBlocked(ctx, workflowClient, after, 20*time.Second); err != nil {
			return evidence, err
		}
		position, err := config.Broker.CommittedOffset(ctx, group, topic, evidence.Published.Partition)
		if err != nil || position != evidence.Published.Offset+1 {
			return evidence, fmt.Errorf("after_offset_commit exposed before administrative verification: got %d: %w", position, err)
		}
		if _, err := workflowClient.Release(ctx, after); err != nil {
			return evidence, err
		}
	}

	quiescence, observation, err := waitPreciseQuiescence(ctx, config, attempt, workflowClient, effectClient, group, topic, evidence.Published)
	if err != nil {
		return evidence, err
	}
	evidence.Quiescence = &quiescence
	evidence.Effects = &observation
	finalCommitted := quiescence.CommittedOffset
	evidence.FinalCommitted = &finalCommitted
	evidence.Deliveries, err = database.WaitDeliveries(ctx, attempt.ObserverDSN, 1)
	if err != nil {
		return evidence, err
	}
	if err := requireCanonicalEffects(observation, evidence.Published, config.Plan.event); err != nil {
		return evidence, err
	}
	evidence.SchemaAfterHealth, err = config.Database.FingerprintAttempt(ctx, attempt)
	if err != nil {
		return evidence, err
	}
	invariantQuery, err := os.ReadFile(filepath.Join(config.ScenarioRoot, config.Plan.invariant.QueryFile))
	if err != nil {
		return evidence, fmt.Errorf("read precise invariant: %w", err)
	}
	evidence.InvariantRows, err = database.Query(ctx, attempt.ObserverDSN, string(invariantQuery))
	if err != nil {
		return evidence, err
	}
	if len(evidence.InvariantRows) != 0 {
		return evidence, errors.New("precise processed-event invariant failed")
	}
	evidence.ObservationRows = effectRows(observation)
	rawObservation := map[string]any{"entries": evidence.ObservationRows, "pending": observation.Pending}
	normalized, applied, err := observe.Normalize(rawObservation, observationRules(config.Scenario, config.Plan.observation.ID))
	if err != nil {
		return evidence, fmt.Errorf("normalize effect observation: %w", err)
	}
	observerEvidence, err := observe.NewEvidence(
		config.Plan.identity,
		"effects",
		config.Plan.observation.Effects.Mode,
		observe.Source{Effects: &observe.HTTPSource{
			Service:  config.Plan.observation.Effects.Service,
			Port:     config.Plan.observation.Effects.Port,
			Path:     config.Plan.observation.Effects.Path,
			Endpoint: sinkEndpoint,
		}},
		normalized,
		applied,
		nil,
	)
	if err != nil {
		return evidence, fmt.Errorf("build effect observation evidence: %w", err)
	}
	evidence.Observations = append(evidence.Observations, observerEvidence)
	evidence.SchemaAfterObservation, err = config.Database.FingerprintAttempt(ctx, attempt)
	if err != nil {
		return evidence, err
	}
	if evidence.SchemaAfterHealth != config.Database.TemplateFingerprint() || evidence.SchemaAfterObservation != evidence.SchemaAfterHealth {
		return evidence, errors.New("precise attempt changed the declared database schema")
	}
	if len(observation.Entries) > 1 {
		evidence.Signature, err = newExternalEffectSignature(config.Plan.invariant, observation, config.Plan.event.AggregateID)
		if err != nil {
			return evidence, err
		}
	}
	artifactName := fmt.Sprintf("%s--%s--%d.json", config.Plan.identity.StepID, config.Plan.identity.ObserverID, config.Plan.identity.Occurrence)
	if err := artifact.WritePublicJSON(filepath.Join(config.Output, "observations", attemptID, artifactName), observerEvidence, config.SecretValues); err != nil {
		return evidence, err
	}
	evidence.Status = "COMPLETE"
	return evidence, nil
}

func checkpointFromSelector(selector spec.CheckpointSelector) probe.Checkpoint {
	return probe.Checkpoint{Name: selector.Name, Service: selector.Service, EventID: selector.EventID, StepID: selector.StepID, Occurrence: selector.Occurrence}
}

func requirePreciseCapabilities(actual probe.Capabilities, declared spec.ProbeDeclaration, current time.Time) error {
	if !declared.Enabled || actual.ProtocolVersion != declared.ProtocolVersion || actual.CommitMode != "manual_sync" || actual.MaxControlledInFlight != 1 || !actual.LogicalClock {
		return errors.New("live probe does not prove manual synchronous single-record processing and logical time")
	}
	for _, checkpoint := range declared.Checkpoints {
		if !slices.Contains(actual.Checkpoints, checkpoint) {
			return fmt.Errorf("live probe omits declared checkpoint %q", checkpoint)
		}
	}
	actualTime, err := time.Parse(time.RFC3339Nano, actual.CurrentTime)
	if err != nil || !actualTime.Equal(current) {
		return fmt.Errorf("live logical time %q does not equal orchestrator time %s", actual.CurrentTime, current.Format(time.RFC3339Nano))
	}
	return nil
}

func waitBlocked(ctx context.Context, client *probeclient.Client, state probe.CheckpointState, timeout time.Duration) (probe.CheckpointState, error) {
	waitContext, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return client.WaitBlocked(waitContext, state)
}

func waitProbeCapabilities(ctx context.Context, client *probeclient.Client, timeout time.Duration) (probe.Capabilities, error) {
	waitContext, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for {
		capabilities, err := client.Capabilities(waitContext)
		if err == nil {
			return capabilities, nil
		}
		lastErr = err
		select {
		case <-waitContext.Done():
			return probe.Capabilities{}, &probeclient.CapabilityError{Err: fmt.Errorf("probe unavailable within %s: %w", timeout, lastErr)}
		case <-ticker.C:
		}
	}
}

func waitProbeDelivery(ctx context.Context, client *probeclient.Client, timeout time.Duration) (probe.DeliveryReceipt, error) {
	waitContext, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		receipts, err := client.Deliveries(waitContext)
		if err != nil {
			return probe.DeliveryReceipt{}, err
		}
		if len(receipts) != 0 {
			return receipts[0], nil
		}
		select {
		case <-waitContext.Done():
			return probe.DeliveryReceipt{}, fmt.Errorf("wait for broker delivery receipt: %w", waitContext.Err())
		case <-ticker.C:
		}
	}
}

func requireProbeRecord(published broker.RecordIdentity, receipt probe.DeliveryReceipt) error {
	if receipt.Topic != published.Topic || receipt.Partition != published.Partition || receipt.Offset != published.Offset || receipt.Key != published.Key || receipt.EventSHA256 != published.EventHash {
		return errors.New("probe delivery is not the published physical broker record")
	}
	return nil
}

func waitEffects(ctx context.Context, client *effects.Client, count int, timeout time.Duration) (effects.Observation, error) {
	waitContext, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		observation, err := client.Observe(waitContext)
		if err != nil {
			return effects.Observation{}, err
		}
		if len(observation.Entries) >= count && observation.Pending == 0 {
			return observation, nil
		}
		select {
		case <-waitContext.Done():
			return effects.Observation{}, fmt.Errorf("wait for effect ledger: %w", waitContext.Err())
		case <-ticker.C:
		}
	}
}

func waitPreciseQuiescence(ctx context.Context, config preciseAttemptConfig, attempt database.Attempt, probeClient *probeclient.Client, effectClient *effects.Client, group, topic string, published broker.RecordIdentity) (QuiescenceEvidence, effects.Observation, error) {
	deadlineContext, cancel := context.WithTimeout(ctx, config.Scenario.Spec.Quiescence.Timeout.Duration)
	defer cancel()
	window := config.Scenario.Spec.Quiescence.StabilityWindow.Duration
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var stableSince time.Time
	samples := 0
	for {
		samples++
		position, positionErr := config.Broker.CommittedOffset(deadlineContext, group, topic, published.Partition)
		probeState, probeErr := probeClient.Quiescence(deadlineContext)
		effectState, effectErr := effectClient.Observe(deadlineContext)
		processed, processedErr := database.CountProcessedEvent(deadlineContext, attempt.ObserverDSN, config.Plan.event.ID)
		unpublished, outboxErr := database.CountUnpublishedOutbox(deadlineContext, attempt.ObserverDSN)
		conditions := map[string]bool{
			"groupLagZero":     positionErr == nil && position == published.Offset+1,
			"probeIdle":        probeErr == nil && probeState.InFlight == 0,
			"checkpointsClear": probeErr == nil && probeState.Armed == 0 && probeState.Blocked == 0,
			"outboxEmpty":      outboxErr == nil && unpublished == 0,
			"effectsIdle":      effectErr == nil && effectState.Pending == 0 && len(effectState.Entries) >= 1,
			"terminalState":    processedErr == nil && processed == 1,
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
			if time.Since(stableSince) >= window {
				return QuiescenceEvidence{
					StableForMilliseconds: time.Since(stableSince).Milliseconds(), Samples: samples, Conditions: conditions,
					Probe: probeState, EffectPending: effectState.Pending, ProcessedEvents: processed, CommittedOffset: position,
				}, effectState, nil
			}
		} else {
			stableSince = time.Time{}
		}
		select {
		case <-deadlineContext.Done():
			return QuiescenceEvidence{}, effects.Observation{}, fmt.Errorf("declared quiescence did not remain stable: %w", deadlineContext.Err())
		case <-ticker.C:
		}
	}
}

func requireCanonicalEffect(entry effects.Entry, published broker.RecordIdentity, event spec.CloudEvent) error {
	amount, ok := exactInt64(event.Data["amount"])
	if !ok || entry.Kind != "payment_capture" || entry.EventID != event.ID || entry.BusinessKey != event.AggregateID || entry.Amount != amount || entry.SourceTopic != published.Topic || entry.SourcePartition != published.Partition || entry.SourceOffset != published.Offset {
		return errors.New("effect ledger entry does not retain the published business and broker identity")
	}
	return nil
}

func exactInt64(value any) (int64, bool) {
	switch number := value.(type) {
	case int:
		return int64(number), true
	case int64:
		return number, true
	case float64:
		return int64(number), number == float64(int64(number))
	default:
		return 0, false
	}
}

func requireCanonicalEffects(observation effects.Observation, published broker.RecordIdentity, event spec.CloudEvent) error {
	for _, entry := range observation.Entries {
		if err := requireCanonicalEffect(entry, published, event); err != nil {
			return err
		}
	}
	return nil
}

func effectRows(observation effects.Observation) []map[string]any {
	rows := make([]map[string]any, 0, len(observation.Entries))
	for _, entry := range observation.Entries {
		rows = append(rows, map[string]any{
			"kind": entry.Kind, "event_id": entry.EventID, "business_key": entry.BusinessKey, "amount": entry.Amount,
			"idempotency_key": entry.IdempotencyKey, "source_topic": entry.SourceTopic,
			"source_partition": entry.SourcePartition, "source_offset": entry.SourceOffset,
		})
	}
	return rows
}

func newExternalEffectSignature(invariant spec.Invariant, observation effects.Observation, businessKey string) (*FailureSignature, error) {
	signature := &FailureSignature{
		InvariantID: invariant.ID, Classification: "EXTERNAL_EFFECT_REGRESSION", ObservationID: "capture-effects",
		RowKey: "business_key=" + businessKey, Pointer: "/entries/count", Expected: 1, Actual: len(observation.Entries),
	}
	document, err := json.Marshal(signature)
	if err != nil {
		return nil, fmt.Errorf("encode external-effect signature: %w", err)
	}
	digest := sha256.Sum256(document)
	signature.Digest = hex.EncodeToString(digest[:])
	return signature, nil
}

func randomRuntimeToken() (string, error) {
	value := make([]byte, 32)
	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf("generate runtime token: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(value), nil
}
