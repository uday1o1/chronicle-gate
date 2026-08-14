package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"github.com/uday1o1/chronicle-gate/internal/artifact"
	"github.com/uday1o1/chronicle-gate/internal/broker"
	"github.com/uday1o1/chronicle-gate/internal/controlcontract"
	"github.com/uday1o1/chronicle-gate/internal/database"
	"github.com/uday1o1/chronicle-gate/internal/observe"
	"github.com/uday1o1/chronicle-gate/internal/probeclient"
	"github.com/uday1o1/chronicle-gate/internal/runlog"
	cruntime "github.com/uday1o1/chronicle-gate/internal/runtime"
	"github.com/uday1o1/chronicle-gate/internal/spec"
	"github.com/uday1o1/chronicle-gate/pkg/probe"
)

const maxControlledCheckpointDwell = 45 * time.Second

var controlledTopicPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)

type controlledPlan struct {
	service      spec.Service
	mode         string
	expectLate   bool
	ordered      []spec.Step
	publishes    map[string]plannedPublish
	publication  []plannedPublish
	observations []plannedObservation
	invariants   []spec.Invariant
	runtime      controlcontract.Config
	clockStart   time.Time
}

func isControlledScenario(scenario spec.Scenario) bool {
	if scenario.Spec.Control != nil {
		return true
	}
	for _, step := range scenario.Spec.Steps {
		if step.AdvanceClock != nil || step.ArmCheckpoint != nil && step.ArmCheckpoint.Name == "before_handler" || step.ReleaseCheckpoint != nil && step.ReleaseCheckpoint.Name == "before_handler" {
			return true
		}
	}
	return false
}

func buildControlledPlan(scenario spec.Scenario, target spec.Target) (controlledPlan, error) {
	if scenario.Spec.Control == nil {
		return controlledPlan{}, errors.New("controlled scenario requires spec.control")
	}
	if len(target.Spec.Services) != 1 || target.Spec.Services[0].Name != "order-workflow" {
		return controlledPlan{}, errors.New("controlled scenarios require exactly one order-workflow service")
	}
	service := target.Spec.Services[0]
	if !service.Probe.Enabled || service.Probe.ProtocolVersion != "chronicle-probe/v1alpha1" || service.Probe.CommitMode != "manual_sync" || service.Probe.MaxControlledInFlight != 2 || !service.Probe.LogicalClock {
		return controlledPlan{}, errors.New("controlled order-workflow requires the live v1 probe, manual_sync commits, capacity 2, and logical clock")
	}
	foundBeforeHandler := false
	for _, checkpoint := range service.Probe.Checkpoints {
		if checkpoint == "before_handler" {
			foundBeforeHandler = true
		}
	}
	if !foundBeforeHandler {
		return controlledPlan{}, errors.New("controlled order-workflow does not declare before_handler")
	}
	clockStart, err := time.Parse(time.RFC3339Nano, scenario.Spec.Clock.Start)
	if err != nil {
		return controlledPlan{}, errors.New("controlled scenario requires an RFC3339 logical clock start")
	}
	ordered, err := topologicalSteps(scenario.Spec.Steps)
	if err != nil {
		return controlledPlan{}, err
	}
	ancestors := engineAncestors(scenario.Spec.Steps)
	plan := controlledPlan{
		service: service, mode: scenario.Spec.Control.Mode, expectLate: scenario.Spec.Control.ExpectLate,
		ordered: ordered, publishes: map[string]plannedPublish{}, invariants: append([]spec.Invariant(nil), scenario.Spec.Invariants...),
		clockStart: clockStart.UTC(),
	}
	declarations := map[string]spec.Observation{}
	for _, declaration := range scenario.Spec.Observations {
		declarations[declaration.ID] = declaration
	}
	occurrences := map[string]int{}
	arms := map[spec.CheckpointSelector]string{}
	awaits := map[spec.CheckpointSelector]string{}
	releases := map[spec.CheckpointSelector]string{}
	advanceSteps := []spec.Step{}
	for _, step := range ordered {
		switch {
		case step.Publish != nil:
			event, exists := scenario.Spec.Events[step.Publish.Event]
			if !exists {
				return controlledPlan{}, fmt.Errorf("published event %q is undeclared", step.Publish.Event)
			}
			if step.Publish.Key == "" || step.Publish.Key != event.AggregateID || event.AggregateVersion <= 0 {
				return controlledPlan{}, fmt.Errorf("controlled publication %q requires key=aggregateid and a positive aggregateversion", step.ID)
			}
			if step.Publish.Partition != 0 || !controlledTopicPattern.MatchString(step.Publish.Topic) {
				return controlledPlan{}, fmt.Errorf("controlled publication %q requires partition 0 and a simple logical topic", step.ID)
			}
			publication := plannedPublish{StepID: step.ID, Action: *step.Publish, Event: event}
			plan.publishes[step.ID] = publication
			plan.publication = append(plan.publication, publication)
		case step.ArmCheckpoint != nil:
			selector := step.ArmCheckpoint.CheckpointSelector
			if selector.Name != "before_handler" || selector.Service != service.Name || selector.Occurrence != 1 {
				return controlledPlan{}, fmt.Errorf("controlled arm %q must identify order-workflow before_handler occurrence 1", step.ID)
			}
			arms[selector] = step.ID
		case step.Await != nil:
			if step.Await.Checkpoint == nil || step.Await.Checkpoint.Name != "before_handler" {
				return controlledPlan{}, fmt.Errorf("controlled await %q must wait for an exact before_handler checkpoint", step.ID)
			}
			if step.Timeout.Duration <= 0 || step.Timeout.Duration > maxControlledCheckpointDwell {
				return controlledPlan{}, fmt.Errorf("controlled checkpoint await %q must be bounded within (0,%s]", step.ID, maxControlledCheckpointDwell)
			}
			awaits[*step.Await.Checkpoint] = step.ID
		case step.ReleaseCheckpoint != nil:
			selector := step.ReleaseCheckpoint.CheckpointSelector
			if selector.Name != "before_handler" {
				return controlledPlan{}, fmt.Errorf("controlled release %q must release before_handler", step.ID)
			}
			releases[selector] = step.ID
		case step.AdvanceClock != nil:
			if step.AdvanceClock.By.Duration <= 0 {
				return controlledPlan{}, fmt.Errorf("controlled clock step %q must advance by a positive duration", step.ID)
			}
			advanceSteps = append(advanceSteps, step)
		case step.Observe != nil:
			declaration, exists := declarations[step.Observe.Observation]
			if !exists {
				return controlledPlan{}, fmt.Errorf("controlled observe step %q references an undeclared observer", step.ID)
			}
			occurrences[declaration.ID]++
			plan.observations = append(plan.observations, plannedObservation{
				Identity:    observe.Identity{StepID: step.ID, ObserverID: declaration.ID, Occurrence: occurrences[declaration.ID]},
				Declaration: declaration, Timeout: step.Timeout.Duration, Rules: observationRules(scenario, declaration.ID),
			})
		default:
			return controlledPlan{}, fmt.Errorf("controlled scenario step %q uses an unsupported action", step.ID)
		}
	}
	if len(plan.publication) != 2 || len(arms) != 2 || len(awaits) != 2 || len(releases) != 2 || len(plan.observations) == 0 {
		return controlledPlan{}, errors.New("controlled scenarios require exactly two publications and two complete checkpoint lifecycles plus observations")
	}
	if len(plan.observations) != len(scenario.Spec.Observations) {
		return controlledPlan{}, errors.New("controlled scenario must execute every declared observation exactly once")
	}
	for selector, armID := range arms {
		publication, exists := plan.publishes[selector.StepID]
		if !exists || publication.Event.ID != selector.EventID {
			return controlledPlan{}, fmt.Errorf("controlled checkpoint armed by %q does not frame its exact publication", armID)
		}
		awaitID, awaited := awaits[selector]
		releaseID, released := releases[selector]
		if !awaited || !released || !isAncestor(ancestors, armID, selector.StepID) || !isAncestor(ancestors, selector.StepID, awaitID) || !isAncestor(ancestors, awaitID, releaseID) {
			return controlledPlan{}, fmt.Errorf("controlled checkpoint for publication %q lacks an exact arm-publish-await-release dependency chain", selector.StepID)
		}
	}
	if plan.publication[0].Event.AggregateID != plan.publication[1].Event.AggregateID {
		return controlledPlan{}, errors.New("controlled publications must address one aggregate")
	}
	if err := validateControlledShape(plan, advanceSteps, arms, awaits, releases, ancestors); err != nil {
		return controlledPlan{}, err
	}
	runtimeConfig, err := buildControlledRuntime(plan.publication, service.Probe.MaxControlledInFlight)
	if err != nil {
		return controlledPlan{}, err
	}
	plan.runtime = runtimeConfig
	return plan, nil
}

func validateControlledShape(plan controlledPlan, advances []spec.Step, arms, awaits, releases map[spec.CheckpointSelector]string, ancestors map[string]map[string]struct{}) error {
	first, second := plan.publication[0], plan.publication[1]
	firstSelector, secondSelector, err := publicationSelectors(first, second, arms)
	if err != nil {
		return err
	}
	switch plan.mode {
	case "aggregate-version":
		if len(advances) != 0 || first.Action.Topic != second.Action.Topic || first.Action.Partition != second.Action.Partition || first.Event.AggregateVersion == second.Event.AggregateVersion {
			return errors.New("aggregate-version control requires two unequal versions on one topic and partition without clock advancement")
		}
		if !isAncestor(ancestors, releases[firstSelector], arms[secondSelector]) {
			return errors.New("aggregate-version control must release and commit the first delivery before arming the second")
		}
	case "cross-stream":
		if len(advances) != 0 || first.Action.Topic == second.Action.Topic || first.Action.Partition != 0 || second.Action.Partition != 0 {
			return errors.New("cross-stream control requires two distinct logical topics on partition 0 and no clock advancement")
		}
		for _, releaseID := range releases {
			for _, awaitID := range awaits {
				if !isAncestor(ancestors, awaitID, releaseID) {
					return errors.New("cross-stream control must prove both handlers blocked before either release")
				}
			}
		}
	case "event-time":
		if len(advances) != 1 || first.Action.Topic != second.Action.Topic || first.Event.AggregateVersion >= second.Event.AggregateVersion {
			return errors.New("event-time control requires one topic, one clock advance, and a monotonically increasing aggregate version")
		}
		advance := advances[0]
		if !isAncestor(ancestors, releases[firstSelector], advance.ID) || !isAncestor(ancestors, advance.ID, arms[secondSelector]) {
			return errors.New("event-time control must commit the prior transition, advance the clock, then arm the later publication")
		}
		secondTime, err := time.Parse(time.RFC3339Nano, second.Event.Time)
		if err != nil {
			return fmt.Errorf("parse controlled event time: %w", err)
		}
		watermark := plan.clockStart.Add(advance.AdvanceClock.By.Duration)
		late := secondTime.Before(watermark)
		if late != plan.expectLate {
			return fmt.Errorf("event-time control expectLate=%t does not match event time %s and acknowledged watermark %s", plan.expectLate, secondTime, watermark)
		}
	default:
		return fmt.Errorf("unsupported controlled mode %q", plan.mode)
	}
	return nil
}

func publicationSelectors(first, second plannedPublish, arms map[spec.CheckpointSelector]string) (spec.CheckpointSelector, spec.CheckpointSelector, error) {
	var firstSelector, secondSelector spec.CheckpointSelector
	for selector := range arms {
		switch selector.StepID {
		case first.StepID:
			firstSelector = selector
		case second.StepID:
			secondSelector = selector
		}
	}
	if firstSelector.StepID == "" || secondSelector.StepID == "" {
		return spec.CheckpointSelector{}, spec.CheckpointSelector{}, errors.New("every controlled publication requires an exact checkpoint selector")
	}
	return firstSelector, secondSelector, nil
}

func buildControlledRuntime(publications []plannedPublish, capacity int) (controlcontract.Config, error) {
	byTopic := map[string][]controlcontract.Binding{}
	for _, publication := range publications {
		byTopic[publication.Action.Topic] = append(byTopic[publication.Action.Topic], controlcontract.Binding{EventID: publication.Event.ID, StepID: publication.StepID})
	}
	config := controlcontract.Config{APIVersion: controlcontract.APIVersion, ProbeCapacity: capacity, ConsumerCapacity: controlcontract.PerConsumerCapacity, Streams: []controlcontract.Stream{}}
	for topic, bindings := range byTopic {
		config.Streams = append(config.Streams, controlcontract.Stream{
			LogicalTopic: topic, Partition: 0, GroupSuffix: "order-workflow." + topic, ClientSuffix: topic, Bindings: bindings,
		})
	}
	return controlcontract.Normalize(config)
}

func engineAncestors(steps []spec.Step) map[string]map[string]struct{} {
	byID := map[string]spec.Step{}
	for _, step := range steps {
		byID[step.ID] = step
	}
	result := map[string]map[string]struct{}{}
	var collect func(string) map[string]struct{}
	collect = func(id string) map[string]struct{} {
		if cached, exists := result[id]; exists {
			return cached
		}
		set := map[string]struct{}{}
		for _, dependency := range byID[id].DependsOn {
			set[dependency] = struct{}{}
			for ancestor := range collect(dependency) {
				set[ancestor] = struct{}{}
			}
		}
		result[id] = set
		return set
	}
	for id := range byID {
		collect(id)
	}
	return result
}

func isAncestor(ancestors map[string]map[string]struct{}, ancestor, descendant string) bool {
	_, exists := ancestors[descendant][ancestor]
	return exists
}

type controlledAttemptConfig struct {
	RunID        string
	Index        int
	Role         string
	Scenario     spec.Scenario
	ScenarioRoot string
	Output       string
	Plan         controlledPlan
	Target       spec.Target
	Environment  *cruntime.Environment
	Database     *database.Manager
	Broker       *broker.Admin
	Journal      *runlog.Journal
	SecretValues []string
	Baseline     *AttemptEvidence
}

type controlledStreamRuntime struct {
	contract controlcontract.Stream
	physical string
	group    string
	clientID string
}

func executeControlledAttempt(ctx context.Context, config controlledAttemptConfig) (evidence AttemptEvidence, resultErr error) {
	attemptID := fmt.Sprintf("%s-%d", config.Role, config.Index)
	databaseName := fmt.Sprintf("cg_%s_%s_%d", config.RunID, config.Role, config.Index)
	prefix := "cg." + config.RunID + "." + config.Role + "." + fmt.Sprint(config.Index)
	runtimeDocument, runtimeDigest, err := controlcontract.Encode(config.Plan.runtime)
	if err != nil {
		return evidence, err
	}
	streams := make([]controlledStreamRuntime, 0, len(config.Plan.runtime.Streams))
	for _, stream := range config.Plan.runtime.Streams {
		streams = append(streams, controlledStreamRuntime{
			contract: stream,
			physical: prefix + "." + stream.LogicalTopic,
			group:    prefix + "." + stream.GroupSuffix,
			clientID: "chronicle-" + attemptID + "-" + stream.ClientSuffix,
		})
	}
	evidence = AttemptEvidence{
		AttemptID: attemptID, Role: config.Role, Status: "INCOMPLETE", Database: databaseName,
		AuthoredImage: config.Plan.service.Image, Publications: []broker.RecordIdentity{}, Deliveries: []database.Delivery{},
		ProbeCapabilities: []probe.Capabilities{}, ProbeDeliveries: []probe.DeliveryReceipt{},
		ControlMode: config.Plan.mode, CheckpointMode: "controlled-" + config.Plan.mode,
		ControlledConfigSHA256: runtimeDigest, ControlledTopology: []ControlledStreamEvidence{},
		CheckpointReleases: []CheckpointReleaseEvidence{}, LogicalClockTransitions: []LogicalClockTransition{},
		AggregateTransitions: []database.AggregateTransition{}, GroupInitializations: map[string]broker.InitializationEvidence{},
		Observations: []observe.Evidence{}, ObservationRows: []map[string]any{}, InvariantRows: []map[string]any{},
	}
	for _, stream := range streams {
		evidence.ControlledTopology = append(evidence.ControlledTopology, ControlledStreamEvidence{
			LogicalTopic: stream.contract.LogicalTopic, PhysicalTopic: stream.physical, Partition: stream.contract.Partition,
			Group: stream.group, ClientID: stream.clientID, ProbeCapacity: config.Plan.runtime.ProbeCapacity,
			ConsumerCapacity: config.Plan.runtime.ConsumerCapacity,
		})
	}
	if len(streams) == 1 {
		evidence.Topic, evidence.Group = streams[0].physical, streams[0].group
	}
	var attempt database.Attempt
	var service *cruntime.ServiceRuntime
	var client *probeclient.Client
	createdTopics := []string{}
	databaseCreated := false
	probeToken, err := randomRuntimeToken()
	if err != nil {
		return evidence, err
	}
	config.SecretValues = append(config.SecretValues, probeToken)
	config.Journal.SetSecretValues(config.SecretValues)
	defer func() {
		if resultErr != nil {
			evidence.Error = resultErr.Error()
		}
		if writeErr := journaled(config.Journal, executionState(config.Role, "OBSERVING"), "persist_controlled_attempt_evidence", map[string]any{"attemptId": attemptID}, func() error {
			return artifact.WritePublicJSON(filepath.Join(config.Output, "attempts", attemptID+".json"), evidence, config.SecretValues)
		}); writeErr != nil {
			resultErr = joinInfrastructure(resultErr, writeErr)
		}
		cleanupContext, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if client != nil {
			client.Close()
		}
		if service != nil {
			resultErr = joinInfrastructure(resultErr, journaled(config.Journal, executionState(config.Role, "STOPPING"), "terminate_controlled_service", map[string]any{"attemptId": attemptID}, func() error { return service.Terminate(cleanupContext) }))
		}
		for index := len(createdTopics) - 1; index >= 0; index-- {
			topic := createdTopics[index]
			resultErr = joinInfrastructure(resultErr, journaled(config.Journal, executionState(config.Role, "STOPPING"), "delete_controlled_topic", map[string]any{"attemptId": attemptID, "topic": topic}, func() error { return config.Broker.DeleteTopic(cleanupContext, topic) }))
		}
		if databaseCreated {
			resultErr = joinInfrastructure(resultErr, journaled(config.Journal, executionState(config.Role, "STOPPING"), "drop_controlled_database", map[string]any{"attemptId": attemptID, "database": databaseName}, func() error { return config.Database.DropAttempt(cleanupContext, databaseName) }))
		}
	}()

	if err := journaled(config.Journal, executionState(config.Role, "STARTING"), "clone_controlled_database", map[string]any{"attemptId": attemptID}, func() error {
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
	for _, stream := range streams {
		if err := journaled(config.Journal, executionState(config.Role, "STARTING"), "create_controlled_topic", map[string]any{"attemptId": attemptID, "topic": stream.physical}, func() error { return config.Broker.CreateTopic(ctx, stream.physical) }); err != nil {
			return evidence, err
		}
		createdTopics = append(createdTopics, stream.physical)
		var initialized broker.InitializationEvidence
		if err := journaled(config.Journal, executionState(config.Role, "STARTING"), "initialize_controlled_group", map[string]any{"attemptId": attemptID, "group": stream.group}, func() error {
			var initializeErr error
			initialized, initializeErr = config.Broker.InitializeEmptyGroupAtZero(ctx, stream.group, stream.physical, stream.contract.Partition)
			return initializeErr
		}); err != nil {
			return evidence, err
		}
		evidence.GroupInitializations[stream.group] = initialized
	}
	err = journaled(config.Journal, executionState(config.Role, "STARTING"), "start_controlled_service", map[string]any{"attemptId": attemptID, "image": config.Plan.service.Image, "configSha256": runtimeDigest}, func() error {
		service, err = cruntime.StartService(ctx, cruntime.ServiceConfig{
			RunID: config.RunID, AttemptID: attemptID, Service: config.Plan.service, Network: config.Environment.Network,
			DatabaseDSN: attempt.ServiceDSN, InternalBroker: config.Environment.InternalBroker,
			TopicPrefix: prefix, GroupPrefix: prefix, SecretDirectory: filepath.Join(config.Output, ".secrets"),
			Environment: map[string]string{"CHRONICLE_LOGICAL_CLOCK_CURRENT": config.Plan.clockStart.Format(time.RFC3339Nano)},
			Secrets: []cruntime.SecretMount{
				{Environment: "CHRONICLE_PROBE_TOKEN_FILE", Filename: "probe-token", Value: probeToken},
				{Environment: "CHRONICLE_CONTROLLED_CONFIG_FILE", Filename: "controlled-runtime.json", Value: string(runtimeDocument)},
			},
			ExposedPorts: []int{9090},
		})
		return err
	})
	if err != nil {
		return evidence, err
	}
	evidence.ExecutedImageID = service.ImageID
	if err := service.AssertHardened(ctx, 8080, 9090); err != nil {
		return evidence, err
	}
	probeEndpoint, err := service.PortEndpoint(ctx, 9090)
	if err != nil {
		return evidence, err
	}
	client, err = probeclient.New(probeEndpoint, probeToken)
	if err != nil {
		return evidence, err
	}
	capabilities, err := client.Capabilities(ctx)
	if err != nil {
		return evidence, err
	}
	if err := requireControlledCapabilities(capabilities, config.Plan); err != nil {
		return evidence, err
	}
	evidence.ProbeCapabilities = append(evidence.ProbeCapabilities, capabilities)
	for _, stream := range streams {
		assignmentContext, cancel := context.WithTimeout(ctx, 20*time.Second)
		err := config.Broker.WaitGroupAssignment(assignmentContext, stream.group, stream.clientID, stream.physical, stream.contract.Partition)
		cancel()
		if err != nil {
			return evidence, err
		}
	}
	if err := config.Journal.State(executionState(config.Role, "EXECUTING"), ""); err != nil {
		return evidence, err
	}
	evidence.SchemaAfterHealth, err = config.Database.FingerprintAttempt(ctx, attempt)
	if err != nil {
		return evidence, err
	}
	if evidence.SchemaAfterHealth != config.Database.TemplateFingerprint() {
		return evidence, errors.New("controlled attempt schema differs after health")
	}

	armed := map[spec.CheckpointSelector]probe.CheckpointState{}
	published := map[string]broker.RecordIdentity{}
	released := 0
	currentClock := config.Plan.clockStart
	for _, step := range config.Plan.ordered {
		switch {
		case step.ArmCheckpoint != nil:
			checkpoint := controlledCheckpoint(step.ArmCheckpoint.CheckpointSelector)
			state, armErr := client.Arm(ctx, capabilities.InstanceID, checkpoint)
			if armErr != nil {
				return evidence, armErr
			}
			armed[step.ArmCheckpoint.CheckpointSelector] = state
		case step.Publish != nil:
			publication := config.Plan.publishes[step.ID]
			document, marshalErr := json.Marshal(publication.Event)
			if marshalErr != nil {
				return evidence, marshalErr
			}
			digest := sha256.Sum256(document)
			stream, exists := controlledStreamRuntimeFor(streams, publication.Action.Topic)
			if !exists {
				return evidence, fmt.Errorf("controlled stream %q is unavailable", publication.Action.Topic)
			}
			var record broker.RecordIdentity
			err = journaled(config.Journal, executionState(config.Role, "EXECUTING"), "publish_controlled_record", map[string]any{"attemptId": attemptID, "stepId": step.ID, "topic": stream.physical}, func() error {
				var publishErr error
				record, publishErr = config.Broker.Publish(ctx, stream.physical, stream.contract.Partition, []byte(publication.Action.Key), document, hex.EncodeToString(digest[:]))
				return publishErr
			})
			if err != nil {
				return evidence, err
			}
			published[step.ID] = record
			evidence.Publications = append(evidence.Publications, record)
			if len(evidence.Publications) == 1 {
				evidence.Published = record
			}
		case step.Await != nil:
			state, exists := armed[*step.Await.Checkpoint]
			if !exists {
				return evidence, errors.New("controlled await has no armed handle")
			}
			waitContext, cancel := context.WithTimeout(ctx, step.Timeout.Duration)
			blocked, waitErr := client.WaitBlocked(waitContext, state)
			cancel()
			if waitErr != nil {
				return evidence, waitErr
			}
			armed[*step.Await.Checkpoint] = blocked
		case step.ReleaseCheckpoint != nil:
			selector := step.ReleaseCheckpoint.CheckpointSelector
			state, exists := armed[selector]
			if !exists {
				return evidence, errors.New("controlled release has no armed handle")
			}
			if _, releaseErr := client.Release(ctx, state); releaseErr != nil {
				return evidence, releaseErr
			}
			record, exists := published[selector.StepID]
			if !exists {
				return evidence, errors.New("controlled release has no published record")
			}
			publication := config.Plan.publishes[selector.StepID]
			stream, _ := controlledStreamRuntimeFor(streams, publication.Action.Topic)
			commitContext, cancel := context.WithTimeout(ctx, 20*time.Second)
			commitErr := config.Broker.WaitCommitted(commitContext, stream.group, record.Topic, record.Partition, record.Offset+1)
			cancel()
			if commitErr != nil {
				return evidence, commitErr
			}
			position, positionErr := config.Broker.CommittedOffset(ctx, stream.group, record.Topic, record.Partition)
			if positionErr != nil {
				return evidence, positionErr
			}
			released++
			evidence.CheckpointReleases = append(evidence.CheckpointReleases, CheckpointReleaseEvidence{
				Order: released, Checkpoint: controlledCheckpoint(selector), Topic: record.Topic, Partition: record.Partition,
				Offset: record.Offset, Group: stream.group, CommittedOffset: position,
			})
			if err := waitControlledTransitionCount(ctx, attempt.ObserverDSN, released); err != nil {
				return evidence, err
			}
		case step.AdvanceClock != nil:
			if err := waitControlledPrefixStable(ctx, config, client, attempt.ObserverDSN, streams, published, released); err != nil {
				return evidence, err
			}
			intended := currentClock.Add(step.AdvanceClock.By.Duration).UTC()
			var acknowledged time.Time
			err = journaled(config.Journal, executionState(config.Role, "EXECUTING"), "advance_logical_clock", map[string]any{
				"attemptId": attemptID, "stepId": step.ID, "from": currentClock.Format(time.RFC3339Nano),
				"by": step.AdvanceClock.By.String(), "intended": intended.Format(time.RFC3339Nano),
			}, func() error {
				advanceContext, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()
				var advanceErr error
				acknowledged, advanceErr = client.AdvanceClock(advanceContext, step.AdvanceClock.By.Duration)
				return advanceErr
			})
			if err != nil {
				return evidence, err
			}
			if !acknowledged.Equal(intended) {
				return evidence, fmt.Errorf("logical clock acknowledged %s, want %s", acknowledged, intended)
			}
			evidence.LogicalClockTransitions = append(evidence.LogicalClockTransitions, LogicalClockTransition{
				StepID: step.ID, From: currentClock.Format(time.RFC3339Nano), By: step.AdvanceClock.By.String(),
				Intended: intended.Format(time.RFC3339Nano), Acknowledged: acknowledged.UTC().Format(time.RFC3339Nano),
			})
			currentClock = acknowledged.UTC()
		case step.Observe != nil:
		}
	}

	quiescence, err := waitControlledQuiescence(ctx, config, client, attempt.ObserverDSN, streams, published)
	if err != nil {
		return evidence, fmt.Errorf("%w; service=%s; logs=%q", err, service.Diagnostics(context.Background()), service.RecentLogs(context.Background()))
	}
	evidence.Quiescence = &quiescence
	evidence.ProbeDeliveries, err = client.Deliveries(ctx)
	if err != nil {
		return evidence, err
	}
	evidence.Deliveries, err = database.WaitDeliveries(ctx, attempt.ObserverDSN, len(config.Plan.publication))
	if err != nil {
		return evidence, err
	}
	evidence.AggregateTransitions, err = database.AggregateTransitions(ctx, attempt.ObserverDSN)
	if err != nil {
		return evidence, err
	}
	if err := validateControlledAttempt(config.Plan, evidence); err != nil {
		return evidence, &unresolvedFailure{err: err}
	}
	if err := config.Journal.State(executionState(config.Role, "OBSERVING"), ""); err != nil {
		return evidence, err
	}
	for _, planned := range config.Plan.observations {
		observation, collectErr := collectControlledObservation(ctx, config, planned, attempt, service, streams)
		if collectErr != nil {
			return evidence, collectErr
		}
		evidence.Observations = append(evidence.Observations, observation)
		artifactName := fmt.Sprintf("%s--%s--%d.json", planned.Identity.StepID, planned.Identity.ObserverID, planned.Identity.Occurrence)
		if err := artifact.WritePublicJSON(filepath.Join(config.Output, "observations", attemptID, artifactName), observation, config.SecretValues); err != nil {
			return evidence, err
		}
		if planned.Declaration.SQL != nil {
			if value, ok := observation.Value.(map[string]any); ok {
				if rows, ok := value["rows"].([]any); ok {
					for _, item := range rows {
						if row, ok := item.(map[string]any); ok {
							evidence.ObservationRows = append(evidence.ObservationRows, row)
						}
					}
				}
			}
		}
	}
	for _, invariant := range config.Plan.invariants {
		document, readErr := os.ReadFile(filepath.Join(config.ScenarioRoot, invariant.QueryFile))
		if readErr != nil {
			return evidence, readErr
		}
		rows, queryErr := database.Query(ctx, attempt.ObserverDSN, string(document))
		if queryErr != nil {
			return evidence, queryErr
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
		return evidence, errors.New("controlled attempt schema changed during execution or observation")
	}
	if config.Baseline != nil && evidence.Signature == nil {
		evidence.Signature, err = compareGeneralAttempts(*config.Baseline, evidence, generalPlan{service: config.Plan.service, observations: config.Plan.observations, invariants: config.Plan.invariants})
		if err != nil {
			return evidence, err
		}
	}
	evidence.Status = "COMPLETE"
	return evidence, nil
}

func requireControlledCapabilities(capabilities probe.Capabilities, plan controlledPlan) error {
	if capabilities.Service != plan.service.Name || capabilities.CommitMode != "manual_sync" || capabilities.MaxControlledInFlight != 2 || !capabilities.LogicalClock {
		return &probeclient.CapabilityError{Err: errors.New("live controlled probe capabilities differ from the authored contract")}
	}
	found := false
	for _, name := range capabilities.Checkpoints {
		if name == "before_handler" {
			found = true
		}
	}
	if !found {
		return &probeclient.CapabilityError{Err: errors.New("live controlled probe omits before_handler")}
	}
	current, err := time.Parse(time.RFC3339Nano, capabilities.CurrentTime)
	if err != nil || !current.Equal(plan.clockStart) {
		return &probeclient.CapabilityError{Err: errors.New("live controlled logical clock does not match the authored seed")}
	}
	return nil
}

func controlledCheckpoint(selector spec.CheckpointSelector) probe.Checkpoint {
	return probe.Checkpoint{Name: selector.Name, Service: selector.Service, EventID: selector.EventID, StepID: selector.StepID, Occurrence: selector.Occurrence}
}

func controlledStreamRuntimeFor(streams []controlledStreamRuntime, logicalTopic string) (controlledStreamRuntime, bool) {
	for _, stream := range streams {
		if stream.contract.LogicalTopic == logicalTopic {
			return stream, true
		}
	}
	return controlledStreamRuntime{}, false
}

func waitControlledTransitionCount(ctx context.Context, observerDSN string, expected int) error {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	waitContext, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for {
		transitions, err := database.AggregateTransitions(waitContext, observerDSN)
		if err != nil {
			return err
		}
		if len(transitions) == expected {
			return nil
		}
		if len(transitions) > expected {
			return fmt.Errorf("controlled transition count advanced to %d before expected %d", len(transitions), expected)
		}
		select {
		case <-waitContext.Done():
			return fmt.Errorf("wait for controlled transition count %d: %w", expected, waitContext.Err())
		case <-ticker.C:
		}
	}
}

func waitControlledPrefixStable(ctx context.Context, config controlledAttemptConfig, client *probeclient.Client, observerDSN string, streams []controlledStreamRuntime, published map[string]broker.RecordIdentity, released int) error {
	deadline := time.Now().Add(config.Scenario.Spec.Quiescence.Timeout.Duration)
	stableSince := time.Time{}
	for time.Now().Before(deadline) {
		transitions, err := database.AggregateTransitions(ctx, observerDSN)
		if err != nil {
			return err
		}
		state, err := client.Quiescence(ctx)
		if err != nil {
			return err
		}
		committed := true
		for stepID, record := range published {
			publication := config.Plan.publishes[stepID]
			stream, _ := controlledStreamRuntimeFor(streams, publication.Action.Topic)
			position, offsetErr := config.Broker.CommittedOffset(ctx, stream.group, record.Topic, record.Partition)
			if offsetErr != nil || position != record.Offset+1 {
				committed = false
				break
			}
		}
		ready := len(transitions) == released && state.InFlight == 0 && state.Armed == 0 && state.Blocked == 0 && committed
		if ready {
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
		case <-time.After(50 * time.Millisecond):
		}
	}
	return &unresolvedFailure{err: errors.New("prior controlled transition was not fully stable before logical clock advancement")}
}

func waitControlledQuiescence(ctx context.Context, config controlledAttemptConfig, client *probeclient.Client, observerDSN string, streams []controlledStreamRuntime, published map[string]broker.RecordIdentity) (QuiescenceEvidence, error) {
	deadline := time.Now().Add(config.Scenario.Spec.Quiescence.Timeout.Duration)
	stableSince := time.Time{}
	samples := 0
	for time.Now().Before(deadline) {
		probeState, err := client.Quiescence(ctx)
		if err != nil {
			return QuiescenceEvidence{}, err
		}
		transitions, err := database.AggregateTransitions(ctx, observerDSN)
		if err != nil {
			return QuiescenceEvidence{}, err
		}
		outbox, err := database.CountUnpublishedOutbox(ctx, observerDSN)
		if err != nil {
			return QuiescenceEvidence{}, err
		}
		positions := map[string]int64{}
		groupsReady := true
		type expectedPosition struct {
			stream controlledStreamRuntime
			record broker.RecordIdentity
		}
		expectedByGroup := map[string]expectedPosition{}
		for stepID, record := range published {
			publication := config.Plan.publishes[stepID]
			stream, _ := controlledStreamRuntimeFor(streams, publication.Action.Topic)
			key := stream.group + "|" + record.Topic + "|" + fmt.Sprint(record.Partition)
			current, exists := expectedByGroup[key]
			if !exists || record.Offset > current.record.Offset {
				expectedByGroup[key] = expectedPosition{stream: stream, record: record}
			}
		}
		for key, expected := range expectedByGroup {
			position, offsetErr := config.Broker.CommittedOffset(ctx, expected.stream.group, expected.record.Topic, expected.record.Partition)
			if offsetErr != nil || position != expected.record.Offset+1 {
				groupsReady = false
				continue
			}
			positions[key] = position
		}
		conditions := map[string]bool{
			"all-group-lag-zero":  groupsReady,
			"probe-idle":          probeState.InFlight == 0,
			"checkpoints-clear":   probeState.Armed == 0 && probeState.Blocked == 0,
			"outbox-empty":        outbox == 0,
			"all-events-recorded": len(transitions) == len(config.Plan.publication),
		}
		ready := true
		for _, passed := range conditions {
			ready = ready && passed
		}
		if ready {
			samples++
			if stableSince.IsZero() {
				stableSince = time.Now()
			}
			if time.Since(stableSince) >= config.Scenario.Spec.Quiescence.StabilityWindow.Duration {
				return QuiescenceEvidence{
					StableForMilliseconds: time.Since(stableSince).Milliseconds(), Samples: samples,
					Conditions: conditions, Probe: probeState, EffectPending: 0,
					ProcessedEvents: int64(len(transitions)), AggregateEvents: int64(len(transitions)), CommittedOffsets: positions,
				}, nil
			}
		} else {
			stableSince = time.Time{}
			samples = 0
		}
		select {
		case <-ctx.Done():
			return QuiescenceEvidence{}, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
	return QuiescenceEvidence{}, &unresolvedFailure{err: errors.New("declared controlled quiescence was not stable before its deadline")}
}

func validateControlledAttempt(plan controlledPlan, evidence AttemptEvidence) error {
	if len(evidence.Publications) != 2 || len(evidence.ProbeDeliveries) != 2 || len(evidence.Deliveries) != 2 || len(evidence.CheckpointReleases) != 2 || len(evidence.AggregateTransitions) != 2 {
		return errors.New("controlled attempt evidence inventory is incomplete")
	}
	publicationByEvent := map[string]broker.RecordIdentity{}
	for index, publication := range plan.publication {
		publicationByEvent[publication.Event.ID] = evidence.Publications[index]
	}
	for _, receipt := range evidence.ProbeDeliveries {
		record, exists := publicationByEvent[receipt.EventID]
		if !exists || receipt.Topic != record.Topic || receipt.Partition != record.Partition || receipt.Offset != record.Offset || receipt.Key != record.Key || receipt.EventSHA256 != record.EventHash {
			return fmt.Errorf("controlled receipt for event %q does not match its published record", receipt.EventID)
		}
	}
	for index, release := range evidence.CheckpointReleases {
		if release.Order != index+1 || release.CommittedOffset != release.Offset+1 {
			return errors.New("controlled checkpoint release order or commit proof is invalid")
		}
		transition := evidence.AggregateTransitions[index]
		if transition.Sequence != int64(index+1) || transition.EventID != release.Checkpoint.EventID || transition.SourceTopic != release.Topic || transition.SourcePartition != release.Partition || transition.SourceOffset != release.Offset {
			return errors.New("aggregate transition sequence does not match the exact authored release schedule")
		}
	}
	switch plan.mode {
	case "aggregate-version":
		first, second := evidence.AggregateTransitions[0], evidence.AggregateTransitions[1]
		if first.SourceTopic != second.SourceTopic || first.SourcePartition != second.SourcePartition || second.SourceOffset <= first.SourceOffset {
			return errors.New("aggregate-version evidence does not prove later processing in one physical partition")
		}
	case "cross-stream":
		if evidence.AggregateTransitions[0].SourceTopic == evidence.AggregateTransitions[1].SourceTopic {
			return errors.New("cross-stream evidence unexpectedly used one physical topic")
		}
	case "event-time":
		if len(evidence.LogicalClockTransitions) != 1 {
			return errors.New("event-time evidence requires one acknowledged logical clock transition")
		}
		acknowledged, err := time.Parse(time.RFC3339Nano, evidence.LogicalClockTransitions[0].Acknowledged)
		if err != nil {
			return err
		}
		late := evidence.AggregateTransitions[1]
		eventTime, eventErr := time.Parse(time.RFC3339Nano, late.EventTime)
		deliveryTime, deliveryErr := time.Parse(time.RFC3339Nano, late.DeliveryLogicalTime)
		if eventErr != nil || deliveryErr != nil || deliveryTime.Before(acknowledged) {
			return errors.New("event-time evidence does not prove acknowledged watermark at delivery")
		}
		if plan.expectLate && !eventTime.Before(acknowledged) || !plan.expectLate && eventTime.Before(acknowledged) {
			return errors.New("event-time evidence does not match the authored lateness classification")
		}
		first := evidence.AggregateTransitions[0]
		if late.AggregateVersion <= first.AggregateVersion || late.SourceTopic != first.SourceTopic || late.SourceOffset <= first.SourceOffset {
			return errors.New("event-time evidence conflates lateness with a stale version or reordered physical delivery")
		}
	}
	return nil
}

func collectControlledObservation(ctx context.Context, config controlledAttemptConfig, planned plannedObservation, attempt database.Attempt, service *cruntime.ServiceRuntime, streams []controlledStreamRuntime) (observe.Evidence, error) {
	collectionContext, cancel := context.WithTimeout(ctx, planned.Timeout)
	defer cancel()
	declaration := planned.Declaration
	switch {
	case declaration.SQL != nil:
		return observe.CollectSQL(collectionContext, planned.Identity, config.ScenarioRoot, attempt.ObserverDSN, declaration, planned.Rules)
	case declaration.Kafka != nil:
		stream, exists := controlledStreamRuntimeFor(streams, declaration.Kafka.Topic)
		if !exists {
			return observe.Evidence{}, fmt.Errorf("controlled Kafka observer topic %q is undeclared", declaration.Kafka.Topic)
		}
		bounds, err := config.Broker.Bounds(collectionContext, stream.physical, int32(declaration.Kafka.Partition))
		if err != nil {
			return observe.Evidence{}, err
		}
		return observe.CollectKafka(collectionContext, observe.KafkaConfig{
			Brokers: []string{config.Environment.HostBroker}, Identity: planned.Identity, Root: config.ScenarioRoot,
			PhysicalTopic: stream.physical, Declaration: *declaration.Kafka, Rules: planned.Rules, Bounds: bounds,
		})
	case declaration.HTTP != nil:
		if declaration.HTTP.Service != config.Plan.service.Name {
			return observe.Evidence{}, errors.New("controlled HTTP observer must address order-workflow")
		}
		endpoint, err := service.PortEndpoint(collectionContext, declaration.HTTP.Port)
		if err != nil {
			return observe.Evidence{}, err
		}
		return observe.CollectHTTP(collectionContext, planned.Identity, "http", declaration.HTTP.Mode, observe.HTTPSource{
			Service: declaration.HTTP.Service, Port: declaration.HTTP.Port, Path: declaration.HTTP.Path, Endpoint: endpoint,
		}, planned.Rules, "")
	default:
		return observe.Evidence{}, fmt.Errorf("controlled observer %q is unsupported", declaration.ID)
	}
}
