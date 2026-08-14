package engine

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/uday1o1/chronicle-gate/internal/artifact"
	"github.com/uday1o1/chronicle-gate/internal/broker"
	"github.com/uday1o1/chronicle-gate/internal/bundle"
	"github.com/uday1o1/chronicle-gate/internal/database"
	"github.com/uday1o1/chronicle-gate/internal/effects"
	"github.com/uday1o1/chronicle-gate/internal/imagelock"
	"github.com/uday1o1/chronicle-gate/internal/minimize"
	"github.com/uday1o1/chronicle-gate/internal/observe"
	"github.com/uday1o1/chronicle-gate/internal/registry"
	creport "github.com/uday1o1/chronicle-gate/internal/report"
	"github.com/uday1o1/chronicle-gate/internal/runlog"
	cruntime "github.com/uday1o1/chronicle-gate/internal/runtime"
	"github.com/uday1o1/chronicle-gate/internal/spec"
	"github.com/uday1o1/chronicle-gate/pkg/probe"
)

var databaseURLPattern = regexp.MustCompile(`(?i)postgres(?:ql)?://[^\s]+`)

type Config struct {
	Scenario           spec.Scenario
	Baseline           spec.Target
	Candidate          spec.Target
	ScenarioRoot       string
	Output             string
	ImageLock          string
	ScenarioPath       string
	BaselinePath       string
	CandidatePath      string
	NoMinimize         bool
	SourceBundleSHA256 string
	ExpectedSignature  string
}

type Report struct {
	APIVersion        string              `json:"apiVersion"`
	Kind              string              `json:"kind"`
	RunID             string              `json:"runId"`
	State             string              `json:"state"`
	Classification    string              `json:"classification"`
	StartedAt         string              `json:"startedAt"`
	CompletedAt       string              `json:"completedAt"`
	NonportableImages bool                `json:"nonportableImages"`
	Environment       EnvironmentEvidence `json:"environment"`
	Baseline          *AttemptEvidence    `json:"baseline,omitempty"`
	Candidate         []AttemptEvidence   `json:"candidate"`
	FailureSignature  *FailureSignature   `json:"failureSignature,omitempty"`
	Violations        []Violation         `json:"violations"`
	Confirmations     int                 `json:"confirmations"`
	Error             string              `json:"error,omitempty"`
	Minimization      minimize.Summary    `json:"minimization"`
	Bundle            string              `json:"bundle,omitempty"`
	Replay            *ReplayEvidence     `json:"replay,omitempty"`
}

type ReplayEvidence struct {
	SourceBundleSHA256 string `json:"sourceBundleSha256"`
	ExpectedSignature  string `json:"expectedSignature"`
}

type Violation struct {
	Classification string `json:"classification"`
	ObservationID  string `json:"observationId"`
	Pointer        string `json:"pointer"`
	RowKey         string `json:"rowKey"`
	Expected       any    `json:"expected"`
	Actual         any    `json:"actual"`
	ExpectedHash   string `json:"expectedHash"`
	ActualHash     string `json:"actualHash"`
	Message        string `json:"message"`
}

type EnvironmentEvidence struct {
	NetworkName            string `json:"networkName,omitempty"`
	HostBroker             string `json:"hostBroker,omitempty"`
	InternalBroker         string `json:"internalBroker,omitempty"`
	HostSchemaRegistry     string `json:"hostSchemaRegistry,omitempty"`
	InternalSchemaRegistry string `json:"internalSchemaRegistry,omitempty"`
	PostgresSchemaTemplate string `json:"postgresSchemaTemplate,omitempty"`
}

type AttemptEvidence struct {
	AttemptID               string                                   `json:"attemptId"`
	Role                    string                                   `json:"role"`
	Status                  string                                   `json:"status"`
	Database                string                                   `json:"database"`
	Topic                   string                                   `json:"topic"`
	Group                   string                                   `json:"group"`
	AuthoredImage           string                                   `json:"authoredImage"`
	ExecutedImageID         string                                   `json:"executedImageId,omitempty"`
	ServiceImages           []ServiceImageEvidence                   `json:"serviceImages,omitempty"`
	Published               broker.RecordIdentity                    `json:"published"`
	Publications            []broker.RecordIdentity                  `json:"publications"`
	Deliveries              []database.Delivery                      `json:"deliveries"`
	Rewind                  broker.RewindEvidence                    `json:"rewind"`
	GroupInitialization     broker.InitializationEvidence            `json:"groupInitialization,omitempty"`
	GroupInitializations    map[string]broker.InitializationEvidence `json:"groupInitializations,omitempty"`
	ProbeCapabilities       []probe.Capabilities                     `json:"probeCapabilities,omitempty"`
	ProbeDeliveries         []probe.DeliveryReceipt                  `json:"probeDeliveries,omitempty"`
	CheckpointMode          string                                   `json:"checkpointMode,omitempty"`
	ControlMode             string                                   `json:"controlMode,omitempty"`
	ControlledConfigSHA256  string                                   `json:"controlledConfigSha256,omitempty"`
	ControlledTopology      []ControlledStreamEvidence               `json:"controlledTopology,omitempty"`
	CheckpointReleases      []CheckpointReleaseEvidence              `json:"checkpointReleases,omitempty"`
	LogicalClockTransitions []LogicalClockTransition                 `json:"logicalClockTransitions,omitempty"`
	AggregateTransitions    []database.AggregateTransition           `json:"aggregateTransitions,omitempty"`
	CommittedWhileBlocked   *int64                                   `json:"committedWhileBlocked,omitempty"`
	FinalCommitted          *int64                                   `json:"finalCommitted,omitempty"`
	Effects                 *effects.Observation                     `json:"effects,omitempty"`
	EffectProjection        []effects.SemanticEntry                  `json:"effectProjection,omitempty"`
	Outbox                  []database.OutboxState                   `json:"outbox,omitempty"`
	OutboxPublishes         []database.OutboxPublish                 `json:"outboxPublishes,omitempty"`
	TopicBounds             map[string]broker.OffsetBounds           `json:"topicBounds,omitempty"`
	GroupOffsets            map[string]int64                         `json:"groupOffsets,omitempty"`
	Observations            []observe.Evidence                       `json:"observations,omitempty"`
	Registry                []registry.Evidence                      `json:"registry,omitempty"`
	Quiescence              *QuiescenceEvidence                      `json:"quiescence,omitempty"`
	SchemaAfterHealth       string                                   `json:"schemaAfterHealth,omitempty"`
	SchemaAfterObservation  string                                   `json:"schemaAfterObservation,omitempty"`
	ObservationRows         []map[string]any                         `json:"observationRows"`
	InvariantRows           []map[string]any                         `json:"invariantRows"`
	Signature               *FailureSignature                        `json:"signature,omitempty"`
	Error                   string                                   `json:"error,omitempty"`
}

type ServiceImageEvidence struct {
	Service         string `json:"service"`
	AuthoredImage   string `json:"authoredImage"`
	ExecutedImageID string `json:"executedImageId"`
}

type QuiescenceEvidence struct {
	StableForMilliseconds int64                 `json:"stableForMilliseconds"`
	Samples               int                   `json:"samples"`
	Conditions            map[string]bool       `json:"conditions"`
	Probe                 probe.QuiescenceState `json:"probe"`
	EffectPending         int                   `json:"effectPending"`
	ProcessedEvents       int64                 `json:"processedEvents"`
	CommittedOffset       int64                 `json:"committedOffset"`
	CommittedOffsets      map[string]int64      `json:"committedOffsets,omitempty"`
	AggregateEvents       int64                 `json:"aggregateEvents,omitempty"`
}

// ControlledStreamEvidence separates service-wide checkpoint capacity from the
// independently assigned one-record consumer capacity.
type ControlledStreamEvidence struct {
	LogicalTopic     string `json:"logicalTopic"`
	PhysicalTopic    string `json:"physicalTopic"`
	Partition        int32  `json:"partition"`
	Group            string `json:"group"`
	ClientID         string `json:"clientId"`
	ProbeCapacity    int    `json:"probeCapacity"`
	ConsumerCapacity int    `json:"consumerCapacity"`
}

type CheckpointReleaseEvidence struct {
	Order           int              `json:"order"`
	Checkpoint      probe.Checkpoint `json:"checkpoint"`
	Topic           string           `json:"topic"`
	Partition       int32            `json:"partition"`
	Offset          int64            `json:"offset"`
	Group           string           `json:"group"`
	CommittedOffset int64            `json:"committedOffset"`
}

type LogicalClockTransition struct {
	StepID       string `json:"stepId"`
	From         string `json:"from"`
	By           string `json:"by"`
	Intended     string `json:"intended"`
	Acknowledged string `json:"acknowledged"`
}

type FailureSignature struct {
	InvariantID    string `json:"invariantId"`
	Classification string `json:"classification"`
	ObservationID  string `json:"observationId"`
	RowKey         string `json:"rowKey"`
	Pointer        string `json:"pointer"`
	Expected       any    `json:"expected"`
	Actual         any    `json:"actual"`
	Digest         string `json:"digest"`
}

type verticalPlan struct {
	service     spec.Service
	event       spec.CloudEvent
	publish     spec.PublishAction
	publishes   []plannedPublish
	rewind      spec.RewindOffsetAction
	observation spec.Observation
	invariant   spec.Invariant
}

type plannedPublish struct {
	StepID string
	Action spec.PublishAction
	Event  spec.CloudEvent
}

type infrastructureFailure struct {
	err error
}

type unresolvedFailure struct{ err error }

func (failure *unresolvedFailure) Error() string { return failure.err.Error() }
func (failure *unresolvedFailure) Unwrap() error { return failure.err }

func (failure *infrastructureFailure) Error() string {
	return failure.err.Error()
}

func (failure *infrastructureFailure) Unwrap() error {
	return failure.err
}

func ValidateVerticalSlice(scenario spec.Scenario, target spec.Target) error {
	if isOutboxScenario(scenario) {
		_, err := buildOutboxPlan(scenario, target)
		return err
	}
	if isControlledScenario(scenario) {
		_, err := buildControlledPlan(scenario, target)
		return err
	}
	if isPreciseScenario(scenario) {
		_, err := buildPrecisePlan(scenario, target)
		return err
	}
	if isGeneralObserverScenario(scenario) {
		_, err := buildGeneralPlan(scenario, target)
		return err
	}
	_, err := buildVerticalPlan(scenario, target)
	return err
}

func buildVerticalPlan(scenario spec.Scenario, target spec.Target) (verticalPlan, error) {
	if len(target.Spec.Services) != 1 {
		return verticalPlan{}, fmt.Errorf("milestone 2 requires exactly one target service")
	}
	plan := verticalPlan{service: target.Spec.Services[0]}
	var observationID string
	type authoredPublish struct {
		stepID string
		action spec.PublishAction
	}
	authoredPublishes := []authoredPublish{}
	for _, step := range scenario.Spec.Steps {
		switch {
		case step.Publish != nil:
			authoredPublishes = append(authoredPublishes, authoredPublish{stepID: step.ID, action: *step.Publish})
		case step.RewindOffset != nil:
			if plan.rewind.Topic != "" {
				return verticalPlan{}, fmt.Errorf("milestone 2 supports exactly one rewindOffset action")
			}
			plan.rewind = *step.RewindOffset
		case step.Observe != nil:
			observationID = step.Observe.Observation
		}
	}
	if len(authoredPublishes) == 0 || plan.rewind.Topic == "" || observationID == "" {
		return verticalPlan{}, fmt.Errorf("milestone 2 requires publish, rewindOffset, and observe actions")
	}
	primaryCount := 0
	for _, authored := range authoredPublishes {
		event, exists := scenario.Spec.Events[authored.action.Event]
		if !exists {
			return verticalPlan{}, fmt.Errorf("published event %q is undeclared", authored.action.Event)
		}
		if authored.action.Key == "" || authored.action.Key != event.AggregateID {
			return verticalPlan{}, fmt.Errorf("kafka key must equal the CloudEvent aggregateid")
		}
		plan.publishes = append(plan.publishes, plannedPublish{StepID: authored.stepID, Action: authored.action, Event: event})
		if authored.action.Topic == plan.rewind.Topic && authored.action.Partition == plan.rewind.Partition {
			plan.publish = authored.action
			plan.event = event
			primaryCount++
		}
	}
	if primaryCount != 1 {
		return verticalPlan{}, fmt.Errorf("exactly one publish must identify the rewind topic and partition")
	}
	for _, observation := range scenario.Spec.Observations {
		if observation.ID == observationID {
			plan.observation = observation
		}
	}
	if plan.observation.SQL == nil {
		return verticalPlan{}, fmt.Errorf("milestone 2 requires a SQL observation")
	}
	if len(scenario.Spec.Invariants) != 1 {
		return verticalPlan{}, fmt.Errorf("milestone 2 requires exactly one SQL invariant")
	}
	plan.invariant = scenario.Spec.Invariants[0]
	if plan.rewind.Service != plan.service.Name {
		return verticalPlan{}, fmt.Errorf("rewindOffset service must be the only target service")
	}
	return plan, nil
}

func Run(ctx context.Context, config Config) (report Report) {
	started := time.Now().UTC()
	report = Report{
		APIVersion:        spec.APIVersion,
		Kind:              "Result",
		RunID:             newRunID(),
		State:             "VALIDATING",
		Classification:    "UNRESOLVED",
		StartedAt:         started.Format(time.RFC3339Nano),
		NonportableImages: config.Nonportable(),
		Candidate:         []AttemptEvidence{},
		Violations:        []Violation{},
		Minimization: minimize.Summary{
			Status: "skipped", Minimality: "unavailable", AcceptedTransforms: []string{}, Rejections: []minimize.Rejection{},
		},
	}
	if config.SourceBundleSHA256 != "" {
		report.Replay = &ReplayEvidence{SourceBundleSHA256: config.SourceBundleSHA256, ExpectedSignature: config.ExpectedSignature}
	}
	bundleScenario := config.Scenario
	defer func() {
		if report.CompletedAt == "" {
			report.CompletedAt = time.Now().UTC().Format(time.RFC3339Nano)
		}
	}()
	if err := artifact.PrepareDirectory(config.Output); err != nil {
		report.fail("INFRASTRUCTURE_ERROR", err)
		return report
	}
	journal, err := runlog.Open(filepath.Join(config.Output, "events.ndjson"))
	if err != nil {
		report.fail("INFRASTRUCTURE_ERROR", err)
		return report
	}
	defer func() {
		_ = journal.Close()
	}()
	if err := journal.State("VALIDATING", ""); err != nil {
		report.fail("INFRASTRUCTURE_ERROR", err)
		return report
	}
	if err := journaled(journal, "VALIDATING", "persist_run_inputs", nil, func() error { return persistInputs(config, report) }); err != nil {
		report.fail("INFRASTRUCTURE_ERROR", err)
		finalizeRun(config.Output, &report, journal, nil)
		return report
	}
	outbox := isOutboxScenario(config.Scenario)
	controlled := !outbox && isControlledScenario(config.Scenario)
	precise := !outbox && !controlled && isPreciseScenario(config.Scenario)
	general := !outbox && !controlled && isGeneralObserverScenario(config.Scenario)
	var plan verticalPlan
	var baselineOutboxPlan outboxPlan
	var baselineControlledPlan controlledPlan
	var baselinePrecisePlan precisePlan
	var baselineGeneralPlan generalPlan
	if outbox {
		baselineOutboxPlan, err = buildOutboxPlan(config.Scenario, config.Baseline)
	} else if controlled {
		baselineControlledPlan, err = buildControlledPlan(config.Scenario, config.Baseline)
	} else if precise {
		baselinePrecisePlan, err = buildPrecisePlan(config.Scenario, config.Baseline)
	} else if general {
		baselineGeneralPlan, err = buildGeneralPlan(config.Scenario, config.Baseline)
	} else {
		plan, err = buildVerticalPlan(config.Scenario, config.Baseline)
	}
	if err != nil {
		report.fail("UNRESOLVED", err)
		finalizeRun(config.Output, &report, journal, nil)
		return report
	}

	runContext, cancel := context.WithTimeout(ctx, config.Scenario.Spec.Limits.MaxRunDuration.Duration)
	defer cancel()
	if err := transition(journal, &report, "PROVISIONING"); err != nil {
		report.fail("INFRASTRUCTURE_ERROR", err)
		finalizeRun(config.Output, &report, journal, nil)
		return report
	}
	var environment *cruntime.Environment
	err = journaled(journal, "PROVISIONING", "start_environment", map[string]any{"runId": report.RunID}, func() error {
		environment, err = cruntime.StartEnvironment(runContext, report.RunID, config.ImageLock)
		return err
	})
	if err != nil {
		report.fail(classifyOperationalError(err), err)
		finalizeRun(config.Output, &report, journal, nil)
		return report
	}
	report.Environment = EnvironmentEvidence{
		NetworkName:            environment.NetworkName,
		HostBroker:             environment.HostBroker,
		InternalBroker:         environment.InternalBroker,
		HostSchemaRegistry:     environment.HostSchemaRegistry,
		InternalSchemaRegistry: environment.InternalSchemaRegistry,
		PostgresSchemaTemplate: "chronicle_template",
	}
	secretValues := []string{environment.PostgresAdminPassword, environment.HostPostgresDSN}
	journal.SetSecretValues(secretValues)
	databaseManager, err := database.NewManager(environment.HostPostgresDSN, environment.InternalPostgres, environment.PostgresAdminUser, environment.PostgresAdminPassword)
	if err == nil {
		err = transition(journal, &report, "SEEDING")
	}
	if err == nil {
		err = journaled(journal, "SEEDING", "bootstrap_database_template", nil, func() error { return databaseManager.Bootstrap(runContext) })
	}
	if err == nil {
		err = transition(journal, &report, "SNAPSHOTTING")
	}
	var admin *broker.Admin
	if err == nil {
		admin, err = broker.NewAdmin(environment.HostBroker)
	}
	if admin != nil {
		defer admin.Close()
	}

	if err == nil {
		err = transition(journal, &report, "BASELINE_STARTING")
	}
	if err == nil {
		var baseline AttemptEvidence
		if outbox {
			baseline, err = executeOutboxAttempt(runContext, outboxAttemptConfig{
				RunID: report.RunID, Index: 0, Role: "baseline", Scenario: config.Scenario, ScenarioRoot: config.ScenarioRoot, Output: config.Output,
				Plan: baselineOutboxPlan, Target: config.Baseline, Environment: environment, Database: databaseManager, Broker: admin, Journal: journal, SecretValues: secretValues,
			})
		} else if controlled {
			baseline, err = executeControlledAttempt(runContext, controlledAttemptConfig{
				RunID: report.RunID, Index: 0, Role: "baseline", Scenario: config.Scenario, ScenarioRoot: config.ScenarioRoot, Output: config.Output,
				Plan: baselineControlledPlan, Target: config.Baseline, Environment: environment, Database: databaseManager, Broker: admin, Journal: journal, SecretValues: secretValues,
			})
		} else if precise {
			baseline, err = executePreciseAttempt(runContext, preciseAttemptConfig{
				RunID: report.RunID, Index: 0, Role: "baseline", Scenario: config.Scenario, ScenarioRoot: config.ScenarioRoot, Output: config.Output,
				Plan: baselinePrecisePlan, Target: config.Baseline, Environment: environment, Database: databaseManager, Broker: admin, Journal: journal, SecretValues: secretValues,
			})
		} else if general {
			baseline, err = executeGeneralAttempt(runContext, generalAttemptConfig{
				RunID: report.RunID, Index: 0, Role: "baseline", Scenario: config.Scenario, ScenarioRoot: config.ScenarioRoot, Output: config.Output,
				Plan: baselineGeneralPlan, Target: config.Baseline, Environment: environment, Database: databaseManager, Broker: admin, Journal: journal, SecretValues: secretValues,
			})
		} else {
			baseline, err = executeAttempt(runContext, attemptConfig{
				RunID: report.RunID, Index: 0, Role: "baseline", ScenarioRoot: config.ScenarioRoot, Output: config.Output,
				Plan: plan, Target: config.Baseline, Environment: environment, Database: databaseManager, Broker: admin, Journal: journal, SecretValues: secretValues,
			})
		}
		report.Baseline = &baseline
		if err == nil && len(baseline.InvariantRows) != 0 {
			invariantID := plan.invariant.ID
			if outbox && len(baselineOutboxPlan.invariants) != 0 {
				invariantID = baselineOutboxPlan.invariants[0].ID
			} else if controlled && len(baselineControlledPlan.invariants) != 0 {
				invariantID = baselineControlledPlan.invariants[0].ID
			} else if precise {
				invariantID = baselinePrecisePlan.invariant.ID
			} else if general && len(baselineGeneralPlan.invariants) != 0 {
				invariantID = baselineGeneralPlan.invariants[0].ID
			}
			report.fail("UNRESOLVED", fmt.Errorf("baseline violated invariant %q", invariantID))
			err = errors.New(report.Error)
		} else if err == nil && baseline.Signature != nil {
			report.fail("UNRESOLVED", errors.New("baseline violated the external-effect contract"))
			err = errors.New(report.Error)
		}
	}

	if err == nil {
		err = transition(journal, &report, "RESTORING")
	}
	if err == nil {
		err = transition(journal, &report, "CANDIDATE_STARTING")
	}
	if err == nil {
		var candidatePlan verticalPlan
		var candidateOutboxPlan outboxPlan
		var candidateControlledPlan controlledPlan
		var candidatePrecisePlan precisePlan
		var candidateGeneralPlan generalPlan
		var planErr error
		if outbox {
			candidateOutboxPlan, planErr = buildOutboxPlan(config.Scenario, config.Candidate)
		} else if controlled {
			candidateControlledPlan, planErr = buildControlledPlan(config.Scenario, config.Candidate)
		} else if precise {
			candidatePrecisePlan, planErr = buildPrecisePlan(config.Scenario, config.Candidate)
		} else if general {
			candidateGeneralPlan, planErr = buildGeneralPlan(config.Scenario, config.Candidate)
		} else {
			candidatePlan, planErr = buildVerticalPlan(config.Scenario, config.Candidate)
		}
		if planErr != nil {
			err = planErr
		} else {
			attempts := 1 + config.Scenario.Spec.Limits.ConfirmationAttempts
			for index := 0; index < attempts && err == nil; index++ {
				var attempt AttemptEvidence
				var attemptErr error
				if outbox {
					attempt, attemptErr = executeOutboxAttempt(runContext, outboxAttemptConfig{
						RunID: report.RunID, Index: index, Role: "candidate", Scenario: config.Scenario, ScenarioRoot: config.ScenarioRoot, Output: config.Output,
						Plan: candidateOutboxPlan, Target: config.Candidate, Environment: environment, Database: databaseManager, Broker: admin, Journal: journal,
						SecretValues: secretValues, Baseline: report.Baseline,
					})
				} else if controlled {
					attempt, attemptErr = executeControlledAttempt(runContext, controlledAttemptConfig{
						RunID: report.RunID, Index: index, Role: "candidate", Scenario: config.Scenario, ScenarioRoot: config.ScenarioRoot, Output: config.Output,
						Plan: candidateControlledPlan, Target: config.Candidate, Environment: environment, Database: databaseManager, Broker: admin, Journal: journal,
						SecretValues: secretValues, Baseline: report.Baseline,
					})
				} else if precise {
					attempt, attemptErr = executePreciseAttempt(runContext, preciseAttemptConfig{
						RunID: report.RunID, Index: index, Role: "candidate", Scenario: config.Scenario, ScenarioRoot: config.ScenarioRoot, Output: config.Output,
						Plan: candidatePrecisePlan, Target: config.Candidate, Environment: environment, Database: databaseManager, Broker: admin, Journal: journal, SecretValues: secretValues,
					})
				} else if general {
					attempt, attemptErr = executeGeneralAttempt(runContext, generalAttemptConfig{
						RunID: report.RunID, Index: index, Role: "candidate", Scenario: config.Scenario, ScenarioRoot: config.ScenarioRoot, Output: config.Output,
						Plan: candidateGeneralPlan, Target: config.Candidate, Environment: environment, Database: databaseManager, Broker: admin, Journal: journal,
						SecretValues: secretValues, Baseline: report.Baseline,
					})
				} else {
					attempt, attemptErr = executeAttempt(runContext, attemptConfig{
						RunID: report.RunID, Index: index, Role: "candidate", ScenarioRoot: config.ScenarioRoot, Output: config.Output,
						Plan: candidatePlan, Target: config.Candidate, Environment: environment, Database: databaseManager, Broker: admin, Journal: journal, SecretValues: secretValues,
					})
				}
				report.Candidate = append(report.Candidate, attempt)
				err = attemptErr
			}
		}
	}

	if err == nil {
		err = transition(journal, &report, "COMPARING")
	}
	if err == nil {
		err = transition(journal, &report, "CONFIRMING_FAILURE")
	}
	if err == nil {
		classification, signature := classifyCandidate(report.Candidate)
		report.Classification = classification
		report.FailureSignature = signature
		if signature != nil {
			report.Confirmations = matchingConfirmations(report.Candidate, signature.Digest)
			report.Violations = violationsFromAttempts(report.Candidate)
		}
	}
	if err != nil && report.Error == "" {
		report.fail(classifyOperationalError(err), err)
	}
	if err == nil && report.Classification == "SEMANTIC_REGRESSION" && !config.NoMinimize && !outbox && !controlled && !precise && !general {
		if transitionErr := transition(journal, &report, "MINIMIZING"); transitionErr != nil {
			report.fail("INFRASTRUCTURE_ERROR", transitionErr)
		} else {
			cacheKey, hashErr := reductionClosureHash(config, databaseManager.TemplateFingerprint())
			if hashErr != nil {
				report.fail("INFRASTRUCTURE_ERROR", hashErr)
			} else {
				attemptIndex := 1000
				predicate := proposalPredicate(runContext, config, report.RunID, report.FailureSignature, environment, databaseManager, admin, journal, secretValues, &attemptIndex)
				reducer := minimize.Reducer{MaxTrials: config.Scenario.Spec.Limits.MinimizationTrials, Deadline: time.Now().Add(config.Scenario.Spec.Limits.MinimizationDuration.Duration), CacheKey: cacheKey}
				minimized, summary := reducer.Reduce(runContext, config.Scenario, predicate)
				bundleScenario = minimized
				report.Minimization = summary
				if writeErr := journaled(journal, "MINIMIZING", "persist_minimized_scenario", nil, func() error {
					return artifact.WritePublicJSON(filepath.Join(config.Output, "minimized", "scenario.json"), minimized, secretValues)
				}); writeErr != nil {
					report.fail("INFRASTRUCTURE_ERROR", writeErr)
				}
			}
		}
	}

	interrupted := report.State == "INTERRUPTED"
	_ = transition(journal, &report, "CLEANING")
	cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
	cleanupErr := journaled(journal, "CLEANING", "cleanup_environment", map[string]any{"runId": report.RunID}, func() error { return environment.Cleanup(cleanupContext) })
	cleanupCancel()
	if cleanupErr != nil {
		report.fail("INFRASTRUCTURE_ERROR", cleanupErr)
	}
	if interrupted && cleanupErr == nil {
		report.State = "INTERRUPTED"
	}
	if strings.HasSuffix(report.Classification, "_REGRESSION") && report.FailureSignature != nil {
		if config.ExpectedSignature != "" && config.ExpectedSignature != report.FailureSignature.Digest {
			report.fail("UNRESOLVED", fmt.Errorf("replay signature mismatch: got %s want %s", report.FailureSignature.Digest, config.ExpectedSignature))
		} else {
			bundleErr := journaled(journal, "REPORTING", "create_reproduction_bundle", nil, func() error {
				return bundle.Create(context.Background(), bundle.CreateConfig{
					Path: filepath.Join(config.Output, "reproduction.zip"), RunID: report.RunID, Scenario: bundleScenario,
					ScenarioRoot: config.ScenarioRoot, Baseline: config.Baseline, Candidate: config.Candidate,
					EnvironmentLock: config.ImageLock, ExpectedSignature: report.FailureSignature.Digest, SecretValues: secretValues,
				})
			})
			if bundleErr != nil {
				report.fail("INFRASTRUCTURE_ERROR", bundleErr)
			} else {
				report.Bundle = "reproduction.zip"
			}
		}
	}
	finalizeRun(config.Output, &report, journal, secretValues)
	return report
}

type attemptConfig struct {
	RunID        string
	Index        int
	Role         string
	ScenarioRoot string
	Output       string
	Plan         verticalPlan
	Target       spec.Target
	Environment  *cruntime.Environment
	Database     *database.Manager
	Broker       *broker.Admin
	Journal      *runlog.Journal
	SecretValues []string
	PhaseState   string
}

func executeAttempt(ctx context.Context, config attemptConfig) (evidence AttemptEvidence, resultErr error) {
	attemptID := fmt.Sprintf("%s-%d", config.Role, config.Index)
	databaseName := fmt.Sprintf("cg_%s_%s_%d", config.RunID, config.Role, config.Index)
	prefix := "cg." + config.RunID + "." + config.Role + "." + fmt.Sprint(config.Index)
	topic := prefix + "." + config.Plan.publish.Topic
	group := prefix + "." + config.Plan.rewind.Group
	evidence = AttemptEvidence{
		AttemptID: attemptID, Role: config.Role, Status: "INCOMPLETE", Database: databaseName, Topic: topic, Group: group,
		AuthoredImage: config.Plan.service.Image, Publications: []broker.RecordIdentity{}, Deliveries: []database.Delivery{}, ObservationRows: []map[string]any{}, InvariantRows: []map[string]any{},
	}
	var attempt database.Attempt
	var service *cruntime.ServiceRuntime
	createdTopics := []string{}
	databaseCreated := false
	defer func() {
		if config.Journal != nil && config.PhaseState == "" && strings.Contains(config.Role, "baseline") {
			resultErr = joinInfrastructure(resultErr, config.Journal.State("BASELINE_STOPPING", ""))
		}
		if resultErr != nil {
			evidence.Error = resultErr.Error()
		}
		if writeErr := journaled(config.Journal, attemptState(config, "OBSERVING"), "persist_attempt_evidence", map[string]any{"attemptId": attemptID}, func() error {
			return artifact.WritePublicJSON(filepath.Join(config.Output, "attempts", attemptID+".json"), evidence, config.SecretValues)
		}); writeErr != nil {
			resultErr = joinInfrastructure(resultErr, writeErr)
		}
		cleanupContext, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if service != nil {
			resultErr = joinInfrastructure(resultErr, journaled(config.Journal, attemptState(config, "STOPPING"), "terminate_service", map[string]any{"attemptId": attemptID}, func() error { return service.Terminate(cleanupContext) }))
		}
		for index := len(createdTopics) - 1; index >= 0; index-- {
			createdTopic := createdTopics[index]
			resultErr = joinInfrastructure(resultErr, journaled(config.Journal, attemptState(config, "STOPPING"), "delete_topic", map[string]any{"attemptId": attemptID, "topic": createdTopic}, func() error { return config.Broker.DeleteTopic(cleanupContext, createdTopic) }))
		}
		if databaseCreated {
			resultErr = joinInfrastructure(resultErr, journaled(config.Journal, attemptState(config, "STOPPING"), "drop_attempt_database", map[string]any{"attemptId": attemptID, "database": databaseName}, func() error { return config.Database.DropAttempt(cleanupContext, databaseName) }))
		}
	}()

	var err error
	err = journaled(config.Journal, attemptState(config, "STARTING"), "clone_attempt_database", map[string]any{"attemptId": attemptID, "database": databaseName}, func() error {
		attempt, err = config.Database.Clone(ctx, databaseName)
		return err
	})
	if err != nil {
		return evidence, err
	}
	databaseCreated = true
	if err := journaled(config.Journal, attemptState(config, "STARTING"), "verify_read_only_observer", map[string]any{"attemptId": attemptID}, func() error { return database.AssertObserverReadOnly(ctx, attempt.ObserverDSN) }); err != nil {
		return evidence, err
	}
	seenTopics := map[string]struct{}{}
	for _, publication := range config.Plan.publishes {
		createdTopic := prefix + "." + publication.Action.Topic
		if _, exists := seenTopics[createdTopic]; exists {
			continue
		}
		if err := journaled(config.Journal, attemptState(config, "STARTING"), "create_topic", map[string]any{"attemptId": attemptID, "topic": createdTopic}, func() error { return config.Broker.CreateTopic(ctx, createdTopic) }); err != nil {
			return evidence, err
		}
		seenTopics[createdTopic] = struct{}{}
		createdTopics = append(createdTopics, createdTopic)
	}
	err = journaled(config.Journal, attemptState(config, "STARTING"), "start_service", map[string]any{"attemptId": attemptID, "image": config.Plan.service.Image}, func() error {
		service, err = cruntime.StartService(ctx, cruntime.ServiceConfig{
			RunID: config.RunID, AttemptID: attemptID, Service: config.Plan.service, Network: config.Environment.Network,
			DatabaseDSN: attempt.ServiceDSN, InternalBroker: config.Environment.InternalBroker, TopicPrefix: prefix, GroupPrefix: prefix,
			SecretDirectory: filepath.Join(config.Output, ".secrets"),
		})
		return err
	})
	if err != nil {
		return evidence, err
	}
	evidence.ExecutedImageID = service.ImageID
	if config.Journal != nil && config.PhaseState == "" {
		if err := config.Journal.State(executionState(config.Role, "EXECUTING"), ""); err != nil {
			return evidence, joinInfrastructure(nil, err)
		}
	}
	err = journaled(config.Journal, attemptState(config, "EXECUTING"), "fingerprint_after_health", map[string]any{"attemptId": attemptID}, func() error {
		evidence.SchemaAfterHealth, err = config.Database.FingerprintAttempt(ctx, attempt)
		return err
	})
	if err != nil {
		return evidence, err
	}
	if evidence.SchemaAfterHealth != config.Database.TemplateFingerprint() {
		return evidence, fmt.Errorf("attempt schema fingerprint does not match the frozen template")
	}

	var published broker.RecordIdentity
	for _, publication := range config.Plan.publishes {
		eventDocument, marshalErr := json.Marshal(publication.Event)
		if marshalErr != nil {
			return evidence, fmt.Errorf("encode CloudEvent: %w", marshalErr)
		}
		eventDigest := sha256.Sum256(eventDocument)
		physicalTopic := prefix + "." + publication.Action.Topic
		var record broker.RecordIdentity
		err = journaled(config.Journal, attemptState(config, "EXECUTING"), "publish_record", map[string]any{"attemptId": attemptID, "stepId": publication.StepID, "topic": physicalTopic}, func() error {
			record, err = config.Broker.Publish(ctx, physicalTopic, int32(publication.Action.Partition), []byte(publication.Action.Key), eventDocument, hex.EncodeToString(eventDigest[:]))
			return err
		})
		if err != nil {
			return evidence, err
		}
		evidence.Publications = append(evidence.Publications, record)
		if publication.Action.Event == config.Plan.publish.Event && publication.Action.Topic == config.Plan.publish.Topic && publication.Action.Partition == config.Plan.publish.Partition {
			published = record
		}
	}
	evidence.Published = published
	if published.Offset != config.Plan.rewind.ToOffset {
		return evidence, fmt.Errorf("scenario rewind offset %d does not equal published record offset %d", config.Plan.rewind.ToOffset, published.Offset)
	}
	waitContext, waitCancel := context.WithTimeout(ctx, 20*time.Second)
	evidence.Deliveries, err = database.WaitDeliveries(waitContext, attempt.ObserverDSN, 1)
	waitCancel()
	if err != nil {
		return evidence, err
	}
	commitContext, commitCancel := context.WithTimeout(ctx, 20*time.Second)
	err = config.Broker.WaitCommitted(commitContext, group, topic, published.Partition, published.Offset+1)
	commitCancel()
	if err != nil {
		return evidence, err
	}
	if err := config.Broker.RequireGroupMember(ctx, group, service.ClientID); err != nil {
		return evidence, err
	}
	if err := journaled(config.Journal, attemptState(config, "EXECUTING"), "stop_service", map[string]any{"attemptId": attemptID}, func() error { return service.Stop(ctx) }); err != nil {
		return evidence, err
	}
	emptyContext, emptyCancel := context.WithTimeout(ctx, 20*time.Second)
	err = config.Broker.WaitGroupEmpty(emptyContext, group)
	emptyCancel()
	if err != nil {
		return evidence, err
	}
	err = journaled(config.Journal, attemptState(config, "EXECUTING"), "rewind_offset", map[string]any{"attemptId": attemptID, "topic": topic, "partition": published.Partition, "offset": config.Plan.rewind.ToOffset}, func() error {
		evidence.Rewind, err = config.Broker.Rewind(ctx, group, topic, published.Partition, config.Plan.rewind.ToOffset)
		return err
	})
	if err != nil {
		return evidence, err
	}
	if err := journaled(config.Journal, attemptState(config, "EXECUTING"), "restart_service", map[string]any{"attemptId": attemptID}, func() error { return service.Start(ctx) }); err != nil {
		return evidence, err
	}
	waitContext, waitCancel = context.WithTimeout(ctx, 20*time.Second)
	evidence.Deliveries, err = database.WaitDeliveries(waitContext, attempt.ObserverDSN, 2)
	waitCancel()
	if err != nil {
		return evidence, err
	}
	if err := requireExactRedelivery(published, evidence.Deliveries); err != nil {
		return evidence, err
	}
	commitContext, commitCancel = context.WithTimeout(ctx, 20*time.Second)
	err = config.Broker.WaitCommitted(commitContext, group, topic, published.Partition, published.Offset+1)
	commitCancel()
	if err != nil {
		return evidence, err
	}
	evidence.Rewind.FinalCommitted, err = config.Broker.CommittedOffset(ctx, group, topic, published.Partition)
	if err != nil {
		return evidence, err
	}
	observationQuery, err := os.ReadFile(filepath.Join(config.ScenarioRoot, config.Plan.observation.SQL.QueryFile))
	if err != nil {
		return evidence, fmt.Errorf("read SQL observation: %w", err)
	}
	if config.Journal != nil && config.PhaseState == "" {
		if err := config.Journal.State(executionState(config.Role, "OBSERVING"), ""); err != nil {
			return evidence, joinInfrastructure(nil, err)
		}
	}
	err = journaled(config.Journal, attemptState(config, "OBSERVING"), "collect_observation", map[string]any{"attemptId": attemptID, "observationId": config.Plan.observation.ID}, func() error {
		evidence.ObservationRows, err = database.Query(ctx, attempt.ObserverDSN, string(observationQuery))
		return err
	})
	if err != nil {
		return evidence, err
	}
	invariantQuery, err := os.ReadFile(filepath.Join(config.ScenarioRoot, config.Plan.invariant.QueryFile))
	if err != nil {
		return evidence, fmt.Errorf("read SQL invariant: %w", err)
	}
	err = journaled(config.Journal, attemptState(config, "OBSERVING"), "collect_invariant", map[string]any{"attemptId": attemptID, "invariantId": config.Plan.invariant.ID}, func() error {
		evidence.InvariantRows, err = database.Query(ctx, attempt.ObserverDSN, string(invariantQuery))
		return err
	})
	if err != nil {
		return evidence, err
	}
	err = journaled(config.Journal, attemptState(config, "OBSERVING"), "fingerprint_after_observation", map[string]any{"attemptId": attemptID}, func() error {
		evidence.SchemaAfterObservation, err = config.Database.FingerprintAttempt(ctx, attempt)
		return err
	})
	if err != nil {
		return evidence, err
	}
	if evidence.SchemaAfterHealth != evidence.SchemaAfterObservation {
		return evidence, fmt.Errorf("attempt schema fingerprint changed during execution")
	}
	if evidence.SchemaAfterObservation != config.Database.TemplateFingerprint() {
		return evidence, fmt.Errorf("observed schema fingerprint does not match the frozen template")
	}
	if len(evidence.InvariantRows) > 0 {
		evidence.Signature, err = NewFailureSignature(config.Plan.invariant, evidence.InvariantRows)
		if err != nil {
			return evidence, err
		}
	}
	observationArtifact := map[string]any{
		"apiVersion": spec.APIVersion, "kind": "Observation", "attemptId": attemptID,
		"observationId": config.Plan.observation.ID, "rows": evidence.ObservationRows,
		"invariantId": config.Plan.invariant.ID, "violations": evidence.InvariantRows,
	}
	if err := journaled(config.Journal, attemptState(config, "OBSERVING"), "persist_observation", map[string]any{"attemptId": attemptID, "observationId": config.Plan.observation.ID}, func() error {
		return artifact.WritePublicJSON(filepath.Join(config.Output, "observations", attemptID, config.Plan.observation.ID+".json"), observationArtifact, config.SecretValues)
	}); err != nil {
		return evidence, err
	}
	evidence.Status = "COMPLETE"
	return evidence, nil
}

func requireExactRedelivery(published broker.RecordIdentity, deliveries []database.Delivery) error {
	if len(deliveries) < 2 {
		return fmt.Errorf("redelivery evidence contains fewer than two deliveries")
	}
	for index, delivery := range deliveries[:2] {
		if delivery.Topic != published.Topic || delivery.Partition != published.Partition || delivery.Offset != published.Offset || delivery.Key != published.Key {
			return fmt.Errorf("delivery %d is not the published physical record", index+1)
		}
	}
	return nil
}

func NewFailureSignature(invariant spec.Invariant, rows []map[string]any) (*FailureSignature, error) {
	if len(rows) == 0 {
		return nil, nil
	}
	canonical, err := database.CanonicalRows(rows)
	if err != nil {
		return nil, err
	}
	var ordered []map[string]any
	if err := json.Unmarshal(canonical, &ordered); err != nil {
		return nil, fmt.Errorf("decode canonical invariant rows: %w", err)
	}
	row := ordered[0]
	actual, exists := row["reservation_count"]
	if !exists {
		return nil, fmt.Errorf("R1 invariant row has no reservation_count")
	}
	signature := &FailureSignature{
		InvariantID: invariant.ID, Classification: invariant.Classification, ObservationID: invariant.ID,
		RowKey:  fmt.Sprintf("order_id=%v|sku=%v", row["order_id"], row["sku"]),
		Pointer: "/rows/0/reservation_count", Expected: float64(1), Actual: actual,
	}
	digestInput := *signature
	digestInput.Digest = ""
	document, err := json.Marshal(digestInput)
	if err != nil {
		return nil, fmt.Errorf("encode failure signature: %w", err)
	}
	digest := sha256.Sum256(document)
	signature.Digest = hex.EncodeToString(digest[:])
	return signature, nil
}

func classifyCandidate(attempts []AttemptEvidence) (string, *FailureSignature) {
	if len(attempts) == 0 {
		return "UNRESOLVED", nil
	}
	first := attempts[0].Signature
	if first == nil {
		for _, attempt := range attempts[1:] {
			if attempt.Signature != nil {
				return "FLAKY", nil
			}
		}
		return "PASS", nil
	}
	for _, attempt := range attempts[1:] {
		if attempt.Signature == nil || attempt.Signature.Digest != first.Digest {
			return "FLAKY", nil
		}
	}
	classification := first.Classification
	if classification == "" {
		classification = "SEMANTIC_REGRESSION"
	}
	return classification, first
}

func classifyOperationalError(err error) string {
	var unresolved *unresolvedFailure
	if errors.As(err, &unresolved) {
		return "UNRESOLVED"
	}
	var infrastructure *infrastructureFailure
	if errors.As(err, &infrastructure) {
		return "INFRASTRUCTURE_ERROR"
	}
	var cleanup *cruntime.CleanupError
	if errors.As(err, &cleanup) {
		return "INFRASTRUCTURE_ERROR"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "TIMEOUT"
	}
	if errors.Is(err, context.Canceled) {
		return "INTERRUPTED"
	}
	return "INFRASTRUCTURE_ERROR"
}

func joinInfrastructure(base, infrastructure error) error {
	if infrastructure == nil {
		return base
	}
	return errors.Join(base, &infrastructureFailure{err: infrastructure})
}

func transition(journal *runlog.Journal, report *Report, state string) error {
	if err := journal.State(state, ""); err != nil {
		return joinInfrastructure(nil, err)
	}
	report.State = state
	return nil
}

func journaled(journal *runlog.Journal, state, operation string, detail map[string]any, function func() error) error {
	if journal == nil {
		return function()
	}
	operationID, err := journal.Before(state, operation, detail)
	if err != nil {
		return joinInfrastructure(nil, err)
	}
	operationErr := function()
	status := "ok"
	cause := ""
	if operationErr != nil {
		status = "error"
		cause = sanitizeError(operationErr)
	}
	if journalErr := journal.After(state, operationID, operation, status, cause, nil); journalErr != nil {
		return joinInfrastructure(operationErr, journalErr)
	}
	return operationErr
}

func executionState(role, phase string) string {
	prefix := "CANDIDATE"
	if strings.Contains(role, "baseline") {
		prefix = "BASELINE"
	}
	return prefix + "_" + phase
}

func attemptState(config attemptConfig, phase string) string {
	if config.PhaseState != "" {
		return config.PhaseState
	}
	return executionState(config.Role, phase)
}

func persistInputs(config Config, report Report) error {
	for _, directory := range []string{"attempts", "logs", "minimized", "observations", "schemas", "traces"} {
		if err := os.MkdirAll(filepath.Join(config.Output, directory), 0o700); err != nil {
			return fmt.Errorf("create artifact directory %q: %w", directory, err)
		}
	}
	runMetadata := map[string]any{
		"apiVersion": spec.APIVersion,
		"kind":       "Run",
		"runId":      report.RunID,
		"startedAt":  report.StartedAt,
	}
	for path, value := range map[string]any{
		"run.json":               runMetadata,
		"scenario.resolved.json": config.Scenario,
		"baseline.target.json":   config.Baseline,
		"candidate.target.json":  config.Candidate,
	} {
		if err := artifact.WritePublicJSON(filepath.Join(config.Output, path), value, nil); err != nil {
			return err
		}
	}
	authored, err := sourceOrJSON(config.ScenarioPath, config.Scenario)
	if err != nil {
		return err
	}
	if err := artifact.ValidatePublic(authored, nil); err != nil {
		return err
	}
	if err := artifact.WriteFile(filepath.Join(config.Output, "scenario.authored.yaml"), authored); err != nil {
		return err
	}
	lock, err := os.ReadFile(config.ImageLock)
	if err != nil {
		return fmt.Errorf("read environment lock: %w", err)
	}
	if err := artifact.WriteFile(filepath.Join(config.Output, "environment.lock.json"), lock); err != nil {
		return err
	}
	if err := copyPublicTree(filepath.Join(config.ScenarioRoot, "schemas"), filepath.Join(config.Output, "schemas")); err != nil {
		return err
	}
	return nil
}

func copyPublicTree(source, destination string) error {
	if _, err := os.Stat(source); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	return filepath.Walk(source, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("artifact source tree contains symlink %q", path)
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		target := filepath.Join(destination, relative)
		if info.IsDir() {
			return os.MkdirAll(target, 0o700)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("artifact source %q is not a regular file", path)
		}
		document, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if err := artifact.ValidatePublic(document, nil); err != nil {
			return err
		}
		return artifact.WriteFile(target, document)
	})
}

func sourceOrJSON(path string, value any) ([]byte, error) {
	if path != "" {
		document, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read authored contract: %w", err)
		}
		return document, nil
	}
	document, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encode authored contract: %w", err)
	}
	return append(document, '\n'), nil
}

func finalizeRun(output string, result *Report, journal *runlog.Journal, secretValues []string) {
	if result.Classification == "" {
		result.Classification = "UNRESOLVED"
	}
	terminal := result.Classification
	if result.State == "INTERRUPTED" {
		terminal = "INTERRUPTED"
	}
	if result.Classification == "PASS" || strings.HasSuffix(result.Classification, "_REGRESSION") {
		terminal = "COMPLETE"
	}
	result.State = terminal
	result.CompletedAt = time.Now().UTC().Format(time.RFC3339Nano)
	if err := journal.State("REPORTING", ""); err != nil {
		result.fail("INFRASTRUCTURE_ERROR", err)
		return
	}
	for _, renderer := range []struct {
		path   string
		format string
	}{{"report.json", "json"}, {"report.txt", "text"}, {"junit.xml", "junit"}, {"report.html", "html"}} {
		path, format := renderer.path, renderer.format
		document, err := creport.Render(*result, format)
		if err == nil {
			err = artifact.ValidatePublic(document, secretValues)
		}
		if err == nil {
			err = journaled(journal, "REPORTING", "write_report", map[string]any{"path": path, "format": format}, func() error { return artifact.WriteFile(filepath.Join(output, path), document) })
		}
		if err != nil {
			result.fail("INFRASTRUCTURE_ERROR", err)
			return
		}
	}
	if err := journaled(journal, "REPORTING", "write_result", map[string]any{"path": "result.json"}, func() error {
		return artifact.WritePublicJSON(filepath.Join(output, "result.json"), result, secretValues)
	}); err != nil {
		result.fail("INFRASTRUCTURE_ERROR", err)
		return
	}
	if err := journaled(journal, "REPORTING", "write_checksums", map[string]any{"excluded": []string{"events.ndjson", "checksums.sha256"}}, func() error {
		return artifact.WriteChecksums(output, map[string]struct{}{"events.ndjson": {}, "checksums.sha256": {}})
	}); err != nil {
		result.fail("INFRASTRUCTURE_ERROR", err)
		return
	}
	if err := journal.State(terminal, result.Error); err != nil {
		result.fail("INFRASTRUCTURE_ERROR", err)
	}
}

func matchingConfirmations(attempts []AttemptEvidence, digest string) int {
	count := 0
	for index, attempt := range attempts {
		if index > 0 && attempt.Signature != nil && attempt.Signature.Digest == digest {
			count++
		}
	}
	return count
}

func violationsFromAttempts(attempts []AttemptEvidence) []Violation {
	seen := map[string]struct{}{}
	violations := []Violation{}
	for _, attempt := range attempts {
		if attempt.Signature == nil {
			continue
		}
		if _, exists := seen[attempt.Signature.Digest]; exists {
			continue
		}
		seen[attempt.Signature.Digest] = struct{}{}
		expectedHash := valueHash(attempt.Signature.Expected)
		actualHash := valueHash(attempt.Signature.Actual)
		violations = append(violations, Violation{
			Classification: attempt.Signature.Classification, ObservationID: attempt.Signature.ObservationID,
			Pointer: attempt.Signature.Pointer, RowKey: attempt.Signature.RowKey, Expected: attempt.Signature.Expected,
			Actual: attempt.Signature.Actual, ExpectedHash: expectedHash, ActualHash: actualHash,
			Message: "candidate invariant differs from the baseline contract",
		})
	}
	sort.Slice(violations, func(left, right int) bool {
		a, b := violations[left], violations[right]
		ap, bp := classificationPrecedence(a.Classification), classificationPrecedence(b.Classification)
		if ap != bp {
			return ap < bp
		}
		return a.ObservationID+a.Pointer+a.RowKey+a.ExpectedHash+a.ActualHash < b.ObservationID+b.Pointer+b.RowKey+b.ExpectedHash+b.ActualHash
	})
	return violations
}

func classificationPrecedence(classification string) int {
	switch classification {
	case "SCHEMA_REGRESSION":
		return 0
	case "EXTERNAL_EFFECT_REGRESSION":
		return 1
	default:
		return 2
	}
}

func valueHash(value any) string {
	document, _ := json.Marshal(value)
	digest := sha256.Sum256(document)
	return hex.EncodeToString(digest[:])
}

func reductionClosureHash(config Config, templateFingerprint string) (string, error) {
	hash := sha256.New()
	_, _ = fmt.Fprintf(hash, "%s\n%s\n%s\n", config.Baseline.Spec.Services[0].Image, config.Candidate.Spec.Services[0].Image, templateFingerprint)
	lock, err := os.ReadFile(config.ImageLock)
	if err != nil {
		return "", err
	}
	_, _ = hash.Write(lock)
	paths := []string{}
	err = filepath.Walk(config.ScenarioRoot, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("scenario closure contains symlink %q", path)
		}
		if info.Mode().IsRegular() {
			paths = append(paths, path)
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	sort.Strings(paths)
	for _, path := range paths {
		document, readErr := os.ReadFile(path)
		if readErr != nil {
			return "", readErr
		}
		_, _ = fmt.Fprintf(hash, "%s\x00", path)
		_, _ = hash.Write(document)
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func proposalPredicate(ctx context.Context, config Config, runID string, original *FailureSignature, environment *cruntime.Environment, databaseManager *database.Manager, admin *broker.Admin, journal *runlog.Journal, secretValues []string, attemptIndex *int) minimize.Predicate {
	return func(ctx context.Context, scenario spec.Scenario) (minimize.Outcome, string) {
		options := spec.ValidationOptions{AllowLocalImageIDs: config.Nonportable()}
		violations := append(spec.ValidateScenarioAndTargetWithOptions(scenario, config.Baseline, config.ScenarioRoot, options), spec.ValidateScenarioAndTargetWithOptions(scenario, config.Candidate, config.ScenarioRoot, options)...)
		violations = append(violations, spec.CompareTargets(config.Baseline, config.Candidate, scenario.Spec.Comparison.AllowedTargetDifferences)...)
		baselinePlan, baselineErr := buildVerticalPlan(scenario, config.Baseline)
		candidatePlan, candidateErr := buildVerticalPlan(scenario, config.Candidate)
		if len(violations) != 0 || baselineErr != nil || candidateErr != nil {
			return minimize.Pass, "transform is not executable under the authored contracts"
		}
		outcomes := make([]minimize.Outcome, 0, 2)
		reasons := []string{}
		for repetition := 0; repetition < 2; repetition++ {
			index := *attemptIndex
			*attemptIndex++
			baseline, err := executeAttempt(ctx, attemptConfig{RunID: runID, Index: index, Role: "minbaseline", ScenarioRoot: config.ScenarioRoot, Output: config.Output, Plan: baselinePlan, Target: config.Baseline, Environment: environment, Database: databaseManager, Broker: admin, Journal: journal, SecretValues: secretValues, PhaseState: "MINIMIZING"})
			if err != nil {
				outcomes = append(outcomes, minimize.Unresolved)
				reasons = append(reasons, classifyOperationalError(err)+": "+sanitizeError(err))
				continue
			}
			candidate, err := executeAttempt(ctx, attemptConfig{RunID: runID, Index: index, Role: "mincandidate", ScenarioRoot: config.ScenarioRoot, Output: config.Output, Plan: candidatePlan, Target: config.Candidate, Environment: environment, Database: databaseManager, Broker: admin, Journal: journal, SecretValues: secretValues, PhaseState: "MINIMIZING"})
			if err != nil {
				outcomes = append(outcomes, minimize.Unresolved)
				reasons = append(reasons, classifyOperationalError(err)+": "+sanitizeError(err))
				continue
			}
			if baseline.Signature != nil {
				outcomes = append(outcomes, minimize.Unresolved)
				reasons = append(reasons, "modified baseline violated its invariant")
				continue
			}
			if candidate.Signature != nil && original != nil && candidate.Signature.Digest == original.Digest {
				outcomes = append(outcomes, minimize.SameFailure)
				reasons = append(reasons, "exact primary signature reproduced")
			} else {
				outcomes = append(outcomes, minimize.Pass)
				reasons = append(reasons, "resolved outcome does not preserve the primary signature")
			}
		}
		for _, outcome := range outcomes {
			if outcome == minimize.Unresolved {
				return minimize.Unresolved, strings.Join(reasons, "; ")
			}
		}
		if len(outcomes) == 2 && outcomes[0] == minimize.SameFailure && outcomes[1] == minimize.SameFailure {
			return minimize.SameFailure, strings.Join(reasons, "; ")
		}
		return minimize.Pass, strings.Join(reasons, "; ")
	}
}

func (config Config) Nonportable() bool {
	for _, target := range []spec.Target{config.Baseline, config.Candidate} {
		for _, service := range target.Spec.Services {
			if imagelock.IsLocalImageID(service.Image) {
				return true
			}
		}
	}
	return false
}

func (report *Report) fail(classification string, err error) {
	if classification == "INTERRUPTED" {
		report.Classification = "UNRESOLVED"
		report.State = "INTERRUPTED"
		report.Error = sanitizeError(err)
		return
	}
	report.Classification = classification
	report.State = classification
	report.Error = sanitizeError(err)
}

func sanitizeError(err error) string {
	if err == nil {
		return ""
	}
	return databaseURLPattern.ReplaceAllString(err.Error(), "[database-url-redacted]")
}

func newRunID() string {
	value := make([]byte, 6)
	if _, err := rand.Read(value); err != nil {
		return fmt.Sprintf("%x", time.Now().UnixNano())
	}
	return hex.EncodeToString(value)
}
