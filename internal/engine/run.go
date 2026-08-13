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
	"strings"
	"time"

	"github.com/uday1o1/chronicle-gate/internal/artifact"
	"github.com/uday1o1/chronicle-gate/internal/broker"
	"github.com/uday1o1/chronicle-gate/internal/database"
	"github.com/uday1o1/chronicle-gate/internal/imagelock"
	cruntime "github.com/uday1o1/chronicle-gate/internal/runtime"
	"github.com/uday1o1/chronicle-gate/internal/spec"
)

const ReportSchemaVersion = "chronicle.dev/run/v1alpha1"

type Config struct {
	Scenario     spec.Scenario
	Baseline     spec.Target
	Candidate    spec.Target
	ScenarioRoot string
	Output       string
	ImageLock    string
}

type Report struct {
	SchemaVersion     string              `json:"schemaVersion"`
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
	Error             string              `json:"error,omitempty"`
	Minimization      string              `json:"minimization"`
}

type EnvironmentEvidence struct {
	NetworkName            string `json:"networkName,omitempty"`
	HostBroker             string `json:"hostBroker,omitempty"`
	InternalBroker         string `json:"internalBroker,omitempty"`
	PostgresSchemaTemplate string `json:"postgresSchemaTemplate,omitempty"`
}

type AttemptEvidence struct {
	AttemptID              string                `json:"attemptId"`
	Role                   string                `json:"role"`
	Status                 string                `json:"status"`
	Database               string                `json:"database"`
	Topic                  string                `json:"topic"`
	Group                  string                `json:"group"`
	AuthoredImage          string                `json:"authoredImage"`
	ExecutedImageID        string                `json:"executedImageId,omitempty"`
	Published              broker.RecordIdentity `json:"published"`
	Deliveries             []database.Delivery   `json:"deliveries"`
	Rewind                 broker.RewindEvidence `json:"rewind"`
	SchemaAfterHealth      string                `json:"schemaAfterHealth,omitempty"`
	SchemaAfterObservation string                `json:"schemaAfterObservation,omitempty"`
	ObservationRows        []map[string]any      `json:"observationRows"`
	InvariantRows          []map[string]any      `json:"invariantRows"`
	Signature              *FailureSignature     `json:"signature,omitempty"`
	Error                  string                `json:"error,omitempty"`
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
	rewind      spec.RewindOffsetAction
	observation spec.Observation
	invariant   spec.Invariant
}

type infrastructureFailure struct {
	err error
}

func (failure *infrastructureFailure) Error() string {
	return failure.err.Error()
}

func (failure *infrastructureFailure) Unwrap() error {
	return failure.err
}

func ValidateVerticalSlice(scenario spec.Scenario, target spec.Target) error {
	_, err := buildVerticalPlan(scenario, target)
	return err
}

func buildVerticalPlan(scenario spec.Scenario, target spec.Target) (verticalPlan, error) {
	if len(target.Spec.Services) != 1 {
		return verticalPlan{}, fmt.Errorf("milestone 2 requires exactly one target service")
	}
	plan := verticalPlan{service: target.Spec.Services[0]}
	var observationID string
	for _, step := range scenario.Spec.Steps {
		switch {
		case step.Publish != nil:
			if plan.publish.Event != "" {
				return verticalPlan{}, fmt.Errorf("milestone 2 supports exactly one publish action")
			}
			plan.publish = *step.Publish
		case step.RewindOffset != nil:
			if plan.rewind.Topic != "" {
				return verticalPlan{}, fmt.Errorf("milestone 2 supports exactly one rewindOffset action")
			}
			plan.rewind = *step.RewindOffset
		case step.Observe != nil:
			observationID = step.Observe.Observation
		}
	}
	if plan.publish.Event == "" || plan.rewind.Topic == "" || observationID == "" {
		return verticalPlan{}, fmt.Errorf("milestone 2 requires publish, rewindOffset, and observe actions")
	}
	if plan.publish.Topic != plan.rewind.Topic || plan.publish.Partition != plan.rewind.Partition {
		return verticalPlan{}, fmt.Errorf("publish and rewindOffset must identify the same topic and partition")
	}
	event, exists := scenario.Spec.Events[plan.publish.Event]
	if !exists {
		return verticalPlan{}, fmt.Errorf("published event %q is undeclared", plan.publish.Event)
	}
	if plan.publish.Key == "" || plan.publish.Key != event.AggregateID {
		return verticalPlan{}, fmt.Errorf("kafka key must equal the CloudEvent aggregateid")
	}
	plan.event = event
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
		SchemaVersion:  ReportSchemaVersion,
		RunID:          newRunID(),
		State:          "VALIDATING",
		Classification: "UNRESOLVED",
		StartedAt:      started.Format(time.RFC3339Nano),
		NonportableImages: imagelock.IsLocalImageID(config.Baseline.Spec.Services[0].Image) ||
			imagelock.IsLocalImageID(config.Candidate.Spec.Services[0].Image),
		Candidate:    []AttemptEvidence{},
		Minimization: "not_available_before_milestone_3",
	}
	defer func() {
		if report.CompletedAt == "" {
			report.CompletedAt = time.Now().UTC().Format(time.RFC3339Nano)
		}
	}()
	if err := artifact.PrepareDirectory(config.Output); err != nil {
		report.fail("INFRASTRUCTURE_ERROR", err)
		return report
	}
	plan, err := buildVerticalPlan(config.Scenario, config.Baseline)
	if err != nil {
		report.fail("UNRESOLVED", err)
		finalizeReport(config.Output, &report)
		return report
	}

	runContext, cancel := context.WithTimeout(ctx, config.Scenario.Spec.Limits.MaxRunDuration.Duration)
	defer cancel()
	report.State = "PROVISIONING"
	environment, err := cruntime.StartEnvironment(runContext, report.RunID, config.ImageLock)
	if err != nil {
		report.fail(classifyOperationalError(err), err)
		finalizeReport(config.Output, &report)
		return report
	}
	report.Environment = EnvironmentEvidence{
		NetworkName:            environment.NetworkName,
		HostBroker:             environment.HostBroker,
		InternalBroker:         environment.InternalBroker,
		PostgresSchemaTemplate: "chronicle_template",
	}
	databaseManager, err := database.NewManager(environment.HostPostgresDSN, environment.InternalPostgres, environment.PostgresAdminUser, environment.PostgresAdminPassword)
	if err == nil {
		report.State = "SEEDING"
		err = databaseManager.Bootstrap(runContext)
	}
	var admin *broker.Admin
	if err == nil {
		admin, err = broker.NewAdmin(environment.HostBroker)
	}
	if admin != nil {
		defer admin.Close()
	}

	if err == nil {
		report.State = "BASELINE_EXECUTING"
		var baseline AttemptEvidence
		baseline, err = executeAttempt(runContext, attemptConfig{
			RunID: report.RunID, Index: 0, Role: "baseline", ScenarioRoot: config.ScenarioRoot, Output: config.Output,
			Plan: plan, Target: config.Baseline, Environment: environment, Database: databaseManager, Broker: admin,
		})
		report.Baseline = &baseline
		if err == nil && len(baseline.InvariantRows) != 0 {
			report.fail("UNRESOLVED", fmt.Errorf("baseline violated invariant %q", plan.invariant.ID))
			err = errors.New(report.Error)
		}
	}

	if err == nil {
		report.State = "CANDIDATE_EXECUTING"
		candidatePlan, planErr := buildVerticalPlan(config.Scenario, config.Candidate)
		if planErr != nil {
			err = planErr
		} else {
			attempts := 1 + config.Scenario.Spec.Limits.ConfirmationAttempts
			for index := 0; index < attempts && err == nil; index++ {
				attempt, attemptErr := executeAttempt(runContext, attemptConfig{
					RunID: report.RunID, Index: index, Role: "candidate", ScenarioRoot: config.ScenarioRoot, Output: config.Output,
					Plan: candidatePlan, Target: config.Candidate, Environment: environment, Database: databaseManager, Broker: admin,
				})
				report.Candidate = append(report.Candidate, attempt)
				err = attemptErr
			}
		}
	}

	if err == nil {
		report.State = "CONFIRMING_FAILURE"
		classification, signature := classifyCandidate(report.Candidate)
		report.Classification = classification
		report.FailureSignature = signature
	}
	if err != nil && report.Error == "" {
		report.fail(classifyOperationalError(err), err)
	}

	report.State = "CLEANING"
	cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
	cleanupErr := environment.Cleanup(cleanupContext)
	cleanupCancel()
	if cleanupErr != nil {
		report.fail("INFRASTRUCTURE_ERROR", cleanupErr)
	}
	if report.Classification == "SEMANTIC_REGRESSION" || report.Classification == "PASS" {
		report.State = "COMPLETE"
	} else {
		report.State = report.Classification
	}
	finalizeReport(config.Output, &report)
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
}

func executeAttempt(ctx context.Context, config attemptConfig) (evidence AttemptEvidence, resultErr error) {
	attemptID := fmt.Sprintf("%s-%d", config.Role, config.Index)
	databaseName := fmt.Sprintf("cg_%s_%s_%d", config.RunID, config.Role, config.Index)
	prefix := "cg." + config.RunID + "." + config.Role + "." + fmt.Sprint(config.Index)
	topic := prefix + "." + config.Plan.publish.Topic
	group := prefix + "." + config.Plan.rewind.Group
	evidence = AttemptEvidence{
		AttemptID: attemptID, Role: config.Role, Status: "INCOMPLETE", Database: databaseName, Topic: topic, Group: group,
		AuthoredImage: config.Plan.service.Image, Deliveries: []database.Delivery{}, ObservationRows: []map[string]any{}, InvariantRows: []map[string]any{},
	}
	var attempt database.Attempt
	var service *cruntime.ServiceRuntime
	topicCreated := false
	databaseCreated := false
	defer func() {
		if resultErr != nil {
			evidence.Error = resultErr.Error()
		}
		if writeErr := artifact.WriteJSON(filepath.Join(config.Output, "attempts", attemptID+".json"), evidence); writeErr != nil {
			resultErr = joinInfrastructure(resultErr, writeErr)
		}
		cleanupContext, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if service != nil {
			resultErr = joinInfrastructure(resultErr, service.Terminate(cleanupContext))
		}
		if topicCreated {
			resultErr = joinInfrastructure(resultErr, config.Broker.DeleteTopic(cleanupContext, topic))
		}
		if databaseCreated {
			resultErr = joinInfrastructure(resultErr, config.Database.DropAttempt(cleanupContext, databaseName))
		}
	}()

	var err error
	attempt, err = config.Database.Clone(ctx, databaseName)
	if err != nil {
		return evidence, err
	}
	databaseCreated = true
	if err := database.AssertObserverReadOnly(ctx, attempt.ObserverDSN); err != nil {
		return evidence, err
	}
	if err := config.Broker.CreateTopic(ctx, topic); err != nil {
		return evidence, err
	}
	topicCreated = true
	service, err = cruntime.StartService(ctx, cruntime.ServiceConfig{
		RunID: config.RunID, AttemptID: attemptID, Service: config.Plan.service, Network: config.Environment.Network,
		DatabaseDSN: attempt.ServiceDSN, InternalBroker: config.Environment.InternalBroker, TopicPrefix: prefix, GroupPrefix: prefix,
		SecretDirectory: filepath.Join(config.Output, ".secrets"),
	})
	if err != nil {
		return evidence, err
	}
	evidence.ExecutedImageID = service.ImageID
	evidence.SchemaAfterHealth, err = config.Database.FingerprintAttempt(ctx, attempt)
	if err != nil {
		return evidence, err
	}
	if evidence.SchemaAfterHealth != config.Database.TemplateFingerprint() {
		return evidence, fmt.Errorf("attempt schema fingerprint does not match the frozen template")
	}

	eventDocument, err := json.Marshal(config.Plan.event)
	if err != nil {
		return evidence, fmt.Errorf("encode CloudEvent: %w", err)
	}
	eventDigest := sha256.Sum256(eventDocument)
	published, err := config.Broker.Publish(ctx, topic, int32(config.Plan.publish.Partition), []byte(config.Plan.publish.Key), eventDocument, hex.EncodeToString(eventDigest[:]))
	if err != nil {
		return evidence, err
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
	if err := service.Stop(ctx); err != nil {
		return evidence, err
	}
	emptyContext, emptyCancel := context.WithTimeout(ctx, 20*time.Second)
	err = config.Broker.WaitGroupEmpty(emptyContext, group)
	emptyCancel()
	if err != nil {
		return evidence, err
	}
	evidence.Rewind, err = config.Broker.Rewind(ctx, group, topic, published.Partition, config.Plan.rewind.ToOffset)
	if err != nil {
		return evidence, err
	}
	if err := service.Start(ctx); err != nil {
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
	evidence.ObservationRows, err = database.Query(ctx, attempt.ObserverDSN, string(observationQuery))
	if err != nil {
		return evidence, err
	}
	invariantQuery, err := os.ReadFile(filepath.Join(config.ScenarioRoot, config.Plan.invariant.QueryFile))
	if err != nil {
		return evidence, fmt.Errorf("read SQL invariant: %w", err)
	}
	evidence.InvariantRows, err = database.Query(ctx, attempt.ObserverDSN, string(invariantQuery))
	if err != nil {
		return evidence, err
	}
	evidence.SchemaAfterObservation, err = config.Database.FingerprintAttempt(ctx, attempt)
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
	return "SEMANTIC_REGRESSION", first
}

func classifyOperationalError(err error) string {
	var infrastructure *infrastructureFailure
	if errors.As(err, &infrastructure) {
		return "INFRASTRUCTURE_ERROR"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "TIMEOUT"
	}
	return "INFRASTRUCTURE_ERROR"
}

func joinInfrastructure(base, infrastructure error) error {
	if infrastructure == nil {
		return base
	}
	return errors.Join(base, &infrastructureFailure{err: infrastructure})
}

func finalizeReport(output string, report *Report) {
	report.CompletedAt = time.Now().UTC().Format(time.RFC3339Nano)
	if err := artifact.WriteJSON(filepath.Join(output, "result.json"), report); err != nil {
		report.fail("INFRASTRUCTURE_ERROR", err)
		report.CompletedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
}

func (report *Report) fail(classification string, err error) {
	report.Classification = classification
	report.State = classification
	report.Error = sanitizeError(err)
}

func sanitizeError(err error) string {
	if err == nil {
		return ""
	}
	return strings.ReplaceAll(err.Error(), "postgres://", "postgres:[redacted]//")
}

func newRunID() string {
	value := make([]byte, 6)
	if _, err := rand.Read(value); err != nil {
		return fmt.Sprintf("%x", time.Now().UnixNano())
	}
	return hex.EncodeToString(value)
}
