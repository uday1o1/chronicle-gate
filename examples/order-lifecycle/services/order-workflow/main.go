package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/uday1o1/chronicle-gate/pkg/probe"
)

var variant = "baseline"

var checkpoints = []string{
	"before_handler",
	"after_state_load",
	"after_external_effect",
	"after_db_commit",
	"before_offset_commit",
	"after_offset_commit",
}

type cloudEvent struct {
	ID          string         `json:"id"`
	AggregateID string         `json:"aggregateid"`
	Data        map[string]any `json:"data"`
}

type effect struct {
	EventID         string `json:"eventId"`
	BusinessKey     string `json:"businessKey"`
	Amount          int64  `json:"amount"`
	IdempotencyKey  string `json:"idempotencyKey"`
	SourceTopic     string `json:"sourceTopic"`
	SourcePartition int32  `json:"sourcePartition"`
	SourceOffset    int64  `json:"sourceOffset"`
}

type workflow struct {
	database    *pgxpool.Pool
	consumer    *kgo.Client
	admin       *kadm.Client
	probe       *probe.Probe
	effectURL   string
	effectToken string
	stepID      string
	http        *http.Client
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	dsn, err := readSecretFile("CHRONICLE_DATABASE_DSN_FILE")
	if err != nil {
		log.Fatal(err)
	}
	probeToken, err := readSecretFile("CHRONICLE_PROBE_TOKEN_FILE")
	if err != nil {
		log.Fatal(err)
	}
	effectToken, err := readSecretFile("CHRONICLE_SINK_WRITER_TOKEN_FILE")
	if err != nil {
		log.Fatal(err)
	}
	clockStart, err := time.Parse(time.RFC3339Nano, required("CHRONICLE_LOGICAL_CLOCK_CURRENT"))
	if err != nil {
		log.Fatalf("parse logical clock: %v", err)
	}
	instrumentation := probe.New(
		probe.WithEnabled(true),
		probe.WithToken(probeToken),
		probe.WithClockStart(clockStart),
		probe.WithCapabilities(probe.Capabilities{
			Service: "order-workflow", CommitMode: "manual_sync", MaxControlledInFlight: 1,
			Checkpoints: checkpoints, LogicalClock: true,
		}),
	)
	if !instrumentation.Ready() {
		log.Fatal("probe configuration is not ready")
	}
	database, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("create PostgreSQL pool: %v", err)
	}
	defer database.Close()
	if err := database.Ping(ctx); err != nil {
		log.Fatalf("ping PostgreSQL: %v", err)
	}
	topic := required("CHRONICLE_TOPIC_PREFIX") + ".payments"
	group := required("CHRONICLE_GROUP_PREFIX") + ".order-workflow"
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(splitRequired("CHRONICLE_BROKERS")...),
		kgo.ClientID("chronicle-"+required("CHRONICLE_ATTEMPT_ID")),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.SessionTimeout(6*time.Second),
		kgo.HeartbeatInterval(2*time.Second),
	)
	if err != nil {
		log.Fatalf("create Kafka client: %v", err)
	}
	defer consumer.CloseAllowingRebalance()
	application := &workflow{
		database: database, consumer: consumer, admin: kadm.NewClient(consumer), probe: instrumentation,
		effectURL: required("CHRONICLE_EFFECT_SINK_URL"), effectToken: effectToken,
		stepID: required("CHRONICLE_CONTROLLED_STEP_ID"),
		http:   &http.Client{Timeout: 5 * time.Second},
	}
	healthServer := boundedServer(8080, healthHandler(instrumentation))
	probeServer := boundedServer(9090, instrumentation.Handler())
	for _, server := range []*http.Server{healthServer, probeServer} {
		current := server
		go func() {
			if listenErr := current.ListenAndServe(); listenErr != nil && !errors.Is(listenErr, http.ErrServerClosed) {
				log.Printf("service endpoint: %v", listenErr)
				stop()
			}
		}()
	}
	defer func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = healthServer.Shutdown(shutdown)
		_ = probeServer.Shutdown(shutdown)
	}()
	application.consume(ctx)
}

func (workflow *workflow) consume(ctx context.Context) {
	for ctx.Err() == nil {
		fetches := workflow.consumer.PollRecords(ctx, 1)
		if ctx.Err() != nil {
			return
		}
		if fetchErrors := fetches.Errors(); len(fetchErrors) > 0 {
			log.Fatalf("poll Kafka: %v", fetchErrors)
		}
		for _, record := range fetches.Records() {
			if err := workflow.process(ctx, record); err != nil {
				log.Fatalf("process record: %v", err)
			}
			workflow.consumer.AllowRebalance()
		}
	}
}

func (workflow *workflow) process(ctx context.Context, record *kgo.Record) error {
	var event cloudEvent
	if err := json.Unmarshal(record.Value, &event); err != nil {
		return fmt.Errorf("decode CloudEvent: %w", err)
	}
	if event.ID == "" || event.AggregateID == "" || string(record.Key) != event.AggregateID {
		return errors.New("record key does not match a complete CloudEvent aggregate identity")
	}
	amountNumber, ok := event.Data["amount"].(float64)
	if !ok || amountNumber <= 0 || amountNumber != float64(int64(amountNumber)) {
		return errors.New("event amount must be a positive integer")
	}
	digest := sha256.Sum256(record.Value)
	if err := workflow.probe.RecordDelivery(probe.DeliveryReceipt{
		Topic: record.Topic, Partition: record.Partition, Offset: record.Offset, Key: string(record.Key),
		EventID: event.ID, EventSHA256: hex.EncodeToString(digest[:]),
	}); err != nil {
		return fmt.Errorf("record delivery receipt: %w", err)
	}
	endWork := workflow.probe.BeginWork(probe.WorkLabels{Service: "order-workflow", Kind: "kafka", EventID: event.ID})
	defer endWork()
	checkpoint := func(name string) error {
		return workflow.probe.Enter(ctx, probe.Checkpoint{Name: name, Service: "order-workflow", EventID: event.ID, StepID: workflow.stepID})
	}
	if err := checkpoint("before_handler"); err != nil {
		return err
	}
	var processed bool
	if err := workflow.database.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM processed_events WHERE event_id = $1)", event.ID).Scan(&processed); err != nil {
		return fmt.Errorf("load processed event state: %w", err)
	}
	if err := checkpoint("after_state_load"); err != nil {
		return err
	}
	if !processed {
		idempotencyKey := event.ID
		if variant == "candidate-r2" {
			suffix := make([]byte, 8)
			if _, err := rand.Read(suffix); err != nil {
				return fmt.Errorf("create candidate idempotency key: %w", err)
			}
			idempotencyKey += "-" + hex.EncodeToString(suffix)
		}
		if err := workflow.sendEffect(ctx, effect{
			EventID: event.ID, BusinessKey: event.AggregateID, Amount: int64(amountNumber), IdempotencyKey: idempotencyKey,
			SourceTopic: record.Topic, SourcePartition: record.Partition, SourceOffset: record.Offset,
		}); err != nil {
			return err
		}
	}
	if err := checkpoint("after_external_effect"); err != nil {
		return err
	}
	var deliveryID int64
	if err := pgx.BeginFunc(ctx, workflow.database, func(transaction pgx.Tx) error {
		if _, err := transaction.Exec(ctx, "INSERT INTO processed_events (event_id, aggregate_id) VALUES ($1, $2) ON CONFLICT (event_id) DO NOTHING", event.ID, event.AggregateID); err != nil {
			return fmt.Errorf("persist processed event: %w", err)
		}
		return transaction.QueryRow(ctx, `
INSERT INTO delivery_ledger (event_id, topic, partition_id, record_offset, record_key)
VALUES ($1, $2, $3, $4, $5)
RETURNING id`, event.ID, record.Topic, record.Partition, record.Offset, string(record.Key)).Scan(&deliveryID)
	}); err != nil {
		return fmt.Errorf("commit workflow state: %w", err)
	}
	if err := checkpoint("after_db_commit"); err != nil {
		return err
	}
	if err := checkpoint("before_offset_commit"); err != nil {
		return err
	}
	commitContext, cancelCommit := context.WithTimeout(ctx, 10*time.Second)
	commitErr := workflow.consumer.CommitRecords(commitContext, record)
	cancelCommit()
	if commitErr != nil {
		return fmt.Errorf("synchronous Kafka commit: %w", commitErr)
	}
	verifyContext, cancelVerify := context.WithTimeout(ctx, 10*time.Second)
	verifyErr := workflow.verifyCommitted(verifyContext, record, record.Offset+1)
	cancelVerify()
	if verifyErr != nil {
		return verifyErr
	}
	if _, err := workflow.database.Exec(ctx, "UPDATE delivery_ledger SET commit_confirmed = true WHERE id = $1", deliveryID); err != nil {
		return fmt.Errorf("record verified commit evidence: %w", err)
	}
	if err := checkpoint("after_offset_commit"); err != nil {
		return err
	}
	return nil
}

func (workflow *workflow) verifyCommitted(ctx context.Context, record *kgo.Record, expected int64) error {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		offsets, err := workflow.admin.FetchOffsets(ctx, required("CHRONICLE_GROUP_PREFIX")+".order-workflow")
		if err != nil {
			return fmt.Errorf("administratively fetch committed offset: %w", err)
		}
		position, exists := offsets.Lookup(record.Topic, record.Partition)
		if !exists || position.Err != nil {
			return errors.New("administrative committed offset is unavailable")
		}
		if position.At == expected {
			return nil
		}
		if position.At != record.Offset {
			return fmt.Errorf("administrative committed offset %d is neither current %d nor expected %d", position.At, record.Offset, expected)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("verify explicit commit position: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func (workflow *workflow) sendEffect(ctx context.Context, value effect) error {
	document, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode effect: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, workflow.effectURL+"/v1/effects", bytes.NewReader(document))
	if err != nil {
		return fmt.Errorf("create effect request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+workflow.effectToken)
	request.Header.Set("Content-Type", "application/json")
	response, err := workflow.http.Do(request)
	if err != nil {
		return fmt.Errorf("call effect sink: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 32<<10))
	if response.StatusCode != http.StatusCreated && response.StatusCode != http.StatusOK {
		return fmt.Errorf("effect sink returned %d", response.StatusCode)
	}
	return nil
}

func boundedServer(port int, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              ":" + strconv.Itoa(port),
		Handler:           handler,
		ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout:       3 * time.Second,
		WriteTimeout:      3 * time.Second,
		IdleTimeout:       15 * time.Second,
		MaxHeaderBytes:    8 << 10,
	}
}

func healthHandler(instrumentation *probe.Probe) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/healthz" {
			http.NotFound(writer, request)
			return
		}
		if !instrumentation.Ready() {
			http.Error(writer, "not ready", http.StatusServiceUnavailable)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"status":"ready"}`))
	})
}

func readSecretFile(environment string) (string, error) {
	path := strings.TrimSpace(os.Getenv(environment))
	if path == "" {
		return "", fmt.Errorf("%s is required", environment)
	}
	value, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", environment, err)
	}
	value = []byte(strings.TrimSpace(string(value)))
	if len(value) < 32 {
		return "", fmt.Errorf("%s must contain at least 32 bytes", environment)
	}
	return string(value), nil
}

func required(name string) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		log.Fatalf("%s is required", name)
	}
	return value
}

func splitRequired(name string) []string {
	values := strings.Split(required(name), ",")
	for index := range values {
		values[index] = strings.TrimSpace(values[index])
	}
	return values
}
