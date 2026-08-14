package main

import (
	"context"
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
	"github.com/uday1o1/chronicle-gate/internal/controlcontract"
	"github.com/uday1o1/chronicle-gate/pkg/probe"
)

var variant = "baseline"

var checkpoints = []string{"before_handler", "after_db_commit", "before_offset_commit", "after_offset_commit"}

type consumerWorker struct {
	database *pgxpool.Pool
	consumer *kgo.Client
	admin    *kadm.Client
	probe    *probe.Probe
	stream   controlcontract.Stream
	group    string
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
	runtimeConfig, err := controlcontract.Load(required("CHRONICLE_CONTROLLED_CONFIG_FILE"))
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
			Service: "order-workflow", CommitMode: "manual_sync", MaxControlledInFlight: runtimeConfig.ProbeCapacity,
			Checkpoints: checkpoints, LogicalClock: true,
		}),
	)
	if !instrumentation.Ready() {
		log.Fatal("controlled probe configuration is not ready")
	}
	database, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("create PostgreSQL pool: %v", err)
	}
	defer database.Close()
	if err := database.Ping(ctx); err != nil {
		log.Fatalf("ping PostgreSQL: %v", err)
	}
	workers := make([]*consumerWorker, 0, len(runtimeConfig.Streams))
	for _, stream := range runtimeConfig.Streams {
		topic := required("CHRONICLE_TOPIC_PREFIX") + "." + stream.LogicalTopic
		group := required("CHRONICLE_GROUP_PREFIX") + "." + stream.GroupSuffix
		clientID := "chronicle-" + required("CHRONICLE_ATTEMPT_ID") + "-" + stream.ClientSuffix
		consumer, createErr := kgo.NewClient(
			kgo.SeedBrokers(splitRequired("CHRONICLE_BROKERS")...),
			kgo.ClientID(clientID),
			kgo.ConsumerGroup(group),
			kgo.ConsumeTopics(topic),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
			kgo.DisableAutoCommit(),
			kgo.BlockRebalanceOnPoll(),
			kgo.RebalanceTimeout(90*time.Second),
			kgo.SessionTimeout(10*time.Second),
			kgo.HeartbeatInterval(2*time.Second),
		)
		if createErr != nil {
			log.Fatalf("create controlled Kafka client for %s: %v", stream.LogicalTopic, createErr)
		}
		workers = append(workers, &consumerWorker{database: database, consumer: consumer, admin: kadm.NewClient(consumer), probe: instrumentation, stream: stream, group: group})
	}
	defer func() {
		for _, worker := range workers {
			worker.consumer.CloseAllowingRebalance()
		}
	}()
	healthServer := boundedServer(8080, healthHandler(instrumentation))
	probeServer := boundedServer(9090, instrumentation.Handler())
	for _, server := range []*http.Server{healthServer, probeServer} {
		current := server
		go func() {
			if listenErr := current.ListenAndServe(); listenErr != nil && !errors.Is(listenErr, http.ErrServerClosed) {
				log.Printf("controlled service endpoint: %v", listenErr)
				stop()
			}
		}()
	}
	errChannel := make(chan error, len(workers))
	for _, worker := range workers {
		current := worker
		go func() { errChannel <- current.consume(ctx) }()
	}
	select {
	case <-ctx.Done():
	case consumeErr := <-errChannel:
		if consumeErr != nil {
			log.Printf("controlled consume failed: %v", consumeErr)
		}
		stop()
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = healthServer.Shutdown(shutdown)
	_ = probeServer.Shutdown(shutdown)
}

func (worker *consumerWorker) consume(ctx context.Context) error {
	for ctx.Err() == nil {
		fetches := worker.consumer.PollRecords(ctx, 1)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if fetchErrors := fetches.Errors(); len(fetchErrors) > 0 {
			return fmt.Errorf("poll controlled Kafka record: %v", fetchErrors)
		}
		for _, record := range fetches.Records() {
			if err := worker.process(ctx, record); err != nil {
				return err
			}
			worker.consumer.AllowRebalance()
		}
	}
	return nil
}

func (worker *consumerWorker) process(ctx context.Context, record *kgo.Record) error {
	var event domainEvent
	if err := json.Unmarshal(record.Value, &event); err != nil {
		return fmt.Errorf("decode controlled CloudEvent: %w", err)
	}
	eventTime, err := time.Parse(time.RFC3339Nano, event.Time)
	if err != nil || event.ID == "" || event.AggregateID == "" || string(record.Key) != event.AggregateID {
		return errors.New("controlled record lacks a valid time or matching aggregate key")
	}
	binding, exists := worker.stream.BindingFor(event.ID)
	if !exists {
		return fmt.Errorf("event %q has no controlled runtime binding on %q", event.ID, worker.stream.LogicalTopic)
	}
	deliveryLogicalTime := worker.probe.Clock().Now().UTC()
	digest := sha256.Sum256(record.Value)
	if err := worker.probe.RecordDelivery(probe.DeliveryReceipt{
		Topic: record.Topic, Partition: record.Partition, Offset: record.Offset, Key: string(record.Key),
		EventID: event.ID, EventSHA256: hex.EncodeToString(digest[:]),
	}); err != nil {
		return fmt.Errorf("record controlled delivery receipt: %w", err)
	}
	endWork := worker.probe.BeginWork(probe.WorkLabels{Service: "order-workflow", Kind: "kafka", EventID: event.ID})
	defer endWork()
	checkpoint := func(name string) error {
		return worker.probe.Enter(ctx, probe.Checkpoint{Name: name, Service: "order-workflow", EventID: event.ID, StepID: binding.StepID, Occurrence: 1})
	}
	if err := checkpoint("before_handler"); err != nil {
		return err
	}
	var deliveryID int64
	if err := pgx.BeginFunc(ctx, worker.database, func(transaction pgx.Tx) error {
		if _, err := transaction.Exec(ctx, `
INSERT INTO order_state (aggregate_id, aggregate_version, status, watermark, last_event_time)
VALUES ($1, 0, 'pending', $2, $3)
ON CONFLICT (aggregate_id) DO NOTHING`, event.AggregateID, deliveryLogicalTime, eventTime.UTC()); err != nil {
			return fmt.Errorf("initialize controlled aggregate: %w", err)
		}
		var current aggregateState
		if err := transaction.QueryRow(ctx, `
SELECT aggregate_id, aggregate_version, status, payment_received,
       inventory_received, watermark, last_event_time
FROM order_state WHERE aggregate_id = $1 FOR UPDATE`, event.AggregateID).Scan(
			&current.AggregateID, &current.Version, &current.Status, &current.PaymentReceived,
			&current.InventoryReceived, &current.Watermark, &current.LastEventTime,
		); err != nil {
			return fmt.Errorf("lock controlled aggregate: %w", err)
		}
		result, err := applyTransition(variant, current, event, eventTime.UTC(), deliveryLogicalTime)
		if err != nil {
			return err
		}
		if _, err := transaction.Exec(ctx, `
UPDATE order_state
SET aggregate_version = $2, status = $3, payment_received = $4,
    inventory_received = $5, watermark = $6, last_event_time = $7,
    updated_at = clock_timestamp()
WHERE aggregate_id = $1`, result.State.AggregateID, result.State.Version, result.State.Status,
			result.State.PaymentReceived, result.State.InventoryReceived, result.State.Watermark, result.State.LastEventTime); err != nil {
			return fmt.Errorf("persist controlled aggregate: %w", err)
		}
		if _, err := transaction.Exec(ctx, `
INSERT INTO aggregate_event_evidence (
  event_id, aggregate_id, event_type, event_time, delivery_logical_time,
  aggregate_version, prior_version, resulting_version, disposition,
  resulting_status, source_topic, source_partition, source_offset
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
			event.ID, event.AggregateID, event.Type, eventTime.UTC(), deliveryLogicalTime,
			event.AggregateVersion, current.Version, result.State.Version, result.Disposition,
			result.State.Status, record.Topic, record.Partition, record.Offset); err != nil {
			return fmt.Errorf("persist aggregate transition evidence: %w", err)
		}
		return transaction.QueryRow(ctx, `
INSERT INTO delivery_ledger (event_id, topic, partition_id, record_offset, record_key)
VALUES ($1,$2,$3,$4,$5) RETURNING id`, event.ID, record.Topic, record.Partition, record.Offset, string(record.Key)).Scan(&deliveryID)
	}); err != nil {
		return fmt.Errorf("commit controlled state: %w", err)
	}
	if err := checkpoint("after_db_commit"); err != nil {
		return err
	}
	if err := checkpoint("before_offset_commit"); err != nil {
		return err
	}
	commitContext, cancelCommit := context.WithTimeout(ctx, 10*time.Second)
	commitErr := worker.consumer.CommitRecords(commitContext, record)
	cancelCommit()
	if commitErr != nil {
		return fmt.Errorf("synchronous controlled Kafka commit: %w", commitErr)
	}
	verifyContext, cancelVerify := context.WithTimeout(ctx, 10*time.Second)
	verifyErr := worker.verifyCommitted(verifyContext, record, record.Offset+1)
	cancelVerify()
	if verifyErr != nil {
		return verifyErr
	}
	if _, err := worker.database.Exec(ctx, "UPDATE delivery_ledger SET commit_confirmed = true WHERE id = $1", deliveryID); err != nil {
		return fmt.Errorf("record controlled commit evidence: %w", err)
	}
	if err := checkpoint("after_offset_commit"); err != nil {
		return err
	}
	return nil
}

func (worker *consumerWorker) verifyCommitted(ctx context.Context, record *kgo.Record, expected int64) error {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		offsets, err := worker.admin.FetchOffsets(ctx, worker.group)
		if err != nil {
			return fmt.Errorf("administratively fetch controlled committed offset: %w", err)
		}
		position, exists := offsets.Lookup(record.Topic, record.Partition)
		if !exists || position.Err != nil {
			return errors.New("controlled committed offset is unavailable")
		}
		if position.At == expected {
			return nil
		}
		if position.At != record.Offset {
			return fmt.Errorf("controlled committed offset %d is neither current %d nor expected %d", position.At, record.Offset, expected)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("verify controlled explicit commit position: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func boundedServer(port int, handler http.Handler) *http.Server {
	return &http.Server{
		Addr: ":" + strconv.Itoa(port), Handler: handler,
		ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 3 * time.Second,
		WriteTimeout: 3 * time.Second, IdleTimeout: 15 * time.Second, MaxHeaderBytes: 8 << 10,
	}
}

func healthHandler(instrumentation *probe.Probe) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet || request.URL.Path != "/healthz" || !instrumentation.Ready() {
			http.NotFound(writer, request)
			return
		}
		writer.WriteHeader(http.StatusOK)
	})
}

func readSecretFile(environment string) (string, error) {
	path := required(environment)
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open secret file for %s: %w", environment, err)
	}
	defer func() { _ = file.Close() }()
	value, err := io.ReadAll(io.LimitReader(file, 64<<10))
	if err != nil {
		return "", fmt.Errorf("read secret file for %s: %w", environment, err)
	}
	result := strings.TrimSpace(string(value))
	if result == "" {
		return "", fmt.Errorf("secret file for %s is empty", environment)
	}
	return result, nil
}

func required(name string) string {
	value := os.Getenv(name)
	if value == "" {
		log.Fatalf("%s is required", name)
	}
	return value
}

func splitRequired(name string) []string {
	return strings.Split(required(name), ",")
}
