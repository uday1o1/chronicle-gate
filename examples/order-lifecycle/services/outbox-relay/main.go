package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
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

type outboxRow struct {
	ID              int64
	LogicalEventID  string
	AggregateID     string
	BusinessKey     string
	Amount          int64
	Event           []byte
	PublishAttempts int
}

type relay struct {
	database    *pgxpool.Pool
	producer    *kgo.Client
	probe       *probe.Probe
	outputTopic string
	stepID      string
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	dsn, err := readSecretFile("CHRONICLE_DATABASE_DSN_FILE")
	if err != nil {
		return err
	}
	probeToken, err := readSecretFile("CHRONICLE_PROBE_TOKEN_FILE")
	if err != nil {
		return err
	}
	clockStart, err := time.Parse(time.RFC3339Nano, required("CHRONICLE_LOGICAL_CLOCK_CURRENT"))
	if err != nil {
		return fmt.Errorf("parse logical clock: %w", err)
	}
	database, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("create relay database pool: %w", err)
	}
	defer database.Close()
	if err := database.Ping(ctx); err != nil {
		return fmt.Errorf("ping relay database: %w", err)
	}
	brokers := splitRequired("CHRONICLE_BROKERS")
	prefix := required("CHRONICLE_TOPIC_PREFIX")
	groupPrefix := required("CHRONICLE_GROUP_PREFIX")
	qualification := strings.TrimSpace(os.Getenv("CHRONICLE_QUALIFICATION_RELAY_LATCH")) == "enabled"
	stepID := ""
	if qualification {
		stepID = required("CHRONICLE_CONTROLLED_STEP_ID")
	}
	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ClientID("chronicle-outbox-producer-"+required("CHRONICLE_ATTEMPT_ID")))
	if err != nil {
		return fmt.Errorf("create relay producer: %w", err)
	}
	defer producer.Close()
	serviceProbe := probe.New(
		probe.WithEnabled(true), probe.WithToken(probeToken), probe.WithClockStart(clockStart),
		probe.WithCapabilities(probe.Capabilities{
			Service: "outbox-relay", CommitMode: "manual_sync", MaxControlledInFlight: 1,
			Checkpoints: []string{"after_outbox_publish"}, LogicalClock: true,
		}),
	)
	if !serviceProbe.Ready() {
		return fmt.Errorf("outbox relay probe is not ready")
	}
	worker := &relay{database: database, producer: producer, probe: serviceProbe, outputTopic: prefix + ".orders-created", stepID: stepID}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"status":"ready"}`))
	})
	healthServer := &http.Server{Addr: ":8080", Handler: mux, ReadHeaderTimeout: 2 * time.Second}
	probeServer := &http.Server{Addr: ":9090", Handler: serviceProbe.Handler(), ReadHeaderTimeout: 2 * time.Second}
	serverErrors := make(chan error, 2)
	go serve(healthServer, serverErrors)
	go serve(probeServer, serverErrors)

	workerErrors := make(chan error, 2)
	if qualification {
		triggered := make(chan string, 1)
		go func() {
			if triggerErr := consumeTrigger(ctx, brokers, prefix+".outbox-relay-trigger", groupPrefix+".outbox-relay-trigger", serviceProbe, triggered); triggerErr != nil && !errors.Is(triggerErr, context.Canceled) {
				workerErrors <- triggerErr
			}
		}()
		go func() {
			if err := worker.process(ctx, true, ""); err != nil {
				workerErrors <- err
				return
			}
			for {
				select {
				case <-ctx.Done():
					workerErrors <- ctx.Err()
					return
				case triggerID := <-triggered:
					if err := worker.process(ctx, false, triggerID); err != nil {
						workerErrors <- err
						return
					}
				}
			}
		}()
	} else {
		go func() {
			ticker := time.NewTicker(250 * time.Millisecond)
			defer ticker.Stop()
			for {
				if err := worker.process(ctx, false, ""); err != nil {
					workerErrors <- err
					return
				}
				select {
				case <-ctx.Done():
					workerErrors <- ctx.Err()
					return
				case <-ticker.C:
				}
			}
		}()
	}

	var resultErr error
	select {
	case <-ctx.Done():
	case resultErr = <-serverErrors:
		stop()
	case resultErr = <-workerErrors:
		if !errors.Is(resultErr, context.Canceled) {
			stop()
		}
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = healthServer.Shutdown(shutdown)
	_ = probeServer.Shutdown(shutdown)
	if resultErr != nil && !errors.Is(resultErr, context.Canceled) {
		return resultErr
	}
	return nil
}

func (worker *relay) process(ctx context.Context, retriesOnly bool, triggerID string) error {
	for {
		row, found, err := worker.claim(ctx, retriesOnly)
		if err != nil {
			return err
		}
		if !found {
			return nil
		}
		emittedID := emittedEventID(variant, row.LogicalEventID, row.AggregateID, row.PublishAttempts)
		document, err := rewriteEventID(row.Event, emittedID)
		if err != nil {
			return err
		}
		record := &kgo.Record{Topic: worker.outputTopic, Partition: 0, Key: []byte(row.AggregateID), Value: document}
		results := worker.producer.ProduceSync(ctx, record)
		if err := results.FirstErr(); err != nil {
			return fmt.Errorf("synchronously publish outbox row %d: %w", row.ID, err)
		}
		if len(results) != 1 || results[0].Record == nil || results[0].Record.Offset < 0 {
			return fmt.Errorf("outbox publish returned incomplete broker identity")
		}
		published := results[0].Record
		digest := sha256.Sum256(document)
		if _, err := worker.database.Exec(ctx, `
INSERT INTO outbox_publish_evidence
  (outbox_id, logical_event_id, emitted_event_id, publish_attempt, topic, partition_id, record_offset, value_sha256)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
			row.ID, row.LogicalEventID, emittedID, row.PublishAttempts,
			published.Topic, published.Partition, published.Offset, hex.EncodeToString(digest[:]),
		); err != nil {
			return fmt.Errorf("record acknowledged outbox publish: %w", err)
		}
		if triggerID != "" {
			endWork := worker.probe.BeginWork(probe.WorkLabels{Service: "outbox-relay", Kind: "outbox_publish", EventID: row.LogicalEventID})
			err = worker.probe.Enter(ctx, probe.Checkpoint{
				Name: "after_outbox_publish", Service: "outbox-relay", EventID: triggerID,
				StepID: worker.stepID, Occurrence: 1,
			})
			endWork()
			if err != nil {
				return err
			}
			triggerID = ""
		}
		if _, err := worker.database.Exec(ctx, "UPDATE outbox SET published_at = clock_timestamp() WHERE id = $1 AND published_at IS NULL", row.ID); err != nil {
			return fmt.Errorf("mark outbox row published: %w", err)
		}
	}
}

func (worker *relay) claim(ctx context.Context, retriesOnly bool) (outboxRow, bool, error) {
	var selected outboxRow
	err := pgx.BeginFunc(ctx, worker.database, func(transaction pgx.Tx) error {
		query := `
SELECT id, event_id, aggregate_id, business_key, amount, event, publish_attempts
FROM outbox
WHERE published_at IS NULL`
		if retriesOnly {
			query += " AND publish_attempts > 0"
		}
		query += " ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED"
		if err := transaction.QueryRow(ctx, query).Scan(
			&selected.ID, &selected.LogicalEventID, &selected.AggregateID, &selected.BusinessKey,
			&selected.Amount, &selected.Event, &selected.PublishAttempts,
		); err != nil {
			return err
		}
		selected.PublishAttempts++
		_, err := transaction.Exec(ctx, "UPDATE outbox SET publish_attempts = $2 WHERE id = $1", selected.ID, selected.PublishAttempts)
		return err
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return outboxRow{}, false, nil
	}
	if err != nil {
		return outboxRow{}, false, fmt.Errorf("claim outbox row: %w", err)
	}
	return selected, true, nil
}

func consumeTrigger(ctx context.Context, brokers []string, topic, group string, serviceProbe *probe.Probe, triggered chan<- string) error {
	clientID := "chronicle-outbox-trigger-" + required("CHRONICLE_ATTEMPT_ID")
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...), kgo.ClientID(clientID), kgo.ConsumerGroup(group), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll(),
	)
	if err != nil {
		return fmt.Errorf("create relay trigger consumer: %w", err)
	}
	defer client.CloseAllowingRebalance()
	admin := kadm.NewClient(client)
	defer admin.Close()
	for ctx.Err() == nil {
		fetches := client.PollRecords(ctx, 1)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if fetchErrors := fetches.Errors(); len(fetchErrors) != 0 {
			return fmt.Errorf("poll relay trigger: %v", fetchErrors)
		}
		for _, record := range fetches.Records() {
			var event struct {
				ID          string `json:"id"`
				AggregateID string `json:"aggregateid"`
			}
			if err := json.Unmarshal(record.Value, &event); err != nil || event.ID == "" || event.AggregateID == "" || string(record.Key) != event.AggregateID {
				return errors.New("relay trigger lacks a valid CloudEvent identity")
			}
			digest := sha256.Sum256(record.Value)
			if err := serviceProbe.RecordDelivery(probe.DeliveryReceipt{
				Topic: record.Topic, Partition: record.Partition, Offset: record.Offset, Key: string(record.Key),
				EventID: event.ID, EventSHA256: hex.EncodeToString(digest[:]),
			}); err != nil {
				return fmt.Errorf("record relay trigger delivery: %w", err)
			}
			commitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			commitErr := client.CommitRecords(commitCtx, record)
			cancel()
			if commitErr != nil {
				return fmt.Errorf("commit relay trigger: %w", commitErr)
			}
			for {
				offsets, fetchErr := admin.FetchOffsets(ctx, group)
				if fetchErr == nil {
					position, exists := offsets.Lookup(topic, record.Partition)
					if exists && position.Err == nil && position.At == record.Offset+1 {
						break
					}
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(100 * time.Millisecond):
				}
			}
			client.AllowRebalance()
			select {
			case triggered <- event.ID:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return ctx.Err()
}

func emittedEventID(selectedVariant, logicalID, aggregateID string, attempt int) string {
	if selectedVariant != "candidate-r7" || attempt <= 1 {
		return logicalID
	}
	digest := sha256.Sum256([]byte(logicalID + "\x00retry\x00" + aggregateID))
	return "retry-" + hex.EncodeToString(digest[:16])
}

func rewriteEventID(document []byte, eventID string) ([]byte, error) {
	var event map[string]any
	if err := json.Unmarshal(document, &event); err != nil {
		return nil, fmt.Errorf("decode outbox CloudEvent: %w", err)
	}
	if current, _ := event["id"].(string); current == "" {
		return nil, errors.New("outbox CloudEvent has no id")
	}
	event["id"] = eventID
	rewritten, err := json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("encode emitted CloudEvent: %w", err)
	}
	return rewritten, nil
}

func serve(server *http.Server, failures chan<- error) {
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		failures <- err
	}
}

func readSecretFile(environment string) (string, error) {
	path := required(environment)
	value, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", environment, err)
	}
	if strings.TrimSpace(string(value)) == "" {
		return "", fmt.Errorf("%s is empty", environment)
	}
	return strings.TrimSpace(string(value)), nil
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
	result := make([]string, 0, len(values))
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	if len(result) == 0 {
		log.Fatalf("%s has no values", name)
	}
	return result
}
