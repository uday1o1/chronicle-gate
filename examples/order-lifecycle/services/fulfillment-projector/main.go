package main

import (
	"context"
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
	"github.com/twmb/franz-go/pkg/kgo"
)

var variant = "baseline"

type cloudEvent struct {
	SpecVersion     string         `json:"specversion"`
	ID              string         `json:"id"`
	Source          string         `json:"source"`
	Type            string         `json:"type"`
	Subject         string         `json:"subject"`
	Time            string         `json:"time"`
	DataContentType string         `json:"datacontenttype"`
	AggregateID     string         `json:"aggregateid"`
	Data            map[string]any `json:"data"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dsn, err := readSecretFile("CHRONICLE_DATABASE_DSN_FILE")
	if err != nil {
		log.Fatal(err)
	}
	database, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("create PostgreSQL pool: %v", err)
	}
	defer database.Close()
	if err := database.Ping(ctx); err != nil {
		log.Fatalf("ping PostgreSQL: %v", err)
	}

	brokers := splitRequired("CHRONICLE_BROKERS")
	logicalTopic := "inventory"
	if variant == "baseline-r4" || variant == "candidate-r4" {
		logicalTopic = "fulfillment"
	}
	topic := required("CHRONICLE_TOPIC_PREFIX") + "." + logicalTopic
	group := required("CHRONICLE_GROUP_PREFIX") + ".fulfillment-projector"
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ClientID("chronicle-"+required("CHRONICLE_ATTEMPT_ID")),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
	)
	if err != nil {
		log.Fatalf("create Kafka client: %v", err)
	}
	defer client.CloseAllowingRebalance()

	server := &http.Server{Addr: ":8080", ReadHeaderTimeout: 2 * time.Second}
	http.HandleFunc("/healthz", func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"status":"ready"}`))
	})
	http.HandleFunc("/state", func(writer http.ResponseWriter, request *http.Request) {
		rows, queryErr := database.Query(request.Context(), "SELECT order_id, event_id, fulfillment_mode, status, updated_at FROM fulfillment_projection ORDER BY order_id")
		if queryErr != nil {
			http.Error(writer, queryErr.Error(), http.StatusInternalServerError)
			return
		}
		defer rows.Close()
		values := []map[string]any{}
		for rows.Next() {
			var orderID, eventID, mode, status string
			var updatedAt time.Time
			if scanErr := rows.Scan(&orderID, &eventID, &mode, &status, &updatedAt); scanErr != nil {
				http.Error(writer, scanErr.Error(), http.StatusInternalServerError)
				return
			}
			values = append(values, map[string]any{"orderId": orderID, "eventId": eventID, "fulfillmentMode": mode, "status": status, "updatedAt": updatedAt.UTC().Format(time.RFC3339Nano)})
		}
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(map[string]any{"rows": values})
	})
	go func() {
		if listenErr := server.ListenAndServe(); listenErr != nil && !errors.Is(listenErr, http.ErrServerClosed) {
			log.Printf("health server: %v", listenErr)
			stop()
		}
	}()
	defer func() {
		shutdownContext, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownContext)
	}()

	for ctx.Err() == nil {
		fetches := client.PollRecords(ctx, 1)
		if ctx.Err() != nil {
			return
		}
		if fetchErrors := fetches.Errors(); len(fetchErrors) > 0 {
			log.Fatalf("poll Kafka: %v", fetchErrors)
		}
		for _, record := range fetches.Records() {
			deliveryID, output, err := processRecord(ctx, database, record)
			if err != nil {
				log.Fatalf("process record: %v", err)
			}
			if output != nil {
				if err := client.ProduceSync(ctx, output).FirstErr(); err != nil {
					log.Fatalf("publish fulfillment result: %v", err)
				}
			}
			commitContext, cancel := context.WithTimeout(ctx, 10*time.Second)
			commitErr := client.CommitRecords(commitContext, record)
			cancel()
			client.AllowRebalance()
			if commitErr != nil {
				log.Fatalf("commit record: %v", commitErr)
			}
			if _, err := database.Exec(ctx, "UPDATE delivery_ledger SET commit_confirmed = true WHERE id = $1", deliveryID); err != nil {
				log.Fatalf("record commit evidence: %v", err)
			}
		}
	}
}

func processRecord(ctx context.Context, database *pgxpool.Pool, record *kgo.Record) (int64, *kgo.Record, error) {
	var event cloudEvent
	if err := json.Unmarshal(record.Value, &event); err != nil {
		return 0, nil, fmt.Errorf("decode CloudEvent: %w", err)
	}
	if event.ID == "" || event.AggregateID == "" || string(record.Key) != event.AggregateID {
		return 0, nil, fmt.Errorf("record key does not match a complete CloudEvent aggregate identity")
	}
	orderID, ok := event.Data["orderId"].(string)
	if !ok || orderID == "" {
		return 0, nil, fmt.Errorf("event data orderId is missing")
	}
	if event.Type == "dev.chronicle.fulfillment.requested" {
		return processFulfillment(ctx, database, record, event, orderID)
	}
	sku, ok := event.Data["sku"].(string)
	if !ok || sku == "" {
		return 0, nil, fmt.Errorf("event data sku is missing")
	}
	quantity, ok := event.Data["quantity"].(float64)
	if !ok || quantity < 1 {
		return 0, nil, fmt.Errorf("event data quantity is invalid")
	}

	var deliveryID int64
	err := pgx.BeginFunc(ctx, database, func(transaction pgx.Tx) error {
		if err := transaction.QueryRow(ctx, `
INSERT INTO delivery_ledger (event_id, topic, partition_id, record_offset, record_key)
VALUES ($1, $2, $3, $4, $5)
RETURNING id`, event.ID, record.Topic, record.Partition, record.Offset, string(record.Key)).Scan(&deliveryID); err != nil {
			return fmt.Errorf("insert delivery evidence: %w", err)
		}
		attemptID := os.Getenv("CHRONICLE_ATTEMPT_ID")
		guardEnabled := variant == "baseline" || variant == "flaky-r1" && strings.HasSuffix(attemptID, "-1")
		if guardEnabled {
			var exists bool
			if err := transaction.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM inventory_reservations WHERE event_id = $1)", event.ID).Scan(&exists); err != nil {
				return fmt.Errorf("check idempotency guard: %w", err)
			}
			if exists {
				return nil
			}
		}
		if _, err := transaction.Exec(ctx, `
INSERT INTO inventory_reservations (event_id, order_id, sku, quantity)
VALUES ($1, $2, $3, $4)`, event.ID, orderID, sku, int(quantity)); err != nil {
			return fmt.Errorf("insert reservation: %w", err)
		}
		return nil
	})
	if err != nil {
		return 0, nil, err
	}
	return deliveryID, nil, nil
}

func processFulfillment(ctx context.Context, database *pgxpool.Pool, record *kgo.Record, event cloudEvent, orderID string) (int64, *kgo.Record, error) {
	mode, _ := event.Data["fulfillmentMode"].(string)
	if mode == "" {
		mode = "standard"
		if variant == "candidate-r4" {
			mode = "expedited"
		}
	}
	var deliveryID int64
	err := pgx.BeginFunc(ctx, database, func(transaction pgx.Tx) error {
		if err := transaction.QueryRow(ctx, `
INSERT INTO delivery_ledger (event_id, topic, partition_id, record_offset, record_key)
VALUES ($1, $2, $3, $4, $5)
RETURNING id`, event.ID, record.Topic, record.Partition, record.Offset, string(record.Key)).Scan(&deliveryID); err != nil {
			return err
		}
		_, err := transaction.Exec(ctx, `
INSERT INTO fulfillment_projection (order_id, event_id, fulfillment_mode, status)
VALUES ($1, $2, $3, $4)
ON CONFLICT (order_id) DO UPDATE
SET event_id = EXCLUDED.event_id, fulfillment_mode = EXCLUDED.fulfillment_mode,
    status = EXCLUDED.status, updated_at = clock_timestamp()`, orderID, event.ID, mode, "ready")
		return err
	})
	if err != nil {
		return 0, nil, err
	}
	outputEvent := cloudEvent{
		SpecVersion: "1.0", ID: "fulfillment-result-" + event.ID, Source: "/fulfillment-projector",
		Type: "dev.chronicle.fulfillment.ready", Subject: orderID, Time: time.Now().UTC().Format(time.RFC3339Nano),
		DataContentType: "application/json", AggregateID: orderID,
		Data: map[string]any{"orderId": orderID, "fulfillmentMode": mode, "status": "ready"},
	}
	if loggingContext, _ := event.Data["loggingContext"].(string); variant == "candidate-r4" && loggingContext == "invalid-output-schema" {
		delete(outputEvent.Data, "status")
	}
	document, err := json.Marshal(outputEvent)
	if err != nil {
		return 0, nil, err
	}
	output := &kgo.Record{Topic: required("CHRONICLE_TOPIC_PREFIX") + ".fulfillment-results", Partition: 0, Key: []byte(orderID), Value: document}
	output.Headers = []kgo.RecordHeader{{Key: "traceparent", Value: []byte("00-" + event.ID + "-0000000000000001-01")}}
	return deliveryID, output, nil
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
	parts := strings.Split(required(name), ",")
	for index := range parts {
		parts[index] = strings.TrimSpace(parts[index])
	}
	return parts
}
