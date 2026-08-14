package main

import (
	"bytes"
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
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

type cloudEvent struct {
	SpecVersion      string         `json:"specversion"`
	ID               string         `json:"id"`
	Source           string         `json:"source"`
	Type             string         `json:"type"`
	Subject          string         `json:"subject"`
	Time             string         `json:"time"`
	DataContentType  string         `json:"datacontenttype"`
	AggregateID      string         `json:"aggregateid"`
	AggregateVersion int64          `json:"aggregateversion"`
	CausationID      string         `json:"causationid,omitempty"`
	Data             map[string]any `json:"data"`
}

type workflow struct {
	database   *pgxpool.Pool
	producer   *kgo.Client
	topics     map[string]string
	sinkURL    string
	sinkToken  string
	httpClient *http.Client
}

type consumer struct {
	name     string
	topic    string
	group    string
	database *pgxpool.Pool
	handler  func(context.Context, *kgo.Record) error
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
	sinkToken, err := readSecretFile("CHRONICLE_SINK_WRITER_TOKEN_FILE")
	if err != nil {
		return err
	}
	database, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("create lifecycle database pool: %w", err)
	}
	defer database.Close()
	if err := database.Ping(ctx); err != nil {
		return fmt.Errorf("ping lifecycle database: %w", err)
	}
	brokers := splitRequired("CHRONICLE_BROKERS")
	prefix := required("CHRONICLE_TOPIC_PREFIX")
	groupPrefix := required("CHRONICLE_GROUP_PREFIX")
	topics := map[string]string{
		"orders-created": prefix + ".orders-created", "payment-requests": prefix + ".payment-requests",
		"inventory-requests": prefix + ".inventory-requests", "payment-outcomes": prefix + ".payment-outcomes",
		"inventory-outcomes": prefix + ".inventory-outcomes", "fulfillment": prefix + ".fulfillment",
	}
	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ClientID("chronicle-lifecycle-producer-"+required("CHRONICLE_ATTEMPT_ID")))
	if err != nil {
		return fmt.Errorf("create lifecycle producer: %w", err)
	}
	defer producer.Close()
	worker := &workflow{
		database: database, producer: producer, topics: topics,
		sinkURL: strings.TrimRight(required("CHRONICLE_EFFECT_SINK_URL"), "/"), sinkToken: sinkToken,
		httpClient: &http.Client{Timeout: 3 * time.Second},
	}
	consumers := []consumer{
		{name: "created", topic: topics["orders-created"], group: groupPrefix + ".order-workflow", database: database, handler: worker.handleOrderCreated},
		{name: "payment-responder", topic: topics["payment-requests"], group: groupPrefix + ".payment-responder", database: database, handler: worker.handlePaymentRequest},
		{name: "inventory-responder", topic: topics["inventory-requests"], group: groupPrefix + ".inventory-responder", database: database, handler: worker.handleInventoryRequest},
		{name: "payment-outcome", topic: topics["payment-outcomes"], group: groupPrefix + ".payment-outcome", database: database, handler: worker.handlePaymentOutcome},
		{name: "inventory-outcome", topic: topics["inventory-outcomes"], group: groupPrefix + ".inventory-outcome", database: database, handler: worker.handleInventoryOutcome},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"status":"ready"}`))
	})
	server := &http.Server{Addr: ":8080", Handler: mux, ReadHeaderTimeout: 2 * time.Second}
	serverErrors := make(chan error, 1)
	go func() {
		if serveErr := server.ListenAndServe(); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			serverErrors <- serveErr
		}
	}()
	consumerErrors := make(chan error, len(consumers))
	for _, declaration := range consumers {
		current := declaration
		go func() { consumerErrors <- consume(ctx, brokers, current) }()
	}
	var resultErr error
	select {
	case <-ctx.Done():
	case resultErr = <-serverErrors:
		stop()
	case resultErr = <-consumerErrors:
		if !errors.Is(resultErr, context.Canceled) {
			stop()
		}
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdown)
	if resultErr != nil && !errors.Is(resultErr, context.Canceled) {
		return resultErr
	}
	return nil
}

func consume(ctx context.Context, brokers []string, declaration consumer) error {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...), kgo.ClientID("chronicle-lifecycle-"+declaration.name+"-"+required("CHRONICLE_ATTEMPT_ID")),
		kgo.ConsumerGroup(declaration.group), kgo.ConsumeTopics(declaration.topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll(),
	)
	if err != nil {
		return fmt.Errorf("create %s consumer: %w", declaration.name, err)
	}
	defer client.CloseAllowingRebalance()
	for ctx.Err() == nil {
		fetches := client.PollRecords(ctx, 1)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if fetchErrors := fetches.Errors(); len(fetchErrors) != 0 {
			return fmt.Errorf("poll %s: %v", declaration.name, fetchErrors)
		}
		for _, record := range fetches.Records() {
			if err := declaration.handler(ctx, record); err != nil {
				return fmt.Errorf("handle %s record: %w", declaration.name, err)
			}
			commitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			commitErr := client.CommitRecords(commitCtx, record)
			cancel()
			if commitErr != nil {
				return fmt.Errorf("commit %s record: %w", declaration.name, commitErr)
			}
			if _, err := declarationLedger(ctx, declaration.database, record); err != nil {
				return err
			}
			client.AllowRebalance()
		}
	}
	return ctx.Err()
}

func declarationLedger(ctx context.Context, database *pgxpool.Pool, record *kgo.Record) (int64, error) {
	var event cloudEvent
	if err := json.Unmarshal(record.Value, &event); err != nil {
		return 0, err
	}
	var id int64
	err := database.QueryRow(ctx, `
INSERT INTO delivery_ledger (event_id, topic, partition_id, record_offset, record_key, commit_confirmed)
VALUES ($1, $2, $3, $4, $5, true)
RETURNING id`, event.ID, record.Topic, record.Partition, record.Offset, string(record.Key)).Scan(&id)
	return id, err
}

func (worker *workflow) handleOrderCreated(ctx context.Context, record *kgo.Record) error {
	event, orderID, amount, err := decodeEvent(record, "dev.chronicle.order.created")
	if err != nil {
		return err
	}
	fresh, err := worker.guard(ctx, event.ID, orderID)
	if err != nil || !fresh {
		return err
	}
	payment := derivedEvent(event, "dev.chronicle.payment.requested", amount)
	inventory := derivedEvent(event, "dev.chronicle.inventory.requested", amount)
	if err := worker.publish(ctx, worker.topics["payment-requests"], orderID, payment); err != nil {
		return err
	}
	return worker.publish(ctx, worker.topics["inventory-requests"], orderID, inventory)
}

func (worker *workflow) handlePaymentRequest(ctx context.Context, record *kgo.Record) error {
	event, orderID, amount, err := decodeEvent(record, "dev.chronicle.payment.requested")
	if err != nil {
		return err
	}
	if err := worker.applyEffect(ctx, record, event, orderID, amount); err != nil {
		return err
	}
	outcome := derivedEvent(event, "dev.chronicle.payment.confirmed", amount)
	return worker.publish(ctx, worker.topics["payment-outcomes"], orderID, outcome)
}

func (worker *workflow) handleInventoryRequest(ctx context.Context, record *kgo.Record) error {
	event, orderID, amount, err := decodeEvent(record, "dev.chronicle.inventory.requested")
	if err != nil {
		return err
	}
	if required("CHRONICLE_OUTCOME_ORDER") == "payment-first" {
		if err := waitForPayment(ctx, worker.database, orderID); err != nil {
			return err
		}
	}
	outcome := derivedEvent(event, "dev.chronicle.inventory.reserved", amount)
	return worker.publish(ctx, worker.topics["inventory-outcomes"], orderID, outcome)
}

func (worker *workflow) handlePaymentOutcome(ctx context.Context, record *kgo.Record) error {
	event, orderID, amount, err := decodeEvent(record, "dev.chronicle.payment.confirmed")
	if err != nil {
		return err
	}
	ready, fresh, err := worker.applyOutcome(ctx, event, orderID, amount, true)
	if err != nil || !fresh || !ready {
		return err
	}
	return worker.publish(ctx, worker.topics["fulfillment"], orderID, derivedEvent(event, "dev.chronicle.fulfillment.requested", amount))
}

func (worker *workflow) handleInventoryOutcome(ctx context.Context, record *kgo.Record) error {
	event, orderID, amount, err := decodeEvent(record, "dev.chronicle.inventory.reserved")
	if err != nil {
		return err
	}
	ready, fresh, err := worker.applyOutcome(ctx, event, orderID, amount, false)
	if err != nil || !fresh || !ready {
		return err
	}
	return worker.publish(ctx, worker.topics["fulfillment"], orderID, derivedEvent(event, "dev.chronicle.fulfillment.requested", amount))
}

func (worker *workflow) guard(ctx context.Context, eventID, orderID string) (bool, error) {
	result, err := worker.database.Exec(ctx, "INSERT INTO processed_events (event_id, aggregate_id) VALUES ($1, $2) ON CONFLICT DO NOTHING", eventID, orderID)
	return result.RowsAffected() == 1, err
}

func (worker *workflow) applyOutcome(ctx context.Context, event cloudEvent, orderID string, amount int64, payment bool) (bool, bool, error) {
	ready := false
	fresh := false
	err := pgx.BeginFunc(ctx, worker.database, func(transaction pgx.Tx) error {
		result, err := transaction.Exec(ctx, "INSERT INTO processed_events (event_id, aggregate_id) VALUES ($1, $2) ON CONFLICT DO NOTHING", event.ID, orderID)
		if err != nil || result.RowsAffected() == 0 {
			return err
		}
		fresh = true
		if payment {
			if _, err := transaction.Exec(ctx, "INSERT INTO payments (event_id, order_id, amount, status) VALUES ($1, $2, $3, 'confirmed')", event.ID, orderID, amount); err != nil {
				return err
			}
		} else if _, err := transaction.Exec(ctx, "INSERT INTO inventory_reservations (event_id, order_id, sku, quantity) VALUES ($1, $2, 'default-sku', 1)", event.ID, orderID); err != nil {
			return err
		}
		var status string
		if err := transaction.QueryRow(ctx, "SELECT status FROM orders WHERE order_id = $1 FOR UPDATE", orderID).Scan(&status); err != nil {
			return err
		}
		var payments, reservations int
		if err := transaction.QueryRow(ctx, "SELECT (SELECT count(*) FROM payments WHERE order_id = $1), (SELECT count(*) FROM inventory_reservations WHERE order_id = $1)", orderID).Scan(&payments, &reservations); err != nil {
			return err
		}
		if status != "ready" && payments > 0 && reservations > 0 {
			if _, err := transaction.Exec(ctx, "UPDATE orders SET status = 'ready', updated_at = clock_timestamp() WHERE order_id = $1", orderID); err != nil {
				return err
			}
			ready = true
		}
		return nil
	})
	return ready, fresh, err
}

func (worker *workflow) publish(ctx context.Context, topic, key string, event cloudEvent) error {
	document, err := json.Marshal(event)
	if err != nil {
		return err
	}
	if err := worker.producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Partition: 0, Key: []byte(key), Value: document}).FirstErr(); err != nil {
		return fmt.Errorf("publish %s: %w", event.Type, err)
	}
	return nil
}

func (worker *workflow) applyEffect(ctx context.Context, record *kgo.Record, event cloudEvent, orderID string, amount int64) error {
	payload := map[string]any{
		"kind": "payment_capture", "eventId": event.ID, "businessKey": orderID, "amount": amount,
		"idempotencyKey": event.ID, "sourceTopic": record.Topic, "sourcePartition": record.Partition, "sourceOffset": record.Offset,
	}
	document, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, worker.sinkURL+"/v1/effects", bytes.NewReader(document))
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+worker.sinkToken)
	request.Header.Set("Content-Type", "application/json")
	response, err := worker.httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("record payment effect: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 32<<10))
	if response.StatusCode != http.StatusCreated && response.StatusCode != http.StatusOK {
		return fmt.Errorf("effect sink returned %d", response.StatusCode)
	}
	return nil
}

func decodeEvent(record *kgo.Record, expectedType string) (cloudEvent, string, int64, error) {
	var event cloudEvent
	if err := json.Unmarshal(record.Value, &event); err != nil {
		return event, "", 0, err
	}
	orderID, orderOK := event.Data["orderId"].(string)
	amountValue, amountOK := event.Data["amount"].(float64)
	if event.SpecVersion != "1.0" || event.ID == "" || event.Type != expectedType || event.AggregateID == "" || !orderOK || orderID != event.AggregateID || !amountOK || amountValue < 1 || string(record.Key) != event.AggregateID {
		return event, "", 0, fmt.Errorf("invalid %s CloudEvent", expectedType)
	}
	return event, orderID, int64(amountValue), nil
}

func derivedEvent(parent cloudEvent, eventType string, amount int64) cloudEvent {
	rootID := parent.ID
	if value, _ := parent.Data["rootEventId"].(string); value != "" {
		rootID = value
	}
	id := derivedID(rootID, eventType, parent.AggregateID)
	return cloudEvent{
		SpecVersion: "1.0", ID: id, Source: "/order-workflow", Type: eventType,
		Subject: parent.AggregateID, Time: parent.Time, DataContentType: "application/json",
		AggregateID: parent.AggregateID, AggregateVersion: parent.AggregateVersion + 1, CausationID: parent.ID,
		Data: map[string]any{"orderId": parent.AggregateID, "amount": amount, "rootEventId": rootID},
	}
}

func derivedID(parentID, eventType, aggregateID string) string {
	digest := sha256.Sum256([]byte(parentID + "\x00" + eventType + "\x00" + aggregateID))
	return "evt-" + hex.EncodeToString(digest[:16])
}

func waitForPayment(ctx context.Context, database *pgxpool.Pool, orderID string) error {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		var count int
		if err := database.QueryRow(ctx, "SELECT count(*) FROM payments WHERE order_id = $1", orderID).Scan(&count); err != nil {
			return err
		}
		if count > 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
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
	return result
}
