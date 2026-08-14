package database

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	templateDatabase = "chronicle_template"
	ownerRole        = "chronicle_owner"
	serviceRole      = "chronicle_service"
	observerRole     = "chronicle_observer"
	orderAPIRole     = "chronicle_order_api"
	outboxRelayRole  = "chronicle_outbox_relay"
)

var identifierPattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)

// Manager owns the isolated PostgreSQL template and attempt databases.
type Manager struct {
	adminDSN         string
	internalEndpoint string
	adminUser        string
	adminPassword    string
	servicePassword  string
	observerPassword string
	orderAPIPassword string
	outboxPassword   string
	templateHash     string
}

// Attempt is one database cloned from the frozen template.
type Attempt struct {
	Name        string `json:"name"`
	ServiceDSN  string `json:"-"`
	ObserverDSN string `json:"-"`
	OrderAPIDSN string `json:"-"`
	OutboxDSN   string `json:"-"`
}

// OutboxPublish is durable evidence of one acknowledged relay publication.
type OutboxPublish struct {
	Sequence       int64  `json:"sequence"`
	OutboxID       int64  `json:"outboxId"`
	LogicalEventID string `json:"logicalEventId"`
	EmittedEventID string `json:"emittedEventId"`
	Attempt        int    `json:"attempt"`
	Topic          string `json:"topic"`
	Partition      int32  `json:"partition"`
	Offset         int64  `json:"offset"`
	ValueSHA256    string `json:"valueSha256"`
	AckObservedAt  string `json:"ackObservedAt"`
}

// OutboxState is the semantic and relay state of one transactional row.
type OutboxState struct {
	ID              int64  `json:"id"`
	EventID         string `json:"eventId"`
	AggregateID     string `json:"aggregateId"`
	BusinessKey     string `json:"businessKey"`
	Amount          int64  `json:"amount"`
	PublishAttempts int    `json:"publishAttempts"`
	Published       bool   `json:"published"`
}

// Delivery identifies one processed physical Kafka record.
type Delivery struct {
	EventID   string `json:"eventId"`
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Key       string `json:"key"`
}

// AggregateTransition is append-only evidence of one controlled domain event.
type AggregateTransition struct {
	Sequence            int64  `json:"sequence"`
	EventID             string `json:"eventId"`
	AggregateID         string `json:"aggregateId"`
	EventType           string `json:"eventType"`
	EventTime           string `json:"eventTime"`
	DeliveryLogicalTime string `json:"deliveryLogicalTime"`
	AggregateVersion    int64  `json:"aggregateVersion"`
	PriorVersion        int64  `json:"priorVersion"`
	ResultingVersion    int64  `json:"resultingVersion"`
	Disposition         string `json:"disposition"`
	ResultingStatus     string `json:"resultingStatus"`
	SourceTopic         string `json:"sourceTopic"`
	SourcePartition     int32  `json:"sourcePartition"`
	SourceOffset        int64  `json:"sourceOffset"`
}

func NewManager(adminDSN, internalEndpoint, adminUser, adminPassword string) (*Manager, error) {
	servicePassword, err := randomSecret()
	if err != nil {
		return nil, err
	}
	observerPassword, err := randomSecret()
	if err != nil {
		return nil, err
	}
	orderAPIPassword, err := randomSecret()
	if err != nil {
		return nil, err
	}
	outboxPassword, err := randomSecret()
	if err != nil {
		return nil, err
	}
	return &Manager{
		adminDSN:         adminDSN,
		internalEndpoint: internalEndpoint,
		adminUser:        adminUser,
		adminPassword:    adminPassword,
		servicePassword:  servicePassword,
		observerPassword: observerPassword,
		orderAPIPassword: orderAPIPassword,
		outboxPassword:   outboxPassword,
	}, nil
}

func (manager *Manager) Bootstrap(ctx context.Context) error {
	admin, err := pgx.Connect(ctx, manager.adminDSN)
	if err != nil {
		return fmt.Errorf("connect PostgreSQL admin: %w", err)
	}
	defer func() { _ = admin.Close(context.Background()) }()

	statements := []string{
		"CREATE ROLE " + ownerRole + " NOLOGIN",
		"CREATE ROLE " + serviceRole + " LOGIN PASSWORD " + quoteLiteral(manager.servicePassword),
		"CREATE ROLE " + observerRole + " LOGIN PASSWORD " + quoteLiteral(manager.observerPassword),
		"CREATE ROLE " + orderAPIRole + " LOGIN PASSWORD " + quoteLiteral(manager.orderAPIPassword),
		"CREATE ROLE " + outboxRelayRole + " LOGIN PASSWORD " + quoteLiteral(manager.outboxPassword),
		"ALTER ROLE " + observerRole + " SET default_transaction_read_only = on",
		"ALTER ROLE " + observerRole + " SET statement_timeout = '5s'",
		"REVOKE CREATE ON SCHEMA public FROM PUBLIC",
		"CREATE DATABASE " + templateDatabase + " OWNER " + ownerRole,
	}
	for _, statement := range statements {
		if _, err := admin.Exec(ctx, statement); err != nil {
			return fmt.Errorf("bootstrap PostgreSQL with %q: %w", statement, err)
		}
	}
	template, err := pgx.Connect(ctx, manager.hostDSN(manager.adminUser, manager.adminPassword, templateDatabase))
	if err != nil {
		return fmt.Errorf("connect template database: %w", err)
	}
	migrateErr := migrate(ctx, template)
	if migrateErr == nil {
		manager.templateHash, migrateErr = Fingerprint(ctx, template)
	}
	closeErr := template.Close(ctx)
	if migrateErr != nil {
		return migrateErr
	}
	if closeErr != nil {
		return fmt.Errorf("close template database: %w", closeErr)
	}
	if _, err := admin.Exec(ctx, "ALTER DATABASE "+templateDatabase+" ALLOW_CONNECTIONS false"); err != nil {
		return fmt.Errorf("freeze template database: %w", err)
	}
	var allowsConnections bool
	var activeConnections int
	if err := admin.QueryRow(ctx, `
SELECT datallowconn,
       (SELECT count(*) FROM pg_stat_activity WHERE datname = $1)
FROM pg_database
WHERE datname = $1`, templateDatabase).Scan(&allowsConnections, &activeConnections); err != nil {
		return fmt.Errorf("verify frozen template database: %w", err)
	}
	if allowsConnections || activeConnections != 0 {
		return fmt.Errorf("template database did not freeze cleanly: allowsConnections=%t activeConnections=%d", allowsConnections, activeConnections)
	}
	return nil
}

func migrate(ctx context.Context, connection *pgx.Conn) error {
	statements := []string{
		"SET ROLE " + ownerRole,
		`CREATE TABLE inventory_reservations (
id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
event_id text NOT NULL,
order_id text NOT NULL,
sku text NOT NULL,
quantity integer NOT NULL CHECK (quantity > 0),
observed_at timestamptz NOT NULL DEFAULT clock_timestamp()
)`,
		`CREATE TABLE delivery_ledger (
id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
event_id text NOT NULL,
topic text NOT NULL,
partition_id integer NOT NULL,
record_offset bigint NOT NULL,
record_key text NOT NULL,
commit_confirmed boolean NOT NULL DEFAULT false,
delivered_at timestamptz NOT NULL DEFAULT clock_timestamp()
)`,
		`CREATE TABLE processed_events (
event_id text PRIMARY KEY,
aggregate_id text NOT NULL,
processed_at timestamptz NOT NULL DEFAULT clock_timestamp()
)`,
		`CREATE TABLE external_effects (
id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
event_id text NOT NULL,
business_key text NOT NULL,
idempotency_key text NOT NULL UNIQUE,
recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
)`,
		`CREATE TABLE orders (
order_id text PRIMARY KEY,
request_id text NOT NULL UNIQUE,
amount bigint NOT NULL CHECK (amount > 0),
status text NOT NULL,
created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
)`,
		`CREATE TABLE payments (
id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
event_id text NOT NULL UNIQUE,
order_id text NOT NULL,
amount bigint NOT NULL CHECK (amount > 0),
status text NOT NULL,
recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
)`,
		`CREATE TABLE outbox (
id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
event_id text NOT NULL UNIQUE,
aggregate_id text NOT NULL,
business_key text NOT NULL,
amount bigint NOT NULL CHECK (amount > 0),
event jsonb NOT NULL,
publish_attempts integer NOT NULL DEFAULT 0 CHECK (publish_attempts >= 0),
published_at timestamptz
)`,
		`CREATE TABLE outbox_publish_evidence (
publish_sequence bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
outbox_id bigint NOT NULL REFERENCES outbox(id),
logical_event_id text NOT NULL,
emitted_event_id text NOT NULL,
publish_attempt integer NOT NULL CHECK (publish_attempt > 0),
topic text NOT NULL,
partition_id integer NOT NULL,
record_offset bigint NOT NULL,
value_sha256 text NOT NULL CHECK (length(value_sha256) = 64),
ack_observed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
UNIQUE (outbox_id, publish_attempt),
UNIQUE (topic, partition_id, record_offset)
)`,
		`CREATE TABLE fulfillment_projection (
order_id text PRIMARY KEY,
event_id text NOT NULL,
fulfillment_mode text NOT NULL,
status text NOT NULL,
updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
)`,
		`CREATE TABLE order_state (
aggregate_id text PRIMARY KEY,
aggregate_version bigint NOT NULL CHECK (aggregate_version >= 0),
status text NOT NULL,
payment_received boolean NOT NULL DEFAULT false,
inventory_received boolean NOT NULL DEFAULT false,
watermark timestamptz NOT NULL,
last_event_time timestamptz NOT NULL,
updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
)`,
		`CREATE TABLE aggregate_event_evidence (
transition_sequence bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
event_id text NOT NULL UNIQUE,
aggregate_id text NOT NULL,
event_type text NOT NULL,
event_time timestamptz NOT NULL,
delivery_logical_time timestamptz NOT NULL,
aggregate_version bigint NOT NULL CHECK (aggregate_version >= 0),
prior_version bigint NOT NULL CHECK (prior_version >= 0),
resulting_version bigint NOT NULL CHECK (resulting_version >= 0),
disposition text NOT NULL,
resulting_status text NOT NULL,
source_topic text NOT NULL,
source_partition integer NOT NULL,
source_offset bigint NOT NULL,
recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
UNIQUE (source_topic, source_partition, source_offset)
)`,
		"RESET ROLE",
		"REVOKE ALL ON SCHEMA public FROM PUBLIC",
		"GRANT CONNECT ON DATABASE " + templateDatabase + " TO " + serviceRole + ", " + observerRole + ", " + orderAPIRole + ", " + outboxRelayRole,
		"GRANT USAGE ON SCHEMA public TO " + serviceRole + ", " + observerRole + ", " + orderAPIRole + ", " + outboxRelayRole,
		"GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA public TO " + serviceRole,
		"GRANT UPDATE (status, updated_at) ON orders TO " + serviceRole,
		"GRANT UPDATE (commit_confirmed) ON delivery_ledger TO " + serviceRole,
		"GRANT UPDATE (event_id, fulfillment_mode, status, updated_at) ON fulfillment_projection TO " + serviceRole,
		"GRANT UPDATE (aggregate_version, status, payment_received, inventory_received, watermark, last_event_time, updated_at) ON order_state TO " + serviceRole,
		"GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO " + serviceRole,
		"GRANT SELECT ON ALL TABLES IN SCHEMA public TO " + observerRole,
		"GRANT SELECT, INSERT ON orders, outbox TO " + orderAPIRole,
		"GRANT USAGE, SELECT ON SEQUENCE outbox_id_seq TO " + orderAPIRole,
		"GRANT SELECT ON outbox TO " + outboxRelayRole,
		"GRANT UPDATE (publish_attempts, published_at) ON outbox TO " + outboxRelayRole,
		"GRANT INSERT ON outbox_publish_evidence TO " + outboxRelayRole,
		"GRANT USAGE, SELECT ON SEQUENCE outbox_publish_evidence_publish_sequence_seq TO " + outboxRelayRole,
	}
	for _, statement := range statements {
		if _, err := connection.Exec(ctx, statement); err != nil {
			return fmt.Errorf("apply reference migration: %w", err)
		}
	}
	return nil
}

func (manager *Manager) Clone(ctx context.Context, name string) (Attempt, error) {
	if !identifierPattern.MatchString(name) || name == templateDatabase {
		return Attempt{}, fmt.Errorf("invalid attempt database name %q", name)
	}
	if err := manager.VerifyTemplate(ctx); err != nil {
		return Attempt{}, err
	}
	admin, err := pgx.Connect(ctx, manager.adminDSN)
	if err != nil {
		return Attempt{}, fmt.Errorf("connect PostgreSQL admin: %w", err)
	}
	defer func() { _ = admin.Close(context.Background()) }()
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name+" WITH TEMPLATE "+templateDatabase+" OWNER "+ownerRole); err != nil {
		return Attempt{}, fmt.Errorf("clone attempt database %q: %w", name, err)
	}
	if _, err := admin.Exec(ctx, "GRANT CONNECT ON DATABASE "+name+" TO "+serviceRole+", "+observerRole+", "+orderAPIRole+", "+outboxRelayRole); err != nil {
		return Attempt{}, fmt.Errorf("grant attempt database access: %w", err)
	}
	return Attempt{
		Name:        name,
		ServiceDSN:  manager.internalDSN(serviceRole, manager.servicePassword, name),
		ObserverDSN: manager.hostDSN(observerRole, manager.observerPassword, name),
		OrderAPIDSN: manager.internalDSN(orderAPIRole, manager.orderAPIPassword, name),
		OutboxDSN:   manager.internalDSN(outboxRelayRole, manager.outboxPassword, name),
	}, nil
}

func (manager *Manager) VerifyTemplate(ctx context.Context) error {
	admin, err := pgx.Connect(ctx, manager.adminDSN)
	if err != nil {
		return fmt.Errorf("connect PostgreSQL admin: %w", err)
	}
	defer func() { _ = admin.Close(context.Background()) }()
	var sessions int
	if err := admin.QueryRow(ctx, "SELECT count(*) FROM pg_stat_activity WHERE datname = $1", templateDatabase).Scan(&sessions); err != nil {
		return fmt.Errorf("count template sessions: %w", err)
	}
	if sessions != 0 {
		return fmt.Errorf("template database has %d active connections", sessions)
	}
	if _, err := admin.Exec(ctx, "ALTER DATABASE "+templateDatabase+" ALLOW_CONNECTIONS true"); err != nil {
		return fmt.Errorf("temporarily unfreeze template: %w", err)
	}
	frozen := false
	defer func() {
		if !frozen {
			_, _ = admin.Exec(context.Background(), "ALTER DATABASE "+templateDatabase+" ALLOW_CONNECTIONS false")
		}
	}()
	template, err := pgx.Connect(ctx, manager.hostDSN(manager.adminUser, manager.adminPassword, templateDatabase))
	if err != nil {
		return fmt.Errorf("connect template for fingerprint: %w", err)
	}
	fingerprint, fingerprintErr := Fingerprint(ctx, template)
	closeErr := template.Close(ctx)
	if fingerprintErr != nil {
		return fingerprintErr
	}
	if closeErr != nil {
		return fmt.Errorf("close template fingerprint connection: %w", closeErr)
	}
	if fingerprint != manager.templateHash {
		return fmt.Errorf("template schema fingerprint changed: want %s, got %s", manager.templateHash, fingerprint)
	}
	if _, err := admin.Exec(ctx, "ALTER DATABASE "+templateDatabase+" ALLOW_CONNECTIONS false"); err != nil {
		return fmt.Errorf("refreeze template database: %w", err)
	}
	frozen = true
	return nil
}

func (manager *Manager) DropAttempt(ctx context.Context, name string) error {
	if !identifierPattern.MatchString(name) || name == templateDatabase {
		return fmt.Errorf("invalid attempt database name %q", name)
	}
	admin, err := pgx.Connect(ctx, manager.adminDSN)
	if err != nil {
		return fmt.Errorf("connect PostgreSQL admin: %w", err)
	}
	defer func() { _ = admin.Close(context.Background()) }()
	if _, err := admin.Exec(ctx, "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1", name); err != nil {
		return fmt.Errorf("terminate attempt connections: %w", err)
	}
	if _, err := admin.Exec(ctx, "DROP DATABASE IF EXISTS "+name); err != nil {
		return fmt.Errorf("drop attempt database %q: %w", name, err)
	}
	return nil
}

func (manager *Manager) FingerprintAttempt(ctx context.Context, attempt Attempt) (string, error) {
	connection, err := pgx.Connect(ctx, manager.hostDSN(manager.adminUser, manager.adminPassword, attempt.Name))
	if err != nil {
		return "", fmt.Errorf("connect attempt for schema fingerprint: %w", err)
	}
	defer func() { _ = connection.Close(context.Background()) }()
	return Fingerprint(ctx, connection)
}

// AssertWorkloadRoles proves the order API and relay credentials cannot mutate
// tables outside their declared responsibilities.
func (manager *Manager) AssertWorkloadRoles(ctx context.Context, attempt Attempt) error {
	tests := []struct {
		name      string
		dsn       string
		statement string
	}{
		{name: "order API unrelated update", dsn: manager.hostDSN(orderAPIRole, manager.orderAPIPassword, attempt.Name), statement: "UPDATE order_state SET status = 'forbidden'"},
		{name: "order API publish evidence", dsn: manager.hostDSN(orderAPIRole, manager.orderAPIPassword, attempt.Name), statement: "INSERT INTO outbox_publish_evidence (outbox_id, logical_event_id, emitted_event_id, publish_attempt, topic, partition_id, record_offset, value_sha256) VALUES (1, 'x', 'x', 1, 'x', 0, 0, repeat('0', 64))"},
		{name: "relay order insert", dsn: manager.hostDSN(outboxRelayRole, manager.outboxPassword, attempt.Name), statement: "INSERT INTO orders (order_id, request_id, amount, status) VALUES ('forbidden', 'forbidden', 1, 'forbidden')"},
		{name: "relay unrelated update", dsn: manager.hostDSN(outboxRelayRole, manager.outboxPassword, attempt.Name), statement: "UPDATE fulfillment_projection SET status = 'forbidden'"},
	}
	for _, test := range tests {
		connection, err := pgx.Connect(ctx, test.dsn)
		if err != nil {
			return fmt.Errorf("connect for %s role check: %w", test.name, err)
		}
		_, executeErr := connection.Exec(ctx, test.statement)
		closeErr := connection.Close(ctx)
		if executeErr == nil {
			return fmt.Errorf("%s unexpectedly succeeded", test.name)
		}
		if closeErr != nil {
			return fmt.Errorf("close %s role check: %w", test.name, closeErr)
		}
	}
	return nil
}

func (manager *Manager) TemplateFingerprint() string {
	return manager.templateHash
}

func Fingerprint(ctx context.Context, connection *pgx.Conn) (string, error) {
	rows, err := connection.Query(ctx, `
SELECT kind, object_name, definition
FROM (
  SELECT 'extension' AS kind, extname AS object_name, extversion AS definition FROM pg_extension
  UNION ALL
  SELECT 'table', schemaname || '.' || tablename, tableowner FROM pg_tables WHERE schemaname = 'public'
  UNION ALL
  SELECT 'column', table_schema || '.' || table_name || '.' || column_name,
         data_type || '|' || is_nullable || '|' || coalesce(column_default, '')
    FROM information_schema.columns WHERE table_schema = 'public'
  UNION ALL
  SELECT 'constraint', n.nspname || '.' || c.relname || '.' || con.conname, pg_get_constraintdef(con.oid, true)
    FROM pg_constraint con JOIN pg_class c ON c.oid = con.conrelid JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public'
  UNION ALL
  SELECT 'index', schemaname || '.' || indexname, indexdef FROM pg_indexes WHERE schemaname = 'public'
  UNION ALL
  SELECT 'function', n.nspname || '.' || p.proname || '(' || pg_get_function_identity_arguments(p.oid) || ')', pg_get_function_result(p.oid)
    FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'public'
) objects
ORDER BY kind, object_name, definition`)
	if err != nil {
		return "", fmt.Errorf("query schema fingerprint: %w", err)
	}
	defer rows.Close()
	objects := make([][3]string, 0)
	for rows.Next() {
		var object [3]string
		if err := rows.Scan(&object[0], &object[1], &object[2]); err != nil {
			return "", fmt.Errorf("scan schema fingerprint: %w", err)
		}
		objects = append(objects, object)
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("iterate schema fingerprint: %w", err)
	}
	document, err := json.Marshal(objects)
	if err != nil {
		return "", fmt.Errorf("encode schema fingerprint: %w", err)
	}
	digest := sha256.Sum256(document)
	return hex.EncodeToString(digest[:]), nil
}

func Query(ctx context.Context, dsn, query string) ([]map[string]any, error) {
	result, err := QueryDetailed(ctx, dsn, query)
	return result.Rows, err
}

type QueryColumn struct {
	Name string `json:"name"`
	OID  uint32 `json:"oid"`
}

type QueryResult struct {
	Rows    []map[string]any `json:"rows"`
	Columns []QueryColumn    `json:"columns"`
}

// QueryDetailed executes a read-only observation and retains PostgreSQL field metadata.
func QueryDetailed(ctx context.Context, dsn, query string) (QueryResult, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return QueryResult{}, fmt.Errorf("create read-only observer pool: %w", err)
	}
	defer pool.Close()
	transaction, err := pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return QueryResult{}, fmt.Errorf("begin read-only observation: %w", err)
	}
	defer func() { _ = transaction.Rollback(context.Background()) }()
	rows, err := transaction.Query(ctx, query)
	if err != nil {
		return QueryResult{}, fmt.Errorf("execute read-only observation: %w", err)
	}
	defer rows.Close()
	fields := rows.FieldDescriptions()
	columns := make([]QueryColumn, len(fields))
	for index, field := range fields {
		columns[index] = QueryColumn{Name: string(field.Name), OID: uint32(field.DataTypeOID)}
	}
	result := make([]map[string]any, 0)
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return QueryResult{}, fmt.Errorf("read observation row: %w", err)
		}
		row := make(map[string]any, len(fields))
		for index, field := range fields {
			row[string(field.Name)] = values[index]
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return QueryResult{}, fmt.Errorf("iterate observation: %w", err)
	}
	if err := transaction.Commit(ctx); err != nil {
		return QueryResult{}, fmt.Errorf("commit read-only observation: %w", err)
	}
	return QueryResult{Rows: result, Columns: columns}, nil
}

func CountProcessedEvent(ctx context.Context, dsn, eventID string) (int64, error) {
	connection, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return 0, fmt.Errorf("connect processed-event observer: %w", err)
	}
	defer func() { _ = connection.Close(context.Background()) }()
	var count int64
	if err := connection.QueryRow(ctx, "SELECT count(*) FROM processed_events WHERE event_id = $1", eventID).Scan(&count); err != nil {
		return 0, fmt.Errorf("count processed event: %w", err)
	}
	return count, nil
}

func CountUnpublishedOutbox(ctx context.Context, dsn string) (int64, error) {
	connection, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return 0, fmt.Errorf("connect outbox observer: %w", err)
	}
	defer func() { _ = connection.Close(context.Background()) }()
	var count int64
	if err := connection.QueryRow(ctx, "SELECT count(*) FROM outbox WHERE published_at IS NULL").Scan(&count); err != nil {
		return 0, fmt.Errorf("count unpublished outbox rows: %w", err)
	}
	return count, nil
}

// OutboxStates returns transactional outbox rows in insertion order.
func OutboxStates(ctx context.Context, dsn string) ([]OutboxState, error) {
	connection, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect outbox observer: %w", err)
	}
	defer func() { _ = connection.Close(context.Background()) }()
	rows, err := connection.Query(ctx, `
SELECT id, event_id, aggregate_id, business_key, amount, publish_attempts,
       published_at IS NOT NULL
FROM outbox
ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("query outbox states: %w", err)
	}
	defer rows.Close()
	states := []OutboxState{}
	for rows.Next() {
		var state OutboxState
		if err := rows.Scan(&state.ID, &state.EventID, &state.AggregateID, &state.BusinessKey, &state.Amount, &state.PublishAttempts, &state.Published); err != nil {
			return nil, fmt.Errorf("scan outbox state: %w", err)
		}
		states = append(states, state)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate outbox states: %w", err)
	}
	return states, nil
}

// OutboxPublishes returns acknowledged relay publications in durable order.
func OutboxPublishes(ctx context.Context, dsn string) ([]OutboxPublish, error) {
	connection, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect outbox publish observer: %w", err)
	}
	defer func() { _ = connection.Close(context.Background()) }()
	rows, err := connection.Query(ctx, `
SELECT publish_sequence, outbox_id, logical_event_id, emitted_event_id,
       publish_attempt, topic, partition_id, record_offset, value_sha256,
       ack_observed_at
FROM outbox_publish_evidence
ORDER BY publish_sequence`)
	if err != nil {
		return nil, fmt.Errorf("query outbox publish evidence: %w", err)
	}
	defer rows.Close()
	publishes := []OutboxPublish{}
	for rows.Next() {
		var publish OutboxPublish
		var observed time.Time
		if err := rows.Scan(
			&publish.Sequence, &publish.OutboxID, &publish.LogicalEventID, &publish.EmittedEventID,
			&publish.Attempt, &publish.Topic, &publish.Partition, &publish.Offset,
			&publish.ValueSHA256, &observed,
		); err != nil {
			return nil, fmt.Errorf("scan outbox publish evidence: %w", err)
		}
		publish.AckObservedAt = observed.UTC().Format(time.RFC3339Nano)
		publishes = append(publishes, publish)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate outbox publish evidence: %w", err)
	}
	return publishes, nil
}

// AggregateTransitions returns canonical controlled-event evidence in commit order.
func AggregateTransitions(ctx context.Context, dsn string) ([]AggregateTransition, error) {
	connection, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect aggregate transition observer: %w", err)
	}
	defer func() { _ = connection.Close(context.Background()) }()
	rows, err := connection.Query(ctx, `
SELECT transition_sequence, event_id, aggregate_id, event_type,
       event_time, delivery_logical_time, aggregate_version, prior_version,
       resulting_version, disposition, resulting_status,
       source_topic, source_partition, source_offset
FROM aggregate_event_evidence
ORDER BY transition_sequence`)
	if err != nil {
		return nil, fmt.Errorf("query aggregate transition evidence: %w", err)
	}
	defer rows.Close()
	transitions := []AggregateTransition{}
	for rows.Next() {
		var transition AggregateTransition
		var eventTime, deliveryTime time.Time
		if err := rows.Scan(
			&transition.Sequence, &transition.EventID, &transition.AggregateID, &transition.EventType,
			&eventTime, &deliveryTime, &transition.AggregateVersion, &transition.PriorVersion,
			&transition.ResultingVersion, &transition.Disposition, &transition.ResultingStatus,
			&transition.SourceTopic, &transition.SourcePartition, &transition.SourceOffset,
		); err != nil {
			return nil, fmt.Errorf("scan aggregate transition evidence: %w", err)
		}
		transition.EventTime = eventTime.UTC().Format(time.RFC3339Nano)
		transition.DeliveryLogicalTime = deliveryTime.UTC().Format(time.RFC3339Nano)
		transitions = append(transitions, transition)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate aggregate transition evidence: %w", err)
	}
	return transitions, nil
}

func AssertObserverReadOnly(ctx context.Context, dsn string) error {
	connection, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect read-only observer: %w", err)
	}
	defer func() { _ = connection.Close(context.Background()) }()
	var readOnly string
	var statementTimeout string
	if err := connection.QueryRow(ctx, "SHOW default_transaction_read_only").Scan(&readOnly); err != nil {
		return fmt.Errorf("inspect observer read-only setting: %w", err)
	}
	if err := connection.QueryRow(ctx, "SHOW statement_timeout").Scan(&statementTimeout); err != nil {
		return fmt.Errorf("inspect observer statement timeout: %w", err)
	}
	if readOnly != "on" || statementTimeout == "0" {
		return fmt.Errorf("observer safety settings are incomplete: readOnly=%s statementTimeout=%s", readOnly, statementTimeout)
	}
	if _, err := connection.Exec(ctx, "INSERT INTO inventory_reservations (event_id, order_id, sku, quantity) VALUES ('forbidden', 'forbidden', 'forbidden', 1)"); err == nil {
		return fmt.Errorf("observer role unexpectedly accepted INSERT")
	}
	if _, err := connection.Exec(ctx, "CREATE TABLE forbidden_observer_ddl (id integer)"); err == nil {
		return fmt.Errorf("observer role unexpectedly accepted DDL")
	}
	return nil
}

func WaitDeliveries(ctx context.Context, observerDSN string, count int) ([]Delivery, error) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		rows, err := Query(ctx, observerDSN, "SELECT event_id, topic, partition_id, record_offset, record_key FROM delivery_ledger WHERE commit_confirmed ORDER BY id")
		if err == nil && len(rows) >= count {
			deliveries := make([]Delivery, 0, len(rows))
			for _, row := range rows {
				delivery, convertErr := deliveryFromRow(row)
				if convertErr != nil {
					return nil, convertErr
				}
				deliveries = append(deliveries, delivery)
			}
			return deliveries, nil
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("wait for %d delivery records: %w", count, ctx.Err())
		case <-ticker.C:
		}
	}
}

func deliveryFromRow(row map[string]any) (Delivery, error) {
	partition, partitionOK := row["partition_id"].(int32)
	if !partitionOK {
		if value, ok := row["partition_id"].(int64); ok {
			partition, partitionOK = int32(value), true
		}
	}
	offset, offsetOK := row["record_offset"].(int64)
	eventID, eventOK := row["event_id"].(string)
	topic, topicOK := row["topic"].(string)
	key, keyOK := row["record_key"].(string)
	if !partitionOK || !offsetOK || !eventOK || !topicOK || !keyOK {
		return Delivery{}, fmt.Errorf("delivery row has unexpected PostgreSQL types: %#v", row)
	}
	return Delivery{EventID: eventID, Topic: topic, Partition: partition, Offset: offset, Key: key}, nil
}

func CanonicalRows(rows []map[string]any) ([]byte, error) {
	canonical := append([]map[string]any(nil), rows...)
	sort.SliceStable(canonical, func(left, right int) bool {
		leftJSON, _ := json.Marshal(canonical[left])
		rightJSON, _ := json.Marshal(canonical[right])
		return string(leftJSON) < string(rightJSON)
	})
	return json.Marshal(canonical)
}

func (manager *Manager) hostDSN(user, password, database string) string {
	parsed, _ := url.Parse(manager.adminDSN)
	parsed.User = url.UserPassword(user, password)
	parsed.Path = "/" + database
	return parsed.String()
}

func (manager *Manager) internalDSN(user, password, database string) string {
	return (&url.URL{Scheme: "postgres", User: url.UserPassword(user, password), Host: manager.internalEndpoint, Path: database, RawQuery: "sslmode=disable"}).String()
}

func randomSecret() (string, error) {
	value := make([]byte, 24)
	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf("generate database credential: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(value), nil
}

func quoteLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}
