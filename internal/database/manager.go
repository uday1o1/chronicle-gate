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
	templateHash     string
}

// Attempt is one database cloned from the frozen template.
type Attempt struct {
	Name        string `json:"name"`
	ServiceDSN  string `json:"-"`
	ObserverDSN string `json:"-"`
}

// Delivery identifies one processed physical Kafka record.
type Delivery struct {
	EventID   string `json:"eventId"`
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Key       string `json:"key"`
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
	return &Manager{
		adminDSN:         adminDSN,
		internalEndpoint: internalEndpoint,
		adminUser:        adminUser,
		adminPassword:    adminPassword,
		servicePassword:  servicePassword,
		observerPassword: observerPassword,
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
		"RESET ROLE",
		"REVOKE ALL ON SCHEMA public FROM PUBLIC",
		"GRANT CONNECT ON DATABASE " + templateDatabase + " TO " + serviceRole + ", " + observerRole,
		"GRANT USAGE ON SCHEMA public TO " + serviceRole + ", " + observerRole,
		"GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA public TO " + serviceRole,
		"GRANT UPDATE (commit_confirmed) ON delivery_ledger TO " + serviceRole,
		"GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO " + serviceRole,
		"GRANT SELECT ON ALL TABLES IN SCHEMA public TO " + observerRole,
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
	if _, err := admin.Exec(ctx, "GRANT CONNECT ON DATABASE "+name+" TO "+serviceRole+", "+observerRole); err != nil {
		return Attempt{}, fmt.Errorf("grant attempt database access: %w", err)
	}
	return Attempt{
		Name:        name,
		ServiceDSN:  manager.internalDSN(serviceRole, manager.servicePassword, name),
		ObserverDSN: manager.hostDSN(observerRole, manager.observerPassword, name),
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
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("create read-only observer pool: %w", err)
	}
	defer pool.Close()
	transaction, err := pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, fmt.Errorf("begin read-only observation: %w", err)
	}
	defer func() { _ = transaction.Rollback(context.Background()) }()
	rows, err := transaction.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("execute read-only observation: %w", err)
	}
	defer rows.Close()
	fields := rows.FieldDescriptions()
	result := make([]map[string]any, 0)
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, fmt.Errorf("read observation row: %w", err)
		}
		row := make(map[string]any, len(fields))
		for index, field := range fields {
			row[string(field.Name)] = values[index]
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate observation: %w", err)
	}
	if err := transaction.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit read-only observation: %w", err)
	}
	return result, nil
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
