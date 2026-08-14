package observe

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/uday1o1/chronicle-gate/internal/broker"
	"github.com/uday1o1/chronicle-gate/internal/database"
	"github.com/uday1o1/chronicle-gate/internal/spec"
)

const maxHTTPObservationBytes = 1 << 20

var ErrRangeExpired = errors.New("kafka observation range expired before collection")

type PayloadValidationError struct {
	RecordIndex int
	Offset      int64
	SchemaFile  string
	Err         error
}

func (failure *PayloadValidationError) Error() string {
	return fmt.Sprintf("kafka payload %d at offset %d does not satisfy %s: %v", failure.RecordIndex, failure.Offset, failure.SchemaFile, failure.Err)
}

func (failure *PayloadValidationError) Unwrap() error { return failure.Err }

func CollectSQL(ctx context.Context, identity Identity, root, dsn string, declaration spec.Observation, rules []spec.Normalization) (Evidence, error) {
	if declaration.SQL == nil {
		return Evidence{}, errors.New("sql collector received a non-SQL declaration")
	}
	query, err := readBoundedFile(root+"/"+declaration.SQL.QueryFile, 2<<20)
	if err != nil {
		return Evidence{}, err
	}
	result, err := database.QueryDetailed(ctx, dsn, string(query))
	if err != nil {
		return Evidence{}, err
	}
	if err := verifySQLOrdering(result.Rows, declaration.SQL.OrderBy); err != nil {
		return Evidence{}, err
	}
	rows := make([]any, len(result.Rows))
	for index := range result.Rows {
		rows[index] = result.Rows[index]
	}
	normalized, applied, err := Normalize(map[string]any{"rows": rows}, rules)
	if err != nil {
		return Evidence{}, err
	}
	digest := sha256.Sum256(query)
	columns := make([]Column, len(result.Columns))
	for index, column := range result.Columns {
		columns[index] = Column{Name: column.Name, OID: column.OID}
	}
	evidence, err := NewEvidence(identity, "sql", declaration.SQL.Mode, Source{SQL: &SQLSource{
		QueryFile: declaration.SQL.QueryFile, QuerySHA256: hex.EncodeToString(digest[:]),
		OrderBy: append([]string(nil), declaration.SQL.OrderBy...), ColumnTypes: columns,
	}}, normalized, applied, nil)
	if err == nil {
		evidence.Count = len(result.Rows)
	}
	return evidence, err
}

type KafkaConfig struct {
	Brokers       []string
	Identity      Identity
	Root          string
	PhysicalTopic string
	Declaration   spec.KafkaObservation
	Rules         []spec.Normalization
	Bounds        broker.OffsetBounds
}

func CollectKafka(ctx context.Context, config KafkaConfig) (Evidence, error) {
	declaration := config.Declaration
	partition := int32(declaration.Partition)
	rangeEvidence := &KafkaRange{
		LogicalTopic: declaration.Topic, PhysicalTopic: config.PhysicalTopic, Partition: partition,
		AuthoredStart: declaration.StartOffset, AuthoredEnd: declaration.EndOffset,
		FrozenLogStart: config.Bounds.Start, FrozenLogEnd: config.Bounds.End, Records: []BrokerRecord{},
	}
	if declaration.StartOffset < config.Bounds.Start {
		rangeEvidence.RangeExpired = true
		return Evidence{APIVersion: EvidenceAPIVersion, Kind: "Observation", Identity: config.Identity, Type: "kafka", Mode: declaration.Mode, Source: Source{Kafka: rangeEvidence}}, ErrRangeExpired
	}
	rangeEvidence.EffectiveStart = max(declaration.StartOffset, config.Bounds.Start)
	rangeEvidence.EffectiveEnd = min(declaration.EndOffset, config.Bounds.End)
	rangeEvidence.RangeShortfall = config.Bounds.End < declaration.EndOffset
	if rangeEvidence.EffectiveEnd < rangeEvidence.EffectiveStart {
		rangeEvidence.EffectiveEnd = rangeEvidence.EffectiveStart
	}
	semantic := []any{}
	excluded := []MetadataExclusion{
		{Field: "topic", Reason: "attempt-prefixed broker identity"},
		{Field: "partition", Reason: "broker execution metadata"},
		{Field: "offset", Reason: "broker execution metadata"},
		{Field: "timestamp", Reason: "broker transport metadata"},
		{Field: "leaderEpoch", Reason: "broker execution metadata"},
	}
	if rangeEvidence.EffectiveStart < rangeEvidence.EffectiveEnd {
		client, err := kgo.NewClient(
			kgo.SeedBrokers(config.Brokers...),
			kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{config.PhysicalTopic: {partition: kgo.NewOffset().At(rangeEvidence.EffectiveStart)}}),
			kgo.FetchMaxWait(250*time.Millisecond),
		)
		if err != nil {
			return Evidence{}, fmt.Errorf("create direct Kafka observer: %w", err)
		}
		defer client.Close()
		lastOffset := int64(-1)
		for {
			fetches := client.PollFetches(ctx)
			if ctx.Err() != nil {
				return Evidence{}, ctx.Err()
			}
			if fetchErrors := fetches.Errors(); len(fetchErrors) != 0 {
				return Evidence{}, fmt.Errorf("fetch bounded Kafka observation: %v", fetchErrors)
			}
			done := false
			var projectionErr error
			fetches.EachPartition(func(fetched kgo.FetchTopicPartition) {
				if projectionErr != nil || fetched.Topic != config.PhysicalTopic || fetched.Partition != partition {
					return
				}
				for _, record := range fetched.Records {
					if record.Offset >= rangeEvidence.EffectiveEnd {
						rangeEvidence.BoundaryCrossed = true
						continue
					}
					if record.Offset < rangeEvidence.EffectiveStart || record.Offset <= lastOffset {
						projectionErr = fmt.Errorf("kafka observer received non-monotonic offset %d", record.Offset)
						return
					}
					projection, recordExcluded, err := projectRecord(config.Root, declaration, record)
					if err != nil {
						projectionErr = err
						return
					}
					keyOrder, err := topLevelJSONKeyOrder(record.Value)
					if err != nil {
						projectionErr = fmt.Errorf("inspect JSON wire order at offset %d: %w", record.Offset, err)
						return
					}
					rangeEvidence.Records = append(rangeEvidence.Records, BrokerRecord{
						Offset: record.Offset, Timestamp: record.Timestamp.UTC().Format(time.RFC3339Nano),
						LeaderEpoch: record.LeaderEpoch, TimestampExcluded: true,
						TopLevelJSONKeyOrder: keyOrder, TraceContextFingerprints: traceContextFingerprints(record.Headers),
					})
					semantic = append(semantic, projection)
					excluded = append(excluded, recordExcluded...)
					lastOffset = record.Offset
				}
				cursor := rangeEvidence.EffectiveStart
				if lastOffset >= cursor {
					cursor = lastOffset + 1
				}
				if cursor >= rangeEvidence.EffectiveEnd || fetched.HighWatermark >= rangeEvidence.EffectiveEnd && len(fetched.Records) == 0 {
					done = true
				}
			})
			if projectionErr != nil {
				var payloadFailure *PayloadValidationError
				if errors.As(projectionErr, &payloadFailure) {
					payloadFailure.RecordIndex = len(semantic)
					normalized, applied, normalizeErr := Normalize(semantic, config.Rules)
					if normalizeErr != nil {
						return Evidence{}, normalizeErr
					}
					evidence, evidenceErr := NewEvidence(config.Identity, "kafka", declaration.Mode, Source{Kafka: rangeEvidence}, normalized, applied, deduplicateExclusions(excluded))
					if evidenceErr != nil {
						return Evidence{}, evidenceErr
					}
					evidence.RawSchemaValid = false
					evidence.Count = len(semantic)
					return evidence, payloadFailure
				}
				return Evidence{}, projectionErr
			}
			if done {
				break
			}
		}
	}
	normalized, applied, err := Normalize(semantic, config.Rules)
	if err != nil {
		return Evidence{}, err
	}
	evidence, err := NewEvidence(config.Identity, "kafka", declaration.Mode, Source{Kafka: rangeEvidence}, normalized, applied, deduplicateExclusions(excluded))
	if err == nil {
		evidence.Count = len(semantic)
	}
	return evidence, err
}

func topLevelJSONKeyOrder(document []byte) ([]string, error) {
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.UseNumber()
	opening, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	if delimiter, ok := opening.(json.Delim); !ok || delimiter != '{' {
		return nil, errors.New("top-level JSON value is not an object")
	}
	keys := []string{}
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		key, ok := token.(string)
		if !ok {
			return nil, errors.New("JSON object key is not a string")
		}
		keys = append(keys, key)
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
	}
	closing, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	if delimiter, ok := closing.(json.Delim); !ok || delimiter != '}' {
		return nil, errors.New("top-level JSON object is not closed")
	}
	if token, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("unexpected trailing JSON token %v", token)
	}
	return keys, nil
}

func traceContextFingerprints(headers []kgo.RecordHeader) []TraceContextFingerprint {
	result := []TraceContextFingerprint{}
	for index, header := range headers {
		if !exactASCII(header.Key) || header.Key != "traceparent" && header.Key != "tracestate" && header.Key != "baggage" {
			continue
		}
		digest := sha256.Sum256(header.Value)
		result = append(result, TraceContextFingerprint{Name: header.Key, WireIndex: index, SHA256: hex.EncodeToString(digest[:])})
	}
	return result
}

func projectRecord(root string, declaration spec.KafkaObservation, record *kgo.Record) (map[string]any, []MetadataExclusion, error) {
	value, err := DecodeStrictJSON(record.Value)
	if err != nil {
		return nil, nil, fmt.Errorf("decode structured CloudEvent at offset %d: %w", record.Offset, err)
	}
	document, err := Canonical(value)
	if err != nil {
		return nil, nil, err
	}
	var event spec.CloudEvent
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	if err := decoder.Decode(&event); err != nil {
		return nil, nil, fmt.Errorf("decode CloudEvent contract at offset %d: %w", record.Offset, err)
	}
	if event.SpecVersion != "1.0" || event.ID == "" || event.Source == "" || event.Type == "" || event.Subject == "" || event.Time == "" || event.DataContentType != "application/json" || event.Data == nil {
		return nil, nil, fmt.Errorf("cloud event at offset %d lacks required attributes", record.Offset)
	}
	if event.AggregateID == "" || string(record.Key) != event.AggregateID {
		return nil, nil, fmt.Errorf("record key at offset %d does not equal CloudEvent aggregateid", record.Offset)
	}
	if err := spec.ValidatePayload(root, declaration.SchemaFile, event.Data); err != nil {
		return nil, nil, &PayloadValidationError{RecordIndex: -1, Offset: record.Offset, SchemaFile: declaration.SchemaFile, Err: err}
	}
	retained := make([]Header, 0, len(record.Headers))
	excluded := []MetadataExclusion{}
	nonsemantic := map[string]string{"traceparent": "W3C trace transport", "tracestate": "W3C trace transport", "baggage": "W3C trace transport"}
	for _, header := range declaration.NonsemanticHeaders {
		nonsemantic[header] = "authored nonsemantic header"
	}
	for index, header := range record.Headers {
		if reason, ok := nonsemantic[header.Key]; ok && exactASCII(header.Key) {
			excluded = append(excluded, MetadataExclusion{Field: "header", Header: EncodeBytes([]byte(header.Key)), Reason: reason})
			continue
		}
		retained = append(retained, Header{Name: EncodeBytes([]byte(header.Key)), Value: EncodeBytes(header.Value), WireIndex: index})
	}
	return map[string]any{"key": EncodeBytes(record.Key), "event": value, "headers": retained}, excluded, nil
}

func CollectHTTP(ctx context.Context, identity Identity, observerType, mode string, source HTTPSource, rules []spec.Normalization, token string) (Evidence, error) {
	client := &http.Client{
		Timeout:       5 * time.Second,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse },
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(source.Endpoint, "/")+source.Path, nil)
	if err != nil {
		return Evidence{}, fmt.Errorf("create HTTP observation request: %w", err)
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := client.Do(request)
	if err != nil {
		return Evidence{}, fmt.Errorf("collect HTTP observation: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return Evidence{}, fmt.Errorf("http observation returned status %d", response.StatusCode)
	}
	mediaType := strings.ToLower(strings.TrimSpace(strings.Split(response.Header.Get("Content-Type"), ";")[0]))
	if mediaType != "application/json" {
		return Evidence{}, fmt.Errorf("http observation content type %q is not application/json", response.Header.Get("Content-Type"))
	}
	document, err := io.ReadAll(io.LimitReader(response.Body, maxHTTPObservationBytes+1))
	if err != nil {
		return Evidence{}, fmt.Errorf("read HTTP observation: %w", err)
	}
	if len(document) > maxHTTPObservationBytes {
		return Evidence{}, fmt.Errorf("http observation exceeds %d bytes", maxHTTPObservationBytes)
	}
	value, err := DecodeStrictJSON(document)
	if err != nil {
		return Evidence{}, fmt.Errorf("decode HTTP observation: %w", err)
	}
	normalized, applied, err := Normalize(value, rules)
	if err != nil {
		return Evidence{}, err
	}
	container := Source{HTTP: &source}
	if observerType == "effects" {
		container = Source{Effects: &source}
	}
	return NewEvidence(identity, observerType, mode, container, normalized, applied, nil)
}

// DecodeStrictJSON rejects duplicate names, trailing values, and lossy number conversion.
func DecodeStrictJSON(document []byte) (any, error) {
	decoder := json.NewDecoder(bufio.NewReader(bytes.NewReader(document)))
	decoder.UseNumber()
	value, err := decodeValue(decoder)
	if err != nil {
		return nil, err
	}
	if token, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, fmt.Errorf("trailing JSON token %v", token)
		}
		return nil, err
	}
	return value, nil
}

func decodeValue(decoder *json.Decoder) (any, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	delimiter, compound := token.(json.Delim)
	if !compound {
		return token, nil
	}
	switch delimiter {
	case '{':
		object := map[string]any{}
		for decoder.More() {
			nameToken, err := decoder.Token()
			if err != nil {
				return nil, err
			}
			name, ok := nameToken.(string)
			if !ok {
				return nil, errors.New("json object name is not a string")
			}
			if _, duplicate := object[name]; duplicate {
				return nil, fmt.Errorf("duplicate JSON object name %q", name)
			}
			value, err := decodeValue(decoder)
			if err != nil {
				return nil, err
			}
			object[name] = value
		}
		if closing, err := decoder.Token(); err != nil || closing != json.Delim('}') {
			return nil, errors.New("json object did not close")
		}
		return object, nil
	case '[':
		array := []any{}
		for decoder.More() {
			value, err := decodeValue(decoder)
			if err != nil {
				return nil, err
			}
			array = append(array, value)
		}
		if closing, err := decoder.Token(); err != nil || closing != json.Delim(']') {
			return nil, errors.New("json array did not close")
		}
		return array, nil
	default:
		return nil, fmt.Errorf("unexpected JSON delimiter %q", delimiter)
	}
}

func verifySQLOrdering(rows []map[string]any, keys []string) error {
	previous := ""
	for index, row := range rows {
		values := make([]any, len(keys))
		for keyIndex, key := range keys {
			value, exists := row[key]
			if !exists {
				return fmt.Errorf("sql row %d has no declared order key %q", index, key)
			}
			values[keyIndex] = value
		}
		document, err := Canonical(values)
		if err != nil {
			return err
		}
		current := string(document)
		if index > 0 && current < previous {
			return errors.New("sql rows are not ordered by declared keys")
		}
		previous = current
	}
	return nil
}

func exactASCII(value string) bool {
	for index := 0; index < len(value); index++ {
		if value[index] > 0x7f {
			return false
		}
	}
	return true
}

func deduplicateExclusions(values []MetadataExclusion) []MetadataExclusion {
	sort.SliceStable(values, func(left, right int) bool {
		a, _ := Canonical(values[left])
		b, _ := Canonical(values[right])
		return string(a) < string(b)
	})
	result := values[:0]
	last := ""
	for _, value := range values {
		document, _ := Canonical(value)
		if string(document) != last {
			result = append(result, value)
			last = string(document)
		}
	}
	return result
}

func readBoundedFile(path string, limit int64) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open observer input: %w", err)
	}
	defer func() { _ = file.Close() }()
	document, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(document)) > limit {
		return nil, fmt.Errorf("observer input exceeds %d bytes", limit)
	}
	return document, nil
}
