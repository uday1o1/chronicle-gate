// Package observe implements ChronicleGate's built-in observer evidence model.
package observe

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

const EvidenceAPIVersion = "chronicle.dev/observation/v1alpha1"

// Identity is the stable logical join key shared by baseline and candidate.
type Identity struct {
	StepID     string `json:"stepId"`
	ObserverID string `json:"observerId"`
	Occurrence int    `json:"occurrence"`
}

func (identity Identity) String() string {
	return fmt.Sprintf("%s/%s/%d", identity.StepID, identity.ObserverID, identity.Occurrence)
}

// ByteString is a reversible JSON representation of arbitrary protocol bytes.
type ByteString struct {
	Encoding string `json:"encoding"`
	Data     string `json:"data"`
}

func EncodeBytes(value []byte) ByteString {
	return ByteString{Encoding: "base64", Data: base64.RawStdEncoding.EncodeToString(value)}
}

func (value ByteString) Decode() ([]byte, error) {
	if value.Encoding != "base64" {
		return nil, fmt.Errorf("unsupported byte encoding %q", value.Encoding)
	}
	decoded, err := base64.RawStdEncoding.DecodeString(value.Data)
	if err != nil {
		return nil, fmt.Errorf("decode base64 bytes: %w", err)
	}
	return decoded, nil
}

type Header struct {
	Name      ByteString `json:"name"`
	Value     ByteString `json:"value"`
	WireIndex int        `json:"wireIndex"`
}

type MetadataExclusion struct {
	Field  string     `json:"field"`
	Reason string     `json:"reason"`
	Header ByteString `json:"header,omitempty"`
}

type AppliedNormalization struct {
	RuleID           string   `json:"ruleId"`
	Type             string   `json:"type"`
	AuthoredPointer  string   `json:"authoredPointer"`
	AffectedPointers []string `json:"affectedPointers"`
	AffectedCount    int      `json:"affectedCount"`
	BeforeSHA256     string   `json:"beforeSha256"`
	AfterSHA256      string   `json:"afterSha256"`
}

type SQLSource struct {
	QueryFile   string   `json:"queryFile"`
	QuerySHA256 string   `json:"querySha256"`
	OrderBy     []string `json:"orderBy"`
	ColumnTypes []Column `json:"columnTypes"`
}

type Column struct {
	Name string `json:"name"`
	OID  uint32 `json:"oid"`
}

type KafkaRange struct {
	LogicalTopic    string         `json:"logicalTopic"`
	PhysicalTopic   string         `json:"physicalTopic"`
	Partition       int32          `json:"partition"`
	AuthoredStart   int64          `json:"authoredStart"`
	AuthoredEnd     int64          `json:"authoredEnd"`
	FrozenLogStart  int64          `json:"frozenLogStart"`
	FrozenLogEnd    int64          `json:"frozenLogEnd"`
	EffectiveStart  int64          `json:"effectiveStart"`
	EffectiveEnd    int64          `json:"effectiveEnd"`
	RangeShortfall  bool           `json:"rangeShortfall"`
	BoundaryCrossed bool           `json:"boundaryCrossed"`
	RangeExpired    bool           `json:"rangeExpired"`
	Records         []BrokerRecord `json:"records"`
}

type BrokerRecord struct {
	Offset            int64  `json:"offset"`
	Timestamp         string `json:"timestamp"`
	LeaderEpoch       int32  `json:"leaderEpoch"`
	TimestampExcluded bool   `json:"timestampExcluded"`
}

type HTTPSource struct {
	Service  string `json:"service"`
	Port     int    `json:"port"`
	Path     string `json:"path"`
	Endpoint string `json:"endpoint"`
}

type Source struct {
	SQL     *SQLSource  `json:"sql,omitempty"`
	Kafka   *KafkaRange `json:"kafka,omitempty"`
	HTTP    *HTTPSource `json:"http,omitempty"`
	Effects *HTTPSource `json:"effects,omitempty"`
}

// Evidence is the canonical, versioned result of one declared observe step.
type Evidence struct {
	APIVersion       string                 `json:"apiVersion"`
	Kind             string                 `json:"kind"`
	Identity         Identity               `json:"identity"`
	Type             string                 `json:"type"`
	Mode             string                 `json:"mode"`
	Source           Source                 `json:"source"`
	RawSchemaValid   bool                   `json:"rawSchemaValid"`
	Count            int                    `json:"count"`
	Value            any                    `json:"value"`
	Canonical        json.RawMessage        `json:"canonical"`
	SHA256           string                 `json:"sha256"`
	Applied          []AppliedNormalization `json:"appliedNormalization"`
	ImplicitExcluded []MetadataExclusion    `json:"implicitExclusions"`
}

func NewEvidence(identity Identity, observerType, mode string, source Source, value any, applied []AppliedNormalization, excluded []MetadataExclusion) (Evidence, error) {
	canonical, err := Canonical(value)
	if err != nil {
		return Evidence{}, err
	}
	digest := sha256.Sum256(canonical)
	count := 1
	if values, ok := value.([]any); ok {
		count = len(values)
	}
	return Evidence{
		APIVersion: EvidenceAPIVersion, Kind: "Observation", Identity: identity,
		Type: observerType, Mode: mode, Source: source, RawSchemaValid: true,
		Count: count, Value: value, Canonical: canonical, SHA256: hex.EncodeToString(digest[:]),
		Applied: applied, ImplicitExcluded: excluded,
	}, nil
}

// Canonical returns deterministic JSON while preserving json.Number values.
func Canonical(value any) ([]byte, error) {
	document, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("encode canonical JSON: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.UseNumber()
	var roundTrip any
	if err := decoder.Decode(&roundTrip); err != nil {
		return nil, fmt.Errorf("decode canonical JSON: %w", err)
	}
	canonical, err := json.Marshal(roundTrip)
	if err != nil {
		return nil, fmt.Errorf("re-encode canonical JSON: %w", err)
	}
	return canonical, nil
}
