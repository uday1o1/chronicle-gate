// Package spec defines ChronicleGate's authored and artifact contracts.
package spec

import (
	"encoding/json"
	"fmt"
	"time"

	"go.yaml.in/yaml/v3"
)

const (
	APIVersion = "chronicle.dev/v1alpha1"
)

type Metadata struct {
	Name        string `json:"name" yaml:"name"`
	Description string `json:"description,omitempty" yaml:"description,omitempty"`
}

// Duration requires an explicit unit in every authored encoding.
type Duration struct {
	time.Duration
}

func ParseDuration(value string) (Duration, error) {
	if value == "" || value == "0" {
		return Duration{}, fmt.Errorf("duration %q must include a unit", value)
	}
	duration, err := time.ParseDuration(value)
	if err != nil {
		return Duration{}, fmt.Errorf("duration %q must include a valid unit: %w", value, err)
	}
	return Duration{Duration: duration}, nil
}

func (duration Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(duration.String())
}

func (duration *Duration) UnmarshalJSON(document []byte) error {
	var value string
	if err := json.Unmarshal(document, &value); err != nil {
		return fmt.Errorf("duration must be a string with units: %w", err)
	}
	parsed, err := ParseDuration(value)
	if err != nil {
		return err
	}
	*duration = parsed
	return nil
}

func (duration Duration) MarshalYAML() (any, error) {
	return duration.String(), nil
}

func (duration *Duration) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.ScalarNode || node.Tag != "!!str" {
		return fmt.Errorf("duration at line %d must be a string with units", node.Line)
	}
	parsed, err := ParseDuration(node.Value)
	if err != nil {
		return err
	}
	*duration = parsed
	return nil
}

type CloudEvent struct {
	SpecVersion      string         `json:"specversion" yaml:"specversion"`
	ID               string         `json:"id" yaml:"id"`
	Source           string         `json:"source" yaml:"source"`
	Type             string         `json:"type" yaml:"type"`
	Subject          string         `json:"subject" yaml:"subject"`
	Time             string         `json:"time" yaml:"time"`
	DataContentType  string         `json:"datacontenttype" yaml:"datacontenttype"`
	Data             map[string]any `json:"data" yaml:"data"`
	AggregateID      string         `json:"aggregateid,omitempty" yaml:"aggregateid,omitempty"`
	AggregateVersion int64          `json:"aggregateversion,omitempty" yaml:"aggregateversion,omitempty"`
	CorrelationID    string         `json:"correlationid,omitempty" yaml:"correlationid,omitempty"`
	CausationID      string         `json:"causationid,omitempty" yaml:"causationid,omitempty"`
	SchemaVersion    string         `json:"schemaversion,omitempty" yaml:"schemaversion,omitempty"`
	DataSchema       string         `json:"dataschema,omitempty" yaml:"dataschema,omitempty"`
}

type Comparison struct {
	AllowedTargetDifferences []AllowedTargetDifference `json:"allowedTargetDifferences,omitempty" yaml:"allowedTargetDifferences,omitempty"`
}

type AllowedTargetDifference struct {
	Pointer   string `json:"pointer" yaml:"pointer"`
	Rationale string `json:"rationale" yaml:"rationale"`
}
