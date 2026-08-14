// Package controlcontract defines the immutable runtime schedule contract used
// by ChronicleGate and its repository-trusted controlled reference service.
package controlcontract

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
)

const (
	APIVersion          = "chronicle.dev/controlled-runtime/v1alpha1"
	MaxDocumentBytes    = 64 << 10
	PerConsumerCapacity = 1
)

// Config is the exact stream and checkpoint framing supplied to one service.
type Config struct {
	APIVersion       string   `json:"apiVersion"`
	ProbeCapacity    int      `json:"probeCapacity"`
	ConsumerCapacity int      `json:"consumerCapacity"`
	Streams          []Stream `json:"streams"`
}

// Stream gives one independently assigned consumer its exact bindings.
type Stream struct {
	LogicalTopic string    `json:"logicalTopic"`
	Partition    int32     `json:"partition"`
	GroupSuffix  string    `json:"groupSuffix"`
	ClientSuffix string    `json:"clientSuffix"`
	Bindings     []Binding `json:"bindings"`
}

// Binding maps broker identity to probe execution framing without putting that
// execution metadata into the business event.
type Binding struct {
	EventID string `json:"eventId"`
	StepID  string `json:"stepId"`
}

// Normalize validates and deterministically orders the runtime contract.
func Normalize(config Config) (Config, error) {
	if config.APIVersion != APIVersion {
		return Config{}, fmt.Errorf("controlled runtime apiVersion %q is unsupported", config.APIVersion)
	}
	if config.ProbeCapacity < 1 || config.ConsumerCapacity != PerConsumerCapacity {
		return Config{}, errors.New("controlled runtime capacities are invalid")
	}
	if len(config.Streams) == 0 || len(config.Streams) > config.ProbeCapacity {
		return Config{}, errors.New("controlled runtime stream inventory exceeds probe capacity")
	}
	result := config
	result.Streams = append([]Stream(nil), config.Streams...)
	seenTopics := map[string]struct{}{}
	seenGroups := map[string]struct{}{}
	seenClients := map[string]struct{}{}
	for streamIndex := range result.Streams {
		stream := &result.Streams[streamIndex]
		if stream.LogicalTopic == "" || stream.Partition != 0 || stream.GroupSuffix == "" || stream.ClientSuffix == "" || len(stream.Bindings) == 0 {
			return Config{}, fmt.Errorf("controlled stream %d is incomplete", streamIndex)
		}
		if _, exists := seenTopics[stream.LogicalTopic]; exists {
			return Config{}, fmt.Errorf("controlled logical topic %q is duplicated", stream.LogicalTopic)
		}
		if _, exists := seenGroups[stream.GroupSuffix]; exists {
			return Config{}, fmt.Errorf("controlled group suffix %q is duplicated", stream.GroupSuffix)
		}
		if _, exists := seenClients[stream.ClientSuffix]; exists {
			return Config{}, fmt.Errorf("controlled client suffix %q is duplicated", stream.ClientSuffix)
		}
		seenTopics[stream.LogicalTopic] = struct{}{}
		seenGroups[stream.GroupSuffix] = struct{}{}
		seenClients[stream.ClientSuffix] = struct{}{}
		stream.Bindings = append([]Binding(nil), stream.Bindings...)
		sort.Slice(stream.Bindings, func(left, right int) bool {
			return stream.Bindings[left].EventID+"\x00"+stream.Bindings[left].StepID < stream.Bindings[right].EventID+"\x00"+stream.Bindings[right].StepID
		})
		seenBindings := map[string]struct{}{}
		for _, binding := range stream.Bindings {
			if binding.EventID == "" || binding.StepID == "" {
				return Config{}, errors.New("controlled event binding is incomplete")
			}
			if _, exists := seenBindings[binding.EventID]; exists {
				return Config{}, fmt.Errorf("event %q is duplicated within logical topic %q", binding.EventID, stream.LogicalTopic)
			}
			seenBindings[binding.EventID] = struct{}{}
		}
	}
	sort.Slice(result.Streams, func(left, right int) bool {
		return result.Streams[left].LogicalTopic < result.Streams[right].LogicalTopic
	})
	return result, nil
}

// Encode returns the deterministic document and its content digest.
func Encode(config Config) ([]byte, string, error) {
	normalized, err := Normalize(config)
	if err != nil {
		return nil, "", err
	}
	document, err := json.Marshal(normalized)
	if err != nil {
		return nil, "", fmt.Errorf("encode controlled runtime contract: %w", err)
	}
	digest := sha256.Sum256(document)
	return document, hex.EncodeToString(digest[:]), nil
}

// Decode strictly reads one bounded controlled runtime document.
func Decode(document []byte) (Config, error) {
	if len(document) > MaxDocumentBytes {
		return Config{}, errors.New("controlled runtime contract exceeds size limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.DisallowUnknownFields()
	var config Config
	if err := decoder.Decode(&config); err != nil {
		return Config{}, fmt.Errorf("decode controlled runtime contract: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return Config{}, errors.New("controlled runtime contract contains trailing JSON")
	}
	return Normalize(config)
}

// Load reads one private read-only runtime contract.
func Load(path string) (Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return Config{}, fmt.Errorf("open controlled runtime contract: %w", err)
	}
	defer func() { _ = file.Close() }()
	document, err := io.ReadAll(io.LimitReader(file, MaxDocumentBytes+1))
	if err != nil {
		return Config{}, fmt.Errorf("read controlled runtime contract: %w", err)
	}
	return Decode(document)
}

// BindingFor resolves one unambiguous topic and event pair.
func (stream Stream) BindingFor(eventID string) (Binding, bool) {
	for _, binding := range stream.Bindings {
		if binding.EventID == eventID {
			return binding, true
		}
	}
	return Binding{}, false
}
