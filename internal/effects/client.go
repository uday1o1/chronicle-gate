package effects

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"
)

type Entry struct {
	Kind            string `json:"kind"`
	EventID         string `json:"eventId"`
	BusinessKey     string `json:"businessKey"`
	Amount          int64  `json:"amount"`
	IdempotencyKey  string `json:"idempotencyKey"`
	SourceTopic     string `json:"sourceTopic"`
	SourcePartition int32  `json:"sourcePartition"`
	SourceOffset    int64  `json:"sourceOffset"`
	Sequence        int64  `json:"sequence"`
}

type Observation struct {
	Entries []Entry `json:"entries"`
	Pending int     `json:"pending"`
	SHA256  string  `json:"sha256"`
}

// SemanticEntry is the workload-owned effect projection used for comparison.
// Physical broker identity remains in Observation as integrity evidence.
type SemanticEntry struct {
	Kind           string `json:"kind"`
	BusinessKey    string `json:"businessKey"`
	Amount         int64  `json:"amount"`
	IdempotencyKey string `json:"idempotencyKey"`
}

// Project returns the exact semantic effect fields defined by the workload.
func Project(observation Observation) []SemanticEntry {
	projected := make([]SemanticEntry, len(observation.Entries))
	for index, entry := range observation.Entries {
		projected[index] = SemanticEntry{
			Kind: entry.Kind, BusinessKey: entry.BusinessKey, Amount: entry.Amount, IdempotencyKey: entry.IdempotencyKey,
		}
	}
	return projected
}

type Client struct {
	url   string
	token string
	http  *http.Client
}

func New(url, token string) (*Client, error) {
	if strings.TrimSpace(url) == "" || token == "" {
		return nil, errors.New("effect observer URL and token are required")
	}
	return &Client{url: strings.TrimRight(url, "/"), token: token, http: &http.Client{Timeout: 3 * time.Second}}, nil
}

func (client *Client) Observe(ctx context.Context) (Observation, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, client.url+"/v1/effects", nil)
	if err != nil {
		return Observation{}, fmt.Errorf("create effect observation request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+client.token)
	response, err := client.http.Do(request)
	if err != nil {
		return Observation{}, fmt.Errorf("observe effect ledger: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	document, err := io.ReadAll(io.LimitReader(response.Body, 256<<10))
	if err != nil {
		return Observation{}, fmt.Errorf("read effect observation: %w", err)
	}
	if response.StatusCode != http.StatusOK {
		return Observation{}, fmt.Errorf("effect observer returned %d", response.StatusCode)
	}
	var observation Observation
	if err := json.Unmarshal(document, &observation); err != nil {
		return Observation{}, fmt.Errorf("decode effect observation: %w", err)
	}
	if observation.Pending < 0 || len(observation.SHA256) != sha256.Size*2 {
		return Observation{}, errors.New("effect observation is incomplete")
	}
	if _, err := hex.DecodeString(observation.SHA256); err != nil {
		return Observation{}, errors.New("effect observation digest is invalid")
	}
	sort.Slice(observation.Entries, func(left, right int) bool {
		return observation.Entries[left].Sequence < observation.Entries[right].Sequence
	})
	canonical, err := json.Marshal(observation.Entries)
	if err != nil {
		return Observation{}, fmt.Errorf("canonicalize effect observation: %w", err)
	}
	digest := sha256.Sum256(canonical)
	if hex.EncodeToString(digest[:]) != observation.SHA256 {
		return Observation{}, errors.New("effect observation digest mismatch")
	}
	return observation, nil
}
