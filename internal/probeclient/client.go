package probeclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/uday1o1/chronicle-gate/pkg/probe"
)

const responseLimit = 256 << 10

// CapabilityError means a precise scenario cannot safely use the target probe.
type CapabilityError struct {
	Err error
}

func (err *CapabilityError) Error() string { return "probe capability error: " + err.Err.Error() }
func (err *CapabilityError) Unwrap() error { return err.Err }

// Client is a bounded authenticated client for one private probe process.
type Client struct {
	baseURL string
	token   string
	http    *http.Client
}

// New constructs a loopback or private-network probe client.
func New(baseURL, token string) (*Client, error) {
	baseURL = strings.TrimRight(baseURL, "/")
	if baseURL == "" || token == "" {
		return nil, errors.New("probe URL and token are required")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = (&net.Dialer{Timeout: 2 * time.Second, KeepAlive: 15 * time.Second}).DialContext
	transport.ResponseHeaderTimeout = 2 * time.Second
	return &Client{
		baseURL: baseURL,
		token:   token,
		http:    &http.Client{Transport: transport, Timeout: 3 * time.Second},
	}, nil
}

func (client *Client) Close() {
	client.http.CloseIdleConnections()
}

func (client *Client) Capabilities(ctx context.Context) (probe.Capabilities, error) {
	var capabilities probe.Capabilities
	if err := client.call(ctx, http.MethodGet, "/v1/capabilities", nil, &capabilities); err != nil {
		return probe.Capabilities{}, &CapabilityError{Err: err}
	}
	if capabilities.ProtocolVersion != probe.ProtocolVersion || capabilities.InstanceID == "" {
		return probe.Capabilities{}, &CapabilityError{Err: fmt.Errorf("unsupported protocol %q or missing instance ID", capabilities.ProtocolVersion)}
	}
	return capabilities, nil
}

type checkpointRequest struct {
	InstanceID string           `json:"instanceId"`
	Handle     string           `json:"handle,omitempty"`
	Checkpoint probe.Checkpoint `json:"checkpoint"`
}

func (client *Client) Arm(ctx context.Context, instanceID string, checkpoint probe.Checkpoint) (probe.CheckpointState, error) {
	var state probe.CheckpointState
	err := client.call(ctx, http.MethodPost, "/v1/checkpoints/arm", checkpointRequest{InstanceID: instanceID, Checkpoint: checkpoint}, &state)
	return state, err
}

func (client *Client) State(ctx context.Context, armed probe.CheckpointState) (probe.CheckpointState, error) {
	var state probe.CheckpointState
	err := client.call(ctx, http.MethodPost, "/v1/checkpoints/state", checkpointRequest{InstanceID: armed.InstanceID, Handle: armed.Handle, Checkpoint: armed.Checkpoint}, &state)
	return state, err
}

func (client *Client) WaitBlocked(ctx context.Context, armed probe.CheckpointState) (probe.CheckpointState, error) {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		state, err := client.State(ctx, armed)
		if err != nil {
			return probe.CheckpointState{}, err
		}
		if state.Status == "blocked" {
			return state, nil
		}
		if state.Status != "armed" {
			return probe.CheckpointState{}, fmt.Errorf("checkpoint reached terminal state %q before blocking", state.Status)
		}
		select {
		case <-ctx.Done():
			return probe.CheckpointState{}, fmt.Errorf("wait for exact checkpoint: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func (client *Client) Release(ctx context.Context, armed probe.CheckpointState) (probe.CheckpointState, error) {
	var state probe.CheckpointState
	err := client.call(ctx, http.MethodPost, "/v1/checkpoints/release", checkpointRequest{InstanceID: armed.InstanceID, Handle: armed.Handle, Checkpoint: armed.Checkpoint}, &state)
	return state, err
}

func (client *Client) Quiescence(ctx context.Context) (probe.QuiescenceState, error) {
	var state probe.QuiescenceState
	err := client.call(ctx, http.MethodGet, "/v1/quiescence", nil, &state)
	return state, err
}

func (client *Client) Deliveries(ctx context.Context) ([]probe.DeliveryReceipt, error) {
	var response struct {
		Deliveries []probe.DeliveryReceipt `json:"deliveries"`
	}
	if err := client.call(ctx, http.MethodGet, "/v1/deliveries", nil, &response); err != nil {
		return nil, err
	}
	return response.Deliveries, nil
}

func (client *Client) AdvanceClock(ctx context.Context, delta time.Duration) (time.Time, error) {
	var response struct {
		CurrentTime string `json:"currentTime"`
	}
	if err := client.call(ctx, http.MethodPost, "/v1/clock/advance", map[string]string{"by": delta.String()}, &response); err != nil {
		return time.Time{}, err
	}
	current, err := time.Parse(time.RFC3339Nano, response.CurrentTime)
	if err != nil {
		return time.Time{}, fmt.Errorf("decode probe logical time: %w", err)
	}
	return current, nil
}

func (client *Client) call(ctx context.Context, method, path string, input, output any) error {
	var body io.Reader
	if input != nil {
		document, err := json.Marshal(input)
		if err != nil {
			return fmt.Errorf("encode probe request: %w", err)
		}
		body = bytes.NewReader(document)
	}
	request, err := http.NewRequestWithContext(ctx, method, client.baseURL+path, body)
	if err != nil {
		return fmt.Errorf("create probe request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+client.token)
	request.Header.Set("Accept", "application/json")
	if input != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := client.http.Do(request)
	if err != nil {
		return fmt.Errorf("call probe %s: %w", path, err)
	}
	defer func() { _ = response.Body.Close() }()
	reader := io.LimitReader(response.Body, responseLimit+1)
	document, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("read probe response: %w", err)
	}
	if len(document) > responseLimit {
		return errors.New("probe response exceeds size limit")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		var failure struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(document, &failure)
		if failure.Error == "" {
			failure.Error = http.StatusText(response.StatusCode)
		}
		return fmt.Errorf("probe %s returned %d: %s", path, response.StatusCode, failure.Error)
	}
	if output != nil {
		if err := json.Unmarshal(document, output); err != nil {
			return fmt.Errorf("decode probe response: %w", err)
		}
	}
	return nil
}
