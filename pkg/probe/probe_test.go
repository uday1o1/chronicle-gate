package probe

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

const testToken = "0123456789abcdef0123456789abcdef"

func TestExactCheckpointBlocksAndReleases(t *testing.T) {
	t.Parallel()
	p := testProbe(t, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	server := httptest.NewServer(p.Handler())
	t.Cleanup(server.Close)
	capabilities := getCapabilities(t, server.URL, testToken)
	cp := Checkpoint{Name: "after_external_effect", Service: "order-workflow", EventID: "event-1", StepID: "publish-payment", Occurrence: 1}
	state := checkpointCall(t, server.URL+"/v1/checkpoints/arm", testToken, http.StatusCreated, checkpointRequest{InstanceID: capabilities.InstanceID, Checkpoint: cp})

	done := make(chan error, 1)
	go func() { done <- p.Enter(context.Background(), cp) }()
	waitState(t, server.URL, testToken, state, "blocked")

	wrong := state
	wrong.Checkpoint.EventID = "other"
	checkpointCall(t, server.URL+"/v1/checkpoints/release", testToken, http.StatusNotFound, checkpointRequest{InstanceID: wrong.InstanceID, Handle: wrong.Handle, Checkpoint: wrong.Checkpoint})
	select {
	case err := <-done:
		t.Fatalf("wrong tuple released checkpoint: %v", err)
	default:
	}
	checkpointCall(t, server.URL+"/v1/checkpoints/release", testToken, http.StatusOK, checkpointRequest{InstanceID: state.InstanceID, Handle: state.Handle, Checkpoint: state.Checkpoint})
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Enter returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("checkpoint did not release")
	}
}

func TestCheckpointFailuresAreClosed(t *testing.T) {
	t.Parallel()
	p := testProbe(t, time.Now().UTC())
	server := httptest.NewServer(p.Handler())
	t.Cleanup(server.Close)
	capabilities := getCapabilities(t, server.URL, testToken)
	cp := Checkpoint{Name: "unknown", Service: "order-workflow", EventID: "event-1", StepID: "step", Occurrence: 1}
	checkpointCall(t, server.URL+"/v1/checkpoints/arm", testToken, http.StatusBadRequest, checkpointRequest{InstanceID: capabilities.InstanceID, Checkpoint: cp})
	cp.Name = "after_external_effect"
	checkpointCall(t, server.URL+"/v1/checkpoints/arm", "wrong", http.StatusUnauthorized, checkpointRequest{InstanceID: capabilities.InstanceID, Checkpoint: cp})
	cp.Occurrence = 2
	if err := p.Enter(context.Background(), cp); err == nil {
		t.Fatal("wrong process occurrence was accepted")
	}
}

func TestStaleHandleCannotReleaseReplacementProcess(t *testing.T) {
	t.Parallel()
	start := time.Now().UTC()
	first := testProbe(t, start)
	firstServer := httptest.NewServer(first.Handler())
	defer firstServer.Close()
	firstCapabilities := getCapabilities(t, firstServer.URL, testToken)
	cp := Checkpoint{Name: "after_external_effect", Service: "order-workflow", EventID: "event-1", StepID: "step", Occurrence: 1}
	state := checkpointCall(t, firstServer.URL+"/v1/checkpoints/arm", testToken, http.StatusCreated, checkpointRequest{InstanceID: firstCapabilities.InstanceID, Checkpoint: cp})

	replacement := testProbe(t, start)
	replacementServer := httptest.NewServer(replacement.Handler())
	defer replacementServer.Close()
	checkpointCall(t, replacementServer.URL+"/v1/checkpoints/release", testToken, http.StatusConflict, checkpointRequest{InstanceID: state.InstanceID, Handle: state.Handle, Checkpoint: state.Checkpoint})
}

func TestLogicalClockAdvancesAndRejectsBackwardMovement(t *testing.T) {
	t.Parallel()
	start := time.Date(2026, 4, 5, 6, 7, 8, 0, time.UTC)
	p := testProbe(t, start)
	server := httptest.NewServer(p.Handler())
	defer server.Close()
	response := request(t, http.MethodPost, server.URL+"/v1/clock/advance", testToken, map[string]string{"by": "90m"})
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("advance status = %d", response.StatusCode)
	}
	if got := p.Clock().Now(); !got.Equal(start.Add(90 * time.Minute)) {
		t.Fatalf("Now = %s, want %s", got, start.Add(90*time.Minute))
	}
	for _, invalid := range []string{"0s", "-1s", "8761h"} {
		response = request(t, http.MethodPost, server.URL+"/v1/clock/advance", testToken, map[string]string{"by": invalid})
		_ = response.Body.Close()
		if response.StatusCode != http.StatusBadRequest {
			t.Fatalf("advance %q status = %d", invalid, response.StatusCode)
		}
	}
}

func TestDeliveryAndQuiescenceEvidence(t *testing.T) {
	t.Parallel()
	p := testProbe(t, time.Now().UTC())
	digest := sha256.Sum256([]byte("event"))
	if err := p.RecordDelivery(DeliveryReceipt{Topic: "orders", Partition: 0, Offset: 4, Key: "order-1", EventID: "event-1", EventSHA256: hex.EncodeToString(digest[:])}); err != nil {
		t.Fatal(err)
	}
	end := p.BeginWork(WorkLabels{Service: "order-workflow", Kind: "kafka", EventID: "event-1"})
	server := httptest.NewServer(p.Handler())
	defer server.Close()
	response := request(t, http.MethodGet, server.URL+"/v1/quiescence", testToken, nil) //nolint:bodyclose // decodeResponse owns the response body.
	var state QuiescenceState
	decodeResponse(t, response, http.StatusOK, &state)
	if state.InFlight != 1 {
		t.Fatalf("inFlight = %d, want 1", state.InFlight)
	}
	end()
	end()
	response = request(t, http.MethodGet, server.URL+"/v1/deliveries", testToken, nil) //nolint:bodyclose // decodeResponse owns the response body.
	var deliveries struct {
		Deliveries []DeliveryReceipt `json:"deliveries"`
	}
	decodeResponse(t, response, http.StatusOK, &deliveries)
	if len(deliveries.Deliveries) != 1 || deliveries.Deliveries[0].Offset != 4 {
		t.Fatalf("deliveries = %#v", deliveries.Deliveries)
	}
}

func TestBlockedCheckpointHonorsCancellation(t *testing.T) {
	t.Parallel()
	p := testProbe(t, time.Now().UTC())
	server := httptest.NewServer(p.Handler())
	defer server.Close()
	capabilities := getCapabilities(t, server.URL, testToken)
	cp := Checkpoint{Name: "after_external_effect", Service: "order-workflow", EventID: "event-1", StepID: "step", Occurrence: 1}
	state := checkpointCall(t, server.URL+"/v1/checkpoints/arm", testToken, http.StatusCreated, checkpointRequest{InstanceID: capabilities.InstanceID, Checkpoint: cp})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Enter(ctx, cp) }()
	waitState(t, server.URL, testToken, state, "blocked")
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Enter error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled checkpoint remained blocked")
	}
}

func testProbe(t *testing.T, start time.Time) *Probe {
	t.Helper()
	p := New(
		WithEnabled(true),
		WithToken(testToken),
		WithClockStart(start),
		WithCapabilities(Capabilities{
			Service: "order-workflow", CommitMode: "manual_sync", MaxControlledInFlight: 1,
			Checkpoints: []string{"before_handler", "after_external_effect", "before_offset_commit", "after_offset_commit"}, LogicalClock: true,
		}),
	)
	if !p.Ready() {
		t.Fatal("probe is not ready")
	}
	return p
}

func getCapabilities(t *testing.T, baseURL, token string) Capabilities {
	t.Helper()
	response := request(t, http.MethodGet, baseURL+"/v1/capabilities", token, nil) //nolint:bodyclose // decodeResponse owns the response body.
	var capabilities Capabilities
	decodeResponse(t, response, http.StatusOK, &capabilities)
	return capabilities
}

func checkpointCall(t *testing.T, url, token string, status int, body checkpointRequest) CheckpointState {
	t.Helper()
	response := request(t, http.MethodPost, url, token, body) //nolint:bodyclose // decodeResponse owns the response body.
	if status >= 400 {
		decodeResponse(t, response, status, &map[string]string{})
		return CheckpointState{}
	}
	var state CheckpointState
	decodeResponse(t, response, status, &state)
	return state
}

func waitState(t *testing.T, baseURL, token string, state CheckpointState, wanted string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		observed := checkpointCall(t, baseURL+"/v1/checkpoints/state", token, http.StatusOK, checkpointRequest{InstanceID: state.InstanceID, Handle: state.Handle, Checkpoint: state.Checkpoint})
		if observed.Status == wanted {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("checkpoint did not reach %q", wanted)
}

func request(t *testing.T, method, url, token string, body any) *http.Response {
	t.Helper()
	var document []byte
	if body != nil {
		var err error
		document, err = json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
	}
	request, err := http.NewRequest(method, url, bytes.NewReader(document))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func decodeResponse(t *testing.T, response *http.Response, status int, target any) {
	t.Helper()
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != status {
		t.Fatalf("status = %d, want %d", response.StatusCode, status)
	}
	if err := json.NewDecoder(response.Body).Decode(target); err != nil {
		t.Fatal(err)
	}
}
