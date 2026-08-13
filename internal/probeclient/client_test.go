package probeclient

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/uday1o1/chronicle-gate/pkg/probe"
)

func TestClientExercisesAuthenticatedClockAndRestartState(t *testing.T) {
	t.Parallel()
	const token = "0123456789abcdef0123456789abcdef"
	start := time.Date(2026, 8, 13, 0, 0, 0, 0, time.UTC)
	first := newTestProbe(t, token, start)
	server := httptest.NewServer(first.Handler())
	client, err := New(server.URL, token)
	if err != nil {
		t.Fatal(err)
	}
	capabilities, err := client.Capabilities(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	current, err := client.AdvanceClock(context.Background(), 3*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if !current.Equal(start.Add(3 * time.Hour)) {
		t.Fatalf("advanced time = %s", current)
	}
	client.Close()
	server.Close()

	replacement := newTestProbe(t, token, current)
	replacementServer := httptest.NewServer(replacement.Handler())
	defer replacementServer.Close()
	replacementClient, err := New(replacementServer.URL, token)
	if err != nil {
		t.Fatal(err)
	}
	defer replacementClient.Close()
	restarted, err := replacementClient.Capabilities(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if restarted.InstanceID == capabilities.InstanceID || restarted.CurrentTime != current.Format(time.RFC3339Nano) {
		t.Fatalf("restart state moved backward or reused identity: %#v", restarted)
	}
}

func TestWrongTokenAndUnavailableProbeAreCapabilityErrors(t *testing.T) {
	t.Parallel()
	const token = "0123456789abcdef0123456789abcdef"
	p := newTestProbe(t, token, time.Now().UTC())
	server := httptest.NewServer(p.Handler())
	wrong, err := New(server.URL, "wrong-token")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wrong.Capabilities(context.Background()); err == nil {
		t.Fatal("wrong probe token was accepted")
	}
	wrong.Close()
	server.Close()
	unavailable, err := New(server.URL, token)
	if err != nil {
		t.Fatal(err)
	}
	defer unavailable.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	if _, err := unavailable.Capabilities(ctx); err == nil {
		t.Fatal("unavailable probe returned a capability handshake")
	}
}

func newTestProbe(t *testing.T, token string, current time.Time) *probe.Probe {
	t.Helper()
	p := probe.New(
		probe.WithEnabled(true), probe.WithToken(token), probe.WithClockStart(current),
		probe.WithCapabilities(probe.Capabilities{
			Service: "order-workflow", CommitMode: "manual_sync", MaxControlledInFlight: 1,
			Checkpoints: []string{"before_offset_commit"}, LogicalClock: true,
		}),
	)
	if !p.Ready() {
		t.Fatal("test probe is not ready")
	}
	return p
}
