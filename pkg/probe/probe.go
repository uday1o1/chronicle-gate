// Package probe provides deterministic, authenticated fault checkpoints for
// services that opt in to ChronicleGate qualification runs.
package probe

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"sync"
	"time"
)

const (
	ProtocolVersion = "chronicle-probe/v1alpha1"
	maxRequestBytes = 32 << 10
	maxAdvance      = 365 * 24 * time.Hour
	maxReceipts     = 64
)

// Clock is a deterministic time source owned by a Probe.
type Clock interface {
	Now() time.Time
}

// Checkpoint identifies one exact checkpoint occurrence in one process.
type Checkpoint struct {
	Name       string `json:"name"`
	Service    string `json:"service"`
	EventID    string `json:"eventId"`
	StepID     string `json:"stepId"`
	Occurrence int    `json:"occurrence"`
}

// WorkLabels describe one unit of service work for quiescence accounting.
type WorkLabels struct {
	Service string `json:"service"`
	Kind    string `json:"kind"`
	EventID string `json:"eventId"`
}

// DeliveryReceipt records the physical broker identity seen by a consumer.
type DeliveryReceipt struct {
	Topic       string `json:"topic"`
	Partition   int32  `json:"partition"`
	Offset      int64  `json:"offset"`
	Key         string `json:"key"`
	EventID     string `json:"eventId"`
	EventSHA256 string `json:"eventSha256"`
}

// Capabilities are proven by the live service before a precise fault runs.
type Capabilities struct {
	ProtocolVersion       string   `json:"protocolVersion"`
	Service               string   `json:"service"`
	CommitMode            string   `json:"commitMode"`
	MaxControlledInFlight int      `json:"maxControlledInFlight"`
	Checkpoints           []string `json:"checkpoints"`
	LogicalClock          bool     `json:"logicalClock"`
	InstanceID            string   `json:"instanceId"`
	CurrentTime           string   `json:"currentTime"`
}

// QuiescenceState is bounded service-local completion evidence.
type QuiescenceState struct {
	InFlight int `json:"inFlight"`
	Armed    int `json:"armed"`
	Blocked  int `json:"blocked"`
}

// CheckpointState describes an armed checkpoint gate.
type CheckpointState struct {
	InstanceID string     `json:"instanceId"`
	Handle     string     `json:"handle"`
	Checkpoint Checkpoint `json:"checkpoint"`
	Status     string     `json:"status"`
}

// Option configures a Probe.
type Option func(*Probe) error

// WithEnabled explicitly enables the private administration handler.
func WithEnabled(enabled bool) Option {
	return func(probe *Probe) error {
		probe.enabled = enabled
		return nil
	}
}

// WithToken sets the per-process bearer token.
func WithToken(token string) Option {
	return func(probe *Probe) error {
		if token == "" {
			return errors.New("probe bearer token is empty")
		}
		probe.tokenHash = sha256.Sum256([]byte(token))
		probe.hasToken = true
		return nil
	}
}

// WithClockStart sets the authoritative logical time supplied by the orchestrator.
func WithClockStart(start time.Time) Option {
	return func(probe *Probe) error {
		if start.IsZero() {
			return errors.New("logical clock start is zero")
		}
		probe.clock.current = start.UTC()
		probe.clock.configured = true
		return nil
	}
}

// WithCapabilities declares the live service capabilities exposed by the probe.
func WithCapabilities(capabilities Capabilities) Option {
	return func(probe *Probe) error {
		probe.capabilities = capabilities
		return nil
	}
}

type logicalClock struct {
	mu         sync.RWMutex
	current    time.Time
	configured bool
}

func (clock *logicalClock) Now() time.Time {
	clock.mu.RLock()
	defer clock.mu.RUnlock()
	return clock.current
}

func (clock *logicalClock) advance(delta time.Duration) (time.Time, error) {
	if delta <= 0 || delta > maxAdvance {
		return time.Time{}, fmt.Errorf("logical clock advance must be within (0,%s]", maxAdvance)
	}
	clock.mu.Lock()
	defer clock.mu.Unlock()
	if !clock.configured {
		return time.Time{}, errors.New("logical clock is not configured")
	}
	clock.current = clock.current.Add(delta)
	return clock.current, nil
}

type gate struct {
	checkpoint Checkpoint
	handle     string
	status     string
	release    chan struct{}
	once       sync.Once
}

// Probe is safe for concurrent service handlers and private API requests.
type Probe struct {
	mu           sync.Mutex
	enabled      bool
	ready        bool
	hasToken     bool
	tokenHash    [sha256.Size]byte
	instanceID   string
	capabilities Capabilities
	clock        *logicalClock
	gates        map[string]*gate
	occurrences  map[string]int
	inFlight     int
	receipts     []DeliveryReceipt
	initErr      error
}

// New constructs a disabled probe unless WithEnabled(true) is supplied.
func New(options ...Option) *Probe {
	probe := &Probe{
		clock:       &logicalClock{},
		gates:       map[string]*gate{},
		occurrences: map[string]int{},
	}
	probe.instanceID, probe.initErr = randomIdentifier(16)
	for _, option := range options {
		if probe.initErr != nil {
			break
		}
		probe.initErr = option(probe)
	}
	if probe.capabilities.ProtocolVersion == "" {
		probe.capabilities.ProtocolVersion = ProtocolVersion
	}
	probe.capabilities.InstanceID = probe.instanceID
	probe.capabilities.Checkpoints = append([]string(nil), probe.capabilities.Checkpoints...)
	slices.Sort(probe.capabilities.Checkpoints)
	probe.capabilities.Checkpoints = slices.Compact(probe.capabilities.Checkpoints)
	if probe.enabled {
		switch {
		case probe.initErr != nil:
		case !probe.hasToken:
			probe.initErr = errors.New("enabled probe requires a bearer token")
		case probe.capabilities.Service == "":
			probe.initErr = errors.New("enabled probe requires a service capability")
		case probe.capabilities.LogicalClock && !probe.clock.configured:
			probe.initErr = errors.New("logical-clock capability requires a start time")
		}
	}
	probe.ready = probe.enabled && probe.initErr == nil
	return probe
}

// Clock returns the probe-owned deterministic time source.
func (probe *Probe) Clock() Clock {
	return probe.clock
}

// Ready reports whether the enabled probe has a valid private configuration.
func (probe *Probe) Ready() bool {
	probe.mu.Lock()
	defer probe.mu.Unlock()
	return probe.ready
}

// BeginWork increments in-flight work and returns an idempotent completion function.
func (probe *Probe) BeginWork(_ WorkLabels) func() {
	probe.mu.Lock()
	probe.inFlight++
	probe.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			probe.mu.Lock()
			defer probe.mu.Unlock()
			if probe.inFlight > 0 {
				probe.inFlight--
			}
		})
	}
}

// RecordDelivery retains bounded physical broker evidence for the current process.
func (probe *Probe) RecordDelivery(receipt DeliveryReceipt) error {
	if receipt.Topic == "" || receipt.Offset < 0 || receipt.EventID == "" || len(receipt.EventSHA256) != sha256.Size*2 {
		return errors.New("delivery receipt is incomplete")
	}
	if _, err := hex.DecodeString(receipt.EventSHA256); err != nil {
		return errors.New("delivery receipt event digest is invalid")
	}
	probe.mu.Lock()
	defer probe.mu.Unlock()
	probe.receipts = append(probe.receipts, receipt)
	if len(probe.receipts) > maxReceipts {
		probe.receipts = append([]DeliveryReceipt(nil), probe.receipts[len(probe.receipts)-maxReceipts:]...)
	}
	return nil
}

// Enter blocks only when an exact armed checkpoint occurrence is reached.
func (probe *Probe) Enter(ctx context.Context, checkpoint Checkpoint) error {
	if checkpoint.Name == "" || checkpoint.Service == "" || checkpoint.EventID == "" || checkpoint.StepID == "" {
		return errors.New("checkpoint tuple is incomplete")
	}
	base := checkpointBase(checkpoint)
	probe.mu.Lock()
	actual := probe.occurrences[base] + 1
	probe.occurrences[base] = actual
	if checkpoint.Occurrence == 0 {
		checkpoint.Occurrence = actual
	}
	if checkpoint.Occurrence != actual {
		probe.mu.Unlock()
		return fmt.Errorf("checkpoint occurrence %d does not match process occurrence %d", checkpoint.Occurrence, actual)
	}
	selected := probe.findGateLocked(checkpoint)
	if selected == nil {
		probe.mu.Unlock()
		return nil
	}
	selected.status = "blocked"
	probe.mu.Unlock()

	select {
	case <-ctx.Done():
		probe.mu.Lock()
		if selected.status == "blocked" {
			selected.status = "cancelled"
		}
		probe.mu.Unlock()
		return ctx.Err()
	case <-selected.release:
		return nil
	}
}

func (probe *Probe) findGateLocked(checkpoint Checkpoint) *gate {
	for _, candidate := range probe.gates {
		if candidate.status == "armed" && candidate.checkpoint == checkpoint {
			return candidate
		}
	}
	return nil
}

// Handler returns the bounded private administration API.
func (probe *Probe) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/capabilities", probe.auth(probe.getCapabilities))
	mux.HandleFunc("POST /v1/checkpoints/arm", probe.auth(probe.armCheckpoint))
	mux.HandleFunc("POST /v1/checkpoints/state", probe.auth(probe.checkpointState))
	mux.HandleFunc("POST /v1/checkpoints/release", probe.auth(probe.releaseCheckpoint))
	mux.HandleFunc("GET /v1/quiescence", probe.auth(probe.getQuiescence))
	mux.HandleFunc("GET /v1/deliveries", probe.auth(probe.getDeliveries))
	mux.HandleFunc("POST /v1/clock/advance", probe.auth(probe.advanceClock))
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if !probe.enabled {
			http.NotFound(writer, request)
			return
		}
		if probe.initErr != nil {
			writeError(writer, http.StatusServiceUnavailable, probe.initErr.Error())
			return
		}
		request.Body = http.MaxBytesReader(writer, request.Body, maxRequestBytes)
		mux.ServeHTTP(writer, request)
	})
}

func (probe *Probe) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		provided := request.Header.Get("Authorization")
		const prefix = "Bearer "
		if len(provided) <= len(prefix) || provided[:len(prefix)] != prefix {
			writeError(writer, http.StatusUnauthorized, "authentication failed")
			return
		}
		providedHash := sha256.Sum256([]byte(provided[len(prefix):]))
		if subtle.ConstantTimeCompare(providedHash[:], probe.tokenHash[:]) != 1 {
			writeError(writer, http.StatusUnauthorized, "authentication failed")
			return
		}
		next(writer, request)
	}
}

func (probe *Probe) getCapabilities(writer http.ResponseWriter, _ *http.Request) {
	probe.mu.Lock()
	capabilities := probe.capabilities
	capabilities.InstanceID = probe.instanceID
	capabilities.CurrentTime = probe.clock.Now().Format(time.RFC3339Nano)
	probe.mu.Unlock()
	writeJSON(writer, http.StatusOK, capabilities)
}

type checkpointRequest struct {
	InstanceID string     `json:"instanceId"`
	Handle     string     `json:"handle,omitempty"`
	Checkpoint Checkpoint `json:"checkpoint"`
}

func (probe *Probe) armCheckpoint(writer http.ResponseWriter, request *http.Request) {
	var body checkpointRequest
	if err := decodeJSON(request, &body); err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	if body.InstanceID != probe.instanceID {
		writeError(writer, http.StatusConflict, "probe instance mismatch")
		return
	}
	if err := probe.validateCheckpoint(body.Checkpoint); err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	handle, err := randomIdentifier(24)
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "create checkpoint handle")
		return
	}
	probe.mu.Lock()
	for _, existing := range probe.gates {
		if existing.checkpoint == body.Checkpoint && (existing.status == "armed" || existing.status == "blocked") {
			probe.mu.Unlock()
			writeError(writer, http.StatusConflict, "checkpoint is already active")
			return
		}
	}
	created := &gate{checkpoint: body.Checkpoint, handle: handle, status: "armed", release: make(chan struct{})}
	probe.gates[handle] = created
	probe.mu.Unlock()
	writeJSON(writer, http.StatusCreated, CheckpointState{InstanceID: probe.instanceID, Handle: handle, Checkpoint: body.Checkpoint, Status: "armed"})
}

func (probe *Probe) checkpointState(writer http.ResponseWriter, request *http.Request) {
	body, ok := probe.requestedGate(writer, request)
	if !ok {
		return
	}
	probe.mu.Lock()
	selected, exists := probe.gates[body.Handle]
	if !exists || selected.checkpoint != body.Checkpoint {
		probe.mu.Unlock()
		writeError(writer, http.StatusNotFound, "checkpoint handle and tuple do not match")
		return
	}
	state := CheckpointState{InstanceID: probe.instanceID, Handle: selected.handle, Checkpoint: selected.checkpoint, Status: selected.status}
	probe.mu.Unlock()
	writeJSON(writer, http.StatusOK, state)
}

func (probe *Probe) releaseCheckpoint(writer http.ResponseWriter, request *http.Request) {
	body, ok := probe.requestedGate(writer, request)
	if !ok {
		return
	}
	probe.mu.Lock()
	selected, exists := probe.gates[body.Handle]
	if !exists || selected.checkpoint != body.Checkpoint {
		probe.mu.Unlock()
		writeError(writer, http.StatusNotFound, "checkpoint handle and tuple do not match")
		return
	}
	if selected.status != "blocked" {
		probe.mu.Unlock()
		writeError(writer, http.StatusConflict, "checkpoint is not blocked")
		return
	}
	selected.status = "released"
	selected.once.Do(func() { close(selected.release) })
	state := CheckpointState{InstanceID: probe.instanceID, Handle: selected.handle, Checkpoint: selected.checkpoint, Status: selected.status}
	probe.mu.Unlock()
	writeJSON(writer, http.StatusOK, state)
}

func (probe *Probe) requestedGate(writer http.ResponseWriter, request *http.Request) (checkpointRequest, bool) {
	var body checkpointRequest
	if err := decodeJSON(request, &body); err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return checkpointRequest{}, false
	}
	if body.InstanceID != probe.instanceID {
		writeError(writer, http.StatusConflict, "probe instance mismatch")
		return checkpointRequest{}, false
	}
	if body.Handle == "" {
		writeError(writer, http.StatusBadRequest, "checkpoint handle is required")
		return checkpointRequest{}, false
	}
	return body, true
}

func (probe *Probe) validateCheckpoint(checkpoint Checkpoint) error {
	if checkpoint.Service != probe.capabilities.Service || checkpoint.Name == "" || checkpoint.EventID == "" || checkpoint.StepID == "" || checkpoint.Occurrence < 1 {
		return errors.New("checkpoint tuple is invalid for this service")
	}
	if !slices.Contains(probe.capabilities.Checkpoints, checkpoint.Name) {
		return fmt.Errorf("checkpoint %q is not a declared capability", checkpoint.Name)
	}
	return nil
}

func (probe *Probe) getQuiescence(writer http.ResponseWriter, _ *http.Request) {
	probe.mu.Lock()
	defer probe.mu.Unlock()
	state := QuiescenceState{InFlight: probe.inFlight}
	for _, gate := range probe.gates {
		switch gate.status {
		case "armed":
			state.Armed++
		case "blocked":
			state.Blocked++
		}
	}
	writeJSON(writer, http.StatusOK, state)
}

func (probe *Probe) getDeliveries(writer http.ResponseWriter, _ *http.Request) {
	probe.mu.Lock()
	receipts := append([]DeliveryReceipt(nil), probe.receipts...)
	probe.mu.Unlock()
	writeJSON(writer, http.StatusOK, map[string]any{"instanceId": probe.instanceID, "deliveries": receipts})
}

type advanceRequest struct {
	By string `json:"by"`
}

func (probe *Probe) advanceClock(writer http.ResponseWriter, request *http.Request) {
	if !probe.capabilities.LogicalClock {
		writeError(writer, http.StatusConflict, "logical clock is not a declared capability")
		return
	}
	var body advanceRequest
	if err := decodeJSON(request, &body); err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	delta, err := time.ParseDuration(body.By)
	if err != nil {
		writeError(writer, http.StatusBadRequest, "clock advance requires an explicit duration")
		return
	}
	current, err := probe.clock.advance(delta)
	if err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, map[string]string{"currentTime": current.Format(time.RFC3339Nano)})
}

func checkpointBase(checkpoint Checkpoint) string {
	return checkpoint.Service + "\x00" + checkpoint.Name + "\x00" + checkpoint.EventID + "\x00" + checkpoint.StepID
}

func decodeJSON(request *http.Request, target any) error {
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode request: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("request contains multiple JSON values")
		}
		return fmt.Errorf("decode request trailer: %w", err)
	}
	return nil
}

func randomIdentifier(bytes int) (string, error) {
	value := make([]byte, bytes)
	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf("generate random identifier: %w", err)
	}
	return hex.EncodeToString(value), nil
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("Cache-Control", "no-store")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}

func writeError(writer http.ResponseWriter, status int, message string) {
	writeJSON(writer, status, map[string]string{"error": message})
}
