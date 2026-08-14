package main

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

type effect struct {
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

type ledger struct {
	mu      sync.Mutex
	entries []effect
	pending int
}

type server struct {
	ledger        *ledger
	writerToken   [sha256.Size]byte
	observerToken [sha256.Size]byte
}

func main() {
	writerToken, err := readToken("CHRONICLE_SINK_WRITER_TOKEN_FILE")
	if err != nil {
		log.Fatal(err)
	}
	observerToken, err := readToken("CHRONICLE_SINK_OBSERVER_TOKEN_FILE")
	if err != nil {
		log.Fatal(err)
	}
	application := &server{ledger: &ledger{}, writerToken: sha256.Sum256(writerToken), observerToken: sha256.Sum256(observerToken)}
	httpServer := &http.Server{
		Addr:              ":8080",
		Handler:           application.handler(),
		ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout:       3 * time.Second,
		WriteTimeout:      3 * time.Second,
		IdleTimeout:       15 * time.Second,
		MaxHeaderBytes:    8 << 10,
	}
	ctx, stop := signal.NotifyContext(contextBackground(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("effect sink server: %v", err)
			stop()
		}
	}()
	<-ctx.Done()
	shutdown, cancel := contextWithTimeout(2 * time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdown)
}

// These small indirections keep signal lifecycle code out of handler tests.
var contextBackground = func() context.Context { return context.Background() }
var contextWithTimeout = func(timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), timeout)
}

func (server *server) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(writer http.ResponseWriter, _ *http.Request) {
		writeJSON(writer, http.StatusOK, map[string]string{"status": "ready"})
	})
	mux.HandleFunc("POST /v1/effects", server.authorize(server.writerToken, server.appendEffect))
	mux.HandleFunc("GET /v1/effects", server.authorize(server.observerToken, server.observeEffects))
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		request.Body = http.MaxBytesReader(writer, request.Body, 32<<10)
		mux.ServeHTTP(writer, request)
	})
}

func (server *server) authorize(expected [sha256.Size]byte, next http.HandlerFunc) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		const prefix = "Bearer "
		value := request.Header.Get("Authorization")
		if !strings.HasPrefix(value, prefix) {
			writeError(writer, http.StatusUnauthorized, "authentication failed")
			return
		}
		actual := sha256.Sum256([]byte(value[len(prefix):]))
		if subtle.ConstantTimeCompare(expected[:], actual[:]) != 1 {
			writeError(writer, http.StatusUnauthorized, "authentication failed")
			return
		}
		next(writer, request)
	}
}

func (server *server) appendEffect(writer http.ResponseWriter, request *http.Request) {
	server.ledger.mu.Lock()
	server.ledger.pending++
	server.ledger.mu.Unlock()
	defer func() {
		server.ledger.mu.Lock()
		server.ledger.pending--
		server.ledger.mu.Unlock()
	}()
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	var received effect
	if err := decoder.Decode(&received); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid effect")
		return
	}
	if received.Kind == "" || received.EventID == "" || received.BusinessKey == "" || received.Amount <= 0 || received.IdempotencyKey == "" || received.SourceTopic == "" || received.SourceOffset < 0 {
		writeError(writer, http.StatusBadRequest, "effect is incomplete")
		return
	}
	server.ledger.mu.Lock()
	defer server.ledger.mu.Unlock()
	for _, existing := range server.ledger.entries {
		if existing.IdempotencyKey == received.IdempotencyKey {
			writeJSON(writer, http.StatusOK, existing)
			return
		}
	}
	received.Sequence = int64(len(server.ledger.entries) + 1)
	server.ledger.entries = append(server.ledger.entries, received)
	writeJSON(writer, http.StatusCreated, received)
}

func (server *server) observeEffects(writer http.ResponseWriter, _ *http.Request) {
	server.ledger.mu.Lock()
	entries := append([]effect(nil), server.ledger.entries...)
	pending := server.ledger.pending
	server.ledger.mu.Unlock()
	sort.Slice(entries, func(left, right int) bool { return entries[left].Sequence < entries[right].Sequence })
	document, err := json.Marshal(entries)
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "encode ledger")
		return
	}
	digest := sha256.Sum256(document)
	writeJSON(writer, http.StatusOK, map[string]any{"entries": entries, "pending": pending, "sha256": hex.EncodeToString(digest[:])})
}

func readToken(environment string) ([]byte, error) {
	path := strings.TrimSpace(os.Getenv(environment))
	if path == "" {
		return nil, fmt.Errorf("%s is required", environment)
	}
	value, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", environment, err)
	}
	value = []byte(strings.TrimSpace(string(value)))
	if len(value) < 32 {
		return nil, fmt.Errorf("%s must contain at least 32 bytes", environment)
	}
	return value, nil
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
