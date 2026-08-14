package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const maxRequestBytes = 16 << 10

type orderRequest struct {
	RequestID string `json:"requestId"`
	OrderID   string `json:"orderId"`
	Amount    int64  `json:"amount"`
}

type orderResponse struct {
	RequestID string `json:"requestId"`
	OrderID   string `json:"orderId"`
	EventID   string `json:"eventId"`
	Status    string `json:"status"`
}

type cloudEvent struct {
	SpecVersion      string         `json:"specversion"`
	ID               string         `json:"id"`
	Source           string         `json:"source"`
	Type             string         `json:"type"`
	Subject          string         `json:"subject"`
	Time             string         `json:"time"`
	DataContentType  string         `json:"datacontenttype"`
	AggregateID      string         `json:"aggregateid"`
	AggregateVersion int64          `json:"aggregateversion"`
	Data             map[string]any `json:"data"`
}

type application struct {
	database *pgxpool.Pool
	now      func() time.Time
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	dsn, err := readSecretFile("CHRONICLE_DATABASE_DSN_FILE")
	if err != nil {
		log.Fatal(err)
	}
	database, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("create order API database pool: %v", err)
	}
	defer database.Close()
	if err := database.Ping(ctx); err != nil {
		log.Fatalf("ping order API database: %v", err)
	}
	app := &application{database: database, now: func() time.Time { return time.Now().UTC() }}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(writer http.ResponseWriter, _ *http.Request) {
		writeJSON(writer, http.StatusOK, map[string]string{"status": "ready"})
	})
	mux.HandleFunc("POST /orders", app.createOrder)
	server := &http.Server{
		Addr: ":8080", Handler: http.MaxBytesHandler(mux, maxRequestBytes),
		ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 3 * time.Second,
		WriteTimeout: 3 * time.Second, IdleTimeout: 15 * time.Second,
	}
	go func() {
		if serveErr := server.ListenAndServe(); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.Printf("order API server: %v", serveErr)
			stop()
		}
	}()
	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdown)
}

func (app *application) createOrder(writer http.ResponseWriter, request *http.Request) {
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	var received orderRequest
	if err := decoder.Decode(&received); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid order request")
		return
	}
	if err := requireEOF(decoder); err != nil || received.RequestID == "" || received.OrderID == "" || received.Amount <= 0 {
		writeError(writer, http.StatusBadRequest, "incomplete order request")
		return
	}
	response, created, err := app.insertOrder(request.Context(), received)
	if err != nil {
		writeError(writer, http.StatusConflict, err.Error())
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(writer, status, response)
}

func (app *application) insertOrder(ctx context.Context, request orderRequest) (orderResponse, bool, error) {
	eventID := "order-created-" + request.RequestID
	response := orderResponse{RequestID: request.RequestID, OrderID: request.OrderID, EventID: eventID, Status: "pending"}
	created := false
	err := pgx.BeginFunc(ctx, app.database, func(transaction pgx.Tx) error {
		var existingOrder string
		var existingAmount int64
		err := transaction.QueryRow(ctx, "SELECT order_id, amount FROM orders WHERE request_id = $1", request.RequestID).Scan(&existingOrder, &existingAmount)
		if err == nil {
			if existingOrder != request.OrderID || existingAmount != request.Amount {
				return errors.New("requestId is already bound to a different order")
			}
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("read existing order: %w", err)
		}
		event := cloudEvent{
			SpecVersion: "1.0", ID: eventID, Source: "/order-api", Type: "dev.chronicle.order.created",
			Subject: request.OrderID, Time: app.now().Format(time.RFC3339Nano), DataContentType: "application/json",
			AggregateID: request.OrderID, AggregateVersion: 1,
			Data: map[string]any{"orderId": request.OrderID, "amount": request.Amount},
		}
		document, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("encode OrderCreated event: %w", err)
		}
		if _, err := transaction.Exec(ctx, "INSERT INTO orders (order_id, request_id, amount, status) VALUES ($1, $2, $3, 'pending')", request.OrderID, request.RequestID, request.Amount); err != nil {
			return fmt.Errorf("insert order: %w", err)
		}
		if _, err := transaction.Exec(ctx, `
INSERT INTO outbox (event_id, aggregate_id, business_key, amount, event)
VALUES ($1, $2, $3, $4, $5)`, eventID, request.OrderID, request.OrderID, request.Amount, document); err != nil {
			return fmt.Errorf("insert outbox row: %w", err)
		}
		created = true
		return nil
	})
	return response, created, err
}

func requireEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("request has trailing JSON")
	}
	return nil
}

func readSecretFile(environment string) (string, error) {
	path := strings.TrimSpace(os.Getenv(environment))
	if path == "" {
		return "", fmt.Errorf("%s is required", environment)
	}
	value, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", environment, err)
	}
	if strings.TrimSpace(string(value)) == "" {
		return "", fmt.Errorf("%s is empty", environment)
	}
	return strings.TrimSpace(string(value)), nil
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
