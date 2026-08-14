package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestBenchmarkHandler(t *testing.T) {
	server := httptest.NewServer(newHandler(0))
	defer server.Close()
	response, err := http.Post(server.URL+"/work", "application/octet-stream", strings.NewReader("work"))
	if err != nil {
		t.Fatal(err)
	}
	document, readErr := io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil || response.StatusCode != http.StatusOK || string(document) != "chronicle-benchmark-response" {
		t.Fatalf("response status=%d body=%q read=%v close=%v", response.StatusCode, document, readErr, closeErr)
	}
	started := time.Now()
	response, err = http.Post(server.URL+"/work", "application/octet-stream", strings.NewReader(strings.Repeat("x", maxBodyBytes+1)))
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusBadRequest || time.Since(started) > time.Second {
		t.Fatalf("oversized status=%d duration=%s", response.StatusCode, time.Since(started))
	}
}
