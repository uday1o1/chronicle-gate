package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestEffectSinkEnforcesCredentialPrivileges(t *testing.T) {
	t.Parallel()
	writerToken := "writer-token-0123456789abcdef0123456789"
	observerToken := "observer-token-0123456789abcdef012345"
	application := &server{ledger: &ledger{}, writerToken: sha256.Sum256([]byte(writerToken)), observerToken: sha256.Sum256([]byte(observerToken))}
	testServer := httptest.NewServer(application.handler())
	defer testServer.Close()
	effectBody := effect{Kind: "payment_capture", EventID: "payment-1", BusinessKey: "order-1", Amount: 1250, IdempotencyKey: "payment-1", SourceTopic: "payments", SourcePartition: 0, SourceOffset: 0}

	for _, test := range []struct {
		method string
		path   string
		token  string
		body   any
		status int
	}{
		{http.MethodPost, "/v1/effects", writerToken, effectBody, http.StatusCreated},
		{http.MethodPost, "/v1/effects", writerToken, effectBody, http.StatusOK},
		{http.MethodGet, "/v1/effects", writerToken, nil, http.StatusUnauthorized},
		{http.MethodPost, "/v1/effects", observerToken, effectBody, http.StatusUnauthorized},
	} {
		response := requestEffect(t, testServer.URL, test.method, test.path, test.token, test.body, test.status)
		if err := response.Body.Close(); err != nil {
			t.Fatal(err)
		}
	}

	response := requestEffect(t, testServer.URL, http.MethodGet, "/v1/effects", observerToken, nil, http.StatusOK)
	defer func() { _ = response.Body.Close() }()
	var observation struct {
		Entries []effect `json:"entries"`
		Pending int      `json:"pending"`
		SHA256  string   `json:"sha256"`
	}
	if err := json.NewDecoder(response.Body).Decode(&observation); err != nil {
		t.Fatal(err)
	}
	if len(observation.Entries) != 1 || observation.Pending != 0 || len(observation.SHA256) != sha256.Size*2 {
		t.Fatalf("unexpected observation: %#v", observation)
	}
}

func requestEffect(t *testing.T, baseURL, method, path, token string, body any, status int) *http.Response {
	t.Helper()
	var document []byte
	if body != nil {
		var err error
		document, err = json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
	}
	request, err := http.NewRequest(method, baseURL+path, bytes.NewReader(document))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != status {
		defer func() { _ = response.Body.Close() }()
		t.Fatalf("%s %s status = %d, want %d", method, path, response.StatusCode, status)
	}
	return response
}
