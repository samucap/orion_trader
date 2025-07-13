package main

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// TestDoRequest tests the DoRequest function for various scenarios
func TestDoRequest(t *testing.T) {
	// Sample AppConfig for tests
	cfg := &AppConfig{
		AlpacaKey:    "test-key",
		AlpacaSecret: "test-secret",
	}

	// Test cases
	tests := []struct {
		name           string
		method         string
		url            string
		params         url.Values
		headers        map[string]string
		body           io.Reader
		retry          bool
		serverResponse func(w http.ResponseWriter, r *http.Request)
		wantStatus     int
		wantBody       string
		wantErr        bool
		wantErrMsg     string
	}{
		{
			name:   "successful GET request",
			method: "GET",
			url:    "/test",
			params: url.Values{"key": {"value"}},
			headers: map[string]string{
				"Custom-Header": "test-value",
			},
			body: nil,
			retry: false,
			serverResponse: func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "GET" {
					t.Errorf("Expected method GET, got %s", r.Method)
				}
				if r.URL.Query().Get("key") != "value" {
					t.Errorf("Expected query param key=value, got %s", r.URL.Query().Get("key"))
				}
				if r.Header.Get("APCA-API-KEY-ID") != "test-key" {
					t.Errorf("Expected APCA-API-KEY-ID=test-key, got %s", r.Header.Get("APCA-API-KEY-ID"))
				}
				if r.Header.Get("APCA-API-SECRET-KEY") != "test-secret" {
					t.Errorf("Expected APCA-API-SECRET-KEY=test-secret, got %s", r.Header.Get("APCA-API-SECRET-KEY"))
				}
				if r.Header.Get("Custom-Header") != "test-value" {
					t.Errorf("Expected Custom-Header=test-value, got %s", r.Header.Get("Custom-Header"))
				}
				w.WriteHeader(http.StatusOK)
				fmt.Fprint(w, `{"status":"success"}`)
			},
			wantStatus: http.StatusOK,
			wantBody:   `{"status":"success"}`,
			wantErr:    false,
		},
		{
			name:   "404 not found",
			method: "GET",
			url:    "/not-found",
			params: nil,
			headers: nil,
			body:   nil,
			retry:  false,
			serverResponse: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNotFound)
				fmt.Fprint(w, `{"error":"resource not found"}`)
			},
			wantErr:    true,
			wantErrMsg: "request failed (status: 404 Not Found): {\"error\":\"resource not found\"}",
		},
		{
			name:   "500 server error with retry",
			method: "GET",
			url:    "/server-error",
			params: nil,
			headers: nil,
			body:   nil,
			retry:  true,
			serverResponse: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprint(w, `{"error":"internal server error"}`)
			},
			wantErr:    true,
			wantErrMsg: "server error (status: 500 Internal Server Error): {\"error\":\"internal server error\"}",
		},
		{
			name:   "POST request with body",
			method: "POST",
			url:    "/post-test",
			params: nil,
			headers: map[string]string{
				"Content-Type": "application/json",
			},
			body: strings.NewReader(`{"data":"test"}`),
			retry: false,
			serverResponse: func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "POST" {
					t.Errorf("Expected method POST, got %s", r.Method)
				}
				if r.Header.Get("Content-Type") != "application/json" {
					t.Errorf("Expected Content-Type=application/json, got %s", r.Header.Get("Content-Type"))
				}
				body, _ := io.ReadAll(r.Body)
				if string(body) != `{"data":"test"}` {
					t.Errorf("Expected body={\"data\":\"test\"}, got %s", string(body))
				}
				w.WriteHeader(http.StatusOK)
				fmt.Fprint(w, `{"status":"posted"}`)
			},
			wantStatus: http.StatusOK,
			wantBody:   `{"status":"posted"}`,
			wantErr:    false,
		},
		{
			name:   "invalid method",
			method: "INVALID",
			url:    "/test",
			params: nil,
			headers: nil,
			body:   nil,
			retry:  false,
			serverResponse: func(w http.ResponseWriter, r *http.Request) {
				// Should not reach here
				t.Error("Server should not be called for invalid method")
			},
			wantErr:    true,
			wantErrMsg: "failed to create request: net/http: invalid method \"INVALID\"",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a test server
			server := httptest.NewServer(http.HandlerFunc(tt.serverResponse))
			defer server.Close()

			// Make the request
			resp, err := DoRequest(tt.method, cfg, server.URL+tt.url, tt.params, tt.headers, tt.body, tt.retry)

			// Check error
			if (err != nil) != tt.wantErr {
				t.Errorf("DoRequest() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil && tt.wantErrMsg != "" && err.Error() != tt.wantErrMsg {
				t.Errorf("DoRequest() error message = %v, want %v", err.Error(), tt.wantErrMsg)
			}

			// Check response for successful cases
			if !tt.wantErr && resp != nil {
				if resp.StatusCode != tt.wantStatus {
					t.Errorf("DoRequest() status = %d, want %d", resp.StatusCode, tt.wantStatus)
				}
				body, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				if string(body) != tt.wantBody {
					t.Errorf("DoRequest() body = %s, want %s", string(body), tt.wantBody)
				}
			}
		})
	}
}
