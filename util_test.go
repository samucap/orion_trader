package main

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

// TestDoRequest tests the DoRequest function with RequestOptions
func TestDoRequest(t *testing.T) {
	tests := []struct {
		name           string
		opts           RequestOptions
		serverResponse func(w http.ResponseWriter, r *http.Request)
		wantStatus     int
		wantBody       string
		wantErr        bool
		wantErrMsg     string
	}{
		{
			name: "successful GET request",
			opts: RequestOptions{
				Method: "GET",
				URL:    "/test",
				Params: url.Values{"key": []string{"value"}},
				Headers: map[string]string{
					"Custom-Header":       "test-value",
					"APCA-API-KEY-ID":     "test-key",
					"APCA-API-SECRET-KEY": "test-secret",
				},
				Retry: false,
			},
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
			name: "404 not found",
			opts: RequestOptions{
				Method:  "GET",
				URL:     "/not-found",
				Headers: map[string]string{},
				Retry:   false,
			},
			serverResponse: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNotFound)
				fmt.Fprint(w, `{"error":"resource not found"}`)
			},
			wantErr:    true,
			wantErrMsg: "request failed (status: 404 Not Found): {\"error\":\"resource not found\"}",
		},
		{
			name: "500 server error with retry",
			opts: RequestOptions{
				Method:  "GET",
				URL:     "/server-error",
				Headers: map[string]string{},
				Retry:   true,
			},
			serverResponse: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprint(w, `{"error":"internal server error"}`)
			},
			wantErr:    true,
			wantErrMsg: "server error (status: 500 Internal Server Error): {\"error\":\"internal server error\"}",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(tt.serverResponse))
			defer server.Close()

			// Update URL to use test server
			tt.opts.URL = server.URL + tt.opts.URL

			resp, err := DoRequest(tt.opts)

			if (err != nil) != tt.wantErr {
				t.Errorf("DoRequest() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil && tt.wantErrMsg != "" && err.Error() != tt.wantErrMsg {
				t.Errorf("DoRequest() error message = %v, want %v", err.Error(), tt.wantErrMsg)
			}

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
