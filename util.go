package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"

	"github.com/go-playground/validator/v10"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// AppConfig holds application configuration
type AppConfig struct {
	MinIOEndpoint      string
	MinIOAccessKey     string
	MinIOSecretKey     string
	MinIOUseSSL        bool
	FeaturesBucketName string
	AlpacaKey          string
	AlpacaSecret       string
}

// Asset represents an Alpaca asset
type Asset struct {
	AlpacaID                    string   `json:"id" validate:"required"`
	Class                       string   `json:"class" validate:"required,oneof=us_equity us_option crypto"`
	Cusip                       *string  `json:"cusip"`
	Exchange                    string   `json:"exchange" validate:"required,oneof=AMEX ARCA BATS NYSE NASDAQ NYSEARCA OTC"`
	Symbol                      string   `json:"symbol" validate:"required"`
	Name                        string   `json:"name" validate:"required,min=1"`
	Status                      string   `json:"status" validate:"required,oneof=active inactive"`
	Tradable                    bool     `json:"tradable" validate:"required"`
	Marginable                  bool     `json:"marginable" validate:"required"`
	Shortable                   bool     `json:"shortable" validate:"required"`
	EasyToBorrow                bool     `json:"easy_to_borrow" validate:"required"`
	Fractionable                bool     `json:"fractionable" validate:"required"`
	MarginRequirementLong       string   `json:"margin_requirement_long"`
	MarginRequirementShort      string   `json:"margin_requirement_short"`
	Attributes                  []string `json:"attributes" validate:"dive,oneof=ptp_no_exception ptp_with_exception ipo has_options options_late_close"`
}

// Bar represents a single price bar
type Bar struct {
	Open      float64 `validate:"gt=0"`
	High      float64 `validate:"gt=0"`
	Low       float64 `validate:"gt=0"`
	Close     float64 `validate:"gt=0"`
	Volume    float64 `validate:"gte=0"`
	Timestamp int64   `validate:"required"`
}

// FetchedData holds price data for a symbol
type FetchedData struct {
	Symbol    string
	PriceData []Bar
}

// Failure represents a fetch or process failure
type Failure struct {
	Type   string // "fetch" or "process"
	Symbol string
	Error  error
}

// UploadJob for queuing MinIO uploads
type UploadJob struct {
	ObjectName string
	Buffer     bytes.Buffer // Pass by value, but small
}

// RequestOptions defines parameters for an HTTP request
type RequestOptions struct {
	Method  string            // HTTP method (e.g., GET, POST)
	URL     string            // Request URL
	Params  url.Values        // Query parameters
	Headers map[string]string // HTTP headers
	Body    io.Reader         // Request body
	Retry   bool              // Retry on network errors or 5xx
}

var (
	validate   = validator.New(validator.WithRequiredStructEnabled())
	vixMap     = make(map[string]float64) // date -> VIXY close
	tradingURL = "https://paper-api.alpaca.markets/v2/"
	dataURL    = "https://data.alpaca.markets/v2/"
)

// minioAdapter wraps minio.Client to adapt GetObject return type
type minioAdapter struct {
	*minio.Client
}

func (m *minioAdapter) GetObject(ctx context.Context, bucket, object string, opts minio.GetObjectOptions) (io.ReadCloser, error) {
	obj, err := m.Client.GetObject(ctx, bucket, object, opts)
	return obj, err
}

// NewMinIOClient initializes a MinIO client
func NewMinIOClient(cfg *AppConfig) (MinIOClient, error) {
	client, err := minio.New(cfg.MinIOEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.MinIOAccessKey, cfg.MinIOSecretKey, ""),
		Secure: cfg.MinIOUseSSL,
	})
	if err != nil {
		return nil, err
	}

	log.Println("Connected to MinIO.")
	err = client.MakeBucket(context.Background(), cfg.FeaturesBucketName, minio.MakeBucketOptions{})
	if err != nil {
		if exists, err := client.BucketExists(context.Background(), cfg.FeaturesBucketName); err == nil && exists {
			log.Printf("Bucket '%s' exists.", cfg.FeaturesBucketName)
		} else {
			return nil, fmt.Errorf("failed to create bucket: %w", err)
		}
	} else {
		log.Printf("Created bucket '%s'.", cfg.FeaturesBucketName)
	}

	return &minioAdapter{client}, nil
}

// DoRequest performs a customizable HTTP request
func DoRequest(opts RequestOptions) (*http.Response, error) {
	u := opts.URL
	if opts.Params != nil && len(opts.Params) > 0 {
		u += "?" + opts.Params.Encode()
	}

	req, err := http.NewRequest(opts.Method, u, opts.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	if opts.Headers != nil {
		for key, value := range opts.Headers {
			req.Header.Set(key, value)
		}
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		if opts.Retry {
			return nil, fmt.Errorf("network error: %w", err)
		}
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if opts.Retry && resp.StatusCode >= 500 {
			return nil, fmt.Errorf("server error (status: %s): %s", resp.Status, string(body))
		}
		return nil, fmt.Errorf("request failed (status: %s): %s", resp.Status, string(body))
	}

	return resp, nil
}

func startPprofServer() {
	log.Println("pprof server on :6060")
	if err := http.ListenAndServe(":6060", nil); err != nil {
		log.Fatalf("FATAL: pprof failed: %v", err)
	}
}

func formatFloat(f float64) string {
	if f == 0.0 {
		return ""
	}
	return fmt.Sprintf("%.2f", f)
}
