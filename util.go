package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

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
	Response http.Response
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

// FetchTask for queuing fetch jobs with date range
type FetchTask struct {
	Symbols []string
	Start   string
	End     string
}

var (
	validate = validator.New(validator.WithRequiredStructEnabled())
	vixMap   = make(map[string]float64) // date -> VIXY close
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

// timeToNextFetch calculates duration to next fetch time (21:00 UTC)
func timeToNextFetch() time.Duration {
	now := time.Now().UTC()
	target := time.Date(now.Year(), now.Month(), now.Day(), 21, 0, 0, 0, time.UTC)
	if now.After(target) || now.Equal(target) {
		target = target.Add(24 * time.Hour)
	}
	return target.Sub(now)
}
