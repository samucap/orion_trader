package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
)

// MockMinIOClient for testing
type MockMinIOClient struct {
	StatObjectFunc func(ctx context.Context, bucket, object string, opts minio.StatObjectOptions) (minio.ObjectInfo, error)
	GetObjectFunc  func(ctx context.Context, bucket, object string, opts minio.GetObjectOptions) (io.ReadCloser, error)
	PutObjectFunc  func(ctx context.Context, bucket, object string, reader io.Reader, objectSize int64, opts minio.PutObjectOptions) (minio.UploadInfo, error)
}

func (m *MockMinIOClient) StatObject(ctx context.Context, bucket, object string, opts minio.StatObjectOptions) (minio.ObjectInfo, error) {
	if m.StatObjectFunc == nil {
		return minio.ObjectInfo{}, &minio.ErrorResponse{Code: "NoSuchKey"}
	}
	return m.StatObjectFunc(ctx, bucket, object, opts)
}

func (m *MockMinIOClient) GetObject(ctx context.Context, bucket, object string, opts minio.GetObjectOptions) (io.ReadCloser, error) {
	if m.GetObjectFunc == nil {
		return io.NopCloser(strings.NewReader("")), nil
	}
	return m.GetObjectFunc(ctx, bucket, object, opts)
}

func (m *MockMinIOClient) PutObject(ctx context.Context, bucket, object string, reader io.Reader, objectSize int64, opts minio.PutObjectOptions) (minio.UploadInfo, error) {
	if m.PutObjectFunc == nil {
		return minio.UploadInfo{}, nil
	}
	return m.PutObjectFunc(ctx, bucket, object, reader, objectSize, opts)
}

func TestProcessAndSave(t *testing.T) {
	// Setup
	ingestor := &Ingestor{
		Cfg: &AppConfig{
			FeaturesBucketName: "features-market-data-test",
		},
		uploadQueue:      make(chan UploadJob, 10),
		failureQueue:     make(chan Failure, 10),
		processedSymbols: atomic.Int64{},
		totalSymbols:     atomic.Int64{},
	}
	data := FetchedData{
		Symbol: "AAPL",
		PriceData: []Bar{
			{Timestamp: time.Now().UnixNano(), Open: 100, High: 105, Low: 95, Close: 102, Volume: 1000},
			{Timestamp: time.Now().AddDate(0, 0, 1).UnixNano(), Open: 102, High: 107, Low: 97, Close: 104, Volume: 1200},
		},
	}

	// Mock MinIO client
	mockMinIO := &MockMinIOClient{}
	ingestor.MinIO = mockMinIO

	// Test
	ctx := context.Background()
	processor := Processor{ingestor: ingestor}
	err := processor.ProcessAndSave(ctx, data)
	if err == nil {
		t.Error("Expected error due to insufficient bars, got none")
	}

	// Add enough bars
	data.PriceData = make([]Bar, 50)
	for i := 0; i < 50; i++ {
		data.PriceData[i] = Bar{
			// Use 2023 to avoid future year issues
			Timestamp: time.Date(2023, 1, i+1, 0, 0, 0, 0, time.UTC).UnixNano(),
			Open:      100 + float64(i),
			High:      105 + float64(i),
			Low:       95 + float64(i),
			Close:     102 + float64(i),
			Volume:    1000 + float64(i*10),
		}
	}

	// Reset mock for successful case
	var uploadedData bytes.Buffer
	mockMinIO.PutObjectFunc = func(ctx context.Context, bucket, object string, reader io.Reader, objectSize int64, opts minio.PutObjectOptions) (minio.UploadInfo, error) {
		_, err := io.Copy(&uploadedData, reader)
		return minio.UploadInfo{}, err
	}

	// Run
	err = processor.ProcessAndSave(ctx, data)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	// Drain uploadQueue to simulate uploader and trigger PutObject
	for job := range ingestor.uploadQueue {
		_, err := mockMinIO.PutObject(ctx, ingestor.Cfg.FeaturesBucketName, job.ObjectName, bytes.NewReader(job.Buffer.Bytes()), int64(job.Buffer.Len()), minio.PutObjectOptions{ContentType: "text/csv"})
		if err != nil {
			t.Errorf("Failed to process upload job: %v", err)
		}
		break // Assuming one job for this test
	}

	// Verify uploaded CSV
	reader := csv.NewReader(&uploadedData)
	records, err := reader.ReadAll()
	if err != nil {
		t.Errorf("Failed to read uploaded CSV: %v", err)
	}
	if len(records) < 2 {
		t.Errorf("Expected at least 2 CSV records, got %d", len(records))
	} else if records[0][0] != "Date" {
		t.Errorf("Expected header to start with 'Date', got %s", records[0][0])
	}
}

func TestProcessorConcurrency(t *testing.T) {
	// Setup
	ingestor := &Ingestor{
		Cfg: &AppConfig{
			FeaturesBucketName: "features-market-data",
		},
		processQueue:     make(chan FetchedData, 10),
		failureQueue:     make(chan Failure, 10),
		uploadQueue:      make(chan UploadJob, 10),
		processedSymbols: atomic.Int64{},
		totalSymbols:     atomic.Int64{},
		MinIO: &MockMinIOClient{
			StatObjectFunc: func(ctx context.Context, bucket, object string, opts minio.StatObjectOptions) (minio.ObjectInfo, error) {
				return minio.ObjectInfo{}, &minio.ErrorResponse{Code: "NoSuchKey"}
			},
			PutObjectFunc: func(ctx context.Context, bucket, object string, reader io.Reader, objectSize int64, opts minio.PutObjectOptions) (minio.UploadInfo, error) {
				return minio.UploadInfo{}, nil
			},
			GetObjectFunc: func(ctx context.Context, bucket, object string, opts minio.GetObjectOptions) (io.ReadCloser, error) {
				return io.NopCloser(strings.NewReader("")), nil
			},
		},
	}
	processor := NewProcessor(ingestor)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Feed jobs
	var wg sync.WaitGroup
	processor.Start(ctx, &wg)
	for i := 0; i < 10; i++ {
		data := FetchedData{
			Symbol:    fmt.Sprintf("TEST%d", i),
			PriceData: make([]Bar, 100),
		}
		for j := 0; j < 100; j++ {
			data.PriceData[j] = Bar{
				// Use 2023 to avoid future year issues
				Timestamp: time.Date(2023, 1, j+1, 0, 0, 0, 0, time.UTC).UnixNano(),
				Open:      100 + float64(j),
				High:      105 + float64(j),
				Low:       95 + float64(j),
				Close:     102 + float64(j),
				Volume:    1000 + float64(j*10),
			}
		}
		ingestor.processQueue <- data
	}
	close(ingestor.processQueue)

	// Drainer for uploadQueue to prevent blocking
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case job, open := <-ingestor.uploadQueue:
				if !open {
					return
				}
				_, _ = ingestor.MinIO.PutObject(ctx, ingestor.Cfg.FeaturesBucketName, job.ObjectName, bytes.NewReader(job.Buffer.Bytes()), int64(job.Buffer.Len()), minio.PutObjectOptions{ContentType: "text/csv"})
			}
		}
	}()

	// Wait and verify
	wg.Wait()
	if ingestor.processedSymbols.Load() != 10 {
		t.Errorf("Expected 10 processed symbols, got %d", ingestor.processedSymbols.Load())
	}
}
