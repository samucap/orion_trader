package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
)

// MockMinIOClient mocks the MinIO client for testing
type MockMinIOClient struct {
	Objects map[string][]byte
}

func (m *MockMinIOClient) StatObject(ctx context.Context, bucket, object string, opts minio.StatObjectOptions) (minio.ObjectInfo, error) {
	if _, exists := m.Objects[object]; exists {
		return minio.ObjectInfo{}, nil
	}
	return minio.ObjectInfo{}, minio.ErrorResponse{Code: "NoSuchKey"}
}

func (m *MockMinIOClient) GetObject(ctx context.Context, bucket, object string, opts minio.GetObjectOptions) (*minio.Object, error) {
	data, exists := m.Objects[object]
	if !exists {
		return nil, fmt.Errorf("object %s not found", object)
	}
	return &minio.Object{Object: io.NopCloser(bytes.NewReader(data))}, nil
}

func (m *MockMinIOClient) PutObject(ctx context.Context, bucket, object string, reader io.Reader, objectSize int64, opts minio.PutObjectOptions) (minio.UploadInfo, error) {
	data, err := io.ReadAll(reader)
	if err != nil {
		return minio.UploadInfo{}, err
	}
	m.Objects[object] = data
	return minio.UploadInfo{}, nil
}

// TestProcessAndSave tests the processAndSave method
func TestProcessAndSave(t *testing.T) {
	cfg := &AppConfig{FeaturesBucketName: "test-bucket"}
	minioClient := &MockMinIOClient{Objects: make(map[string][]byte)}
	failureQueue := make(chan Failure, 10)
	processor := NewProcessor(cfg, minioClient, make(chan FetchedData, 10), failureQueue)

	data := FetchedData{
		Symbol: "TEST",
		PriceData: makeBars(60),
	}

	vixMap["2023-01-01"] = 20.5
	vixMap["2023-01-02"] = 21.0

	ctx := context.Background()
	err := processor.processAndSave(ctx, data)
	if err != nil {
		t.Errorf("processAndSave() error = %v, want nil", err)
	}

	// Check written CSV
	objectName := "TEST/2023/features.csv"
	csvData, exists := minioClient.Objects[objectName]
	if !exists {
		t.Fatalf("CSV file %s not created", objectName)
	}

	reader := csv.NewReader(bytes.NewReader(csvData))
	records, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("Failed to read CSV: %v", err)
	}

	if len(records) < 2 {
		t.Errorf("CSV has %d rows, want at least 2 (header + data)", len(records))
	}

	// Verify header
	expectedHeader := []string{
		"Date", "Open", "High", "Low", "Close", "Volume",
		"SMA20", "EMA50", "RSI14", "MACD", "MACDSignal",
		"BBUpper", "BBMiddle", "BBLower", "ATR14", "OBV",
		"ADX14", "CCI20", "StochK", "StochD", "VWAP",
		"ROC12", "CMO14", "VIX",
	}
	if !reflect.DeepEqual(records[0], expectedHeader) {
		t.Errorf("CSV header = %v, want %v", records[0], expectedHeader)
	}

	// Verify at least one data row
	if len(records) > 1 {
		row := records[1]
		if row[0] != "2023-01-01" {
			t.Errorf("CSV row date = %s, want 2023-01-01", row[0])
		}
		if row[len(row)-1] != "20.50" {
			t.Errorf("CSV row VIX = %s, want 20.50", row[len(row)-1])
		}
	}
}

// TestStartAndWorker tests the Start and worker methods
func TestStartAndWorker(t *testing.T) {
	cfg := &AppConfig{FeaturesBucketName: "test-bucket"}
	minioClient := &MockMinIOClient{Objects: make(map[string][]byte)}
	failureQueue := make(chan Failure, 10)
	processQueue := make(chan FetchedData, 10)
	processor := NewProcessor(cfg, minioClient, processQueue, failureQueue)
	ingestor := &Ingestor{totalSymbols: atomic.Int64{}, processedSymbols: atomic.Int64{}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	processor.Start(ctx, &wg, ingestor)

	// Send test data
	go func() {
		processQueue <- FetchedData{Symbol: "TEST", PriceData: makeBars(60)}
		close(processQueue)
	}()

	wg.Wait()

	if len(minioClient.Objects) == 0 {
		t.Error("No objects written to MinIO")
	}
}

// makeBars creates test price bars
func makeBars(n int) []Bar {
	bars := make([]Bar, n)
	for i := 0; i < n; i++ {
		bars[i] = Bar{
			Open:      float64(10 + i),
			High:      float64(12 + i),
			Low:       float64(8 + i),
			Close:     float64(10 + i),
			Volume:    float64(100 + i*10),
			Timestamp: time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(i) * 24 * time.Hour).UnixNano(),
		}
	}
	return bars
}
