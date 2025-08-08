package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/joho/godotenv"
	"golang.org/x/time/rate"
)

// Ingestor manages the data ingestion pipeline
type Ingestor struct {
	Cfg              *AppConfig
	MinIO            MinIOClient
	fetchQueue       chan FetchTask // Batches with date ranges
	processQueue     chan FetchedData
	uploadQueue      chan UploadJob
	failureQueue     chan Failure
	totalSymbols     atomic.Int64
	fetchedSymbols   atomic.Int64
	processedSymbols atomic.Int64
	fetchFailures    atomic.Int64
	processFailures  atomic.Int64
	rateLimiter      *rate.Limiter
	startTime        time.Time
	symbols          []string // Cached symbols
}

// IngestorInterface defines the methods for the Ingestor
type IngestorInterface interface {
	Run(ctx context.Context) error
}

// NewIngestor initializes a new Ingestor
func NewIngestor() (*Ingestor, error) {
	if err := godotenv.Load(); err != nil {
		log.Println("WARN: .env not found, using env vars.")
	}

	cfg := &AppConfig{
		MinIOEndpoint:      os.Getenv("MINIO_ENDPOINT"),
		MinIOAccessKey:     os.Getenv("MINIO_ROOT_USER"),
		MinIOSecretKey:     os.Getenv("MINIO_ROOT_PASSWORD"),
		MinIOUseSSL:        os.Getenv("MINIO_USE_SSL") == "true",
		FeaturesBucketName: "features-market-data",
		AlpacaKey:          os.Getenv("ALPACA_API_KEY"),
		AlpacaSecret:       os.Getenv("ALPACA_API_SECRET"),
	}

	minioClient, err := NewMinIOClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize MinIO: %w", err)
	}

	return &Ingestor{
		Cfg:          cfg,
		MinIO:        minioClient,
		fetchQueue:   make(chan FetchTask, 100),
		processQueue: make(chan FetchedData, 1000),
		uploadQueue:  make(chan UploadJob, 1000),
		failureQueue: make(chan Failure, 1000),
		rateLimiter:  rate.NewLimiter(rate.Every(300*time.Millisecond), 1),
		startTime:    time.Now(),
	}, nil
}

// Run starts the ingestion pipeline
func (i *Ingestor) Run(ctx context.Context) error {
	log.Printf("Starting pipeline")
	go i.startMonitoring(ctx)
	go startPprofServer()
	go i.handleFailures(ctx)

	processor := NewProcessor(i)
	fetcher := NewFetcher(i)
	uploader := NewUploader(i)

	var wg sync.WaitGroup
	fetcher.Start(ctx, &wg)
	processor.Start(ctx, &wg)
	uploader.Start(ctx, &wg)

	// Initial historical fetch
	if err := fetcher.FetchAssets(ctx); err != nil {
		return fmt.Errorf("failed to fetch assets: %w", err)
	}
	// Daily update loop
	for {
		dur := timeToNextFetch()
		log.Printf("Sleeping %v until next fetch", dur)
		select {
		case <-time.After(dur):
			if err := fetcher.QueueDailyJobs(ctx); err != nil {
				log.Printf("Daily queue failed: %v", err)
			}
		case <-ctx.Done():
			log.Println("Shutdown received, stopping loop")
			close(i.fetchQueue)
			close(i.processQueue)
			close(i.uploadQueue)
			close(i.failureQueue)
			wg.Wait()
			elapsed := time.Since(i.startTime).Seconds()
			log.Printf("\033[31m[FINAL REPORT] Total Symbols: %d | Fetched: %d | Processed: %d | Fetch Failures: %d | Process Failures: %d | Pending Failures: %d | Success Rate: %.2f%% | Time Elapsed: %.2fs\033[0m",
				i.totalSymbols.Load(), i.fetchedSymbols.Load(), i.processedSymbols.Load(), i.fetchFailures.Load(), i.processFailures.Load(),
				len(i.failureQueue), float64(i.processedSymbols.Load())/float64(i.totalSymbols.Load())*100, elapsed)
			return ctx.Err()
		}
	}
}

func (i *Ingestor) handleFailures(ctx context.Context) {
	for {
		select {
		case failure, ok := <-i.failureQueue:
			if !ok {
				return
			}
			switch failure.Type {
			case "fetch":
				i.fetchFailures.Add(1)
				log.Printf("Fetch failure recorded for %s: %v", failure.Symbol, failure.Error)
			case "process":
				i.processFailures.Add(1)
				log.Printf("Process failure recorded for %s: %v", failure.Symbol, failure.Error)
			}
		case <-ctx.Done():
			log.Printf("Failure handler stopped due to context cancellation")
			return
		}
	}
}

func (i *Ingestor) startMonitoring(ctx context.Context) {
	log.Printf("Monitoring started")
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			elapsed := time.Since(i.startTime).Seconds()
			log.Printf("[MONITOR] Fetchers: %d | Processors: %d | Fetched: %d/%d | Processed: %d/%d | Fetch Failures: %d | Process Failures: %d | Pending Failures: %d | Elapsed: %.2fs",
				runtime.NumCPU(), runtime.NumCPU(), i.fetchedSymbols.Load(), i.totalSymbols.Load(),
				i.processedSymbols.Load(), i.totalSymbols.Load(), i.fetchFailures.Load(), i.processFailures.Load(), len(i.failureQueue), elapsed)
		case <-ctx.Done():
			log.Println("Monitoring stopped")
			return
		}
	}
}

// TODO put into util
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func minStr(a, b int) string {
	if a < b {
		return fmt.Sprintf("... (+%d more)", b-a)
	}
	return ""
}
