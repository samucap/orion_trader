package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"
	"net/url"
	"encoding/json"
	"strings"

	"github.com/joho/godotenv"
	"github.com/minio/minio-go/v7"
)

// Ingestor manages the data ingestion pipeline
type Ingestor struct {
	Cfg              *AppConfig
	MinIO            *minio.Client
	Fetcher          *Fetcher
	Processor        *Processor
	fetchQueue       chan Job
	processQueue     chan FetchedData
	failureQueue     chan Failure
	totalSymbols     atomic.Int64
	fetchedSymbols   atomic.Int64
	processedSymbols atomic.Int64
	fetchFailures    atomic.Int64
	processFailures  atomic.Int64
	startTime        time.Time
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

	fetchQueue := make(chan Job, 1000)
	processQueue := make(chan FetchedData, 1000)
	failureQueue := make(chan Failure, 1000)

	fetcher := NewFetcher(cfg, fetchQueue, processQueue, failureQueue)
	processor := NewProcessor(cfg, minioClient, processQueue, failureQueue)

	return &Ingestor{
		Cfg:              cfg,
		MinIO:            minioClient,
		Fetcher:          fetcher,
		Processor:        processor,
		fetchQueue:       fetchQueue,
		processQueue:     processQueue,
		failureQueue:     failureQueue,
		startTime:        time.Now(),
	}, nil
}

// Run starts the ingestion pipeline
func (i *Ingestor) Run(ctx context.Context) error {
	go i.startMonitoring(ctx)
	go startPprofServer()
	go i.handleFailures(ctx)

	var wg sync.WaitGroup
	i.Fetcher.Start(ctx, &wg, i)

	// Fetch assets and queue jobs before starting processors
	if err := i.fetchAssetsAndQueueJobs(ctx); err != nil {
		return fmt.Errorf("failed to fetch assets: %w", err)
	}

	// Start processors after totalSymbols is set
	i.Processor.Start(ctx, &wg, i)

	<-ctx.Done()
	log.Println("Waiting for workers to finish...")
	close(i.fetchQueue)
	close(i.processQueue)
	close(i.failureQueue)
	wg.Wait()

	// Log final report in red
	elapsed := time.Since(i.startTime).Seconds()
	log.Printf("\033[31m[FINAL REPORT] Total Symbols: %d | Fetched: %d | Processed: %d | Fetch Failures: %d | Process Failures: %d | Pending Failures: %d | Success Rate: %.2f%% | Time Elapsed: %.2fs\033[0m",
		i.totalSymbols.Load(), i.fetchedSymbols.Load(), i.processedSymbols.Load(), i.fetchFailures.Load(), i.processFailures.Load(),
		len(i.failureQueue), float64(i.processedSymbols.Load())/float64(i.totalSymbols.Load())*100, elapsed)

	return nil
}

func (i *Ingestor) fetchAssetsAndQueueJobs(ctx context.Context) error {
	resp, err := DoRequest("GET", i.Cfg, tradingURL+"assets", url.Values{
		"status":      {"active"},
		"asset_class": {"us_equity"},
	}, nil, nil, false)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var assets []Asset
	if err := json.NewDecoder(resp.Body).Decode(&assets); err != nil {
		return fmt.Errorf("failed to decode assets: %w", err)
	}

	var symbols []string
	for _, a := range assets {
		if a.Tradable {
			symbols = append(symbols, a.Symbol)
		}
	}
	i.totalSymbols.Store(int64(len(symbols)))
	log.Printf("Found %d tradable symbols: %s", len(symbols), strings.Join(symbols[:min(5, len(symbols))], ", ")+minStr(5, len(symbols)))

	// Fetch VIXY data
	if err := i.fetchVixData(); err != nil {
		log.Printf("WARN: Failed to fetch VIXY: %v", err)
	}

	// Queue symbols
	for _, s := range symbols {
		select {
		case i.fetchQueue <- Job{Symbol: s}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (i *Ingestor) fetchVixData() error {
	resp, err := DoRequest("GET", i.Cfg, dataURL+"stocks/bars", url.Values{
		"symbols":    {"VIXY"},
		"timeframe":  {"1Day"},
		"start":      {"2023-01-01"},
		"end":        {"2023-12-31"},
		"limit":      {"10000"},
		"adjustment": {"all"},
	}, nil, nil, false)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var result struct {
		Bars map[string][]struct {
			T string  `json:"t"`
			C float64 `json:"c"`
		} `json:"bars"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return err
	}

	for _, b := range result.Bars["VIXY"] {
		ts, err := time.Parse(time.RFC3339, b.T)
		if err != nil {
			continue
		}
		vixMap[ts.Format("2006-01-02")] = b.C
	}
	log.Println("Fetched VIXY data.")
	return nil
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
			return
		}
	}
}

func (i *Ingestor) startMonitoring(ctx context.Context) {
	log.Printf("Monitoring started.")
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			elapsed := time.Since(i.startTime).Seconds()
			log.Printf("[MONITOR] Fetchers: %d | Processors: %d | Fetched: %d/%d | Processed: %d/%d | Fetch Failures: %d | Process Failures: %d | Pending Failures: %d | Elapsed: %.2fs",
				i.Fetcher.workers.Load(), i.Processor.workers.Load(), i.fetchedSymbols.Load(), i.totalSymbols.Load(),
				i.processedSymbols.Load(), i.totalSymbols.Load(), i.fetchFailures.Load(), i.processFailures.Load(), len(i.failureQueue), elapsed)
		case <-ctx.Done():
			log.Println("Monitoring stopped.")
			return
		}
	}
}

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
