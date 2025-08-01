package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"net/url"

	"github.com/joho/godotenv"
	"golang.org/x/time/rate"
)

// Ingestor manages the data ingestion pipeline
type Ingestor struct {
	Cfg              *AppConfig
	MinIO            MinIOClient
	fetchQueue       chan []string // Batch of symbols
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
		Cfg:              cfg,
		MinIO:            minioClient,
		fetchQueue:       make(chan []string, 100),
		processQueue:     make(chan FetchedData, 1000),
		uploadQueue:      make(chan UploadJob, 1000),
		failureQueue:     make(chan Failure, 1000),
		rateLimiter:      rate.NewLimiter(rate.Every(300*time.Millisecond), 1),
		startTime:        time.Now(),
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
	if err := i.fetchAssetsAndQueueJobs(ctx); err != nil {
		return fmt.Errorf("failed to fetch assets: %w", err)
	}

	<-ctx.Done()
	log.Println("Waiting for workers to finish...")
	close(i.fetchQueue)
	close(i.processQueue)
	close(i.uploadQueue)
	close(i.failureQueue)
	wg.Wait()

	elapsed := time.Since(i.startTime).Seconds()
	log.Printf("\033[31m[FINAL REPORT] Total Symbols: %d | Fetched: %d | Processed: %d | Fetch Failures: %d | Process Failures: %d | Pending Failures: %d | Success Rate: %.2f%% | Time Elapsed: %.2fs\033[0m",
		i.totalSymbols.Load(), i.fetchedSymbols.Load(), i.processedSymbols.Load(), i.fetchFailures.Load(), i.processFailures.Load(),
		len(i.failureQueue), float64(i.processedSymbols.Load())/float64(i.totalSymbols.Load())*100, elapsed)

	return nil
}

func (i *Ingestor) fetchAssetsAndQueueJobs(ctx context.Context) error {
	log.Printf("Fetching assets from Alpaca API")
	resp, err := DoRequest(RequestOptions{
		Method: "GET",
		URL:    tradingURL + "assets",
		Params: url.Values{
			"status":      []string{"active"},
			"asset_class": []string{"us_equity"},
		},
		Headers: map[string]string{
			"APCA-API-KEY-ID":     i.Cfg.AlpacaKey,
			"APCA-API-SECRET-KEY": i.Cfg.AlpacaSecret,
		},
		Retry: false,
	})
	if err != nil {
		log.Printf("Failed to fetch assets: %v", err)
		return err
	}
	defer resp.Body.Close()
	log.Printf("Received assets response: %s", resp.Status)

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

	if err := i.fetchVixData(); err != nil {
		log.Printf("WARN: Failed to fetch VIXY: %v", err)
	}

	const batchSize = 200
	for j := 0; j < len(symbols); j += batchSize {
		end := min(j+batchSize, len(symbols))
		batch := symbols[j:end]
		select {
		case i.fetchQueue <- batch:
		case <-ctx.Done():
			log.Printf("Stopped queuing batches due to context cancellation")
			return ctx.Err()
		}
	}
	log.Printf("Queued %d batches for fetching", (len(symbols)+batchSize-1)/batchSize)
	return nil
}

func (i *Ingestor) fetchVixData() error {
	log.Printf("Fetching VIXY data from Alpaca API")
	resp, err := DoRequest(RequestOptions{
		Method: "GET",
		URL:    dataURL + "stocks/bars",
		Params: url.Values{
			"symbols":    []string{"VIXY"},
			"timeframe":  []string{"1Day"},
			"start":      []string{"2023-01-01"},
			"end":        []string{"2023-12-31"},
			"limit":      []string{"10000"},
			"adjustment": []string{"all"},
		},
		Headers: map[string]string{
			"APCA-API-KEY-ID":     i.Cfg.AlpacaKey,
			"APCA-API-SECRET-KEY": i.Cfg.AlpacaSecret,
		},
		Retry: false,
	})
	if err != nil {
		log.Printf("Failed to fetch VIXY data: %v", err)
		return err
	}
	defer resp.Body.Close()
	log.Printf("Received VIXY response: %s", resp.Status)

	var result struct {
		Bars map[string][]struct {
			T string  `json:"t"`
			C float64 `json:"c"`
		} `json:"bars"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("failed to decode VIXY data: %w", err)
	}

	for _, b := range result.Bars["VIXY"] {
		ts, err := time.Parse(time.RFC3339, b.T)
		if err != nil {
			log.Printf("Skipping invalid VIXY timestamp %s: %v", b.T, err)
			continue
		}
		vixMap[ts.Format("2006-01-02")] = b.C
	}
	log.Printf("Fetched %d VIXY data points", len(result.Bars["VIXY"]))
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
