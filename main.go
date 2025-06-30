package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof" // Import pprof for side effects: it registers handlers on the default serve mux
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	//"github.com/cinar/indicator/v2/trend" // Correctly referencing v2
	"github.com/joho/godotenv"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// --- Globals for Goroutine Monitoring ---
var (
	fetcherGoroutines   atomic.Int64
	processorGoroutines atomic.Int64
)

// AppConfig holds the configuration for the application.
type AppConfig struct {
	PolygonAPIKey      string
	MinIOEndpoint      string
	MinIOAccessKey     string
	MinIOSecretKey     string
	MinIOUseSSL        bool
	RawDataBucketName  string
	FeaturesBucketName string
}

// PolygonBar represents a single bar from the Polygon.io Aggregates API.
type PolygonBar struct {
	Open      float64 `json:"o"`
	High      float64 `json:"h"`
	Low       float64 `json:"l"`
	Close     float64 `json:"c"`
	Volume    float64 `json:"v"`
	Timestamp int64   `json:"t"` // Unix Msec
}

// PriceDataResponse is the structure of the JSON response from Polygon.io for prices.
type PriceDataResponse struct {
	Ticker  string       `json:"ticker"`
	Results []PolygonBar `json:"results"`
	Status  string       `json:"status"`
}

// NewsArticle represents a single news article from the Polygon.io News API.
type NewsArticle struct {
	Title      string `json:"title"`
	Published  string `json:"published_utc"`
	ArticleURL string `json:"article_url"`
	Sentiment  struct {
		Polarity float64 `json:"polarity"`
	} `json:"sentiment"`
}

// NewsResponse is the structure for the news API call.
type NewsResponse struct {
	Results []NewsArticle `json:"results"`
	NextURL string        `json:"next_url"`
}

// FetchedData is expanded to hold all the raw data for a ticker before processing.
type FetchedData struct {
	Symbol    string
	PriceData PriceDataResponse
	NewsData  []NewsArticle
}

func main() {
	log.Println("Starting OrionTrader Data Engineering Pipeline...")

	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("FATAL: Could not load configuration: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("Shutdown signal received. Cleaning up...")
		cancel()
	}()

	go startPprofServer()
	go startMonitoring(ctx)

	rawDataChan := make(chan FetchedData, 100)
	var wg sync.WaitGroup

	minioClient, err := minio.New(cfg.MinIOEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.MinIOAccessKey, cfg.MinIOSecretKey, ""),
		Secure: cfg.MinIOUseSSL,
	})
	if err != nil {
		log.Fatalf("FATAL: Could not connect to MinIO: %v", err)
	}
	log.Println("Successfully connected to MinIO.")
	setupMinIOBuckets(ctx, minioClient, cfg)

	wg.Add(1)
	//go processData(ctx, &wg, rawDataChan, minioClient, cfg)

	wg.Add(1)
	go fetchTickers(ctx, &wg, rawDataChan, cfg)

	<-ctx.Done()

	log.Println("Waiting for all workers to finish...")
	close(rawDataChan)
	wg.Wait()
	log.Println("OrionTrader pipeline shutdown complete.")
}

func loadConfig() (*AppConfig, error) {
	if err := godotenv.Load(); err != nil {
		log.Println("WARN: .env file not found, relying on environment variables.")
	}
	return &AppConfig{
		PolygonAPIKey:      os.Getenv("POLYGON_API_KEY"),
		MinIOEndpoint:      os.Getenv("MINIO_ENDPOINT"),
		MinIOAccessKey:     os.Getenv("MINIO_ROOT_USER"),
		MinIOSecretKey:     os.Getenv("MINIO_ROOT_PASSWORD"),
		MinIOUseSSL:        os.Getenv("MINIO_USE_SSL") == "true",
		RawDataBucketName:  "raw-market-data",
		FeaturesBucketName: "features-market-data",
	}, nil
}

// fetchTickers now orchestrates fetching prices, news, and the VIX index.
func fetchTickers(ctx context.Context, wg *sync.WaitGroup, rawDataChan chan<- FetchedData, cfg *AppConfig) {
	defer wg.Done()
	log.Println("Starting data fetcher with rate limiting (5 calls/minute)...")

	tickers := []string{"SPY", "QQQ", "AAPL", "MSFT", "GOOG", "NVDA", "TSLA"}
	var fetchWg sync.WaitGroup
	client := &http.Client{Timeout: 30 * time.Second}

	rateLimiter := time.NewTicker(12 * time.Second)
	defer rateLimiter.Stop()

	from := "2024-10-01"
	to := "2024-12-31"
	vixPriceData, err := fetchVixData(ctx, rateLimiter, client, cfg, from, to)
	if err != nil {
		log.Printf("FATAL: Could not fetch VIX data, aborting pipeline: %v", err)
		return
	}

	// Send VIX data FIRST to ensure it's processed before any tickers.
	log.Println("Sending VIX data to processor...")
	vixAsPriceData := PriceDataResponse{Ticker: "VIX", Results: vixPriceData}
	rawDataChan <- FetchedData{Symbol: "VIX_DATA_SIGNAL", PriceData: vixAsPriceData}

	for _, ticker := range tickers {
		fetchWg.Add(1)
		go func(t string) {
			fetcherGoroutines.Add(1)
			defer fetcherGoroutines.Add(-1)
			defer fetchWg.Done()

			<-rateLimiter.C
			log.Printf("Fetching price data for %s...", t)
			priceData, err := fetchPriceData(ctx, client, cfg, t, from, to)
			if err != nil {
				log.Printf("ERROR: Failed to fetch price data for %s: %v", t, err)
				return
			}
			if len(priceData.Results) == 0 {
				log.Printf("WARN: No price data returned for ticker %s", t)
				return
			}

			<-rateLimiter.C
			log.Printf("Fetching news data for %s...", t)
			newsData, err := fetchNewsData(ctx, client, cfg, t, from, to)
			if err != nil {
				log.Printf("WARN: Could not fetch news data for %s: %v", t, err)
			}

			dataPayload := FetchedData{
				Symbol:    t,
				PriceData: *priceData,
				NewsData:  newsData,
			}

			select {
			case <-ctx.Done():
				log.Printf("Fetcher shutting down while preparing data for %s.", t)
				return
			case rawDataChan <- dataPayload:
				log.Printf("Successfully fetched and queued all data for %s", t)
			}
		}(ticker)
	}

	fetchWg.Wait()
	log.Println("Fetcher finished its run.")
}

// processData now integrates VIX and News Sentiment into the feature CSV.
func processData(ctx context.Context, wg *sync.WaitGroup, rawDataChan <-chan FetchedData, minioClient *minio.Client, cfg *AppConfig) {
	defer wg.Done()
	log.Println("Starting data processor...")

	var processWg sync.WaitGroup
	vixDataMap := make(map[string]float64)
	var vixMutex sync.RWMutex // Mutex to protect the VIX map

	for fetchedData := range rawDataChan {
		if fetchedData.Symbol == "VIX_DATA_SIGNAL" {
			log.Println("Processor received VIX data.")
			vixMutex.Lock() // Lock for writing
			for _, bar := range fetchedData.PriceData.Results {
				date := time.Unix(bar.Timestamp/1000, 0).Format("2006-01-02")
				vixDataMap[date] = bar.Close
			}
			vixMutex.Unlock()
			continue
		}

		processWg.Add(1)
		go func(data FetchedData) {
			processorGoroutines.Add(1)
			defer processorGoroutines.Add(-1)
			defer processWg.Done()

			log.Printf("Processing and feature engineering for %s...", data.Symbol)
			bars := data.PriceData.Results
			if len(bars) < 50 { // Increased lookback for EMA50
				log.Printf("WARN: Skipping %s due to insufficient data (%d bars) for EMA50", data.Symbol, len(bars))
				return
			}

			newsSentimentMap := make(map[string]float64)
			dailyScores := make(map[string][]float64)
			for _, article := range data.NewsData {
				// Use RFC3339Nano for more robust time parsing from Polygon
				parsedTime, err := time.Parse(time.RFC3339Nano, article.Published)
				if err != nil {
					continue
				}
				dateStr := parsedTime.Format("2006-01-02")
				dailyScores[dateStr] = append(dailyScores[dateStr], article.Sentiment.Polarity)
			}
			for date, scores := range dailyScores {
				var sum float64
				for _, score := range scores {
					sum += score
				}
				newsSentimentMap[date] = sum / float64(len(scores))
			}

			var closePrices []float64
			for _, bar := range bars {
				closePrices = append(closePrices, bar.Close)
			}

			// *** FIX: Corrected function calls to match `cinar/indicator/v2` API ***
			//sma20 := trend.SMA20(20, closePrices)
			//ema50 := trend.EMA50(50, closePrices)
			//rsi14 := trend.RSI14(14, closePrices)
			//macd, macdSignal := trend.MACDSignal(12, 26, 9, closePrices)
			//bbUpper, bbMiddle, bbLower := trend.BBUpper(20, closePrices), trend.BBMiddle(20, closePrices), trend.BBLower(20, closePrices)

			var buffer bytes.Buffer
			writer := csv.NewWriter(&buffer)
			newHeader := []string{"Date", "Open", "High", "Low", "Close", "Volume", "VIX_Close", "News_Sentiment_Avg"}
			writer.Write(newHeader)

			for _, bar := range bars {
				dateStr := time.Unix(bar.Timestamp/1000, 0).Format("2006-01-02")

				vixMutex.RLock() // Read lock for safety when accessing the map from concurrent goroutines
				vixClose := formatFloat(vixDataMap[dateStr])
				vixMutex.RUnlock()

				newsSentiment := formatFloat(newsSentimentMap[dateStr])

				row := []string{
					dateStr,
					strconv.FormatFloat(bar.Open, 'f', 2, 64),
					strconv.FormatFloat(bar.High, 'f', 2, 64),
					strconv.FormatFloat(bar.Low, 'f', 2, 64),
					strconv.FormatFloat(bar.Close, 'f', 2, 64),
					strconv.FormatFloat(bar.Volume, 'f', 0, 64),
					//formatFloat(sma20[i]),
					//formatFloat(ema50[i]),
					//formatFloat(rsi14[i]),
					//formatFloat(macd[i]),
					//formatFloat(macdSignal[i]),
					//formatFloat(bbUpper[i]),
					//formatFloat(bbMiddle[i]),
					//formatFloat(bbLower[i]),
					vixClose,
					newsSentiment,
				}
				writer.Write(row)
			}
			writer.Flush()

			objectName := fmt.Sprintf("%s/%s-features.csv", data.Symbol, time.Now().Format("2006-01-02"))
			_, err := minioClient.PutObject(ctx, cfg.FeaturesBucketName, objectName, &buffer, int64(buffer.Len()), minio.PutObjectOptions{ContentType: "text/csv"})
			if err != nil {
				log.Printf("ERROR: Failed to upload features for %s: %v", data.Symbol, err)
				return
			}
			log.Printf("Successfully uploaded features for %s to MinIO.", data.Symbol)
		}(fetchedData)
	}

	processWg.Wait()
	log.Println("Processor finished handling all data.")
}

// --- Monitoring & Diagnostics (Unchanged) ---
func startMonitoring(ctx context.Context) {
	log.Printf("Starting goroutine monitor. Logical CPUs available: %d", runtime.GOMAXPROCS(0))
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			log.Printf("[MONITOR] Total Goroutines: %d | Fetchers: %d | Processors: %d", runtime.NumGoroutine(), fetcherGoroutines.Load(), processorGoroutines.Load())
		case <-ctx.Done():
			log.Println("Stopping goroutine monitor.")
			return
		}
	}
}

func startPprofServer() {
	log.Println("Starting pprof server on :6060. Access http://localhost:6060/debug/pprof/")
	if err := http.ListenAndServe(":6060", nil); err != nil {
		log.Fatalf("FATAL: pprof server failed to start: %v", err)
	}
}

// --- Helper & Fetcher Functions ---
func fetchPriceData(ctx context.Context, client *http.Client, cfg *AppConfig, ticker, from, to string) (*PriceDataResponse, error) {
	url := fmt.Sprintf("https://api.polygon.io/v2/aggs/ticker/%s/range/1/day/%s/%s?adjusted=true&sort=asc&apiKey=%s", ticker, from, to, cfg.PolygonAPIKey)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("received non-200 status: %s. Body: %s", resp.Status, string(body))
	}

	var data PriceDataResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, fmt.Errorf("failed to decode JSON: %w", err)
	}
	return &data, nil
}

func fetchVixData(ctx context.Context, rateLimiter *time.Ticker, client *http.Client, cfg *AppConfig, from, to string) ([]PolygonBar, error) {
	log.Println("Fetching VIX data...")
	<-rateLimiter.C
	vixData, err := fetchPriceData(ctx, client, cfg, "I:VIX", from, to)
	if err != nil {
		return nil, err
	}
	log.Printf("Successfully fetched %d data points for VIX.", len(vixData.Results))
	return vixData.Results, nil
}

func fetchNewsData(ctx context.Context, client *http.Client, cfg *AppConfig, ticker, from, to string) ([]NewsArticle, error) {
	var allArticles []NewsArticle
	url := fmt.Sprintf("https://api.polygon.io/v2/reference/news?ticker=%s&published_utc.gte=%s&published_utc.lte=%s&limit=100&apiKey=%s", ticker, from, to, cfg.PolygonAPIKey)

	for url != "" {
		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return nil, err
		}

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return nil, fmt.Errorf("news API returned non-200 status: %s", resp.Status)
		}

		var newsResp NewsResponse
		if err := json.NewDecoder(resp.Body).Decode(&newsResp); err != nil {
			resp.Body.Close()
			return nil, err
		}
		resp.Body.Close()

		allArticles = append(allArticles, newsResp.Results...)
		url = newsResp.NextURL
		if url != "" {
			url += "&apiKey=" + cfg.PolygonAPIKey
		}
	}
	return allArticles, nil
}

func setupMinIOBuckets(ctx context.Context, client *minio.Client, cfg *AppConfig) {
	buckets := []string{cfg.RawDataBucketName, cfg.FeaturesBucketName}
	for _, bucket := range buckets {
		err := client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{})
		if err != nil {
			exists, errBucketExists := client.BucketExists(ctx, bucket)
			if errBucketExists == nil && exists {
				log.Printf("Bucket '%s' already exists.", bucket)
			} else {
				log.Fatalf("FATAL: Could not create or verify bucket '%s': %v", bucket, err)
			}
		} else {
			log.Printf("Successfully created bucket '%s'.", bucket)
		}
	}
}

func formatFloat(f float64) string {
	if f == 0.0 {
		return ""
	}
	return strconv.FormatFloat(f, 'f', 2, 64)
}
