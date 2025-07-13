package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"runtime"

	_ "net/http/pprof"

	"github.com/cinar/indicator"
	"github.com/go-playground/validator/v10"
	"github.com/joho/godotenv"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"golang.org/x/time/rate"
)

// Monitoring globals
var (
	fetchWorkers      atomic.Int64
	processWorkers    atomic.Int64
	totalSymbols      atomic.Int64
	fetchedSymbols    atomic.Int64
	processedSymbols  atomic.Int64
	fetchFailures     atomic.Int64
	processFailures   atomic.Int64
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

// Job represents a fetch task
type Job struct {
	Symbol string
}

var (
	validate     *validator.Validate
	vixMap       = make(map[string]float64) // date -> VIXY close
	tradingURL   = "https://paper-api.alpaca.markets/v2/"
	dataURL      = "https://data.alpaca.markets/v2/"
	rateLimiter  = rate.NewLimiter(rate.Every(300*time.Millisecond), 1) // 200 req/min
	fetchQueue   = make(chan Job, 1000)
	processQueue = make(chan FetchedData, 1000)
)

func main() {
	validate = validator.New(validator.WithRequiredStructEnabled())
	log.Println("Starting OrionTrader Data Pipeline...")

	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("FATAL: Could not load config: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("Shutdown signal received...")
		cancel()
	}()

	go startPprofServer()
	go startMonitoring(ctx)

	minioClient, err := minio.New(cfg.MinIOEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.MinIOAccessKey, cfg.MinIOSecretKey, ""),
		Secure: cfg.MinIOUseSSL,
	})
	if err != nil {
		log.Fatalf("FATAL: Could not connect to MinIO: %v", err)
	}
	log.Println("Connected to MinIO.")
	setupMinIOBuckets(ctx, minioClient, cfg)

	var wg sync.WaitGroup
	startProcessors(ctx, &wg, minioClient, cfg)
	startFetchers(ctx, &wg, cfg)
	fetchAssetsAndQueueJobs(ctx, cfg)

	<-ctx.Done()
	log.Println("Waiting for workers to finish...")
	close(fetchQueue)
	close(processQueue)
	wg.Wait()

	// Log final report in red
	log.Printf("\033[31m[FINAL REPORT] Total Symbols: %d | Fetched: %d | Processed: %d | Fetch Failures: %d | Process Failures: %d | Success Rate: %.2f%%\033[0m",
		totalSymbols.Load(), fetchedSymbols.Load(), processedSymbols.Load(), fetchFailures.Load(), processFailures.Load(),
		float64(processedSymbols.Load())/float64(totalSymbols.Load())*100)
}

func loadConfig() (*AppConfig, error) {
	if err := godotenv.Load(); err != nil {
		log.Println("WARN: .env not found, using env vars.")
	}
	return &AppConfig{
		MinIOEndpoint:      os.Getenv("MINIO_ENDPOINT"),
		MinIOAccessKey:     os.Getenv("MINIO_ROOT_USER"),
		MinIOSecretKey:     os.Getenv("MINIO_ROOT_PASSWORD"),
		MinIOUseSSL:        os.Getenv("MINIO_USE_SSL") == "true",
		FeaturesBucketName: "features-market-data",
		AlpacaKey:          os.Getenv("ALPACA_API_KEY"),
		AlpacaSecret:       os.Getenv("ALPACA_API_SECRET"),
	}, nil
}

func fetchAssetsAndQueueJobs(ctx context.Context, cfg *AppConfig) {
	client := &http.Client{}
	resp, err := doRequest(client, "GET", tradingURL+"assets", url.Values{
		"status":      {"active"},
		"asset_class": {"us_equity"},
	}, cfg, false)
	if err != nil {
		log.Fatalf("FATAL: Failed to fetch assets: %v", err)
	}
	defer resp.Body.Close()
	var assets []Asset
	if err := json.NewDecoder(resp.Body).Decode(&assets); err != nil {
		log.Fatalf("FATAL: Failed to decode assets: %v", err)
	}
	var symbols []string
	for _, a := range assets {
		if a.Tradable {
			symbols = append(symbols, a.Symbol)
		}
	}
	totalSymbols.Store(int64(len(symbols)))
	log.Printf("Found %d tradable symbols.", len(symbols))

	// Fetch VIXY data
	if err := fetchVixData(client, cfg); err != nil {
		log.Printf("WARN: Failed to fetch VIXY: %v", err)
	}

	// Queue symbols
	for _, s := range symbols {
		select {
		case fetchQueue <- Job{Symbol: s}:
		case <-ctx.Done():
			return
		}
	}
}

func startFetchers(ctx context.Context, wg *sync.WaitGroup, cfg *AppConfig) {
	const numFetchers = 10
	for i := 0; i < numFetchers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			fetchWorkers.Add(1)
			defer fetchWorkers.Add(-1)
			log.Printf("Fetcher %d started.", id)
			client := &http.Client{}
			retryClients := make(map[string]*http.Client)
			retryCounts := make(map[string]int)
			for job := range fetchQueue {
				if err := rateLimiter.Wait(ctx); err != nil {
					return
				}
				data, err := fetchBars(client, job.Symbol, cfg, retryClients, retryCounts)
				if err != nil {
					log.Printf("ERROR: Fetch failed for %s: %v", job.Symbol, err)
					fetchFailures.Add(1)
					continue
				}
				fetchedSymbols.Add(1)
				select {
				case processQueue <- data:
				case <-ctx.Done():
					return
				}
			}
			log.Printf("Fetcher %d stopped.", id)
		}(i)
	}
}

func startProcessors(ctx context.Context, wg *sync.WaitGroup, minioClient *minio.Client, cfg *AppConfig) {
	const numProcessors = 10
	for i := 0; i < numProcessors; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			processWorkers.Add(1)
			defer processWorkers.Add(-1)
			log.Printf("Processor %d started.", id)
			for data := range processQueue {
				if err := processAndSave(ctx, data, minioClient, cfg); err != nil {
					log.Printf("ERROR: Processing failed for %s: %v", data.Symbol, err)
					processFailures.Add(1)
					continue
				}
				processedSymbols.Add(1)
				log.Printf("Processed %s. Total: %d/%d", data.Symbol, processedSymbols.Load(), totalSymbols.Load())
			}
			log.Printf("Processor %d stopped.", id)
		}(i)
	}
}

func doRequest(client *http.Client, method, urlStr string, params url.Values, cfg *AppConfig, retry bool) (*http.Response, error) {
	u := urlStr
	if params != nil {
		u += "?" + params.Encode()
	}
	req, err := http.NewRequest(method, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("APCA-API-KEY-ID", cfg.AlpacaKey)
	req.Header.Set("APCA-API-SECRET-KEY", cfg.AlpacaSecret)
	resp, err := client.Do(req)
	if err != nil {
		if retry {
			return nil, fmt.Errorf("network error: %w", err)
		}
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if retry && resp.StatusCode >= 500 {
			return nil, fmt.Errorf("server error (status: %s): %s", resp.Status, string(body))
		}
		return nil, fmt.Errorf("request failed (status: %s): %s", resp.Status, string(body))
	}
	return resp, nil
}

func fetchVixData(client *http.Client, cfg *AppConfig) error {
	resp, err := doRequest(client, "GET", dataURL+"stocks/bars", url.Values{
		"symbols":    {"VIXY"},
		"timeframe":  {"1Day"},
		"start":      {"2023-01-01"},
		"end":        {"2023-12-31"},
		"limit":      {"10000"},
		"adjustment": {"all"},
	}, cfg, false)
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

func fetchBars(client *http.Client, symbol string, cfg *AppConfig, retryClients map[string]*http.Client, retryCounts map[string]int) (FetchedData, error) {
	resp, err := doRequest(client, "GET", dataURL+"stocks/bars", url.Values{
		"symbols":    {symbol},
		"timeframe":  {"1Day"},
		"start":      {"2023-01-01"},
		"end":        {"2023-12-31"},
		"limit":      {"10000"},
		"adjustment": {"all"},
	}, cfg, true)
	if err != nil {
		if retryCounts[symbol] < 3 && !strings.Contains(err.Error(), "status: 4") {
			retryCounts[symbol]++
			if retryClients[symbol] == nil {
				retryClients[symbol] = &http.Client{}
			}
			log.Printf("Retrying fetch for %s (attempt %d)", symbol, retryCounts[symbol])
			return fetchBars(retryClients[symbol], symbol, cfg, retryClients, retryCounts)
		}
		return FetchedData{}, err
	}
	defer resp.Body.Close()
	var result struct {
		Bars map[string][]struct {
			T string  `json:"t"`
			O float64 `json:"o"`
			H float64 `json:"h"`
			L float64 `json:"l"`
			C float64 `json:"c"`
			V float64 `json:"v"`
		} `json:"bars"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return FetchedData{}, err
	}
	bars, ok := result.Bars[symbol]
	if !ok {
		return FetchedData{}, fmt.Errorf("no data for %s", symbol)
	}
	var priceData []Bar
	for _, b := range bars {
		ts, err := time.Parse(time.RFC3339, b.T)
		if err != nil {
			continue
		}
		bar := Bar{
			Open:      b.O,
			High:      b.H,
			Low:       b.L,
			Close:     b.C,
			Volume:    b.V,
			Timestamp: ts.UnixNano(),
		}
		if err := validate.Struct(&bar); err != nil {
			continue
		}
		priceData = append(priceData, bar)
	}
	sort.Slice(priceData, func(i, j int) bool {
		return priceData[i].Timestamp < priceData[j].Timestamp
	})
	return FetchedData{Symbol: symbol, PriceData: priceData}, nil
}

func processAndSave(ctx context.Context, data FetchedData, minioClient *minio.Client, cfg *AppConfig) error {
	if len(data.PriceData) < 50 {
		return fmt.Errorf("insufficient data for %s (%d bars)", data.Symbol, len(data.PriceData))
	}

	var opens, highs, lows, closes, volumes []float64
	for _, bar := range data.PriceData {
		opens = append(opens, bar.Open)
		highs = append(highs, bar.High)
		lows = append(lows, bar.Low)
		closes = append(closes, bar.Close)
		volumes = append(volumes, bar.Volume)
	}

	for i, c := range closes {
		if math.IsNaN(c) || math.IsInf(c, 0) {
			if i > 0 {
				closes[i] = closes[i-1]
			} else {
				closes[i] = 0
			}
		}
	}

	sma20 := indicator.Sma(20, closes)
	ema50 := indicator.Ema(50, closes)
	rsi14, _ := indicator.Rsi(closes)
	macd, macdSignal := indicator.Macd(closes)
	bbUpper, bbMiddle, bbLower := indicator.BollingerBands(closes)
	atr, _ := indicator.Atr(14, highs, lows, closes)
	obv := indicator.Obv(closes, volumes)
	adx := padIndicator(Adx(14, highs, lows, closes), len(closes), 14)
	cci := padIndicator(Cci(20, highs, lows, closes), len(closes), 20)
	stochK, stochD := indicator.StochasticOscillator(highs, lows, closes)
	vwap := Vwap(closes, volumes)
	roc := padIndicator(Roc(12, closes), len(closes), 12)
	cmo := padIndicator(Cmo(14, closes), len(closes), 14)

	var rows [][]string
	for i, bar := range data.PriceData {
		date := time.Unix(0, bar.Timestamp).Format("2006-01-02")
		row := []string{
			date,
			strconv.FormatFloat(bar.Open, 'f', 2, 64),
			strconv.FormatFloat(bar.High, 'f', 2, 64),
			strconv.FormatFloat(bar.Low, 'f', 2, 64),
			strconv.FormatFloat(bar.Close, 'f', 2, 64),
			strconv.FormatFloat(bar.Volume, 'f', 0, 64),
			formatFloat(sma20[i]),
			formatFloat(ema50[i]),
			formatFloat(rsi14[i]),
			formatFloat(macd[i]),
			formatFloat(macdSignal[i]),
			formatFloat(bbUpper[i]),
			formatFloat(bbMiddle[i]),
			formatFloat(bbLower[i]),
			formatFloat(atr[i]),
			formatFloat(obv[i]),
			formatFloat(adx[i]),
			formatFloat(cci[i]),
			formatFloat(stochK[i]),
			formatFloat(stochD[i]),
			formatFloat(vwap[i]),
			formatFloat(roc[i]),
			formatFloat(cmo[i]),
			formatFloat(vixMap[date]),
			//formatFloat(data.Sentiments[date]),
		}
		rows = append(rows, row)
	}

	objectName := fmt.Sprintf("%s/historical-features.csv", data.Symbol)
	var buffer bytes.Buffer
	writer := csv.NewWriter(&buffer)
	exists := false
	if _, err := minioClient.StatObject(ctx, cfg.FeaturesBucketName, objectName, minio.StatObjectOptions{}); err == nil {
		exists = true
		obj, err := minioClient.GetObject(ctx, cfg.FeaturesBucketName, objectName, minio.GetObjectOptions{})
		if err != nil {
			return err
		}
		defer obj.Close()
		_, err = io.Copy(&buffer, obj)
		if err != nil {
			return err
		}
	} else if minio.ToErrorResponse(err).Code != "NoSuchKey" {
		return err
	}
	if !exists {
		writer.Write([]string{"Date", "Open", "High", "Low", "Close", "Volume", "SMA20", "EMA50", "RSI14", "MACD", "MACDSignal", "BBUpper", "BBMiddle", "BBLower", "ATR14", "OBV", "ADX14", "CCI20", "StochK", "StochD", "VWAP", "ROC12", "CMO14", "VIX" /*, "Sentiment"*/})
	}
	for _, row := range rows {
		writer.Write(row)
	}
	writer.Flush()
	_, err := minioClient.PutObject(ctx, cfg.FeaturesBucketName, objectName, &buffer, int64(buffer.Len()), minio.PutObjectOptions{ContentType: "text/csv"})
	return err
}

// Monitoring & Helpers
func startMonitoring(ctx context.Context) {
	log.Printf("Monitoring started. CPUs: %d", runtime.NumCPU())
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			log.Printf("[MONITOR] Fetchers: %d | Processors: %d | Fetched: %d/%d | Processed: %d/%d | Fetch Failures: %d | Process Failures: %d",
				fetchWorkers.Load(), processWorkers.Load(), fetchedSymbols.Load(), totalSymbols.Load(), processedSymbols.Load(), totalSymbols.Load(), fetchFailures.Load(), processFailures.Load())
		case <-ctx.Done():
			log.Println("Monitoring stopped.")
			return
		}
	}
}

func startPprofServer() {
	log.Println("pprof server on :6060")
	if err := http.ListenAndServe(":6060", nil); err != nil {
		log.Fatalf("FATAL: pprof failed: %v", err)
	}
}

func setupMinIOBuckets(ctx context.Context, client *minio.Client, cfg *AppConfig) {
	err := client.MakeBucket(ctx, cfg.FeaturesBucketName, minio.MakeBucketOptions{})
	if err != nil {
		if exists, err := client.BucketExists(ctx, cfg.FeaturesBucketName); err == nil && exists {
			log.Printf("Bucket '%s' exists.", cfg.FeaturesBucketName)
		} else {
			log.Fatalf("FATAL: Bucket setup failed: %v", err)
		}
	} else {
		log.Printf("Created bucket '%s'.", cfg.FeaturesBucketName)
	}
}

func formatFloat(f float64) string {
	if f == 0.0 {
		return ""
	}
	return strconv.FormatFloat(f, 'f', 2, 64)
}

// Indicator Helpers
func Vwap(closes, volumes []float64) []float64 {
	if len(closes) != len(volumes) {
		return nil
	}
	vwap := make([]float64, len(closes))
	var cumVolume, cumPriceVolume float64
	for i := 0; i < len(closes); i++ {
		cumPriceVolume += closes[i] * volumes[i]
		cumVolume += volumes[i]
		if cumVolume == 0 {
			vwap[i] = 0
		} else {
			vwap[i] = cumPriceVolume / cumVolume
		}
	}
	return vwap
}

func Roc(period int, closes []float64) []float64 {
	if len(closes) < period {
		return nil
	}
	roc := make([]float64, len(closes))
	for i := period; i < len(closes); i++ {
		if closes[i-period] == 0 {
			roc[i] = 0
		} else {
			roc[i] = (closes[i] - closes[i-period]) / closes[i-period] * 100
		}
	}
	return roc
}

func Cmo(period int, closes []float64) []float64 {
	if len(closes) < period+1 {
		return nil
	}
	cmo := make([]float64, len(closes))
	for i := period; i < len(closes); i++ {
		var upSum, downSum float64
		for j := 1; j <= period; j++ {
			change := closes[i-j+1] - closes[i-j]
			if change > 0 {
				upSum += change
			} else {
				downSum += math.Abs(change)
			}
		}
		if upSum+downSum == 0 {
			cmo[i] = 0
		} else {
			cmo[i] = (upSum - downSum) / (upSum + downSum) * 100
		}
	}
	return cmo
}

func Adx(period int, highs, lows, closes []float64) []float64 {
	if len(highs) != len(lows) || len(lows) != len(closes) || len(closes) < period+1 {
		return nil
	}
	adx := make([]float64, len(closes))
	tr := make([]float64, len(closes))
	plusDM := make([]float64, len(closes))
	minusDM := make([]float64, len(closes))
	for i := 1; i < len(closes); i++ {
		dh := highs[i] - highs[i-1]
		dl := lows[i-1] - lows[i]
		plusDM[i] = math.Max(dh, 0)
		if dh > dl {
			minusDM[i] = 0
		} else {
			minusDM[i] = math.Max(dl, 0)
		}
		tr[i] = math.Max(highs[i]-lows[i], math.Max(math.Abs(highs[i]-closes[i-1]), math.Abs(lows[i]-closes[i-1])))
	}
	plusDI := indicator.Ema(period, indicator.Sma(period, plusDM)[period-1:])
	minusDI := indicator.Ema(period, indicator.Sma(period, minusDM)[period-1:])
	atr := indicator.Ema(period, indicator.Sma(period, tr)[period-1:])
	for i := period; i < len(closes); i++ {
		dx := 0.0
		if atr[i-period+1] != 0 {
			dx = math.Abs(plusDI[i-period+1]-minusDI[i-period+1]) / (plusDI[i-period+1]+minusDI[i-period+1]) * 100
		}
		adx[i] = dx
	}
	return indicator.Ema(period, adx[period:])
}

func Cci(period int, highs, lows, closes []float64) []float64 {
	if len(highs) != len(lows) || len(lows) != len(closes) || len(closes) < period {
		return nil
	}
	cci := make([]float64, len(closes))
	typPrices := make([]float64, len(closes))
	for i := 0; i < len(closes); i++ {
		typPrices[i] = (highs[i] + lows[i] + closes[i]) / 3
	}
	smaTyp := indicator.Sma(period, typPrices)
	for i := period - 1; i < len(closes); i++ {
		meanDev := 0.0
		for j := i - period + 1; j <= i; j++ {
			meanDev += math.Abs(typPrices[j] - smaTyp[i])
		}
		meanDev /= float64(period)
		if meanDev == 0 {
			cci[i] = 0
		} else {
			cci[i] = (typPrices[i] - smaTyp[i]) / (0.015 * meanDev)
		}
	}
	return cci
}

func padIndicator(ind []float64, fullLen, period int) []float64 {
	if len(ind) == 0 {
		return make([]float64, fullLen)
	}
	padded := make([]float64, fullLen)
	copy(padded[fullLen-len(ind):], ind)
	return padded
}
