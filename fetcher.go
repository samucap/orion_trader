package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
	"net/url"
	"strings"
	"encoding/json"
	"sort"

	"golang.org/x/time/rate"
)

// Fetcher manages data fetching from Alpaca API
type Fetcher struct {
	Cfg          *AppConfig
	fetchQueue   chan Job
	processQueue chan FetchedData
	failureQueue chan Failure
	workers      atomic.Int64
	rateLimiter  *rate.Limiter
}

// FetcherInterface defines the methods for the Fetcher
type FetcherInterface interface {
	Start(ctx context.Context, wg *sync.WaitGroup, ingestor *Ingestor)
}

// NewFetcher initializes a new Fetcher
func NewFetcher(cfg *AppConfig, fetchQueue chan Job, processQueue chan FetchedData, failureQueue chan Failure) *Fetcher {
	return &Fetcher{
		Cfg:          cfg,
		fetchQueue:   fetchQueue,
		processQueue: processQueue,
		failureQueue: failureQueue,
		rateLimiter:  rate.NewLimiter(rate.Every(300*time.Millisecond), 1), // 200 req/min
	}
}

// Start launches fetcher workers
func (f *Fetcher) Start(ctx context.Context, wg *sync.WaitGroup, ingestor *Ingestor) {
	const numFetchers = 10
	for i := 0; i < numFetchers; i++ {
		wg.Add(1)
		go f.worker(ctx, wg, i, ingestor)
	}
}

func (f *Fetcher) worker(ctx context.Context, wg *sync.WaitGroup, id int, ingestor *Ingestor) {
	defer wg.Done()
	f.workers.Add(1)
	defer f.workers.Add(-1)
	log.Printf("Fetcher %d started.", id)

	client := &http.Client{}
	retryClients := make(map[string]*http.Client)
	retryCounts := make(map[string]int)

	for job := range f.fetchQueue {
		log.Printf("Fetching data for %s", job.Symbol)
		if err := f.rateLimiter.Wait(ctx); err != nil {
			return
		}

		data, err := f.fetchBars(client, job.Symbol, retryClients, retryCounts)
		if err != nil {
			select {
			case f.failureQueue <- Failure{Type: "fetch", Symbol: job.Symbol, Error: err}:
			case <-ctx.Done():
				return
			}
			continue
		}

		ingestor.fetchedSymbols.Add(1)
		select {
		case f.processQueue <- data:
		case <-ctx.Done():
			return
		}
	}
	log.Printf("Fetcher %d stopped.", id)
}

func (f *Fetcher) fetchBars(client *http.Client, symbol string, retryClients map[string]*http.Client, retryCounts map[string]int) (FetchedData, error) {
	resp, err := DoRequest("GET", f.Cfg, dataURL+"stocks/bars", url.Values{
		"symbols":    {symbol},
		"timeframe":  {"1Day"},
		"start":      {"2023-01-01"},
		"end":        {"2023-12-31"},
		"limit":      {"10000"},
		"adjustment": {"all"},
	}, nil, nil, true)
	if err != nil {
		if retryCounts[symbol] < 3 && !strings.Contains(err.Error(), "status: 4") {
			retryCounts[symbol]++
			if retryClients[symbol] == nil {
				retryClients[symbol] = &http.Client{}
			}
			log.Printf("Retrying fetch for %s (attempt %d)", symbol, retryCounts[symbol])
			return f.fetchBars(retryClients[symbol], symbol, retryClients, retryCounts)
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
