package main

import (
	"context"
	"log"
	"net/http"
	"runtime"
	"sync"
	"sort"
	"time"
	"encoding/json"
	"strings"
	"net/url"
)

// Fetcher manages data fetching from Alpaca API
type Fetcher struct {
	ingestor *Ingestor
	pool     *WorkerPool
}

// FetcherInterface defines the methods for the Fetcher
type FetcherInterface interface {
	Start(ctx context.Context, wg *sync.WaitGroup)
}

// NewFetcher initializes a new Fetcher
func NewFetcher(ingestor *Ingestor) *Fetcher {
	return &Fetcher{
		ingestor: ingestor,
		pool: NewWorkerPool("Fetcher", runtime.NumCPU(), ingestor.fetchQueue, func(ctx context.Context, id int, job interface{}) error {
			batch := job.([]string)
			log.Printf("Fetcher %d fetching batch of %d symbols", id, len(batch))
			if err := ingestor.rateLimiter.Wait(ctx); err != nil {
				log.Printf("Fetcher %d stopped due to context cancellation: %v", id, err)
				return err
			}

			dataMap, err := fetchBatch(&http.Client{}, batch, ingestor)
			if err != nil {
				for _, symbol := range batch {
					select {
					case ingestor.failureQueue <- Failure{Type: "fetch", Symbol: symbol, Error: err}:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				return err
			}

			for symbol, priceData := range dataMap {
				select {
				case ingestor.processQueue <- FetchedData{Symbol: symbol, PriceData: priceData}:
					ingestor.fetchedSymbols.Add(1)
					log.Printf("Fetcher %d sent %s with %d bars to processQueue", id, symbol, len(priceData))
				case <-ctx.Done():
					log.Printf("Fetcher %d stopped sending %s due to context cancellation", id, symbol)
					return ctx.Err()
				}
			}
			return nil
		}),
	}
}

// Start launches fetcher workers
func (f *Fetcher) Start(ctx context.Context, wg *sync.WaitGroup) {
	f.pool.Start(ctx, wg)
}

func fetchBatch(client *http.Client, symbols []string, ingestor *Ingestor) (map[string][]Bar, error) {
	symbolStr := strings.Join(symbols, ",")
	resp, err := DoRequest(RequestOptions{
		Method: "GET",
		URL:    dataURL + "stocks/bars",
		Params: url.Values{
			"symbols":    []string{symbolStr},
			"timeframe":  []string{"1Day"},
			"start":      []string{"2023-01-01"},
			"end":        []string{"2023-12-31"},
			"limit":      []string{"10000"},
			"adjustment": []string{"all"},
		},
		Headers: map[string]string{
			"APCA-API-KEY-ID":     ingestor.Cfg.AlpacaKey,
			"APCA-API-SECRET-KEY": ingestor.Cfg.AlpacaSecret,
		},
		Retry: true,
	})
	if err != nil {
		return nil, err
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
		return nil, err
	}

	dataMap := make(map[string][]Bar)
	for symbol, bars := range result.Bars {
		var priceData []Bar
		for _, b := range bars {
			ts, err := time.Parse(time.RFC3339, b.T)
			if err != nil {
				log.Printf("Skipping invalid timestamp for %s: %v", symbol, err)
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
				log.Printf("Skipping invalid bar for %s: %v", symbol, err)
				continue
			}
			priceData = append(priceData, bar)
		}
		if len(priceData) == 0 {
			log.Printf("No valid bars for %s", symbol)
			continue
		}
		sort.Slice(priceData, func(i, j int) bool {
			return priceData[i].Timestamp < priceData[j].Timestamp
		})
		dataMap[symbol] = priceData
	}

	for _, sym := range symbols {
		if _, ok := dataMap[sym]; !ok {
			log.Printf("No data returned for %s in batch", sym)
		}
	}

	return dataMap, nil
}
