package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
)

// Fetcher manages data fetching from Alpaca API
type Fetcher struct {
	ingestor *Ingestor
	pool     *WorkerPool[FetchTask]
}

// FetcherInterface defines the methods for the Fetcher
type FetcherInterface interface {
	Start(ctx context.Context, wg *sync.WaitGroup)
	FetchAssets(ctx context.Context) error
	FetchBatch(batch []string, start, end string) (map[string][]Bar, error)
	QueueDailyJobs(ctx context.Context) error
	queueJobs(ctx context.Context, start, end string)
}

func NewFetcher(ingestor *Ingestor) *Fetcher {
	newFetcher := &Fetcher{ingestor: ingestor}
	newFetcher.pool = NewWorkerPool[FetchTask]("Fetcher", runtime.NumCPU(), ingestor.fetchQueue, func(ctx context.Context, id int, job FetchTask) error {
		log.Printf("Fetcher %d fetching batch of %d symbols from %s to %s", id, len(job.Symbols), job.Start, job.End)
		if err := ingestor.rateLimiter.Wait(ctx); err != nil {
			log.Printf("Fetcher %d stopped due to context cancellation: %v", id, err)
			return err
		}

		// TODO remove dataMap, also looks like the fetchBatch inputs an error for the
		// entire batch if 1 symbol returns an error... should not be like this
		// also is there a btter way for me to setup fetchBatch or do I have to do it like this
		dataMap, err := newFetcher.FetchBatch(job.Symbols, job.Start, job.End)
		if err != nil {
			for _, symbol := range job.Symbols {
				select {
				case ingestor.failureQueue <- Failure{Type: "fetch", Symbol: symbol, Error: err}:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return err
		}

		// TODO why in the f word does it save all the data onto a datamap, only to loop through it
		// and push to processQueue???? Why not just push to processQueue at the fetchBatch... like tf
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
	})

	return newFetcher
}

// Start launches fetcher workers
func (f *Fetcher) Start(ctx context.Context, wg *sync.WaitGroup) {
	f.pool.Start(ctx, wg)
}

func (f *Fetcher) FetchAssets(ctx context.Context) error {
	log.Printf("Fetching assets from Alpaca API")
	resp, err := DoRequest(RequestOptions{
		Method: "GET",
		URL:    tradingURL + "assets",
		Params: url.Values{
			"status":      []string{"active"},
			"asset_class": []string{"us_equity"},
		},
		Headers: map[string]string{
			"APCA-API-KEY-ID":     f.ingestor.Cfg.AlpacaKey,
			"APCA-API-SECRET-KEY": f.ingestor.Cfg.AlpacaSecret,
		},
		Retry: false,
	})

	if err != nil {
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

	f.ingestor.symbols = symbols
	f.ingestor.totalSymbols.Store(int64(len(symbols)))
	log.Printf("Found %d tradable symbols: %s", len(f.ingestor.symbols), strings.Join(f.ingestor.symbols[:min(5, len(f.ingestor.symbols))], ", ")+minStr(5, len(f.ingestor.symbols)))
	//fmt.Printf("symbols==================================== %v\n", f.ingestor.symbols[:20])

	now := time.Now()
	start := now.AddDate(-10, 0, 0).Format("2006-01-02")
	end := now.Format("2006-01-02")

	if err := f.fetchVixData(start, end); err != nil {
		log.Printf("WARN: Failed to fetch historical VIXY: %v", err)
	}

	f.queueJobs(ctx, start, end)
	return nil
}

func (f *Fetcher) FetchBatch(batch []string, start, end string) (map[string][]Bar, error) {
	symbolStr := strings.Join(batch, ",")
	params := url.Values{
		"symbols":    []string{symbolStr},
		"timeframe":  []string{"1Day"},
		"start":      []string{start},
		"end":        []string{end},
		"limit":      []string{"10000"},
		"adjustment": []string{"all"},
	}

	//TODO Put this in the ingestor.cfg
	if os.Getenv("IS_FREE") == "true" {
		params["feed"] = []string{"iex"}
	}

	// TODO apparently there could be pagination that
	// needs to be handled here, along with a token that gets returned
	resp, err := DoRequest(RequestOptions{
		Method: "GET",
		URL:    dataURL + "stocks/bars",
		Params: params,
		Headers: map[string]string{
			"APCA-API-KEY-ID":     f.ingestor.Cfg.AlpacaKey,
			"APCA-API-SECRET-KEY": f.ingestor.Cfg.AlpacaSecret,
		},
		Retry: true,
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// all this body decoding needs to happen in the processor
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

	for _, sym := range f.ingestor.symbols {
		if _, ok := dataMap[sym]; !ok {
			log.Printf("No data returned for %s in batch", sym)
		}
	}

	return dataMap, nil
}

func (f *Fetcher) fetchVixData(start, end string) error {
	log.Printf("Fetching VIXY data from %s to %s", start, end)
	params := url.Values{
		"symbols":    []string{"VIXY"},
		"timeframe":  []string{"1Day"},
		"start":      []string{start},
		"end":        []string{end},
		"limit":      []string{"10000"},
		"adjustment": []string{"all"},
	}

	//TODO: Put this in the ingestor.cfg
	if os.Getenv("IS_FREE") == "true" {
		params["feed"] = []string{"iex"}
	}
	resp, err := DoRequest(RequestOptions{
		Method: "GET",
		URL:    dataURL + "stocks/bars",
		Params: params,
		Headers: map[string]string{
			"APCA-API-KEY-ID":     f.ingestor.Cfg.AlpacaKey,
			"APCA-API-SECRET-KEY": f.ingestor.Cfg.AlpacaSecret,
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

func (f *Fetcher) QueueDailyJobs(ctx context.Context) error {
	now := time.Now()
	start := now.AddDate(0, 0, -1).Format("2006-01-02")
	end := now.Format("2006-01-02")

	if err := f.fetchVixData(start, end); err != nil {
		log.Printf("WARN: Failed to fetch daily VIXY: %v", err)
	}

	f.queueJobs(ctx, start, end)
	return nil
}

func (f *Fetcher) queueJobs(ctx context.Context, start, end string) {
	const batchSize = 200
	for j := 0; j < len(f.ingestor.symbols); j += batchSize {
		endIdx := min(j+batchSize, len(f.ingestor.symbols))
		batch := f.ingestor.symbols[j:endIdx]
		select {
		case f.ingestor.fetchQueue <- FetchTask{Symbols: batch, Start: start, End: end}:
		case <-ctx.Done():
			log.Printf("Stopped queuing batches due to context cancellation")
			return
		}
	}
	fmt.Printf("Queued %d batches for fetching from %s to %s", (len(f.ingestor.symbols)+batchSize-1)/batchSize, start, end)
}
