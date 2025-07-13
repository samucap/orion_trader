package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"oriontrader/indicador"

	"github.com/cinar/indicator"
	"github.com/minio/minio-go/v7"
)

// MinIOClient defines the interface for MinIO operations
type MinIOClient interface {
	StatObject(ctx context.Context, bucket, object string, opts minio.StatObjectOptions) (minio.ObjectInfo, error)
	GetObject(ctx context.Context, bucket, object string, opts minio.GetObjectOptions) (*minio.Object, error)
	PutObject(ctx context.Context, bucket, object string, reader io.Reader, objectSize int64, opts minio.PutObjectOptions) (minio.UploadInfo, error)
}

// Processor handles data processing and storage
type Processor struct {
	Cfg          *AppConfig
	MinIO        MinIOClient
	processQueue chan FetchedData
	failureQueue chan Failure
	workers      atomic.Int64
}

// ProcessorInterface defines the methods for the Processor
type ProcessorInterface interface {
	Start(ctx context.Context, wg *sync.WaitGroup, ingestor *Ingestor)
}

// NewProcessor initializes a new Processor
func NewProcessor(cfg *AppConfig, minioClient MinIOClient, processQueue chan FetchedData, failureQueue chan Failure) *Processor {
	return &Processor{
		Cfg:          cfg,
		MinIO:        minioClient,
		processQueue: processQueue,
		failureQueue: failureQueue,
	}
}

// Start launches processor workers
func (p *Processor) Start(ctx context.Context, wg *sync.WaitGroup, ingestor *Ingestor) {
	const numProcessors = 10
	for i := 0; i < numProcessors; i++ {
		wg.Add(1)
		go p.worker(ctx, wg, i, ingestor)
	}
}

func (p *Processor) worker(ctx context.Context, wg *sync.WaitGroup, id int, ingestor *Ingestor) {
	defer wg.Done()
	p.workers.Add(1)
	defer p.workers.Add(-1)
	log.Printf("Processor %d started.", id)

	for data := range p.processQueue {
		if err := p.processAndSave(ctx, data); err != nil {
			select {
			case p.failureQueue <- Failure{Type: "process", Symbol: data.Symbol, Error: err}:
			case <-ctx.Done():
				return
			}
			continue
		}
		ingestor.processedSymbols.Add(1)
		log.Printf("Processed %s. Total: %d/%d", data.Symbol, ingestor.processedSymbols.Load(), ingestor.totalSymbols.Load())
	}
	log.Printf("Processor %d stopped.", id)
}

func (p *Processor) processAndSave(ctx context.Context, data FetchedData) error {
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
	adx := indicador.PadIndicator(indicador.Adx(14, highs, lows, closes), len(closes), 14)
	cci := indicador.PadIndicator(indicador.Cci(20, highs, lows, closes), len(closes), 20)
	stochK, stochD := indicator.StochasticOscillator(highs, lows, closes)
	vwap := indicador.Vwap(closes, volumes)
	roc := indicador.PadIndicator(indicador.Roc(12, closes), len(closes), 12)
	cmo := indicador.PadIndicator(indicador.Cmo(14, closes), len(closes), 14)

	// Group bars by year
	rowsByYear := make(map[string][][]string)
	for i, bar := range data.PriceData {
		year := time.Unix(0, bar.Timestamp).Format("2006")
		date := time.Unix(0, bar.Timestamp).Format("2006-01-02")
		row := []string{
			date,
			fmt.Sprintf("%.2f", bar.Open),
			fmt.Sprintf("%.2f", bar.High),
			fmt.Sprintf("%.2f", bar.Low),
			fmt.Sprintf("%.2f", bar.Close),
			fmt.Sprintf("%.0f", bar.Volume),
			indicador.FormatFloat(sma20[i]),
			indicador.FormatFloat(ema50[i]),
			indicador.FormatFloat(rsi14[i]),
			indicador.FormatFloat(macd[i]),
			indicador.FormatFloat(macdSignal[i]),
			indicador.FormatFloat(bbUpper[i]),
			indicador.FormatFloat(bbMiddle[i]),
			indicador.FormatFloat(bbLower[i]),
			indicador.FormatFloat(atr[i]),
			indicador.FormatFloat(obv[i]),
			indicador.FormatFloat(adx[i]),
			indicador.FormatFloat(cci[i]),
			indicador.FormatFloat(stochK[i]),
			indicador.FormatFloat(stochD[i]),
			indicador.FormatFloat(vwap[i]),
			indicador.FormatFloat(roc[i]),
			indicador.FormatFloat(cmo[i]),
			indicador.FormatFloat(vixMap[date]),
			//indicador.FormatFloat(data.Sentiments[date]),
		}
		rowsByYear[year] = append(rowsByYear[year], row)
	}

	// Process each year's data
	for year, rows := range rowsByYear {
		objectName := fmt.Sprintf("%s/%s/features.csv", data.Symbol, year)
		var buffer bytes.Buffer
		writer := csv.NewWriter(&buffer)

		// Check for existing file
		existingDates := make(map[string]struct{})
		exists := false
		if _, err := p.MinIO.StatObject(ctx, p.Cfg.FeaturesBucketName, objectName, minio.StatObjectOptions{}); err == nil {
			exists = true
			obj, err := p.MinIO.GetObject(ctx, p.Cfg.FeaturesBucketName, objectName, minio.GetObjectOptions{})
			if err != nil {
				return fmt.Errorf("failed to get existing object %s/%s: %w", data.Symbol, year, err)
			}
			defer obj.Close()

			reader := csv.NewReader(obj)
			records, err := reader.ReadAll()
			if err != nil {
				return fmt.Errorf("failed to read existing CSV %s/%s: %w", data.Symbol, year, err)
			}

			// Skip header and collect existing dates
			for i, record := range records {
				if i == 0 || len(record) == 0 {
					continue
				}
				existingDates[record[0]] = struct{}{}
			}
		} else if minio.ToErrorResponse(err).Code != "NoSuchKey" {
			return fmt.Errorf("failed to stat object %s/%s: %w", data.Symbol, year, err)
		}

		// Write header if new file
		if !exists {
			writer.Write([]string{
				"Date", "Open", "High", "Low", "Close", "Volume",
				"SMA20", "EMA50", "RSI14", "MACD", "MACDSignal",
				"BBUpper", "BBMiddle", "BBLower", "ATR14", "OBV",
				"ADX14", "CCI20", "StochK", "StochD", "VWAP",
				"ROC12", "CMO14", "VIX" /*, "Sentiment"*/,
			})
		}

		// Write only new rows
		newRows := 0
		for _, row := range rows {
			if _, exists := existingDates[row[0]]; !exists {
				writer.Write(row)
				newRows++
			}
		}
		writer.Flush()

		if err := writer.Error(); err != nil {
			return fmt.Errorf("failed to write CSV for %s/%s: %w", data.Symbol, year, err)
		}

		// Skip upload if no new rows
		if newRows == 0 && exists {
			log.Printf("No new data to append for %s/%s (all %d dates already exist)", data.Symbol, year, len(rows))
			continue
		}

		_, err := p.MinIO.PutObject(ctx, p.Cfg.FeaturesBucketName, objectName, &buffer, int64(buffer.Len()), minio.PutObjectOptions{ContentType: "text/csv"})
		if err != nil {
			return fmt.Errorf("failed to upload %s/%s: %w", data.Symbol, year, err)
		}
		log.Printf("Appended %d new rows for %s/%s", newRows, data.Symbol, year)
	}

	return nil
}
