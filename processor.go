package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math"
	"runtime"
	"sync"
	"time"

	"oriontrader/indicador"

	"github.com/cinar/indicator"
	"github.com/minio/minio-go/v7"
)

// MinIOClient defines the interface for MinIO operations
type MinIOClient interface {
	StatObject(ctx context.Context, bucket, object string, opts minio.StatObjectOptions) (minio.ObjectInfo, error)
	GetObject(ctx context.Context, bucket, object string, opts minio.GetObjectOptions) (io.ReadCloser, error)
	PutObject(ctx context.Context, bucket, object string, reader io.Reader, objectSize int64, opts minio.PutObjectOptions) (minio.UploadInfo, error)
}

// Processor handles data processing and storage
type Processor struct {
	ingestor *Ingestor
	pool     *WorkerPool[FetchedData]
}

// ProcessorInterface defines the methods for the Processor
type ProcessorInterface interface {
	Start(ctx context.Context, wg *sync.WaitGroup)
}

// NewProcessor initializes a new Processor
func NewProcessor(ingestor *Ingestor) *Processor {
	return &Processor{
		ingestor: ingestor,
		pool: NewWorkerPool[FetchedData]("Processor", runtime.NumCPU(), ingestor.processQueue, func(ctx context.Context, id int, job FetchedData) error {
			log.Printf("Processor %d received data for %s with %d bars", id, job.Symbol, len(job.PriceData))
			p := Processor{ingestor: ingestor}
			if err := p.ProcessAndSave(ctx, job); err != nil {
				select {
				case ingestor.failureQueue <- Failure{Type: "process", Symbol: job.Symbol, Error: err}:
				case <-ctx.Done():
					log.Printf("Processor %d stopped sending failure for %s due to context cancellation", id, job.Symbol)
					return ctx.Err()
				}
				return err
			}
			ingestor.processedSymbols.Add(1)
			log.Printf("Processed %s. Total: %d/%d", job.Symbol, ingestor.processedSymbols.Load(), ingestor.totalSymbols.Load())
			return nil
		}),
	}
}

// Start launches processor workers
func (p *Processor) Start(ctx context.Context, wg *sync.WaitGroup) {
	p.pool.Start(ctx, wg)
}

// ProcessAndSave processes data and queues it for upload
func (p *Processor) ProcessAndSave(ctx context.Context, data FetchedData) error {
	log.Printf("Processing %s with %d bars", data.Symbol, len(data.PriceData))
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
		}
		rowsByYear[year] = append(rowsByYear[year], row)
	}

	for year, rows := range rowsByYear {
		objectName := fmt.Sprintf("%s/%s.csv", year, data.Symbol)
		var buffer bytes.Buffer
		writer := csv.NewWriter(&buffer)
		exists := false

		existingDates := make(map[string]struct{})
		_, err := p.ingestor.MinIO.StatObject(ctx, p.ingestor.Cfg.FeaturesBucketName, objectName, minio.StatObjectOptions{})
		minErr, ok := err.(*minio.ErrorResponse)

		if !ok || minErr.Code != "NoSuchKey" {
			fmt.Errorf("failed to stat object %s: %w", objectName, err)

			writer.Write([]string{
				"Date", "Open", "High", "Low", "Close", "Volume",
				"SMA20", "EMA50", "RSI14", "MACD", "MACDSignal",
				"BBUpper", "BBMiddle", "BBLower", "ATR14", "OBV",
				"ADX14", "CCI20", "StochK", "StochD", "VWAP",
				"ROC12", "CMO14", "VIX",
			})
		} else {
			obj, err := p.ingestor.MinIO.GetObject(ctx, p.ingestor.Cfg.FeaturesBucketName, objectName, minio.GetObjectOptions{})
			if err != nil {
				return fmt.Errorf("failed to get existing object %s: %w", objectName, err)
			}
			defer obj.Close()

			reader := csv.NewReader(obj)
			records, err := reader.ReadAll()
			if err != nil {
				return fmt.Errorf("failed to read existing CSV %s: %w", objectName, err)
			}

			for i, record := range records {
				if i == 0 || len(record) == 0 {
					continue
				}
				existingDates[record[0]] = struct{}{}
			}
		}

		newRows := 0
		for _, row := range rows {
			if _, exists := existingDates[row[0]]; !exists {
				writer.Write(row)
				newRows++
			}
		}

		writer.Flush()
		if err := writer.Error(); err != nil {
			return fmt.Errorf("failed to write CSV for %s: %w", objectName, err)
		}

		if newRows == 0 && exists {
			log.Printf("No new data to append for %s (all %d dates already exist)", objectName, len(rows))
			continue
		}

		select {
		case p.ingestor.uploadQueue <- UploadJob{ObjectName: objectName, Buffer: buffer}:
			log.Printf("Queued upload for %s with %d new rows", objectName, newRows)
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}
