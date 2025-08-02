# OrionTrader - Data Engineering Pipeline

OrionTrader is a robust, production-style data engineering pipeline designed to collect, process, and store financial market data for quantitative analysis and algorithmic trading models. It is written in Go and architected to be scalable, observable, and maintainable.

This service fetches historical price data, market volatility metrics (VIX proxy via VIXY), and ticker-specific news from the [Alpaca Markets](https://alpaca.markets/) API. It then calculates a suite of technical indicators and news sentiment scores, storing the final, feature-rich dataset in a MinIO object storage server, ready for consumption by machine learning models.

---

## Features

- **Concurrent Data Fetching:** Utilizes Go routines to fetch data for multiple stock tickers in parallel, with batching and rate limiting.
- **Rich Feature Set:**
  - **Technical Indicators:** Calculates SMA, EMA, RSI, MACD, and Bollinger Bands from raw price data.
  - **Market Volatility:** Fetches VIXY ETF data as a proxy for VIX index to provide market-wide volatility context.
  - **News Sentiment Analysis:** Ingests news articles and calculates a daily average sentiment score for each ticker using a simple keyword-based analyzer.
- **Rate Limiting:** Respects Alpaca's 200 requests/minute limit using a token-based limiter.
- **Object Storage:** Uses a local MinIO server to simulate a cloud object storage environment (like AWS S3), a best practice for decoupled ML systems.
- **Built-in Monitoring:**
  - Provides a live log of active goroutine counts (fetchers, processors).
  - Exposes a `pprof` endpoint (`:6060`) for advanced, real-time debugging and profiling of the application's performance and concurrency.
- **Containerized Environment:** Fully containerized with Docker and Docker Compose for one-command setup and consistent, reproducible builds.
- **Pure Go Implementation:** All data processing, including technical indicator calculation, is done with pure Go libraries, eliminating CGo dependencies for a lean and portable application.

---

## System Architecture

The system is composed of two main services managed by `docker-compose`:

1. **`app` (The Go Pipeline):** The core application responsible for the entire ETL (Extract, Transform, Load) process.
   - **Extract:** Fetches data from the Alpaca API, including bars, news, and VIXY proxy.
   - **Transform:** Parses the raw data, calculates technical indicators, aggregates news sentiment, and incorporates volatility.
   - **Load:** Uploads the final feature CSV files into the `features-market-data` bucket in MinIO.
2. **`minio` (Object Storage):** A high-performance, S3-compatible object storage server. It provides two "buckets":
   - `features-market-data`: The destination for the final, processed CSV files that will be used by the downstream model training pipeline.

---

## Prerequisites

Before you begin, ensure you have the following installed on your system:

- **[Docker](https://www.docker.com/products/docker-desktop/)**: To run the containerized application and services.
- **[Go (v1.22+)]**: For local development and to run `make` commands.
- **[Alpaca API Keys](https://app.alpaca.markets/signup)**: You will need API key and secret for market data access.

---

## Getting Started

Follow these steps to get the OrionTrader pipeline up and running.

### 1. Clone the Repository

```bash
git clone <your-repo-url>
cd <your-repo-directory>
```

## TODO

X - Go back 5 years and rolling fetches and calculations

- need to verify continuous data fetch
- For testing now you need to 'export GOEXPERIMENT=synctest'
- Need to merge all the data fetching.
- fix the DoRequest func signature, need to abstract some stuff so it's not so disorganized
- finish testing the concurrency pipelines
- NEED TO VERIFY indicators calculations
- more robust benchmarking
- change MinIO writes to be batched
