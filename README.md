# OrionTrader - Data Engineering Pipeline

OrionTrader is a robust, production-style data engineering pipeline designed to collect, process, and store financial market data for quantitative analysis and algorithmic trading models. It is written in Go and architected to be scalable, observable, and maintainable.

This service fetches historical price data, market volatility metrics (VIX), and ticker-specific news from the [Polygon.io](https://polygon.io/) API. It then calculates a suite of technical indicators and news sentiment scores, storing the final, feature-rich dataset in a MinIO object storage server, ready for consumption by machine learning models.

---

## Features

- **Concurrent Data Fetching:** Utilizes Go routines to fetch data for multiple stock tickers in parallel.
- **Rich Feature Set:**
  - **Technical Indicators:** Calculates SMA, EMA, RSI, MACD, and Bollinger Bands from raw price data.
  - **Market Volatility:** Fetches VIX index data to provide a market-wide volatility context.
  - **News Sentiment Analysis:** Ingests news articles and calculates a daily average sentiment score for each ticker.
- **Rate Limiting:** Includes a token bucket rate limiter to stay within the 5 calls/minute limit of the Polygon.io free tier.
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
    - **Extract:** Fetches data from the Polygon.io API.
    - **Transform:** Parses the raw data, calculates technical indicators, and aggregates news sentiment.
    - **Load:** Uploads the final feature CSV files into the `features-market-data` bucket in MinIO.
2. **`minio` (Object Storage):** A high-performance, S3-compatible object storage server. It provides two "buckets":
    - `raw-market-data`: (Currently unused, but available for storing raw API responses).
    - `features-market-data`: The destination for the final, processed CSV files that will be used by the downstream model training pipeline.

---

## Prerequisites

Before you begin, ensure you have the following installed on your system:

- **[Docker](https://www.docker.com/products/docker-desktop/)**: To run the containerized application and services.
- **[Go (v1.22+)]**: For local development and to run `make` commands.
- **[Polygon.io API Key](https://polygon.io/dashboard)**: You will need a free API key to access the data.

---

## Getting Started

Follow these steps to get the OrionTrader pipeline up and running.

### 1. Clone the Repository

```bash
git clone <your-repo-url>
cd <your-repo-directory>
```

### 2. Configure Environment Variables

Create a `.env` file by copying the example file.

```bash
cp .env.example .env
```

Now, open the `.env` file and add your Polygon.io API key:

```
# .env
# Copy this file to .env and fill in your credentials.

# --- Polygon.io API Key ---
# Sign up at [https://polygon.io/](https://polygon.io/) to get your free API key
POLYGON_API_KEY=YOUR_POLYGON_API_KEY_HERE

# --- MinIO Configuration ---
# These can be left as default for local development
MINIO_ENDPOINT=minio:9000
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
MINIO_USE_SSL=false
```

### 3. Run the Pipeline

Use the provided `Makefile` to build and start all services in the background.

```bash
make dev
```

This command will:

1. Build the Go application's Docker image (if it's the first time or the code has changed).
2. Start the `app` and `minio` containers.
3. The `app` service will immediately begin fetching and processing data.

### 4. Verify the Output

- **Check the Logs:** View the live application logs to see the pipeline in action.

  ```bash
  make logs
  ```

- **Check MinIO:**
  1. Open your web browser and navigate to the MinIO console: **`http://localhost:9001`**.
  2. Log in with the credentials from your `.env` file (`minioadmin`/`minioadmin`).
  3. You should see the `features-market-data` bucket. Inside, you will find folders for each ticker containing the final `features.csv` files.

---

## Makefile Commands

- `make dev`: Builds and starts all services in detached mode.
- `make down`: Stops and removes all containers, networks, and volumes.
- `make logs`: Tails the logs from all running services.
- `make build`: Builds the Go binary for your local operating system.
- `make test`: Runs unit tests for the Go application.
- `make tidy`: Tidies the `go.mod` and `go.sum` files.

---

## Monitoring & Debugging

The application includes two built-in monitoring tools:

- **Live Stats:** The application logs will print a status update every minute showing the total number of running goroutines, with a specific breakdown for active `fetchers` and `processors`.

- **`pprof` Profiler:** For deep-dive debugging, a `pprof` server runs on port `6060`.
  - **Dashboard:** `http://localhost:6060/debug/pprof/`
  - **View all goroutine stack traces:** `http://localhost:6060/debug/pprof/goroutine?debug=2` (Extremely useful for finding "hanging" or deadlocked goroutines).
