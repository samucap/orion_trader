# Stock Trader - New Features Guide

## Overview

This document describes the new features added to the stock trading data pipeline, including:
1. **Hybrid Storage Architecture** (MinIO + PostgreSQL)
2. **Interactive Visualizations** (Plotly-based charts)
3. **RAG System** (Natural language query interface)
4. **Vector Database** (Pattern storage and similarity search)

---

## 1. Hybrid Storage Architecture

### PostgreSQL Integration

The system now supports writing processed data to PostgreSQL in addition to MinIO.

#### Setup

1. **Install PostgreSQL** (if not already installed):
   ```bash
   brew install postgresql  # macOS
   # or
   sudo apt install postgresql  # Linux
   ```

2. **Create Database**:
   ```bash
   createdb trader
   ```

3. **Apply Schema**:
   ```bash
   psql -d trader -f schema.sql
   ```

4. **Set Environment Variable**:
   ```bash
   export POSTGRES_CONN="postgresql://user:password@localhost:5432/trader"
   ```

#### Usage

```python
from cleaner import process_year_files

# Configure storage backends
storage_config = {
    'storage_backends': ['minio', 'postgres'],  # Both!
    'minio_client': minio_client,
    'postgres_conn': 'postgresql://...'
}

# Process data - will write to both backends
stats = process_year_files(2020, storage_config=storage_config)
```

#### Schema Highlights

- **Partitioned Tables**: `stock_prices` partitioned by year for performance
- **Indexed Columns**: Fast queries on ticker, date, and indicators
- **Metadata Tracking**: `processing_runs` table tracks data quality
- **Anomaly Detection**: `price_anomalies` table for alerts

---

## 2. Interactive Visualizations

### Available Charts

All visualizations are in `visualizations.py` and use Plotly for interactivity.

#### Candlestick Chart

```python
from visualizations import create_candlestick_chart
import pandas as pd

# Load data
df = pd.read_parquet('2020.parquet')
aapl_data = df[df['ticker'] == 'AAPL']

# Create chart
fig = create_candlestick_chart(
    df=aapl_data,
    ticker='AAPL',
    show_volume=True,
    show_sma=True
)

# Display
fig.show()

# Or save to HTML
fig.write_html('aapl_chart.html')
```

#### Technical Indicators Dashboard

```python
from visualizations import create_technical_indicators_dashboard

fig = create_technical_indicators_dashboard(
    df=aapl_data,
    ticker='AAPL',
    height=1200
)
fig.show()
```

#### Correlation Heatmap

```python
from visualizations import create_correlation_heatmap

fig = create_correlation_heatmap(df)
fig.show()
```

#### Multi-Ticker Comparison

```python
from visualizations import create_multi_ticker_comparison

# Prepare data for multiple tickers
tickers_data = {
    'AAPL': df[df['ticker'] == 'AAPL'],
    'MSFT': df[df['ticker'] == 'MSFT'],
    'GOOGL': df[df['ticker'] == 'GOOGL']
}

fig = create_multi_ticker_comparison(
    dfs=tickers_data,
    normalize=True  # Show % change from start
)
fig.show()
```

#### Returns Distribution

```python
from visualizations import create_returns_distribution

fig = create_returns_distribution(
    df=aapl_data,
    ticker='AAPL'
)
fig.show()
```

---

## 3. RAG SQL Query Agent

### Natural Language to SQL

Ask questions in natural language, get SQL queries and results.

#### Setup

1. **Install Dependencies**:
   ```bash
   pip install openai psycopg2-binary
   ```

2. **Set API Key** (for OpenAI):
   ```bash
   export OPENAI_API_KEY="your-key-here"
   export POSTGRES_CONN="postgresql://..."
   ```

#### Usage

```python
from rag_sql_agent import SQLQueryAgent

# Initialize agent
agent = SQLQueryAgent(
    postgres_conn='postgresql://...',
    model='gpt-4'  # or 'gpt-3.5-turbo'
)

# Ask a question
result = agent.query(
    "Show me all tickers where RSI dropped below 30 in the last week"
)

print(f"SQL: {result['sql']}")
print(f"Results: {result['results']}")
print(f"Response: {result['response']}")
```

#### Using Local Ollama (No API Costs)

```python
agent = SQLQueryAgent(
    postgres_conn='postgresql://...',
    use_ollama=True,
    model='llama2'  # or any Ollama model
)

result = agent.query("Which tech stocks have bullish MACD crossovers?")
```

#### Example Queries

- "Find stocks with the highest volume yesterday"
- "Show me all tickers trading above their SMA60"
- "Which stocks are oversold (RSI < 30) right now?"
- "Find AAPL price anomalies in December 2024"
- "Show bullish divergences (price down but RSI up)"

---

## 4. Vector Database for Pattern Storage

### Store and Search Market Patterns

Find similar historical patterns using natural language.

#### Setup

```bash
pip install chromadb sentence-transformers
```

#### Usage

```python
from vector_db_patterns import StockPatternVectorDB
import pandas as pd

# Initialize database
db = StockPatternVectorDB(
    persist_directory="./chroma_db",
    collection_name="stock_patterns"
)

# Load historical data
df = pd.read_parquet('2020.parquet')
aapl_data = df[df['ticker'] == 'AAPL']

# Bulk add patterns from DataFrame
count = db.bulk_add_from_dataframe(
    df=aapl_data,
    pattern_type="historical",
    calculate_outcomes=True,
    outcome_window=7  # Look 7 days ahead for outcomes
)
print(f"Added {count} patterns")

# Search for similar patterns
results = db.search_similar_patterns(
    query_text="bullish reversal with oversold RSI",
    n_results=5
)

for result in results:
    print(f"Match: {result['document']}")
    print(f"Similarity: {1 - result['distance']:.2f}")
    print(f"Outcome: {result['metadata']['outcome']}")
```

#### Search by Indicators

```python
# Find patterns with similar indicator values
results = db.search_by_indicators(
    indicators={
        'rsi30': 28.5,
        'macd': -2.1
    },
    ticker='AAPL',
    n_results=5
)
```

#### Use Cases

1. **Pattern Recognition**: Find similar historical price patterns
2. **Strategy Backtesting**: See what happened after similar setups
3. **Anomaly Detection**: Identify unusual indicator combinations
4. **Market Intelligence**: "When RSI was this low before, what happened?"

---

## Complete Integration Example

```python
# 1. Process and store data
from cleaner import process_year_files, makeMinIO

minio = makeMinIO()
storage_config = {
    'storage_backends': ['minio', 'postgres'],
    'minio_client': minio,
    'postgres_conn': 'postgresql://...'
}

stats = process_year_files(2020, storage_config=storage_config)

# 2. Create visualizations
from visualizations import create_candlestick_chart
import pandas as pd

df = pd.read_parquet('2020.parquet')
aapl = df[df['ticker'] == 'AAPL']
fig = create_candlestick_chart(aapl, 'AAPL')
fig.write_html('aapl_analysis.html')

# 3. Query with natural language
from rag_sql_agent import SQLQueryAgent

agent = SQLQueryAgent()
result = agent.query("Show oversold tech stocks with high volume")
print(result['response'])

# 4. Find similar patterns
from vector_db_patterns import StockPatternVectorDB

db = StockPatternVectorDB()
db.bulk_add_from_dataframe(aapl)
similar = db.search_similar_patterns("bullish MACD crossover")
```

---

## Environment Variables

Add to your `.env` file:

```bash
# Existing
MINIO_ENDPOINT=minio:9000
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin

# New
POSTGRES_CONN=postgresql://user:pass@localhost:5432/trader
OPENAI_API_KEY=sk-...  # Optional, for RAG with OpenAI
```

---

## Docker Compose Support

Add PostgreSQL to `docker-compose.yml`:

```yaml
services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: trader
      POSTGRES_USER: trader
      POSTGRES_PASSWORD: trader_pass
    ports:
      - "5432:5432"
    volumes:
      - ./postgres_data:/var/lib/postgresql/data
      - ./schema.sql:/docker-entrypoint-initdb.d/schema.sql
```

---

## Performance Notes

### PostgreSQL
- Partitioned by year for fast year-specific queries
- Indexed on ticker, date for optimal performance
- Bulk inserts use `execute_values()` (fast)

### Vector Database
- Uses sentence transformers for embeddings
- Chroma DB with DuckDB backend (fast, embedded)
- Persists to disk, no separate server needed

### Visualizations
- Plotly generates interactive HTML
- Can be embedded in Streamlit/Dash dashboards
- Lazy loading recommended for large datasets

---

## Next Steps

1. **Build Dashboard**: Create Streamlit app using visualizations
2. **Anomaly Detection**: Use vector DB to detect unusual patterns automatically
3. **Real-time Alerts**: Query PostgreSQL for conditions, send notifications
4. **Strategy Backtesting**: Use RAG to test trading strategies on historical data
5. **API Layer**: Expose RAG agent via FastAPI for web access

---

## Troubleshooting

### PostgreSQL Connection Issues
```bash
# Check if PostgreSQL is running
pg_isready

# Test connection
psql -d trader -c "SELECT COUNT(*) FROM stock_prices;"
```

### Vector DB Issues
```bash
# Clear and rebuild
rm -rf ./chroma_db
python vector_db_patterns.py  # Reinitialize
```

### Missing Dependencies
```bash
pip install -r requirements.txt
```

---

## Architecture Diagram

```
┌──────────────┐
│  CSV Files   │
└──────┬───────┘
       │
       ▼
┌──────────────────┐
│ cleaner.py       │
│ (TA indicators)  │
└──────┬───────────┘
       │
       ├─────────────┬──────────────────┐
       ▼             ▼                  ▼
┌─────────────┐ ┌────────────┐  ┌─────────────┐
│   MinIO     │ │ PostgreSQL │  │  Vector DB  │
│  (Parquet)  │ │ (Queries)  │  │  (Patterns) │
└──────┬──────┘ └─────┬──────┘  └──────┬──────┘
       │              │                 │
       └──────┬───────┴─────────────────┘
              ▼
       ┌────────────────┐
       │ Visualizations │
       │ RAG SQL Agent  │
       └────────────────┘
```

---

For questions or issues, refer to the main README.md or test files for examples.

