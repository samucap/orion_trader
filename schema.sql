-- PostgreSQL Schema for Stock Trading Data
-- Optimized for time-series queries with partitioning

-- Main time series table (partitioned by date)
CREATE TABLE IF NOT EXISTS stock_prices (
    id BIGSERIAL,
    ticker VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    
    -- Price data
    open DECIMAL(12, 4) NOT NULL,
    high DECIMAL(12, 4) NOT NULL,
    low DECIMAL(12, 4) NOT NULL,
    close DECIMAL(12, 4) NOT NULL,
    volume BIGINT NOT NULL,
    transactions INTEGER,
    
    -- Technical indicators
    macd DECIMAL(12, 6),
    bollub DECIMAL(12, 4),
    bolllb DECIMAL(12, 4),
    rsi30 DECIMAL(8, 4),
    sma30 DECIMAL(12, 4),
    sma60 DECIMAL(12, 4),
    cci30 DECIMAL(12, 4),
    dx30 DECIMAL(12, 4),
    
    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),
    
    PRIMARY KEY (ticker, date)
) PARTITION BY RANGE (date);

-- Create yearly partitions for 2020-2025
CREATE TABLE IF NOT EXISTS stock_prices_2020 PARTITION OF stock_prices
    FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');

CREATE TABLE IF NOT EXISTS stock_prices_2021 PARTITION OF stock_prices
    FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');

CREATE TABLE IF NOT EXISTS stock_prices_2022 PARTITION OF stock_prices
    FOR VALUES FROM ('2022-01-01') TO ('2023-01-01');

CREATE TABLE IF NOT EXISTS stock_prices_2023 PARTITION OF stock_prices
    FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');

CREATE TABLE IF NOT EXISTS stock_prices_2024 PARTITION OF stock_prices
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

CREATE TABLE IF NOT EXISTS stock_prices_2025 PARTITION OF stock_prices
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_stock_prices_ticker ON stock_prices(ticker);
CREATE INDEX IF NOT EXISTS idx_stock_prices_date ON stock_prices(date);
CREATE INDEX IF NOT EXISTS idx_stock_prices_ticker_date ON stock_prices(ticker, date);

-- Ticker metadata table
CREATE TABLE IF NOT EXISTS tickers (
    ticker VARCHAR(10) PRIMARY KEY,
    company_name VARCHAR(255),
    sector VARCHAR(100),
    industry VARCHAR(100),
    market_cap BIGINT,
    first_seen DATE,
    last_seen DATE,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Processing metadata (track data quality)
CREATE TABLE IF NOT EXISTS processing_runs (
    id BIGSERIAL PRIMARY KEY,
    year INTEGER NOT NULL,
    run_date TIMESTAMPTZ DEFAULT NOW(),
    files_processed INTEGER,
    total_csv_rows INTEGER,
    tickers_processed INTEGER,
    final_df_rows INTEGER,
    success BOOLEAN DEFAULT TRUE,
    errors JSONB,
    duration_seconds INTEGER
);

-- Anomalies/alerts table
CREATE TABLE IF NOT EXISTS price_anomalies (
    id BIGSERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    anomaly_type VARCHAR(50), -- 'spike', 'drop', 'volume_surge', 'indicator_divergence'
    severity VARCHAR(20), -- 'low', 'medium', 'high'
    details JSONB,
    detected_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_anomalies_ticker_date ON price_anomalies(ticker, date);
CREATE INDEX IF NOT EXISTS idx_anomalies_severity ON price_anomalies(severity);

-- Create view for easy querying with indicator explanations
CREATE OR REPLACE VIEW stock_analysis AS
SELECT 
    ticker,
    date,
    close,
    volume,
    macd,
    CASE 
        WHEN macd > 0 THEN 'bullish'
        WHEN macd < 0 THEN 'bearish'
        ELSE 'neutral'
    END as macd_signal,
    rsi30,
    CASE 
        WHEN rsi30 > 70 THEN 'overbought'
        WHEN rsi30 < 30 THEN 'oversold'
        ELSE 'neutral'
    END as rsi_signal,
    bollub,
    bolllb,
    CASE 
        WHEN close > bollub THEN 'above_upper'
        WHEN close < bolllb THEN 'below_lower'
        ELSE 'in_bands'
    END as bollinger_position
FROM stock_prices;

