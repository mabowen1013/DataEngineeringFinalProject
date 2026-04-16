-- Author: Bowen Ma
-- Real-time Trade Analysis Pipeline - Database Initialization
-- This script runs automatically when Postgres container starts for the first time.

-- ============================================================
-- Create the pipeline database (separate from Airflow's DB)
-- ============================================================
CREATE DATABASE trade_pipeline;

-- Connect to the pipeline database
\c trade_pipeline

-- Create a dedicated user for the pipeline
CREATE USER pipeline WITH PASSWORD 'pipeline';

-- ============================================================
-- Raw Layer: stores ingested data with minimal transformation
-- ============================================================

-- Raw trade events from Finnhub WebSocket
CREATE TABLE raw_trades (
    id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    price DECIMAL(12,4) NOT NULL,
    volume DECIMAL(18,4) NOT NULL,
    trade_timestamp TIMESTAMPTZ NOT NULL,
    conditions JSONB,
    ingested_at TIMESTAMPTZ DEFAULT NOW()
);

-- Fast lookups by symbol + time (used by curation DAG)
CREATE INDEX idx_trades_symbol_ts ON raw_trades(symbol, trade_timestamp);

-- Deduplication: same symbol/time/price/volume is treated as duplicate
CREATE UNIQUE INDEX idx_trades_dedup ON raw_trades(symbol, trade_timestamp, price, volume);

-- Raw news articles from Finnhub Company News API
CREATE TABLE raw_news (
    id BIGSERIAL PRIMARY KEY,
    article_id BIGINT UNIQUE NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    headline TEXT NOT NULL,
    summary TEXT,
    source VARCHAR(100),
    url TEXT,
    published_at TIMESTAMPTZ NOT NULL,
    category VARCHAR(50),
    raw_json JSONB,
    ingested_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_news_symbol_published ON raw_news(symbol, published_at);

-- ============================================================
-- Curated Layer: aggregated and joined data for downstream use
-- ============================================================

-- Trade summaries aggregated into fixed time windows (e.g. 5-minute)
CREATE TABLE trade_summaries (
    id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    open_price DECIMAL(12,4),
    close_price DECIMAL(12,4),
    high_price DECIMAL(12,4),
    low_price DECIMAL(12,4),
    total_volume DECIMAL(18,4),
    trade_count INT,
    vwap DECIMAL(12,4),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(symbol, window_start, window_end)
);

-- Core curated table: links each news article to surrounding trade activity
CREATE TABLE news_trade_windows (
    id BIGSERIAL PRIMARY KEY,
    news_id BIGINT REFERENCES raw_news(id),
    symbol VARCHAR(10) NOT NULL,
    published_at TIMESTAMPTZ NOT NULL,
    -- Window definition (configurable)
    pre_window_minutes INT DEFAULT 15,
    post_window_minutes INT DEFAULT 60,
    -- Pre-publication trade stats
    pre_trade_count INT,
    pre_total_volume DECIMAL(18,4),
    pre_vwap DECIMAL(12,4),
    -- Post-publication trade stats
    post_trade_count INT,
    post_total_volume DECIMAL(18,4),
    post_vwap DECIMAL(12,4),
    -- Change metrics
    price_change_pct DECIMAL(8,4),
    volume_change_pct DECIMAL(8,4),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Daily company-level summary combining trades and news
CREATE TABLE company_events (
    id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    event_date DATE NOT NULL,
    news_count INT DEFAULT 0,
    trade_count INT DEFAULT 0,
    total_volume DECIMAL(18,4),
    avg_price DECIMAL(12,4),
    price_range_pct DECIMAL(8,4),
    top_headlines JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(symbol, event_date)
);

-- ============================================================
-- Grant permissions to pipeline user
-- ============================================================
GRANT ALL PRIVILEGES ON DATABASE trade_pipeline TO pipeline;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO pipeline;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO pipeline;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON TABLES TO pipeline;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON SEQUENCES TO pipeline;
