-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ─── raw_prices ────────────────────────────────────────────────────────────────
-- Stores every individual trade event received from Binance @trade streams.
-- Each row is one executed trade (not an OHLCV candle).
CREATE TABLE IF NOT EXISTS raw_prices (
    id              BIGSERIAL,
    symbol          VARCHAR(20)      NOT NULL,
    event_time      TIMESTAMPTZ      NOT NULL,
    price           DOUBLE PRECISION NOT NULL,
    volume          DOUBLE PRECISION NOT NULL,   -- trade quantity (base asset)
    trade_id        BIGINT,
    is_buyer_maker  BOOLEAN,
    ingested_at     TIMESTAMPTZ      NOT NULL DEFAULT NOW()
);

-- Convert to a TimescaleDB hypertable partitioned by event_time.
-- 1-hour chunks work well for high-frequency per-trade data.
SELECT create_hypertable(
    'raw_prices',
    'event_time',
    chunk_time_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_raw_prices_symbol_time
    ON raw_prices (symbol, event_time DESC);

-- ─── anomaly_events ────────────────────────────────────────────────────────────
-- Stores trades flagged as anomalous by the Spark pipeline.
CREATE TABLE IF NOT EXISTS anomaly_events (
    id               BIGSERIAL,
    symbol           VARCHAR(20)      NOT NULL,
    event_time       TIMESTAMPTZ      NOT NULL,
    price            DOUBLE PRECISION NOT NULL,
    volume           DOUBLE PRECISION NOT NULL,
    anomaly_type     VARCHAR(100)     NOT NULL,  -- e.g. "PRICE_ZSCORE|VOLUME_SPIKE"
    anomaly_score    DOUBLE PRECISION NOT NULL,
    zscore           DOUBLE PRECISION,
    rolling_mean     DOUBLE PRECISION,
    rolling_std      DOUBLE PRECISION,
    is_price_anomaly BOOLEAN          NOT NULL DEFAULT FALSE,
    is_iqr_anomaly   BOOLEAN          NOT NULL DEFAULT FALSE,
    is_volume_spike  BOOLEAN          NOT NULL DEFAULT FALSE,
    detected_at      TIMESTAMPTZ      NOT NULL DEFAULT NOW()
);

SELECT create_hypertable(
    'anomaly_events',
    'event_time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_anomaly_events_symbol_time
    ON anomaly_events (symbol, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_anomaly_events_type
    ON anomaly_events (anomaly_type);
CREATE INDEX IF NOT EXISTS idx_anomaly_events_score
    ON anomaly_events (anomaly_score DESC);

-- ─── 1-minute OHLCV continuous aggregate ───────────────────────────────────────
-- Materialises 1-minute OHLCV candles from raw trade ticks.
-- This is derived data; the source of truth is raw_prices.
CREATE MATERIALIZED VIEW IF NOT EXISTS crypto_ohlcv_1m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', event_time) AS bucket,
    symbol,
    first(price, event_time)             AS open,
    max(price)                           AS high,
    min(price)                           AS low,
    last(price, event_time)              AS close,
    sum(volume)                          AS volume,
    count(*)                             AS trade_count
FROM raw_prices
GROUP BY bucket, symbol
WITH NO DATA;

-- Refresh policy: keep the last 7 days of 1-min candles up to date,
-- refreshing every minute with a 5-second lag to allow late arrivals.
SELECT add_continuous_aggregate_policy(
    'crypto_ohlcv_1m',
    start_offset  => INTERVAL '7 days',
    end_offset    => INTERVAL '5 seconds',
    schedule_interval => INTERVAL '1 minute',
    if_not_exists => TRUE
);
