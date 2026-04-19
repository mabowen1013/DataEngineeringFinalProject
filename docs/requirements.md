<!-- Author: Bowen Ma -->
# Pipeline Requirements
## 1. Data sources we consume

| Source | API | Rate | Notes |
|--------|-----|------|-------|
| Finnhub trade stream | WebSocket (`wss://ws.finnhub.io`) | Continuous, many events per second during market hours | Live trades for subscribed symbols |
| Finnhub company news | REST (`/company-news`) | Polled every 30 minutes | Returns articles for a symbol within a date range |

## 2. Downstream use cases supported
### 2.1 News-to-Market Reaction Analysis

**What downstream wants to do:** Compare how the stock behaves before and after a
news article is published — did the price move, did the volume spike?

**We provide:**
- `raw_news` — every article with its `published_at` timestamp and symbol.
- `raw_trades` — every trade with its exact timestamp.
- `news_trade_windows` — pre-computed stats (trade count, total volume, VWAP) for
  the 15 minutes before and 60 minutes after each article, plus percent change.

### 2.2 Event-Centered Windows

**What downstream wants:** A way to group trades into time windows around a news event.

**What we provide:** The `news_trade_windows` table gives one row per news article
with the windows already built. Window sizes are configurable (currently 15 min
before / 60 min after). The raw tables are still there if a different window is
needed.

### 2.3 Abnormal Activity Detection

**What downstream wants:** Detect unusual price or volume changes compared to normal
intra-day activity.

**What we provide:**
- `trade_summaries` — OHLCV + VWAP aggregated into 5-minute windows per symbol.
  Downstream can use this as a baseline to spot windows that are far from normal.
- `price_change_pct` and `volume_change_pct` are already in `news_trade_windows`.

### 2.4 Unified Company Event View

**What downstream wants:** A per-company, per-day view that mixes trade activity
and news together.

**What we provide:** The `company_events` table — one row per (symbol, date) with
trade counts, total volume, average price, price range, news count, and the top
headlines of the day.

### 2.5 Screening and Monitoring

**What downstream wants:** Dashboards or alerts for symbols with both high news
activity and strong market reaction.

**What we provide:** Downstream can filter `company_events` on `news_count` and
`price_range_pct` to find these symbols. Thresholds are decided by downstream,
not by us.

### 2.6 Historical Replay and Backtesting

**What downstream wants:** Reconstruct how the market reacted to past news events.

**What we provide:**
- `raw_trades` in Postgres keeps every trade event with its original timestamp.
- Minio S3 stores a parquet archive of trades (partitioned by date) and the raw
  JSON of every news article. This is the source of truth if the Postgres data
  is ever lost or if downstream wants to re-process from scratch.

## 3. What we guarantee about the data

- **Deduplication:** Trades are unique on (symbol, trade_timestamp, price, volume).
  News articles are unique on `article_id` (Finnhub's ID).
- **Ordering:** Trades use `trade_timestamp` from the exchange, not our ingestion
  time. So out-of-order arrivals are still ordered correctly when queried.
- **Missing fields:** Trades missing any of (symbol, price, volume, timestamp) are
  dropped and logged. Articles without `id` or `headline` are dropped.
- **Freshness:** Trades are in Postgres within ~60 seconds of the WebSocket event.
  News is in Postgres within ~30 minutes of publication.

## 4. What we do NOT do

Things the downstream team needs to handle themselves:
- Sentiment scoring or news classification (schema leaves space for it — `raw_json`
  in `raw_news` has the full article, and downstream can add columns later).
- Defining what "high" activity or "unusual" behavior means. We expose numbers;
  they pick thresholds.
- Alerting and dashboards.
- Any machine learning models.

## 5. Scale assumptions

- Currently tracking 3 symbols (AAPL, MSFT, AMZN). Adding more symbols only requires
  updating the `TRADE_SYMBOLS` config — no schema changes.
- Target: handle ~50 symbols on a 3-node Raspberry Pi cluster without falling behind.
- Postgres is sufficient at this scale; moving to a bigger warehouse would be a
  future decision for downstream, not the pipeline.
