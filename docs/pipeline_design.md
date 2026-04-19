<!-- Author: Bowen Ma -->
# Pipeline Design

This document describes how the pipeline is built — what each component does, how
the DAGs are scheduled, and why we picked the tools we picked.

## 1. Overall architecture

```
     Finnhub WebSocket              Finnhub REST (company-news)
            │                                │
            ▼                                ▼
   Trade Ingestion Service         News Ingestion DAG
   (long-running Pod)              (Airflow, every 30 min)
            │                                │
            └──────────┬─────────────────────┘
                       ▼
         ┌─────────────────────────────┐
         │  Raw layer                   │
         │  Postgres: raw_trades,       │
         │            raw_news          │
         │  Minio S3: trades (parquet), │
         │            news (json)       │
         └─────────────┬───────────────┘
                       ▼
              Data Curation DAG
             (Airflow, hourly)
                       │
                       ▼
         ┌─────────────────────────────┐
         │  Curated layer               │
         │  trade_summaries             │
         │  news_trade_windows          │
         │  company_events              │
         └─────────────────────────────┘
```

Everything runs on Kubernetes (k3s on the Raspberry Pi cluster, or k3d/minikube
locally for testing). Airflow handles the two batch DAGs. The trade ingestion part
runs as a regular K8s Deployment because it needs to keep a WebSocket open — more
on that below.

## 2. Why the trade ingestion is NOT an Airflow DAG

This was the first design decision I had to make and the most important one.

The trade data arrives over a WebSocket that has to stay connected continuously.
Airflow is a batch scheduler — its tasks are supposed to start, do something, and
finish. Keeping a WebSocket open as an Airflow task would mean a task that runs
forever, which defeats the whole point of Airflow's scheduling/retry model.

So the trade ingestion runs as a **standalone K8s Deployment** (`trade-ingestion`).
It connects to the WebSocket, buffers trades in memory, and every 60 seconds
flushes the buffer to Postgres + Minio S3. Airflow only monitors whether this
service is alive, via the `trade_health_check` DAG.

This is a common pattern for mixing streaming and batch: the streaming part lives
outside the batch scheduler, and the batch scheduler just watches it.

## 3. The DAGs

### 3.1 `news_ingestion`

| Property | Value |
|----------|-------|
| Schedule | `*/30 * * * *` (every 30 minutes) |
| Retries | 2, with 5-minute delay |
| Catchup | false |

**Task flow:**

```
fetch_news ──▶ dedup_and_validate ──┬──▶ store_to_s3
                                    └──▶ store_to_postgres
```

- `fetch_news`: Calls `/company-news` for each tracked symbol. Looks back 2 days
  (not just "today") to catch delayed publications.
- `dedup_and_validate`: Deduplicates articles by Finnhub's `id` within the batch
  and drops articles missing `headline` or `datetime`.
- `store_to_s3`: Archives the raw JSON to Minio at `raw/news/{date}/{symbol}/{id}.json`.
- `store_to_postgres`: Inserts into `raw_news` with `ON CONFLICT (article_id) DO NOTHING`
  so duplicates across runs are also handled.

The S3 and Postgres writes run in parallel because they don't depend on each other.

### 3.2 `data_curation`

| Property | Value |
|----------|-------|
| Schedule | `0 * * * *` (every hour) |
| Retries | 1, with 5-minute delay |
| Catchup | false |

**Task flow:**

```
build_trade_summaries ──┐
                        ├──▶ update_company_events ──▶ data_quality_check
build_news_trade_windows ┘
```

- `build_trade_summaries`: Aggregates `raw_trades` from the last 2 hours into
  5-minute windows (OHLCV + VWAP). Uses `ON CONFLICT` upsert so re-running is safe.
- `build_news_trade_windows`: For each news article that doesn't have a window
  entry yet, computes pre/post trade stats and percent changes.
- `update_company_events`: Joins daily trade stats with daily news stats into one
  per-(symbol, day) row.
- `data_quality_check`: Logs counts and warnings — not a hard fail, just visibility.

The first two run in parallel; company_events depends on both; QC runs last.

### 3.3 `trade_health_check`

| Property | Value |
|----------|-------|
| Schedule | `*/5 * * * *` (every 5 minutes) |
| Retries | 0 |
| Catchup | false |

Simply checks whether `raw_trades` has received any rows in the last 10 minutes.
If not, logs a warning (which could be wired up to PagerDuty / Slack later).
During weekends or outside market hours, zero trades is expected, so the warning
message says so explicitly.

## 4. Data model

### Raw layer
Minimal transformation — basically what the API gave us, plus timestamps and
dedup keys.

- `raw_trades` (symbol, price, volume, trade_timestamp, conditions, ingested_at)
  - Unique index on (symbol, trade_timestamp, price, volume)
  - Index on (symbol, trade_timestamp) for time-range queries
- `raw_news` (article_id UNIQUE, symbol, headline, summary, published_at, raw_json …)
  - `raw_json` keeps the entire Finnhub response for future extensibility
    (sentiment scoring, classification, etc.).

### Curated layer
- `trade_summaries` — OHLCV + VWAP per (symbol, 5-min window).
- `news_trade_windows` — one row per news article, with pre/post trade stats.
- `company_events` — one row per (symbol, date) combining trade and news summaries.

### S3 archive
```
s3://trade-pipeline/raw/trades/{date}/{time}.parquet
s3://trade-pipeline/raw/news/{date}/{symbol}/{article_id}.json
```
Partitioned by date. Parquet for trades because they have a fixed schema and
we write them in bulk. JSON for news because the schema is flexible and the
per-article size is small.

## 5. Technology choices

### Postgres (vs MongoDB)
We use Postgres for both the Airflow metadata DB and the pipeline data.

We considered MongoDB for the news data because it's schema-flexible. But the
downstream queries we care about are SQL joins: "for each news article, find the
trades in its time window". Those joins are the core of the curated layer, and
Postgres does them natively.

Postgres' JSONB type gives us the flexibility of a document store for the news
`raw_json` column, without losing SQL. So we get the best of both.

The other reason is resource usage. On 3 Raspberry Pis we are already running
Postgres + Minio + Airflow webserver + scheduler + the trade ingestion service.
Adding MongoDB on top would overload the cluster. Cutting it out is a big win
for the hardware we're on.

### Minio (vs just using Postgres for everything)
Postgres is great for the curated layer but not ideal as a long-term archive
for every raw trade event — that table would grow fast, and a relational DB is
an expensive place to keep years of append-only records.

Minio gives us S3-compatible object storage, which is the standard pattern for
a "data lake" raw zone. If Postgres is ever corrupted or we want to re-process
with different window sizes, we can rebuild from Minio. It's also where
historical replay / backtesting data would live.

Minio runs as a single container on one Pi in our cluster. It's not distributed,
but at our scale it doesn't need to be.

### Airflow (vs cron / a custom scheduler)
Airflow was the required orchestrator for this course. Even without that, it's
the right call here because:
- We have multiple DAGs with internal task dependencies (curation DAG has 4
  tasks with a fan-in pattern).
- We need retries and a UI to inspect failed runs.
- It's the de-facto industry standard, which matters for learning.

The LocalExecutor is used because we have a single scheduler node and don't
need Celery/K8sExecutor complexity for 3 DAGs.

### Kubernetes (vs Docker Compose on a single Pi)
The course's cluster is 3 Pis, and K8s is the standard way to schedule across
nodes. K8s specifically gives us:
- Declarative deployment (`kubectl apply -f k8s/`).
- Automatic restarts when a Pi goes down or a pod crashes.
- A clean way to separate config (ConfigMap) from secrets (Secret) from code.

We run k3s on the Pi cluster because it's lightweight enough for ARM hardware.
Locally I develop with Docker Compose for iteration speed, and validate the K8s
manifests with k3d (which runs k3s inside Docker).

## 6. Handling ambiguity in the data

The project spec calls out specific data-quality issues. Here's how each is handled:

| Issue | Handling |
|-------|----------|
| Duplicate news articles | Unique index on `article_id`; `ON CONFLICT DO NOTHING` on insert |
| Delayed publication | News DAG looks back 2 days, not just since last run |
| Out-of-order trades | We use `trade_timestamp` from Finnhub, not our ingestion time, so queries still see trades in correct order |
| Missing fields in trades | Records missing symbol/price/volume/timestamp are dropped and logged |
| "Which trades relate to this news" is fuzzy | We pick a clear default (15 min before, 60 min after) but store it per-row in `news_trade_windows` so downstream can see what we chose; downstream can also re-query `raw_trades` with its own window definition |

## 7. Extensibility

Things that can be added later without changing the pipeline's structure:
- More symbols — just a config change.
- New streaming event types (e.g. quotes, not just trades) — add a new table at
  the raw layer.
- Sentiment scores — add a column to `raw_news` and a task to the curation DAG.
- Different window sizes — already configurable via env var; or downstream
  queries raw tables directly.

## 8. Local development and testing

`docker-compose.yaml` runs the whole stack locally with the same images used on
the cluster. For validating the K8s manifests, I used k3d (k3s in Docker) to spin
up a 3-node cluster on my laptop and deployed with the same `kubectl apply`
commands that run on the Pi cluster. The `k8s/deploy.sh` script automates this
end-to-end so the same workflow is used locally and on the cluster.
