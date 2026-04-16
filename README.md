# Real-time Trade Analysis Pipeline

Finnhub real-time trade stream + company news data pipeline, orchestrated by Apache Airflow.

## Project Structure

```
dags/                          # Airflow DAGs
  news_ingestion_dag.py        #   Fetches company news from Finnhub REST API (every 30 min)
  data_curation_dag.py         #   Aggregates trades, builds news-trade windows (hourly)
  trade_health_check_dag.py    #   Monitors trade ingestion service health (every 5 min)
services/trade_ingestion/      # Standalone WebSocket client (long-running)
  main.py                      #   Connects to Finnhub, buffers trades, flushes to Postgres + S3
  Dockerfile                   #   Container image for the service
sql/init.sql                   # Database schema (raw + curated tables)
docker-compose.yaml            # Local development environment
ARCHITECTURE_PLAN.md           # Detailed architecture design document
```

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (Windows / macOS)
- A free [Finnhub API key](https://finnhub.io/register)

## Quick Start

### 1. Clone and configure

```bash
git clone <repo-url>
cd final_project
```

Create a `.env` file in the project root:

**macOS / Linux:**
```bash
echo "FINNHUB_API_KEY=your_key_here" > .env
```

**Windows (Git Bash or WSL):**
```bash
echo "FINNHUB_API_KEY=your_key_here" > .env
```

**Windows (PowerShell):**
```powershell
"FINNHUB_API_KEY=your_key_here" | Out-File -Encoding ascii .env
```

> PowerShell defaults to UTF-16 encoding which Docker cannot read. Use `-Encoding ascii` or use Git Bash instead.

### 2. Start all services

```bash
docker compose up -d
```

First run will pull images (~2 min). Once done, verify:

```bash
docker compose ps
```

All services should show `Up` / `healthy`:
- **postgres** (port 5432) -- Airflow metadata + pipeline data
- **minio** (ports 9000/9001) -- S3-compatible object storage
- **airflow-webserver** (port 8080) -- Airflow UI
- **airflow-scheduler** -- DAG scheduling
- **trade-ingestion** -- Finnhub WebSocket client

### 3. Access UIs

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | admin / admin |
| Minio Console | http://localhost:9001 | minioadmin / minioadmin |

### 4. Verify data flow

**Check trades are being ingested:**
```bash
docker compose exec postgres psql -U pipeline -d trade_pipeline \
  -c "SELECT symbol, COUNT(*) FROM raw_trades GROUP BY symbol;"
```

**Trigger news ingestion manually:**
```bash
docker compose exec airflow-webserver airflow dags trigger news_ingestion
```

**Trigger data curation manually:**
```bash
docker compose exec airflow-webserver airflow dags trigger data_curation
```

**Check curated data:**
```bash
docker compose exec postgres psql -U pipeline -d trade_pipeline \
  -c "SELECT symbol, event_date, news_count, trade_count FROM company_events;"
```

> Note: Trade data volume depends on market hours (US Eastern). Outside trading hours you may see very few or no trades.

### 5. Stop

```bash
docker compose down        # stop containers (data preserved in volumes)
docker compose down -v     # stop and delete all data
```

## Database Schema

**Raw layer** -- ingested data with minimal transformation:
- `raw_trades` -- trade events from WebSocket (symbol, price, volume, timestamp)
- `raw_news` -- news articles from REST API (deduplicated by article_id)

**Curated layer** -- aggregated and joined data:
- `trade_summaries` -- OHLCV + VWAP per symbol per 5-min window
- `news_trade_windows` -- trade stats before/after each news article
- `company_events` -- daily per-symbol summary combining trades + news

See `sql/init.sql` for full schema and `ARCHITECTURE_PLAN.md` for design details.
