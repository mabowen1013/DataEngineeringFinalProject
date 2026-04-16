# Author: Bowen Ma
# Data Curation DAG
# Aggregates raw trades into summaries, builds news-trade windows,
# updates company event views, and runs data quality checks.

import os
import logging
from datetime import datetime, timedelta

import psycopg2
from psycopg2.extras import execute_values

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

# ============================================================
# Configuration
# ============================================================
DB_CONN = {
    "host": os.environ.get("DB_HOST", "postgres"),
    "port": os.environ.get("DB_PORT", "5432"),
    "dbname": os.environ.get("DB_NAME", "trade_pipeline"),
    "user": os.environ.get("DB_USER", "pipeline"),
    "password": os.environ.get("DB_PASSWORD", "pipeline"),
}

# Window sizes for news-trade analysis
PRE_WINDOW_MINUTES = int(os.environ.get("PRE_WINDOW_MINUTES", "15"))
POST_WINDOW_MINUTES = int(os.environ.get("POST_WINDOW_MINUTES", "60"))
# Trade summary window size
SUMMARY_WINDOW_MINUTES = int(os.environ.get("SUMMARY_WINDOW_MINUTES", "5"))


def get_db_connection():
    return psycopg2.connect(**DB_CONN)


# ============================================================
# Task 1: Build trade summaries
# ============================================================
def build_trade_summaries(**context):
    """Aggregate raw trades into fixed time-window summaries.

    For each symbol, groups trades into SUMMARY_WINDOW_MINUTES windows
    and computes OHLCV + VWAP statistics.
    Only processes trades from the last 2 hours to stay incremental.
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Use a SQL-based aggregation for efficiency.
            # date_trunc to nearest window, then aggregate.
            sql = f"""
                INSERT INTO trade_summaries
                    (symbol, window_start, window_end,
                     open_price, close_price, high_price, low_price,
                     total_volume, trade_count, vwap)
                SELECT
                    symbol,
                    -- Truncate to window boundary
                    date_trunc('hour', trade_timestamp)
                        + (EXTRACT(minute FROM trade_timestamp)::int
                           / {SUMMARY_WINDOW_MINUTES})
                        * interval '{SUMMARY_WINDOW_MINUTES} minutes'
                        AS window_start,
                    date_trunc('hour', trade_timestamp)
                        + (EXTRACT(minute FROM trade_timestamp)::int
                           / {SUMMARY_WINDOW_MINUTES} + 1)
                        * interval '{SUMMARY_WINDOW_MINUTES} minutes'
                        AS window_end,
                    -- Open: price of the earliest trade in the window
                    (array_agg(price ORDER BY trade_timestamp ASC))[1] AS open_price,
                    -- Close: price of the latest trade in the window
                    (array_agg(price ORDER BY trade_timestamp DESC))[1] AS close_price,
                    MAX(price) AS high_price,
                    MIN(price) AS low_price,
                    SUM(volume) AS total_volume,
                    COUNT(*) AS trade_count,
                    -- VWAP: volume-weighted average price
                    SUM(price * volume) / NULLIF(SUM(volume), 0) AS vwap
                FROM raw_trades
                WHERE trade_timestamp >= NOW() - interval '2 hours'
                GROUP BY symbol, window_start, window_end
                ON CONFLICT (symbol, window_start, window_end)
                DO UPDATE SET
                    open_price = EXCLUDED.open_price,
                    close_price = EXCLUDED.close_price,
                    high_price = EXCLUDED.high_price,
                    low_price = EXCLUDED.low_price,
                    total_volume = EXCLUDED.total_volume,
                    trade_count = EXCLUDED.trade_count,
                    vwap = EXCLUDED.vwap,
                    created_at = NOW()
            """
            cur.execute(sql)
            rows = cur.rowcount

        conn.commit()
        logger.info("Trade summaries: %d windows upserted", rows)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ============================================================
# Task 2: Build news-trade windows
# ============================================================
def build_news_trade_windows(**context):
    """For each news article not yet processed, compute trade stats
    in the pre/post publication windows.

    Pre-window: PRE_WINDOW_MINUTES before publication
    Post-window: POST_WINDOW_MINUTES after publication
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Find news articles that don't have a window entry yet
            cur.execute("""
                SELECT n.id, n.symbol, n.published_at
                FROM raw_news n
                LEFT JOIN news_trade_windows w ON w.news_id = n.id
                WHERE w.id IS NULL
                ORDER BY n.published_at DESC
            """)
            unprocessed = cur.fetchall()

            if not unprocessed:
                logger.info("No new news articles to process")
                return

            logger.info("Processing %d unprocessed news articles", len(unprocessed))

            values = []
            for news_id, symbol, published_at in unprocessed:
                pre_start = published_at - timedelta(minutes=PRE_WINDOW_MINUTES)
                post_end = published_at + timedelta(minutes=POST_WINDOW_MINUTES)

                # Query pre-window trade stats
                cur.execute("""
                    SELECT COUNT(*), COALESCE(SUM(volume), 0),
                           COALESCE(SUM(price * volume) / NULLIF(SUM(volume), 0), 0)
                    FROM raw_trades
                    WHERE symbol = %s
                      AND trade_timestamp >= %s
                      AND trade_timestamp < %s
                """, (symbol, pre_start, published_at))
                pre_count, pre_volume, pre_vwap = cur.fetchone()

                # Query post-window trade stats
                cur.execute("""
                    SELECT COUNT(*), COALESCE(SUM(volume), 0),
                           COALESCE(SUM(price * volume) / NULLIF(SUM(volume), 0), 0)
                    FROM raw_trades
                    WHERE symbol = %s
                      AND trade_timestamp >= %s
                      AND trade_timestamp <= %s
                """, (symbol, published_at, post_end))
                post_count, post_volume, post_vwap = cur.fetchone()

                # Calculate change percentages
                price_change = None
                if pre_vwap and pre_vwap > 0 and post_vwap and post_vwap > 0:
                    price_change = float((post_vwap - pre_vwap) / pre_vwap * 100)

                volume_change = None
                if pre_volume and pre_volume > 0 and post_volume:
                    volume_change = float((post_volume - pre_volume) / pre_volume * 100)

                values.append((
                    news_id, symbol, published_at,
                    PRE_WINDOW_MINUTES, POST_WINDOW_MINUTES,
                    pre_count, float(pre_volume), float(pre_vwap),
                    post_count, float(post_volume), float(post_vwap),
                    price_change, volume_change,
                ))

            sql = """
                INSERT INTO news_trade_windows
                    (news_id, symbol, published_at,
                     pre_window_minutes, post_window_minutes,
                     pre_trade_count, pre_total_volume, pre_vwap,
                     post_trade_count, post_total_volume, post_vwap,
                     price_change_pct, volume_change_pct)
                VALUES %s
            """
            execute_values(cur, sql, values)
            inserted = cur.rowcount

        conn.commit()
        logger.info("News-trade windows: %d rows inserted", inserted)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ============================================================
# Task 3: Update company events
# ============================================================
def update_company_events(**context):
    """Build/update daily company-level summary combining trades and news."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            sql = """
                INSERT INTO company_events
                    (symbol, event_date, news_count, trade_count,
                     total_volume, avg_price, price_range_pct, top_headlines)
                SELECT
                    t.symbol,
                    t.event_date,
                    COALESCE(n.news_count, 0),
                    t.trade_count,
                    t.total_volume,
                    t.avg_price,
                    t.price_range_pct,
                    n.top_headlines
                FROM (
                    -- Trade stats per symbol per day
                    SELECT
                        symbol,
                        DATE(trade_timestamp) AS event_date,
                        COUNT(*) AS trade_count,
                        SUM(volume) AS total_volume,
                        AVG(price) AS avg_price,
                        CASE WHEN MIN(price) > 0
                             THEN (MAX(price) - MIN(price)) / MIN(price) * 100
                             ELSE 0
                        END AS price_range_pct
                    FROM raw_trades
                    WHERE trade_timestamp >= NOW() - interval '2 days'
                    GROUP BY symbol, DATE(trade_timestamp)
                ) t
                LEFT JOIN (
                    -- News stats per symbol per day
                    SELECT
                        symbol,
                        DATE(published_at) AS event_date,
                        COUNT(*) AS news_count,
                        jsonb_agg(headline ORDER BY published_at DESC) AS top_headlines
                    FROM raw_news
                    WHERE published_at >= NOW() - interval '2 days'
                    GROUP BY symbol, DATE(published_at)
                ) n ON t.symbol = n.symbol AND t.event_date = n.event_date
                ON CONFLICT (symbol, event_date)
                DO UPDATE SET
                    news_count = EXCLUDED.news_count,
                    trade_count = EXCLUDED.trade_count,
                    total_volume = EXCLUDED.total_volume,
                    avg_price = EXCLUDED.avg_price,
                    price_range_pct = EXCLUDED.price_range_pct,
                    top_headlines = EXCLUDED.top_headlines,
                    created_at = NOW()
            """
            cur.execute(sql)
            rows = cur.rowcount

        conn.commit()
        logger.info("Company events: %d rows upserted", rows)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ============================================================
# Task 4: Data quality check
# ============================================================
def data_quality_check(**context):
    """Run basic data quality checks and log results."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            checks_passed = True

            # Check 1: Do we have recent trades?
            cur.execute("""
                SELECT COUNT(*), MAX(trade_timestamp)
                FROM raw_trades
                WHERE trade_timestamp >= NOW() - interval '1 day'
            """)
            trade_count, latest_trade = cur.fetchone()
            logger.info("QC - Recent trades (24h): %d, latest: %s",
                        trade_count, latest_trade)
            if trade_count == 0:
                logger.warning("QC WARN: No trades in the last 24 hours")
                checks_passed = False

            # Check 2: Do we have recent news?
            cur.execute("""
                SELECT COUNT(*), MAX(published_at)
                FROM raw_news
                WHERE published_at >= NOW() - interval '2 days'
            """)
            news_count, latest_news = cur.fetchone()
            logger.info("QC - Recent news (48h): %d, latest: %s",
                        news_count, latest_news)
            if news_count == 0:
                logger.warning("QC WARN: No news in the last 48 hours")
                checks_passed = False

            # Check 3: Are trade summaries up to date?
            cur.execute("SELECT COUNT(*) FROM trade_summaries")
            summary_count = cur.fetchone()[0]
            logger.info("QC - Trade summaries total: %d", summary_count)

            # Check 4: News-trade window coverage
            cur.execute("""
                SELECT
                    (SELECT COUNT(*) FROM raw_news) AS total_news,
                    (SELECT COUNT(*) FROM news_trade_windows) AS windows_built
            """)
            total_news, windows_built = cur.fetchone()
            coverage = (windows_built / total_news * 100) if total_news > 0 else 0
            logger.info("QC - News-trade window coverage: %d/%d (%.1f%%)",
                        windows_built, total_news, coverage)

            if checks_passed:
                logger.info("QC - All checks passed")
            else:
                logger.warning("QC - Some checks had warnings (see above)")

    finally:
        conn.close()


# ============================================================
# DAG definition
# ============================================================
default_args = {
    "owner": "bowen",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="data_curation",
    default_args=default_args,
    description="Aggregate trades, build news-trade windows, update company events",
    schedule_interval="0 * * * *",
    start_date=datetime(2026, 4, 15),
    catchup=False,
    tags=["curation", "pipeline"],
) as dag:

    t_summaries = PythonOperator(
        task_id="build_trade_summaries",
        python_callable=build_trade_summaries,
    )

    t_windows = PythonOperator(
        task_id="build_news_trade_windows",
        python_callable=build_news_trade_windows,
    )

    t_events = PythonOperator(
        task_id="update_company_events",
        python_callable=update_company_events,
    )

    t_qc = PythonOperator(
        task_id="data_quality_check",
        python_callable=data_quality_check,
    )

    # [summaries, windows] → events → quality check
    [t_summaries, t_windows] >> t_events >> t_qc
