# Author: Bowen Ma
# Trade Ingestion Service
# Connects to Finnhub WebSocket, receives real-time trades,
# and flushes them to Postgres + Minio S3 in micro-batches.

import json
import os
import signal
import sys
import threading
import time
import logging
from datetime import datetime, timezone
from io import BytesIO

import websocket
import psycopg2
from psycopg2.extras import execute_values
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ============================================================
# Configuration (from environment variables)
# ============================================================
FINNHUB_API_KEY = os.environ["FINNHUB_API_KEY"]
SYMBOLS = os.environ.get("TRADE_SYMBOLS", "AAPL,MSFT,AMZN").split(",")
FLUSH_INTERVAL = int(os.environ.get("FLUSH_INTERVAL", "60"))  # seconds

# Postgres
DB_HOST = os.environ.get("DB_HOST", "postgres")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "trade_pipeline")
DB_USER = os.environ.get("DB_USER", "pipeline")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "pipeline")

# Minio S3
S3_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
S3_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")
S3_BUCKET = os.environ.get("S3_BUCKET", "trade-pipeline")

# ============================================================
# Logging
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("trade-ingestion")

# ============================================================
# Global state
# ============================================================
trade_buffer = []        # buffered trade records
buffer_lock = threading.Lock()  # thread-safe access to buffer
running = True           # graceful shutdown flag


def get_db_connection():
    """Create a new Postgres connection."""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def get_s3_client():
    """Create a Minio-compatible S3 client."""
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name="us-east-1",
    )


# ============================================================
# WebSocket callbacks
# ============================================================
def on_message(ws, message):
    """Called when a WebSocket message is received from Finnhub."""
    data = json.loads(message)

    # Finnhub sends {"type": "trade", "data": [...]} for trade events
    # and {"type": "ping"} for heartbeats
    if data.get("type") != "trade":
        return

    trades = data.get("data", [])
    parsed = []
    for t in trades:
        # Validate required fields
        symbol = t.get("s")
        price = t.get("p")
        volume = t.get("v")
        timestamp_ms = t.get("t")

        if not all([symbol, price is not None, volume is not None, timestamp_ms]):
            logger.warning("Skipping trade with missing fields: %s", t)
            continue

        parsed.append({
            "symbol": symbol,
            "price": float(price),
            "volume": float(volume),
            "trade_timestamp": datetime.fromtimestamp(
                timestamp_ms / 1000, tz=timezone.utc
            ),
            "conditions": json.dumps(t.get("c", [])),
        })

    if parsed:
        with buffer_lock:
            trade_buffer.extend(parsed)


def on_error(ws, error):
    logger.error("WebSocket error: %s", error)


def on_close(ws, close_status, close_msg):
    logger.warning("WebSocket closed: status=%s msg=%s", close_status, close_msg)


def on_open(ws):
    """Subscribe to all configured symbols once connected."""
    logger.info("WebSocket connected. Subscribing to %d symbols...", len(SYMBOLS))
    for symbol in SYMBOLS:
        ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))
        logger.info("  Subscribed: %s", symbol)


# ============================================================
# Flush logic: write buffer to Postgres + S3
# ============================================================
def flush_to_postgres(trades):
    """Batch insert trades into Postgres. Duplicates are silently ignored."""
    if not trades:
        return 0

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            sql = """
                INSERT INTO raw_trades (symbol, price, volume, trade_timestamp, conditions)
                VALUES %s
                ON CONFLICT DO NOTHING
            """
            values = [
                (
                    t["symbol"],
                    t["price"],
                    t["volume"],
                    t["trade_timestamp"],
                    t["conditions"],
                )
                for t in trades
            ]
            execute_values(cur, sql, values)
            inserted = cur.rowcount
        conn.commit()
        return inserted
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def flush_to_s3(trades, s3_client):
    """Write trades as a Parquet file to Minio S3."""
    if not trades:
        return

    df = pd.DataFrame(trades)
    # Convert conditions from JSON string back for storage
    df["conditions"] = df["conditions"].apply(json.loads)

    table = pa.Table.from_pandas(df)
    buf = BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)

    # S3 key: raw/trades/{date}/{timestamp}.parquet
    now = datetime.now(timezone.utc)
    date_str = now.strftime("%Y-%m-%d")
    ts_str = now.strftime("%H%M%S")
    key = f"raw/trades/{date_str}/{ts_str}.parquet"

    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=buf.getvalue(),
    )
    logger.info("Wrote %d trades to s3://%s/%s", len(trades), S3_BUCKET, key)


def flush_buffer():
    """Take all trades from buffer and write to Postgres + S3."""
    with buffer_lock:
        if not trade_buffer:
            return
        trades = trade_buffer.copy()
        trade_buffer.clear()

    logger.info("Flushing %d trades...", len(trades))

    try:
        inserted = flush_to_postgres(trades)
        logger.info("Postgres: %d new rows inserted (%d duplicates skipped)",
                     inserted, len(trades) - inserted)
    except Exception as e:
        logger.error("Postgres flush failed: %s", e)

    try:
        s3_client = get_s3_client()
        flush_to_s3(trades, s3_client)
    except Exception as e:
        logger.error("S3 flush failed: %s", e)


def periodic_flush():
    """Background thread: flush buffer every FLUSH_INTERVAL seconds."""
    while running:
        time.sleep(FLUSH_INTERVAL)
        flush_buffer()


# ============================================================
# Main
# ============================================================
def shutdown(signum, frame):
    """Graceful shutdown: flush remaining buffer, then exit."""
    global running
    logger.info("Shutdown signal received. Flushing remaining buffer...")
    running = False
    flush_buffer()
    logger.info("Shutdown complete.")
    sys.exit(0)


def main():
    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    logger.info("=== Trade Ingestion Service ===")
    logger.info("Symbols: %s", SYMBOLS)
    logger.info("Flush interval: %ds", FLUSH_INTERVAL)

    # Start the periodic flush thread
    flush_thread = threading.Thread(target=periodic_flush, daemon=True)
    flush_thread.start()

    # Connect to Finnhub WebSocket with auto-reconnect
    ws_url = f"wss://ws.finnhub.io?token={FINNHUB_API_KEY}"

    while running:
        try:
            ws = websocket.WebSocketApp(
                ws_url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            # run_forever blocks until connection drops
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception as e:
            logger.error("WebSocket exception: %s", e)

        if running:
            logger.info("Reconnecting in 5 seconds...")
            time.sleep(5)


if __name__ == "__main__":
    main()
