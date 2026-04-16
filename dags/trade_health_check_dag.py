# Author: Bowen Ma
# Trade Health Check DAG
# Monitors whether the trade ingestion service is running
# by checking for recent trade data in Postgres.

import os
import logging
from datetime import datetime, timedelta

import psycopg2

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

STALE_THRESHOLD_MINUTES = int(os.environ.get("STALE_THRESHOLD_MINUTES", "10"))


def get_db_connection():
    return psycopg2.connect(**DB_CONN)


# ============================================================
# Task functions
# ============================================================
def check_recent_trades(**context):
    """Query Postgres for trades in the last STALE_THRESHOLD_MINUTES.
    Pushes the count and latest timestamp to XCom for the alert task.
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*), MAX(trade_timestamp), MAX(ingested_at)
                FROM raw_trades
                WHERE ingested_at >= NOW() - interval '%s minutes'
            """, (STALE_THRESHOLD_MINUTES,))
            count, latest_trade, latest_ingested = cur.fetchone()

            logger.info("Trades in last %d min: %d", STALE_THRESHOLD_MINUTES, count)
            if latest_trade:
                logger.info("Latest trade timestamp: %s", latest_trade)
                logger.info("Latest ingestion time:  %s", latest_ingested)

            context["ti"].xcom_push(key="trade_count", value=count)
            context["ti"].xcom_push(key="latest_trade", value=str(latest_trade))
    finally:
        conn.close()


def alert_if_stale(**context):
    """Log a warning if no trades were received recently.
    During market hours this indicates the ingestion service may be down.
    """
    count = context["ti"].xcom_pull(key="trade_count", task_ids="check_recent_trades")
    latest = context["ti"].xcom_pull(key="latest_trade", task_ids="check_recent_trades")

    if count and count > 0:
        logger.info("Trade ingestion healthy: %d trades in last %d min",
                     count, STALE_THRESHOLD_MINUTES)
    else:
        # Note: outside market hours (weekends, nights) this is expected
        logger.warning(
            "ALERT: No trades received in the last %d minutes. "
            "Last known trade: %s. "
            "Check if trade-ingestion service is running. "
            "(This may be normal outside market hours.)",
            STALE_THRESHOLD_MINUTES, latest,
        )


# ============================================================
# DAG definition
# ============================================================
default_args = {
    "owner": "bowen",
    "retries": 0,
}

with DAG(
    dag_id="trade_health_check",
    default_args=default_args,
    description="Monitor trade ingestion service health",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2026, 4, 15),
    catchup=False,
    tags=["monitoring", "health"],
) as dag:

    t_check = PythonOperator(
        task_id="check_recent_trades",
        python_callable=check_recent_trades,
    )

    t_alert = PythonOperator(
        task_id="alert_if_stale",
        python_callable=alert_if_stale,
    )

    t_check >> t_alert
