# Author: Bowen Ma
# News Ingestion DAG
# Periodically fetches company news from Finnhub REST API,
# deduplicates, and stores to Postgres + Minio S3.

import json
import os
import logging
from datetime import datetime, timedelta

import requests
import psycopg2
from psycopg2.extras import execute_values
import boto3

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

# ============================================================
# Configuration
# ============================================================
FINNHUB_API_KEY = os.environ.get("FINNHUB_API_KEY", "")
SYMBOLS = os.environ.get("TRADE_SYMBOLS", "AAPL,MSFT,AMZN").split(",")

DB_CONN = {
    "host": os.environ.get("DB_HOST", "postgres"),
    "port": os.environ.get("DB_PORT", "5432"),
    "dbname": os.environ.get("DB_NAME", "trade_pipeline"),
    "user": os.environ.get("DB_USER", "pipeline"),
    "password": os.environ.get("DB_PASSWORD", "pipeline"),
}

S3_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
S3_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")
S3_BUCKET = os.environ.get("S3_BUCKET", "trade-pipeline")

FINNHUB_NEWS_URL = "https://finnhub.io/api/v1/company-news"


# ============================================================
# Helper functions
# ============================================================
def get_db_connection():
    return psycopg2.connect(**DB_CONN)


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name="us-east-1",
    )


# ============================================================
# Task functions
# ============================================================
def fetch_news(**context):
    """Fetch company news from Finnhub for all tracked symbols."""
    # Look back 2 days to catch delayed publications
    today = datetime.utcnow().strftime("%Y-%m-%d")
    two_days_ago = (datetime.utcnow() - timedelta(days=2)).strftime("%Y-%m-%d")

    all_articles = []

    for symbol in SYMBOLS:
        logger.info("Fetching news for %s (%s to %s)", symbol, two_days_ago, today)
        resp = requests.get(
            FINNHUB_NEWS_URL,
            params={
                "symbol": symbol,
                "from": two_days_ago,
                "to": today,
                "token": FINNHUB_API_KEY,
            },
            timeout=30,
        )
        resp.raise_for_status()
        articles = resp.json()
        logger.info("  %s: got %d articles", symbol, len(articles))

        # Tag each article with the queried symbol
        for article in articles:
            article["_symbol"] = symbol

        all_articles.extend(articles)

    logger.info("Total articles fetched: %d", len(all_articles))

    # Pass to next task via XCom
    context["ti"].xcom_push(key="raw_articles", value=all_articles)


def dedup_and_validate(**context):
    """Deduplicate articles by article_id and validate required fields."""
    articles = context["ti"].xcom_pull(key="raw_articles", task_ids="fetch_news")

    if not articles:
        logger.info("No articles to process")
        context["ti"].xcom_push(key="valid_articles", value=[])
        return

    # Deduplicate by article id within this batch
    seen_ids = set()
    unique = []
    for a in articles:
        aid = a.get("id")
        if aid is None:
            logger.warning("Article missing id, skipping: %s", a.get("headline", "?"))
            continue
        if aid in seen_ids:
            continue
        seen_ids.add(aid)

        # Validate required fields
        if not a.get("headline") or not a.get("datetime"):
            logger.warning("Article %d missing required fields, skipping", aid)
            continue

        unique.append(a)

    logger.info("After dedup: %d unique articles (from %d raw)", len(unique), len(articles))
    context["ti"].xcom_push(key="valid_articles", value=unique)


def store_to_s3(**context):
    """Archive raw articles as JSON files to Minio S3."""
    articles = context["ti"].xcom_pull(key="valid_articles", task_ids="dedup_and_validate")

    if not articles:
        logger.info("No articles to store in S3")
        return

    s3 = get_s3_client()
    today = datetime.utcnow().strftime("%Y-%m-%d")

    for article in articles:
        symbol = article.get("_symbol", "UNKNOWN")
        article_id = article["id"]
        key = f"raw/news/{today}/{symbol}/{article_id}.json"

        s3.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=json.dumps(article, ensure_ascii=False),
            ContentType="application/json",
        )

    logger.info("Stored %d articles to S3", len(articles))


def store_to_postgres(**context):
    """Insert deduplicated articles into Postgres. Duplicates are skipped."""
    articles = context["ti"].xcom_pull(key="valid_articles", task_ids="dedup_and_validate")

    if not articles:
        logger.info("No articles to store in Postgres")
        return

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            sql = """
                INSERT INTO raw_news
                    (article_id, symbol, headline, summary, source, url,
                     published_at, category, raw_json)
                VALUES %s
                ON CONFLICT (article_id) DO NOTHING
            """
            values = [
                (
                    a["id"],
                    a.get("_symbol", "UNKNOWN"),
                    a.get("headline", ""),
                    a.get("summary", ""),
                    a.get("source", ""),
                    a.get("url", ""),
                    datetime.utcfromtimestamp(a["datetime"]),
                    a.get("category", ""),
                    json.dumps(a),
                )
                for a in articles
            ]
            execute_values(cur, sql, values)
            inserted = cur.rowcount

        conn.commit()
        logger.info("Postgres: %d new articles inserted (%d duplicates skipped)",
                     inserted, len(articles) - inserted)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ============================================================
# DAG definition
# ============================================================
default_args = {
    "owner": "bowen",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="news_ingestion",
    default_args=default_args,
    description="Fetch company news from Finnhub and store to Postgres + S3",
    schedule_interval="*/30 * * * *",
    start_date=datetime(2026, 4, 15),
    catchup=False,
    tags=["ingestion", "finnhub"],
) as dag:

    t_fetch = PythonOperator(
        task_id="fetch_news",
        python_callable=fetch_news,
    )

    t_dedup = PythonOperator(
        task_id="dedup_and_validate",
        python_callable=dedup_and_validate,
    )

    t_s3 = PythonOperator(
        task_id="store_to_s3",
        python_callable=store_to_s3,
    )

    t_postgres = PythonOperator(
        task_id="store_to_postgres",
        python_callable=store_to_postgres,
    )

    # fetch → dedup → [s3, postgres] in parallel
    t_fetch >> t_dedup >> [t_s3, t_postgres]
