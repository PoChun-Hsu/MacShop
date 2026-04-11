from __future__ import annotations

# ptt_macshop_dag_all_historical_page.py
# Lakehouse version:
# PTT Crawl -> shared staging files -> Spark -> Iceberg

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import asyncio
import aiohttp
from bs4 import BeautifulSoup
import hashlib
import json
import random
import re
import shutil
import traceback

import redis


PTT_BOARD = "MacShop"
DEFAULT_START_DATE = datetime(2025, 5, 1)

# keep same meaning as original DAG
BATCH_SIZE = 100
CONCURRENT_SIZE = 10

# keep table-name variables, but now they are Iceberg table names
ARTICLE_TABLE = "ptt_macshop_articles"
PAGE_TABLE = "ptt_macshop_page_dates"

# your current docker-compose mount
PROJECT_ROOT = "/opt/spark-apps"

# shared path:
# host:   /Users/.../airflow3-docker/spark_app/staging
# spark:  /opt/spark-apps/staging
SPARK_APP_DIR = "/opt/spark-apps"
STAGING_ROOT = "/opt/spark-apps/staging"
SPARK_STAGE_ROOT = "/opt/spark-apps/staging"

SPARK_JOB_PATH_HOST = f"{SPARK_APP_DIR}/load_ptt_macshop_to_iceberg.py"
SPARK_JOB_PATH_CONTAINER = "/opt/spark-apps/load_ptt_macshop_to_iceberg.py"

# Iceberg catalog/table
ICEBERG_CATALOG = "iceberg"
ICEBERG_NAMESPACE = "default"
ICEBERG_ARTICLE_TABLE = f"{ICEBERG_CATALOG}.{ICEBERG_NAMESPACE}.{ARTICLE_TABLE}"
ICEBERG_PAGE_TABLE = f"{ICEBERG_CATALOG}.{ICEBERG_NAMESPACE}.{PAGE_TABLE}"

USER_AGENTS = [
    # Chrome - Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",

    # Chrome - Mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_0_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Safari/605.1.15",

    # Edge - Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.2478.67",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.2365.80",

    # Firefox - Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",

    # Firefox - Mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.13; rv:120.0) Gecko/20100101 Firefox/120.0",

    # Safari - Mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15",

    # iPhone
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1",

    # Android
    "Mozilla/5.0 (Linux; Android 14; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 13; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Mobile Safari/537.36",

    # iPad
    "Mozilla/5.0 (iPad; CPU OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
]

default_args = {
    "start_date": DEFAULT_START_DATE,
}


def sanitize_run_id(run_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)


def get_redis_client():
    # lazy init: do not connect at DAG parse time
    return redis.Redis(host="redis", port=6379, db=0, decode_responses=True)


def parse_full_datetime(date_str: str):
    """
    Example:
    Tue Jun 25 21:53:16 2024
    """
    try:
        return datetime.strptime(date_str, "%a %b %d %H:%M:%S %Y")
    except Exception:
        return None


def prepare_stage_dir(**context):
    run_token = sanitize_run_id(context["run_id"])
    run_stage_dir = Path(STAGING_ROOT) / run_token

    if run_stage_dir.exists():
        shutil.rmtree(run_stage_dir)

    (run_stage_dir / "articles").mkdir(parents=True, exist_ok=True)
    (run_stage_dir / "page_dates").mkdir(parents=True, exist_ok=True)

    context["ti"].xcom_push(key="run_token", value=run_token)
    print(f"✅ Stage dir prepared: {run_stage_dir}")


async def fetch_ptt_page_async(session, page_num):
    redis_client = get_redis_client()

    # global ban flag
    if redis_client.get("ptt:ban_flag") == "1":
        print(f"[SKIP] banned recently, skip page {page_num}")
        return {"articles": [], "page_stat": None}

    url = f"https://www.ptt.cc/bbs/{PTT_BOARD}/index{page_num}.html"
    cookies = {"over18": "1"}
    headers = {"User-Agent": random.choice(USER_AGENTS)}

    await asyncio.sleep(random.uniform(0.2, 1.2))

    html = None
    for attempt in range(3):
        try:
            async with session.get(
                url,
                cookies=cookies,
                headers=headers,
                timeout=10,
                ssl=False
            ) as resp:
                html = await resp.text()

                if resp.status in (403, 429) or "over18" in html:
                    redis_client.set("ptt:ban_flag", "1", ex=30)
                    raise Exception(f"ban/verify triggered, status={resp.status}")

            break

        except (aiohttp.ClientError, asyncio.TimeoutError, Exception) as e:
            if attempt == 2:
                print(f"[WARN] page {page_num} failed after retry: {e}")
                return {"articles": [], "page_stat": None}
            await asyncio.sleep(2 ** attempt)

    if not html:
        return {"articles": [], "page_stat": None}

    soup = BeautifulSoup(html, "html.parser")
    articles = []

    for entry in soup.select("div.r-ent"):
        try:
            title_div = entry.select_one("div.title")
            a_tag = title_div.select_one("a") if title_div else None
            title = title_div.text.strip() if title_div else None
            link = "https://www.ptt.cc" + a_tag["href"] if a_tag else None

            if not link:
                continue

            # redis de-dup
            if redis_client.sismember("ptt:macshop:crawled_links", link):
                continue

            author_node = entry.select_one("div.author")
            author = author_node.text.strip() if author_node else None

            date = None
            description = None
            description_hash = None

            art_headers = {"User-Agent": random.choice(USER_AGENTS)}
            await asyncio.sleep(random.uniform(0.1, 0.4))

            art_html = None
            for attempt in range(3):
                try:
                    async with session.get(
                        link,
                        cookies=cookies,
                        headers=art_headers,
                        timeout=10,
                        ssl=False
                    ) as art_resp:
                        art_html = await art_resp.text()

                        if art_resp.status in (403, 429) or "over18" in art_html:
                            redis_client.set("ptt:ban_flag", "1", ex=30)
                            raise Exception(f"article ban/verify triggered, status={art_resp.status}")

                    break

                except (aiohttp.ClientError, asyncio.TimeoutError, Exception) as e:
                    if attempt == 2:
                        print(f"[WARN] article fetch failed, skip link={link}, err={e}")
                        art_html = None
                    await asyncio.sleep(2 ** attempt)

            if not art_html:
                continue

            art_soup = BeautifulSoup(art_html, "html.parser")
            meta_values = art_soup.select("span.article-meta-value")
            if len(meta_values) >= 4:
                date = parse_full_datetime(meta_values[3].text.strip())

            content_div = art_soup.select_one("#main-content")
            description = content_div.get_text(separator="\n", strip=True) if content_div else None
            description_hash = (
                hashlib.sha256(description.encode("utf-8")).hexdigest()
                if description else None
            )

            redis_client.sadd("ptt:macshop:crawled_links", link)

            articles.append(
                {
                    "Title": title,
                    "Author": author,
                    "Created_Date": date,
                    "Link": link,
                    "Description": description,
                    "Description_Hash": description_hash,
                    "Updated_Date": datetime.now(timezone.utc),
                }
            )

        except Exception as e:
            print(f"[WARN] parse entry failed: {e}")
            print(traceback.format_exc())
            continue

    page_stat = None
    valid_dates = [a["Created_Date"] for a in articles if a["Created_Date"]]
    if valid_dates:
        page_stat = {
            "Page_Num": page_num,
            "Url": url,
            "Min_Date": min(valid_dates),
            "Max_Date": max(valid_dates),
            "Updated_Date": datetime.now(timezone.utc),
        }

    return {"articles": articles, "page_stat": page_stat}


async def async_extract_articles_batch(start_page, end_page, concurrent=CONCURRENT_SIZE):
    all_articles = []
    all_page_stats = []

    connector = aiohttp.TCPConnector(limit=concurrent, ssl=False)
    timeout = aiohttp.ClientTimeout(total=30)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = [fetch_ptt_page_async(session, page_num) for page_num in range(start_page, end_page + 1)]

        for future in asyncio.as_completed(tasks):
            try:
                result = await future
                all_articles.extend(result["articles"])
                if result["page_stat"]:
                    all_page_stats.append(result["page_stat"])
            except aiohttp.ClientConnectorError as e:
                print(f"[WARN] connection error, skip one page: {e}")
                continue
            except Exception as e:
                print(f"[ERROR] unexpected async batch error: {e}")
                print(traceback.format_exc())
                raise

    return all_articles, all_page_stats


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            serializable = {}
            for key, value in row.items():
                if isinstance(value, datetime):
                    serializable[key] = value.isoformat()
                else:
                    serializable[key] = value
            f.write(json.dumps(serializable, ensure_ascii=False) + "\n")


def extract_articles_batch(start_page, end_page, **context):
    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)

        articles, page_stats = loop.run_until_complete(
            async_extract_articles_batch(start_page, end_page, concurrent=CONCURRENT_SIZE)
        )
    finally:
        asyncio.set_event_loop(None)
        loop.close()

    run_token = context["ti"].xcom_pull(task_ids="prepare_stage_dir", key="run_token")
    run_stage_dir = Path(STAGING_ROOT) / run_token
    batch_name = f"{start_page}_{end_page}"

    article_file = run_stage_dir / "articles" / f"articles_{batch_name}.jsonl"
    page_file = run_stage_dir / "page_dates" / f"page_dates_{batch_name}.jsonl"

    write_jsonl(article_file, articles)
    write_jsonl(page_file, page_stats)

    print(f"✅ Batch staged: {batch_name}, articles={len(articles)}, page_stats={len(page_stats)}")


async def _get_max_page_async():
    url = f"https://www.ptt.cc/bbs/{PTT_BOARD}/index.html"
    cookies = {"over18": "1"}
    timeout = aiohttp.ClientTimeout(total=30)

    connector = aiohttp.TCPConnector(limit=1, ssl=False)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        for attempt in range(3):
            try:
                async with session.get(
                    url,
                    cookies=cookies,
                    headers={"User-Agent": random.choice(USER_AGENTS)},
                    ssl=False
                ) as resp:
                    html = await resp.text()

                    if resp.status in (403, 429) or "over18" in html:
                        raise Exception(f"ban/verify triggered, status={resp.status}")

                    soup = BeautifulSoup(html, "html.parser")
                    btn = soup.select_one("div.btn-group-paging a.btn.wide:nth-child(2)")

                    if btn and "index" in btn["href"]:
                        return int(btn["href"].split("index")[1].split(".html")[0]) + 1

                    return 1

            except Exception:
                if attempt == 2:
                    raise
                await asyncio.sleep(2 ** attempt)


def get_max_page():
    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(_get_max_page_async())
    finally:
        asyncio.set_event_loop(None)
        loop.close()


def clear_redis_keys():
    redis_client = get_redis_client()
    redis_client.delete("ptt:macshop:crawled_links")
    redis_client.delete("ptt:ban_flag")
    print("✅ Cleared Redis keys")


def generate_batches(**context):
    max_page = get_max_page()
    print(f"PTT MacShop max page: {max_page}")

    batch_list = []
    for i in range(1, max_page + 1, BATCH_SIZE):
        start = i
        end = min(i + BATCH_SIZE - 1, max_page)
        batch_list.append((start, end))

    context["ti"].xcom_push(key="batch_list", value=batch_list)
    print(f"✅ Generated {len(batch_list)} batches")


def run_batch(**context):
    batch_list = context["ti"].xcom_pull(task_ids="generate_batches", key="batch_list")

    for start_page, end_page in batch_list:
        print(f"Processing batch {start_page}-{end_page}")
        extract_articles_batch(start_page, end_page, **context)


def validate_stage_files(**context):
    run_token = context["ti"].xcom_pull(task_ids="prepare_stage_dir", key="run_token")
    run_stage_dir = Path(STAGING_ROOT) / run_token

    article_files = list((run_stage_dir / "articles").glob("*.jsonl"))
    page_files = list((run_stage_dir / "page_dates").glob("*.jsonl"))

    if not article_files:
        raise ValueError("No staged article files found.")

    total_article_bytes = sum(p.stat().st_size for p in article_files)
    total_page_bytes = sum(p.stat().st_size for p in page_files)

    if total_article_bytes == 0:
        raise ValueError("All staged article files are empty.")

    print(f"✅ Stage validation passed: article_files={len(article_files)}, page_files={len(page_files)}")


def write_spark_job_file():
    Path(SPARK_APP_DIR).mkdir(parents=True, exist_ok=True)

    spark_job = f'''from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, trim
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import argparse

ICEBERG_CATALOG = "{ICEBERG_CATALOG}"
ICEBERG_NAMESPACE = "{ICEBERG_NAMESPACE}"
ARTICLE_TABLE = "{ARTICLE_TABLE}"
PAGE_TABLE = "{PAGE_TABLE}"

ICEBERG_ARTICLE_TABLE = "{ICEBERG_ARTICLE_TABLE}"
ICEBERG_PAGE_TABLE = "{ICEBERG_PAGE_TABLE}"

SPARK_STAGE_ROOT = "{SPARK_STAGE_ROOT}"

parser = argparse.ArgumentParser()
parser.add_argument("--run-token", required=True)
args = parser.parse_args()

run_token = args.run_token

spark = (
    SparkSession.builder
    .master("spark://spark:7077")
    .appName("load-ptt-macshop-to-iceberg")
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.iceberg.type", "rest")
    .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181")
    .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.iceberg.s3.endpoint", "http://minio:9000")
    .config("spark.sql.catalog.iceberg.s3.path-style-access", "true")
    .config("spark.sql.catalog.iceberg.s3.access-key-id", "admin")
    .config("spark.sql.catalog.iceberg.s3.secret-access-key", "admin123")
    .config("spark.sql.catalog.iceberg.client.region", "us-east-1")
    .getOrCreate()
)

spark.conf.set("spark.sql.shuffle.partitions", 2)
spark.conf.set("spark.sql.files.maxPartitionBytes", 67108864)  # 64MB

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {{ICEBERG_CATALOG}}.{{ICEBERG_NAMESPACE}}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {{ICEBERG_ARTICLE_TABLE}} (
    title string,
    author string,
    created_date timestamp,
    link string,
    description string,
    description_hash string,
    updated_date timestamp
)
USING iceberg
PARTITIONED BY (days(created_date))
TBLPROPERTIES (
    'format-version'='2',
    'write.distribution-mode'='hash'
)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {{ICEBERG_PAGE_TABLE}} (
    page_num int,
    url string,
    min_date timestamp,
    max_date timestamp,
    updated_date timestamp
)
USING iceberg
PARTITIONED BY (days(max_date))
TBLPROPERTIES (
    'format-version'='2',
    'write.distribution-mode'='hash'
)
""")

article_schema = StructType([
    StructField("Title", StringType(), True),
    StructField("Author", StringType(), True),
    StructField("Created_Date", StringType(), True),
    StructField("Link", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Description_Hash", StringType(), True),
    StructField("Updated_Date", StringType(), True),
])

page_schema = StructType([
    StructField("Page_Num", IntegerType(), True),
    StructField("Url", StringType(), True),
    StructField("Min_Date", StringType(), True),
    StructField("Max_Date", StringType(), True),
    StructField("Updated_Date", StringType(), True),
])

article_path = f"{{SPARK_STAGE_ROOT}}/{{run_token}}/articles/*.jsonl"
print("DEBUG article_path=", article_path)
page_path = f"{{SPARK_STAGE_ROOT}}/{{run_token}}/page_dates/*.jsonl"

article_df = (
    spark.read.json(article_path)
    .select(
        trim(col("Title")).alias("title"),
        trim(col("Author")).alias("author"),
        to_timestamp(col("Created_Date")).alias("created_date"),
        trim(col("Link")).alias("link"),
        col("Description").alias("description"),
        trim(col("Description_Hash")).alias("description_hash"),
        to_timestamp(col("Updated_Date")).alias("updated_date"),
    )
    .filter(col("link").isNotNull())
    .dropDuplicates(["link"])
)

article_df = article_df.repartition(1)

page_df = (
    spark.read.schema(page_schema).json(page_path)
    .select(
        col("Page_Num").alias("page_num"),
        trim(col("Url")).alias("url"),
        to_timestamp(col("Min_Date")).alias("min_date"),
        to_timestamp(col("Max_Date")).alias("max_date"),
        to_timestamp(col("Updated_Date")).alias("updated_date"),
    )
    .filter(col("page_num").isNotNull())
    .dropDuplicates(["page_num"])
)

article_df.createOrReplaceTempView("src_articles")
page_df.createOrReplaceTempView("src_page_dates")

# full historical refresh
spark.sql(f"""
INSERT OVERWRITE {{ICEBERG_ARTICLE_TABLE}}
SELECT
    title,
    author,
    created_date,
    link,
    description,
    description_hash,
    updated_date
FROM src_articles
""")

spark.sql(f"""
INSERT OVERWRITE {{ICEBERG_PAGE_TABLE}}
SELECT
    page_num,
    url,
    min_date,
    max_date,
    updated_date
FROM src_page_dates
""")

print(f"article_count={{article_df.count()}}")
print(f"page_count={{page_df.count()}}")

spark.stop()
'''
    Path(SPARK_JOB_PATH_HOST).write_text(spark_job, encoding="utf-8")
    print(f"✅ Spark job file written: {SPARK_JOB_PATH_HOST}")


with DAG(
    "ptt_macshop_dag_lakehouse_full_sync",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["ptt", "macshop", "lakehouse", "iceberg"],
) as dag:

    clear_redis = PythonOperator(
        task_id="clear_redis_keys",
        python_callable=clear_redis_keys,
    )

    prepare_stage = PythonOperator(
        task_id="prepare_stage_dir",
        python_callable=prepare_stage_dir,
    )

    gen_batches = PythonOperator(
        task_id="generate_batches",
        python_callable=generate_batches,
    )

    process_batches = PythonOperator(
        task_id="process_batches",
        python_callable=run_batch,
    )

    validate_stage = PythonOperator(
        task_id="validate_stage_files",
        python_callable=validate_stage_files,
    )

    write_spark_job = PythonOperator(
        task_id="write_spark_job_file",
        python_callable=write_spark_job_file,
    )

    load_to_iceberg = BashOperator(
        task_id="load_to_iceberg",
        bash_command=r"""
        set -euo pipefail

        docker exec spark cat /opt/spark-apps/load_ptt_macshop_to_iceberg.py

        RUN_TOKEN="{{ ti.xcom_pull(task_ids='prepare_stage_dir', key='run_token') }}"

        docker exec -i spark spark-submit \
        --master spark://spark:7077 \
        {{ params.spark_job_path }} \
        --run-token "${RUN_TOKEN}"
        """,
        params={
            "spark_job_path": SPARK_JOB_PATH_CONTAINER,
        },
    )

    clear_redis >> prepare_stage >> gen_batches >> process_batches >> validate_stage >> write_spark_job >> load_to_iceberg
