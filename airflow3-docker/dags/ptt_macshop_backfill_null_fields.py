# -*- coding: utf-8 -*-
# Ptt MacShop - Backfill NULL fields (parameterized)
# ------------------------------------------------------------------------------
# What this DAG does
# 1) Query Postgres for rows in Ptt_Macshop_Articles where ANY of the configured
#    fields is NULL (or all rows if ONLY_WHEN_NULL=False).
# 2) Fetch each PTT article page once, parse metadata + main content.
# 3) Update only the configured fields, and (by default) ONLY when those fields
#    are currently NULL in DB (no overwriting).
#
# How to use
# - Configure FIELDS_TO_BACKFILL below (whitelist only).
# - Trigger the DAG manually (schedule=None), or set a schedule.
#
# Notes
# - Designed to stay close to your existing incremental async style:
#   aiohttp + asyncio concurrency, Redis ban flag, User-Agent rotation, PostgresHook.
# - Does not use Redis hash de-dup because this is a backfill job for missing columns.
# ------------------------------------------------------------------------------

from __future__ import annotations

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException
from airflow import Dataset
from airflow.operators.empty import EmptyOperator

from datetime import datetime
import asyncio
import aiohttp
import random
import hashlib
import redis
from bs4 import BeautifulSoup


# ===============================
# Parameters (adjust here)
# ===============================
DEFAULT_START_DATE = datetime(2025, 5, 1)
CONCURRENT_SIZE = 100
BACKFILL_LIMIT = 5000

# Safety: if True, only fill when DB column is NULL (recommended).
# If False, it will overwrite existing values (dangerous; use with care).
ONLY_WHEN_NULL = True

# Choose which fields you want to backfill (parameterized)
# Supported fields (whitelist):
# - Created_Date
# - Author
# - Title
# - Description
# - Description_Hash
FIELDS_TO_BACKFILL = [
    "Created_Date",
    # "Author",
    # "Title",
    # "Description",
    # "Description_Hash",
]

# Dataset for downstream awareness (optional)
RAW_UPDATED = Dataset("dataset://ptt_macshop/raw_updated")

# Redis (only used for ban flag; safe even if you don't set it)
redis_client = redis.Redis(host="redis", port=6379, decode_responses=False)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_0_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
]

SUPPORTED_FIELDS = {
    "Created_Date",
    "Author",
    "Title",
    "Description",
    "Description_Hash",
}


# ===============================
# Helpers
# ===============================
def parse_full_datetime(date_str: str | None):
    """
    PTT article meta time string example:
    'Thu Dec  5 10:27:43 2024'
    """
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%a %b %d %H:%M:%S %Y")
    except Exception:
        return None


def normalize_description(main_content: str | None) -> str | None:
    """
    Normalize main content text for storage/hash.
    Keep it simple and stable: strip and collapse.
    """
    if not main_content:
        return None
    text = main_content.strip()
    return text if text else None


def compute_sha256(text: str | None) -> str | None:
    if not text:
        return None
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def validate_fields(fields: list[str]) -> list[str]:
    bad = [f for f in fields if f not in SUPPORTED_FIELDS]
    if bad:
        raise AirflowException(
            f"❌ Unsupported fields in FIELDS_TO_BACKFILL: {bad}. "
            f"Supported: {sorted(SUPPORTED_FIELDS)}"
        )
    # Deduplicate while preserving order
    seen = set()
    out = []
    for f in fields:
        if f not in seen:
            seen.add(f)
            out.append(f)
    return out


async def fetch_article_fields(session: aiohttp.ClientSession, link: str, fields: list[str]) -> dict:
    """
    Fetch article page once, then derive requested fields.
    Returns dict with Link and extracted fields (may be None).
    """
    if redis_client.get("ptt:ban_flag") == b"1":
        return {"Link": link, "_error": "ban_flag"}

    headers = {"User-Agent": random.choice(USER_AGENTS)}
    cookies = {"over18": "1"}

    # Gentle jitter to reduce ban risk
    await asyncio.sleep(random.uniform(0.2, 1.2))

    try:
        async with session.get(link, headers=headers, cookies=cookies, timeout=15) as res:
            html = await res.text()
    except asyncio.TimeoutError:
        return {"Link": link, "_error": "timeout"}
    except Exception as e:
        return {"Link": link, "_error": f"exception:{type(e).__name__}"}

    soup = BeautifulSoup(html, "html.parser")
    meta_values = soup.select("span.article-meta-value")

    # PTT meta pattern commonly:
    # [0] Author, [1] Board, [2] Title, [3] Date
    author = meta_values[0].text.strip() if len(meta_values) >= 1 else None
    title = meta_values[2].text.strip() if len(meta_values) >= 3 else None
    created_dt = parse_full_datetime(meta_values[3].text.strip()) if len(meta_values) >= 4 else None

    content_div = soup.select_one("#main-content")
    description = normalize_description(content_div.get_text(separator="\n", strip=True) if content_div else None)
    description_hash = compute_sha256(description)

    out = {"Link": link, "_error": None}
    if "Author" in fields:
        out["Author"] = author
    if "Title" in fields:
        out["Title"] = title
    if "Created_Date" in fields:
        out["Created_Date"] = created_dt
    if "Description" in fields:
        out["Description"] = description
    if "Description_Hash" in fields:
        # Ensure dependency: if user requests hash but not description, still compute from description
        out["Description_Hash"] = description_hash

    return out


async def backfill_fields_async(links: list[str], fields: list[str]) -> list[dict]:
    if not links:
        return []

    connector = aiohttp.TCPConnector(limit=CONCURRENT_SIZE)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [fetch_article_fields(session, link, fields) for link in links]
        results: list[dict] = []

        for fut in asyncio.as_completed(tasks):
            r = await fut
            # keep even if partially None; updater will decide
            if r.get("_error") is None:
                results.append(r)

        return results


# ===============================
# DAG definition
# ===============================
@dag(
    dag_id="Ptt_Macshop_Backfill_Null_Fields",
    start_date=DEFAULT_START_DATE,
    schedule=None,  # recommended manual trigger for backfill jobs
    catchup=False,
    max_active_runs=1,
    tags=["ptt", "macshop", "backfill", "async"],
)
def ptt_macshop_backfill_null_fields_async():
    fields = validate_fields(FIELDS_TO_BACKFILL)

    @task
    def validate_base_tables():
        pg = PostgresHook(postgres_conn_id="postgres_default")
        try:
            pg.get_first("SELECT 1 FROM Ptt_Macshop_Articles LIMIT 1;")
        except Exception as e:
            raise AirflowException("❌ 找不到 Ptt_Macshop_Articles，請先執行 Full run 建表。") from e

    @task
    def get_links_to_backfill(limit: int = BACKFILL_LIMIT) -> list[str]:
        """
        Return links where ANY of the configured fields is NULL.
        """
        pg = PostgresHook(postgres_conn_id="postgres_default")

        if not fields:
            return []

        if ONLY_WHEN_NULL:
            where_any_null = " OR ".join([f"{f} IS NULL" for f in fields])
            sql = f"""
                SELECT Link
                FROM Ptt_Macshop_Articles
                WHERE Link IS NOT NULL
                  AND ({where_any_null})
                ORDER BY Updated_Date DESC NULLS LAST
                LIMIT %s
            """
        else:
            sql = """
                SELECT Link
                FROM Ptt_Macshop_Articles
                WHERE Link IS NOT NULL
                ORDER BY Updated_Date DESC NULLS LAST
                LIMIT %s
            """

        rows = pg.get_records(sql, parameters=(limit,))
        links = [r[0] for r in rows if r and r[0]]
        print(f"✅ Backfill fields: {fields}")
        print(f"✅ Found {len(links)} links to process.")
        return links

    @task
    def extract_fields(links: list[str]) -> list[dict]:
        if not links:
            print("✅ No links to process.")
            return []
        results = asyncio.run(backfill_fields_async(links, fields))
        print(f"✅ Extracted field payloads: {len(results)}")
        return results

    @task
    def update_fields(rows: list[dict]) -> int:
        """
        Update configured fields. By default, only sets values where DB column is NULL.
        """
        if not rows:
            print("✅ Nothing to update.")
            return 0

        pg = PostgresHook(postgres_conn_id="postgres_default")

        updated = 0
        skipped_no_values = 0

        for r in rows:
            link = r.get("Link")
            if not link:
                continue

            # Build SET clauses dynamically. We will only include fields that:
            # - are configured
            # - have a non-None extracted value
            set_clauses = []
            params = []

            for f in fields:
                if f not in r:
                    continue
                value = r.get(f)
                if value is None:
                    continue

                if ONLY_WHEN_NULL:
                    # Only update if DB currently NULL
                    set_clauses.append(f"{f} = CASE WHEN {f} IS NULL THEN %s ELSE {f} END")
                else:
                    # Force overwrite
                    set_clauses.append(f"{f} = %s")
                params.append(value)

            if not set_clauses:
                skipped_no_values += 1
                continue

            # Always update Updated_Date when we actually write something
            set_clauses.append("Updated_Date = NOW()")

            sql = f"""
                UPDATE Ptt_Macshop_Articles
                SET {", ".join(set_clauses)}
                WHERE Link = %s
            """
            params.append(link)

            pg.run(sql, parameters=tuple(params), autocommit=True)
            updated += 1

        print(f"✅ Updated rows: {updated}")
        if skipped_no_values:
            print(f"ℹ️ Skipped rows (no extracted non-null values): {skipped_no_values}")
        return updated

    publish = EmptyOperator(task_id="publish_raw_updated", outlets=[RAW_UPDATED])

    v = validate_base_tables()
    links = get_links_to_backfill()
    extracted = extract_fields(links)
    upd = update_fields(extracted)

    v >> links >> extracted >> upd >> publish


dag = ptt_macshop_backfill_null_fields_async()
