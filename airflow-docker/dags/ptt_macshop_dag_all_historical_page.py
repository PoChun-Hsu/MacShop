# 20250630_001 - Pochun Hsu - Craw for all pages in PTT MacShop. 100 pages as a batch. 
import asyncio
import aiohttp
import asyncpg
import requests
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import os
import re

DB_DSN = os.getenv('BULK_DB_DSN', "postgresql://airflow:airflow@postgres:5432/airflow")

def get_max_page():
    """取得PTT MacShop目前最大頁碼"""
    url = "https://www.ptt.cc/bbs/MacShop/index.html"
    cookies = {'over18': '1'}
    resp = requests.get(url, cookies=cookies)
    soup = BeautifulSoup(resp.text, "html.parser")
    prev_link = soup.select_one('div.btn-group-paging a.btn.wide:contains("上頁")')
    if prev_link:
        href = prev_link['href']
        m = re.search(r'index(\d+)\.html', href)
        if m:
            return int(m.group(1)) + 1  # 上一頁的indexN.html，所以+1是最新
    return 1

def create_temp_table():
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    pg_hook.run("DROP TABLE IF EXISTS ptt_macshop_articles_temp;")
    pg_hook.run("""
        CREATE TABLE ptt_macshop_articles_temp (
            id SERIAL PRIMARY KEY,
            title TEXT,
            author TEXT,
            post_time TIMESTAMP,
            link TEXT UNIQUE
        );
    """)

async def fetch_article_detail(session, link, cookies):
    try:
        async with session.get(link, cookies=cookies) as resp:
            html = await resp.text()
            soup = BeautifulSoup(html, 'html.parser')
            time_tag = soup.find("span", class_="article-meta-tag", string="時間")
            post_datetime = None
            if time_tag:
                time_value = time_tag.find_next_sibling("span").text.strip()
                try:
                    post_datetime = datetime.strptime(time_value, "%a %b %d %H:%M:%S %Y")
                except Exception:
                    pass
            return post_datetime
    except Exception as e:
        print(f"Error fetch detail: {e}")
        return None

async def extract_and_insert_page_async(page):
    cookies = {'over18': '1'}
    url = f"https://www.ptt.cc/bbs/MacShop/index{page}.html" if page > 1 else "https://www.ptt.cc/bbs/MacShop/index.html"
    async with aiohttp.ClientSession() as session:
        async with session.get(url, cookies=cookies) as res:
            html = await res.text()
            soup = BeautifulSoup(html, 'html.parser')

            rows = []
            tasks = []
            for entry in soup.select("div.r-ent"):
                try:
                    title_div = entry.select_one("div.title")
                    a_tag = title_div.select_one("a")
                    title = title_div.text.strip()
                    link = "https://www.ptt.cc" + a_tag["href"] if a_tag else None
                    author = entry.select_one("div.author").text.strip()
                    if link:
                        tasks.append(fetch_article_detail(session, link, cookies))
                        rows.append({"title": title, "author": author, "link": link})
                except Exception as e:
                    print(f"Error parsing entry: {e}")
                    continue

            post_times = await asyncio.gather(*tasks)
            data = [
                (row["title"], row["author"], post_time, row["link"])
                for row, post_time in zip(rows, post_times)
            ]

    if data:
        try:
            conn = await asyncpg.connect(dsn=DB_DSN)
            await conn.executemany(
                """
                INSERT INTO ptt_macshop_articles_temp (title, author, post_time, link)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (link) DO NOTHING;
                """,
                data
            )
            await conn.close()
        except Exception as e:
            print(f"DB bulk insert error: {e}")

def extract_and_insert_page(page):
    asyncio.run(extract_and_insert_page_async(page))

def extract_and_insert_range(start_page, end_page):
    for page in range(start_page, end_page + 1):
        print(f"Processing page {page}")
        asyncio.run(extract_and_insert_page_async(page))

def swap_table():
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    pg_hook.run("DROP TABLE IF EXISTS ptt_macshop_articles;")
    pg_hook.run("ALTER TABLE ptt_macshop_articles_temp RENAME TO ptt_macshop_articles;")

default_args = {"start_date": datetime(2025, 5, 1)}

with DAG(
    "ptt_macshop_parallel_scraper_async_bulk",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["ptt", "macshop", "postgres", "async", "bulk"],
    max_active_tasks=20,
) as dag:

    t_create_temp = PythonOperator(
        task_id="create_temp_table",
        python_callable=create_temp_table
    )

    max_page = get_max_page()
    page_numbers = list(range(1, max_page + 1))
    batch_size = 100

    with TaskGroup("parallel_scrape") as tg:
        for i in range(0, len(page_numbers), batch_size):
            start = page_numbers[i]
            end = min(start + batch_size - 1, max_page)
            PythonOperator(
                task_id=f"scrape_pages_{start}_{end}",
                python_callable=extract_and_insert_range,
                op_args=[start, end]
            )

    t_swap = PythonOperator(
        task_id="swap_table",
        python_callable=swap_table
    )

    t_create_temp >> tg >> t_swap
