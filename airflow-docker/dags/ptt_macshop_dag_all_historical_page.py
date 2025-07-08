# 20250702_001 - PoChun Hsu - [Alter]  batch insert replace insert row by row.
# 20250702_002 - PoChun Hsu - [Alter]  web crawler with multi thread.
# 20250703_001 - PoChun Hsu - [Create] Implemented rotation of 20 predefined headers to emulate diverse client behavior and bypass PTT anti-crawling measures.
# 20250703_002 - PoChun Hsu - [Create] Added retry mechanism with backoff (30 seconds to 3 minutes) upon ban detection. All threads will be halted immediately when a ban is encountered.
# 20250708_001 - PoChun Hsu - [Alter]  Implemented high-speed concurrent crawler using async/await with aiohttp. Execution time from 60 minutes to 15 minutes.
# 20250708_002 - PoChun Hsu - [Add]    Add new columns: description.

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import random
import asyncio
import aiohttp
from bs4 import BeautifulSoup

PTT_BOARD = "MacShop"
DEFAULT_START_DATE = datetime(2025, 5, 1)
# 每次寫入 temp table的資料筆數 = PTT每頁筆數(20) * BATCH_SIZE
BATCH_SIZE = 100      # 20250702_002
# 控制最大 thread 數，建議不要超過 5~10，避免被 ban
CONCURRENT_SIZE = 100 # 20250702_001

# 20250703_001 >>
# 定義多種設備，避免被判定成機器人，更像不同使用者
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
    "Mozilla/5.0 (iPad; CPU OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1"
]
# 20250703_001 <<

default_args = {
    "start_date": DEFAULT_START_DATE,
}

def parse_full_datetime(date_str):
    """
    例子：Tue Jun 25 21:53:16 2024
    回傳 datetime 物件或 None
    """
    try:
        return datetime.strptime(date_str, "%a %b %d %H:%M:%S %Y")
    except Exception:
        return None

def prepare_temp_table():
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    # 刪除 temp table 如果已存在
    pg_hook.run("DROP TABLE IF EXISTS ptt_macshop_articles_temp;")
    # 建立 temp table
    create_table_sql = """
    CREATE TABLE ptt_macshop_articles_temp (
        id SERIAL PRIMARY KEY,
        title TEXT,
        author TEXT,
        date TIMESTAMP,
        link TEXT,
        description TEXT
    );
    """ # 20250708_001
    pg_hook.run(create_table_sql)

# 20250708_001 >>
async def fetch_ptt_page_async(session, page_num):
    url = f"https://www.ptt.cc/bbs/{PTT_BOARD}/index{page_num}.html"
    cookies = {'over18': '1'}
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    await asyncio.sleep(random.uniform(0.2, 1.2))
    async with session.get(url, cookies=cookies, headers=headers, timeout=10) as resp:
        html = await resp.text()
        # 簡單ban偵測
        if resp.status in (403, 429) or 'over18' in html:
            raise Exception(f"被Ban/驗證，status:{resp.status}")
        soup = BeautifulSoup(html, 'html.parser')
        articles = []
        for entry in soup.select("div.r-ent"):
            try:
                title_div = entry.select_one("div.title")
                a_tag = title_div.select_one("a")
                title = title_div.text.strip()
                link = "https://www.ptt.cc" + a_tag["href"] if a_tag else None
                author = entry.select_one("div.author").text.strip()
                date = None
                if link:
                    art_headers = {"User-Agent": random.choice(USER_AGENTS)}
                    await asyncio.sleep(random.uniform(0.1, 0.4))
                    async with session.get(link, cookies=cookies, headers=art_headers, timeout=10) as art_resp:
                        art_html = await art_resp.text()
                        art_soup = BeautifulSoup(art_html, "html.parser")
                        meta_values = art_soup.select('span.article-meta-value')
                        if len(meta_values) >= 4:
                            date_str = meta_values[3].text.strip()
                            date = parse_full_datetime(date_str)

                        # 20250708_002 >>
                        # 取得文章主體內容
                        content_div = art_soup.select_one("#main-content")
                        description = content_div.get_text(separator="\n", strip=True) if content_div else None
                        # 20250708_002 <<

                articles.append({
                    "title": title,
                    "author": author,
                    "date": date,
                    "link": link,
                    "description": description # 20250708_001
                })
            except Exception as e:
                print(f"Error parsing entry: {e}")
                continue
        return articles

async def async_extract_articles_batch(start_page, end_page, concurrent=CONCURRENT_SIZE):
    articles = []
    connector = aiohttp.TCPConnector(limit=concurrent)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [fetch_ptt_page_async(session, page_num) for page_num in range(start_page, end_page + 1)]
        for future in asyncio.as_completed(tasks):
            try:
                page_articles = await future
                articles.extend(page_articles)
            except Exception as e:
                print(f"[Async error] {e}")
    return articles
# 20250708_001 <<

def extract_articles_batch(start_page, end_page, **context):
    articles = asyncio.run(async_extract_articles_batch(start_page, end_page, concurrent=CONCURRENT_SIZE))
    context['ti'].xcom_push(key='articles', value=articles)
    print(f"[async] Collected {len(articles)} articles from pages {start_page}-{end_page}")

def load_articles_to_temp(**context):
    articles = context['ti'].xcom_pull(key='articles')
    if not articles:
        return

    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    rows = [
        (article['title'], article['author'], article['date'], article['link'], article.get('description')) # 20250708_002
        for article in articles
    ]
    pg_hook.insert_rows(
        table="ptt_macshop_articles_temp",
        rows=rows,
        target_fields=["title", "author", "date", "link", "description"] # 20250708_001
    )

def swap_tables():
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    # 如果 backup 存在，先刪掉
    pg_hook.run("DROP TABLE IF EXISTS ptt_macshop_articles_backup;")
    # 如果正式表存在，rename 成 backup
    result = pg_hook.get_first("""
        SELECT to_regclass('public.ptt_macshop_articles') IS NOT NULL;
    """)
    if result and result[0]:
        pg_hook.run("ALTER TABLE ptt_macshop_articles RENAME TO ptt_macshop_articles_backup;")
    # temp rename to 正式表
    pg_hook.run("ALTER TABLE ptt_macshop_articles_temp RENAME TO ptt_macshop_articles;")
    # 刪除 backup
    pg_hook.run("DROP TABLE IF EXISTS ptt_macshop_articles_backup;")

def get_max_page():
    # 用同步requests抓，這段 async 省不了多少
    import requests
    url = f"https://www.ptt.cc/bbs/{PTT_BOARD}/index.html"
    cookies = {'over18': '1'}
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    res = requests.get(url, cookies=cookies, headers=headers)
    soup = BeautifulSoup(res.text, 'html.parser')
    btn = soup.select_one('div.btn-group-paging a.btn.wide:nth-child(2)')
    if btn and 'index' in btn['href']:
        max_page = int(btn['href'].split('index')[1].split('.html')[0]) + 1
    else:
        max_page = 1
    return max_page

with DAG(
    "ptt_macshop_scraper_async",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["ptt", "macshop", "postgres"],
) as dag:

    prepare_temp = PythonOperator(
        task_id='prepare_temp_table',
        python_callable=prepare_temp_table,
    )

    def generate_batches(**context):
        max_page = get_max_page()
        print(f"PTT MacShop max page: {max_page}")
        batch_list = []
        for i in range(1, max_page + 1, BATCH_SIZE):
            start = i
            end = min(i + BATCH_SIZE - 1, max_page)
            batch_list.append((start, end))
        context['ti'].xcom_push(key='batch_list', value=batch_list)

    gen_batches = PythonOperator(
        task_id='generate_batches',
        python_callable=generate_batches,
        provide_context=True,
    )

    def run_batch(**context):
        batch_list = context['ti'].xcom_pull(task_ids='generate_batches', key='batch_list')
        for start_page, end_page in batch_list:
            print(f"Processing batch {start_page}-{end_page}")
            extract_articles_batch(start_page, end_page, **context)
            load_articles_to_temp(**context)

    process_batches = PythonOperator(
        task_id='process_batches',
        python_callable=run_batch,
        provide_context=True,
    )

    swap = PythonOperator(
        task_id='swap_tables',
        python_callable=swap_tables,
    )

    prepare_temp >> gen_batches >> process_batches >> swap
