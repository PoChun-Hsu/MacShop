# 20250724_001 - PoChun Hsu - [Create]  DAG to update the data in recent 90 days.

# DAG: Incremental Sync for PTT MacShop

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta, timezone
import random
import asyncio
import aiohttp
from bs4 import BeautifulSoup
import redis
import hashlib

PTT_BOARD = "MacShop"
DEFAULT_START_DATE = datetime(2025, 5, 1)
CONCURRENT_SIZE = 100
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
redis_client = redis.Redis(host='redis', port=6379, db=0)

def parse_full_datetime(date_str):
    try:
        return datetime.strptime(date_str, "%a %b %d %H:%M:%S %Y")
    except Exception:
        return None

# 判斷重要頁碼
def determine_incremental_range():
    pg = PostgresHook(postgres_conn_id='postgres_default')
    records = pg.get_records("SELECT page_num, max_date FROM Ptt_Macshop_Page_Dates WHERE Max_Date IS NOT NULL ORDER BY Page_Num ASC")
    ninety_days_ago = datetime.now() - timedelta(days=90)

    start_page = None
    for page_num, max_date in records:
        if max_date and max_date >= ninety_days_ago:
            start_page = page_num
            break
    if not start_page:
        start_page = 1  # fallback

    # Always update to the latest PTT page
    import requests
    url = f"https://www.ptt.cc/bbs/{PTT_BOARD}/index.html"
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    cookies = {"over18": "1"}
    res = requests.get(url, headers=headers, cookies=cookies)
    soup = BeautifulSoup(res.text, 'html.parser')
    btn = soup.select_one('div.btn-group-paging a.btn.wide:nth-child(2)')
    if btn and 'index' in btn['href']:
        latest_page = int(btn['href'].split('index')[1].split('.html')[0]) + 1
    else:
        latest_page = 1

    return start_page, latest_page

# 事先建立所需 temp table
def prepare_temp_table():
    """
    """
    pg = PostgresHook(postgres_conn_id="postgres_default")

    pg.run("DROP TABLE IF EXISTS Ptt_Macshop_Page_Dates_Temp;", autocommit=True)
    pg.run("""
        CREATE TABLE Ptt_Macshop_Page_Dates_Temp
        (LIKE Ptt_Macshop_Page_Dates INCLUDING ALL);
    """, autocommit=True)
    pg.run("""
        INSERT INTO Ptt_Macshop_Page_Dates_Temp
        SELECT * FROM Ptt_Macshop_Page_Dates;
    """, autocommit=True)

    print("✅ Temp table prepared and seeded with existing data")

# async 碼: reuse fetch_ptt_page_async from full DAG, but use formal table
async def fetch_and_check_diff(session, page_num):
    if redis_client.get("ptt:ban_flag") == b"1":
        print(f"[SKIP] 被 ban 過，跳過 page {page_num}")
        return []

    url = f"https://www.ptt.cc/bbs/{PTT_BOARD}/index{page_num}.html"
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    cookies = {"over18": "1"}
    await asyncio.sleep(random.uniform(0.2, 1.2))

    async with session.get(url, headers=headers, cookies=cookies, timeout=10) as resp:
        html = await resp.text()
        if resp.status in (403, 429) or 'over18' in html:
            redis_client.set("ptt:ban_flag", "1", ex=30)
            raise Exception("Ban detected")

        soup = BeautifulSoup(html, 'html.parser')
        pg = PostgresHook(postgres_conn_id="postgres_default")
        articles = []

        for entry in soup.select("div.r-ent"):
            try:
                title_div = entry.select_one("div.title")
                a_tag = title_div.select_one("a")
                link = "https://www.ptt.cc" + a_tag["href"] if a_tag else None
                title = title_div.text.strip()
                author = entry.select_one("div.author").text.strip()
                date = None
                description, description_hash = None, None

                if link:
                    await asyncio.sleep(random.uniform(0.1, 0.3))
                    art_headers = {"User-Agent": random.choice(USER_AGENTS)}
                    async with session.get(link, cookies=cookies, headers=art_headers, timeout=10) as art_resp:
                        art_html = await art_resp.text()
                        art_soup = BeautifulSoup(art_html, "html.parser")
                        meta_values = art_soup.select('span.article-meta-value')
                        if len(meta_values) >= 4:
                            date_str = meta_values[3].text.strip()
                            date = parse_full_datetime(date_str)
                        content_div = art_soup.select_one("#main-content")
                        description = content_div.get_text(separator="\n", strip=True) if content_div else None
                        description_hash = hashlib.sha256(description.encode("utf-8")).hexdigest() if description else None

                if not (link and description_hash):
                    continue

                # Redis 快取檢查
                redis_key = f"Ptt:Macshop:Hash:{link}"
                cached_hash = redis_client.get(redis_key)
                if cached_hash and cached_hash.decode() == description_hash:
                    continue  # no change

                # update Redis
                redis_client.set(redis_key, description_hash)

                articles.append({
                    "Title": title,
                    "Author": author,
                    "Created_Date": date,
                    "Link": link,
                    "Description": description,
                    "Description_Hash": description_hash
                })

                if date:
                    pg.run(
                        """
                        INSERT INTO Ptt_Macshop_Page_Dates_Temp (Page_Num, Url, Min_Date, Max_Date)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (Page_Num) DO UPDATE
                        SET Min_Date = EXCLUDED.Min_Date,
                            Max_Date = EXCLUDED.Max_Date;
                        """,
                        parameters=(page_num, url, date, date), autocommit=True
                    )
            except Exception as e:
                print(f"Error: {e}")
        return articles

async def incremental_crawl(start_page, end_page):
    results = []
    connector = aiohttp.TCPConnector(limit=CONCURRENT_SIZE)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [fetch_and_check_diff(session, p) for p in range(start_page, end_page + 1)]
        for fut in asyncio.as_completed(tasks):
            try:
                articles = await fut
                results.extend(articles)
            except Exception as e:
                print(f"[Async Error] {e}")
    return results

def extract_incremental(**context):
    start_page, end_page = determine_incremental_range()
    print(f"Incremental sync: pages {start_page} to {end_page}")
    articles = asyncio.run(incremental_crawl(start_page, end_page))
    context['ti'].xcom_push(key='inc_articles', value=articles)


def update_articles(**context):
    pg = PostgresHook(postgres_conn_id="postgres_default")
    articles = context['ti'].xcom_pull(key='inc_articles')
    for art in articles:
        pg.run(
            """
            INSERT INTO Ptt_Macshop_Articles (
                Title, Author, Created_Date, Link, Description, Description_Hash, Updated_Date
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (Link) DO UPDATE
            SET Title = EXCLUDED.Title,
                Author = EXCLUDED.Author,
                Created_Date = EXCLUDED.Created_Date,
                Description = EXCLUDED.Description,
                Description_Hash = EXCLUDED.Description_Hash,
                Updated_Date = CURRENT_TIMESTAMP;
            """,
            parameters=(
                art['Title'],
                art['Author'],
                art['Created_Date'], 
                art['Link'], 
                art['Description'], 
                art['Description_Hash'], 
                datetime.now(timezone.utc)
            ),
            autocommit=True
        )
    print(f"✅ Updated {len(articles)} changed articles")


def swap_page_date_table():
    pg = PostgresHook(postgres_conn_id='postgres_default')
    pg.run("DROP TABLE IF EXISTS Ptt_Macshop_Page_Dates_Backup;", autocommit=True)
    pg.run("ALTER TABLE Ptt_Macshop_Page_Dates RENAME TO Ptt_Macshop_Page_Dates_Backup;", autocommit=True)
    pg.run("ALTER TABLE Ptt_Macshop_Page_Dates_Temp RENAME TO Ptt_Macshop_Page_Dates;", autocommit=True)
    pg.run("DROP TABLE IF EXISTS Ptt_Macshop_Page_Dates_Backup;", autocommit=True)
    pg.run("CREATE INDEX IF NOT EXISTS idx_description_Hash ON Ptt_Macshop_Articles(Description_Hash);", autocommit=True)

with DAG(
    "Ptt_Macshop_Incremental_Async",
    default_args={"start_date": DEFAULT_START_DATE},
    schedule_interval="*/15 * * * *",
    catchup=False,
    tags=["ptt", "macshop", "incremental"],
    max_active_runs=1
) as dag:
    prepare_temp = PythonOperator(
        task_id="prepare_temp_table",
        python_callable=prepare_temp_table
    )

    extract = PythonOperator(
        task_id="extract_incremental_articles",
        python_callable=extract_incremental,
        provide_context=True,
    )

    update = PythonOperator(
        task_id="update_articles_to_postgres",
        python_callable=update_articles,
        provide_context=True,
    )

    swap = PythonOperator(
        task_id="swap_page_date_table",
        python_callable=swap_page_date_table
    )

    prepare_temp >> extract >> update >> swap
