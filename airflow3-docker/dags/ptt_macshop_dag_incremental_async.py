# 20250724_001 - PoChun Hsu - [Create]  DAG to update the data in recent 90 days.
# 20250831_001 - PoChun Hsu - [Add]     exception hinting for nonexist table.
# 20251024_001 - PoChun Hsu - [Migrate] Airflow 3.1 TaskFlow + async compatible version.
# 20251102_001 - PoChun Hsu - [Add]     parameterization.
# 20251102_002 - PoChun Hsu - [Add]     mutiple USER_AGENTS back.


from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException
from airflow import Dataset
from airflow.operators.empty import EmptyOperator

from datetime import datetime, timedelta, timezone
import random, asyncio, aiohttp, redis, hashlib
from bs4 import BeautifulSoup


# ===============================
# 基本設定
# ===============================
PTT_BOARD = "MacShop"
DEFAULT_START_DATE = datetime(2025, 5, 1)
CONCURRENT_SIZE    = 100
RAW_UPDATED        = Dataset("dataset://ptt_macshop/raw_updated")
UPDATE_RECENT_DAY   = 90

redis_client = redis.Redis(host="redis", port=6379, decode_responses=False)

# 定義多種設備，避免被判定成機器人，更像不同使用者
# 20251102_002
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


# ===============================
# 共用函式
# ===============================
def parse_ptt_date(date_str):
    """解析 PTT 的日期（例如 '8/27'），自動補年份轉 datetime"""
    if not date_str:
        return None
    try:
        month, day = map(int, date_str.split('/'))
        now = datetime.now()
        year = now.year
        parsed = datetime(year, month, day)

        # 若日期比今天還晚，表示是去年的日期
        if parsed > now:
            parsed = datetime(year - 1, month, day)

        return parsed
    except Exception:
        return None


def parse_full_datetime(date_str):
    """解析完整日期字串"""
    try:
        return datetime.strptime(date_str, "%a %b %d %H:%M:%S %Y")
    except Exception:
        return None


def determine_incremental_range():
    """決定增量同步的頁碼範圍"""
    pg = PostgresHook(postgres_conn_id='postgres_default')
    records = pg.get_records("""
        SELECT Page_Num, Max_Date
        FROM Ptt_Macshop_Page_Dates 
        WHERE Max_Date IS NOT NULL 
        ORDER BY Page_Num ASC
    """)
    update_recent_days_ago = datetime.now() - timedelta(days=UPDATE_RECENT_DAY) # 20251102_001
    start_page = next((p for p, d in records if d and d >= update_recent_days_ago), None) or 1 # 20251102_001

    import requests
    url = f"https://www.ptt.cc/bbs/{PTT_BOARD}/index.html"
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    cookies = {"over18": "1"}
    res = requests.get(url, headers=headers, cookies=cookies)
    soup = BeautifulSoup(res.text, 'html.parser')
    btn = soup.select_one('div.btn-group-paging a.btn.wide:nth-child(2)')
    latest_page = int(btn['href'].split('index')[1].split('.html')[0]) + 1 if btn and 'index' in btn['href'] else 1
    return start_page, latest_page


async def fetch_and_check_diff(session, page_num):
    """抓取頁面並比對差異"""
    if redis_client.get("ptt:ban_flag") == b"1":
        print(f"[SKIP] 被 ban 過，跳過 page {page_num}")
        return []

    url = f"https://www.ptt.cc/bbs/{PTT_BOARD}/index{page_num}.html"
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    cookies = {"over18": "1"}
    await asyncio.sleep(random.uniform(0.2, 1.2))

    async with session.get(url, headers=headers, cookies=cookies) as res:
        text = await res.text()
        soup = BeautifulSoup(text, "html.parser")
        articles = []

        for div in soup.select("div.r-ent"):
            try:
                link_tag = div.select_one("a")
                if not link_tag:
                    continue

                link = "https://www.ptt.cc" + link_tag["href"]
                title = link_tag.text.strip()
                author = div.select_one(".author").text.strip() if div.select_one(".author") else None
                date = div.select_one(".date").text.strip() if div.select_one(".date") else None

                content_div = div.select_one(".title")
                description = content_div.text.strip() if content_div else None
                description_hash = hashlib.sha256(description.encode("utf-8")).hexdigest() if description else None

                if not (link and description_hash):
                    continue

                redis_key = f"Ptt:Macshop:Hash:{link}"
                cached_hash = redis_client.get(redis_key)
                if cached_hash and cached_hash.decode() == description_hash:
                    continue

                redis_client.set(redis_key, description_hash)

                articles.append({
                    "Title": title,
                    "Author": author,
                    "Created_Date": parse_ptt_date(date),  # ✅ 修正
                    "Link": link,
                    "Description": description,
                    "Description_Hash": description_hash
                })
            except Exception as e:
                print(f"Error parsing page {page_num}: {e}")

        return articles


async def incremental_crawl(start_page, end_page):
    """增量爬取範圍頁面"""
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


# ===============================
# DAG 定義
# ===============================
@dag(
    dag_id="Ptt_Macshop_Incremental_Async",
    start_date=DEFAULT_START_DATE,
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["ptt", "macshop", "incremental"],
)
def macshop_incremental_async_dag():
    """Airflow 3.1 - TaskFlow async compatible"""

    # -----------------------------------------
    # Step 1: 準備暫存表
    # -----------------------------------------
    @task
    def prepare_temp_table():
        pg = PostgresHook(postgres_conn_id="postgres_default")
        try:
            pg.run('DROP TABLE IF EXISTS Ptt_Macshop_Page_Dates_Temp;', autocommit=True)
            pg.run('CREATE TABLE Ptt_Macshop_Page_Dates_Temp (LIKE Ptt_Macshop_Page_Dates INCLUDING ALL);', autocommit=True)
            pg.run('INSERT INTO Ptt_Macshop_Page_Dates_Temp SELECT * FROM Ptt_Macshop_Page_Dates;', autocommit=True)
            print("✅ Temp table prepared.")
        except Exception as e:
            if "UndefinedTable" in str(e):
                raise RuntimeError("❌ 找不到基礎表，請先執行 Full run。") from e
            raise

    # -----------------------------------------
    # Step 2: 抓取增量資料 (asyncio.run)
    # -----------------------------------------
    @task
    def extract_incremental():
        start_page, end_page = determine_incremental_range()
        print(f"Incremental sync: pages {start_page} to {end_page}")
        articles = asyncio.run(incremental_crawl(start_page, end_page))
        return articles

    # -----------------------------------------
    # Step 3: 更新資料庫
    # -----------------------------------------
    @task
    def update_articles(articles: list):
        if not isinstance(articles, list):
            raise AirflowException(f"XCom return_value 型別異常：{type(articles)}")
        if not articles:
            print("No incremental articles to update.")
            return

        pg = PostgresHook(postgres_conn_id="postgres_default")
        for art in articles:
            pg.run("""
                INSERT INTO Ptt_Macshop_Articles (
                    Title, Author, Created_Date, Link, Description, Description_Hash, Updated_Date
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (Link) DO UPDATE
                SET Title = EXCLUDED.Title,
                    Author = EXCLUDED.Author,
                    Created_Date = EXCLUDED.Created_Date,
                    Description = EXCLUDED.Description,
                    Description_Hash = EXCLUDED.Description_Hash,
                    Updated_Date = NOW();
            """, parameters=(
                art["Title"], art["Author"], art["Created_Date"],
                art["Link"], art["Description"], art["Description_Hash"],
                datetime.now(timezone.utc)
            ), autocommit=True)
        print(f"✅ Updated/Upserted {len(articles)} articles.")

    # -----------------------------------------
    # Step 4: 交換 Page_Date 表
    # -----------------------------------------
    @task
    def swap_page_date_table():
        pg = PostgresHook(postgres_conn_id='postgres_default')
        pg.run('DROP TABLE IF EXISTS Ptt_Macshop_Page_Dates_Backup;', autocommit=True)
        pg.run('ALTER TABLE Ptt_Macshop_Page_Dates RENAME TO Ptt_Macshop_Page_Dates_Backup;', autocommit=True)
        pg.run('ALTER TABLE Ptt_Macshop_Page_Dates_Temp RENAME TO Ptt_Macshop_Page_Dates;', autocommit=True)
        pg.run('DROP TABLE IF EXISTS Ptt_Macshop_Page_Dates_Backup;', autocommit=True)
        pg.run('CREATE INDEX IF NOT EXISTS idx_description_Hash ON Ptt_Macshop_Articles(Description_Hash);', autocommit=True)
        print("✅ Swapped page date table.")

    # -----------------------------------------
    # Step 5: 發布 Dataset
    # -----------------------------------------
    publish = EmptyOperator(task_id="publish_raw_updated", outlets=[RAW_UPDATED])

    # -----------------------------------------
    # Pipeline 定義
    # -----------------------------------------
    prepare = prepare_temp_table()
    extracted = extract_incremental()
    updated = update_articles(extracted)
    swapped = swap_page_date_table()

    prepare >> extracted >> updated >> swapped >> publish


dag = macshop_incremental_async_dag()
