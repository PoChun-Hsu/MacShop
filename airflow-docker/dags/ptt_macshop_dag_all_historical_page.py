# 20250702_001 - PoChun Hsu - [Alter] batch insert replace insert row by row.
# 20250702_002 - PoChun Hsu - [Alter] web crawler with multi thread.
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import concurrent.futures
import random
import time

BATCH_SIZE = 10 # 20250702_002
# 控制最大 thread 數，建議不要超過 5~10，避免被 ban
CONCURRENT_SIZE = 5 # 20250702_001
PTT_BOARD = "MacShop"
DEFAULT_START_DATE = datetime(2025, 5, 1)

default_args = {
    "start_date": DEFAULT_START_DATE,
}

def get_max_page():
    url = f"https://www.ptt.cc/bbs/{PTT_BOARD}/index.html"
    cookies = {'over18': '1'}
    res = requests.get(url, cookies=cookies)
    soup = BeautifulSoup(res.text, 'html.parser')
    btn = soup.select_one('div.btn-group-paging a.btn.wide:nth-child(2)')
    if btn and 'index' in btn['href']:
        max_page = int(btn['href'].split('index')[1].split('.html')[0]) + 1
    else:
        max_page = 1
    return max_page

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
        link TEXT
    );
    """
    pg_hook.run(create_table_sql)

def fetch_ptt_page(page_num):
    cookies = {'over18': '1'}
    url = f"https://www.ptt.cc/bbs/{PTT_BOARD}/index{page_num}.html"
    # 避免被 ban，加隨機 slee
    time.sleep(random.uniform(0.5, 1.0))
    try:
        res = requests.get(url, cookies=cookies)
        if not res.ok:
            print(f"Error fetching page {page_num}")
            return []
        
        soup = BeautifulSoup(res.text, 'html.parser')
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
                    try:
                        # 這裡也建議加 sleep
                        #time.sleep(random.uniform(0.1, 0.4))
                        art_res = requests.get(link, cookies=cookies)
                        art_soup = BeautifulSoup(art_res.text, "html.parser")
                        meta_values = art_soup.select('span.article-meta-value')
                        if len(meta_values) >= 4:
                            full_datetime = meta_values[3].text.strip()
                            date = parse_full_datetime(full_datetime)
                    except Exception as e:
                        print(f"Error fetching article datetime: {e}")

                articles.append({
                    "title": title,
                    "author": author,
                    "date": date,
                    "link": link
                })
            except Exception as e:
                print(f"Error parsing entry: {e}")
                continue
        return articles
    except Exception as e:
        print(f"Error in fetch_ptt_page: {e}")
        return []

# 加入５個平行處理後，５分鐘就處理原本１７分鐘處理完的資料量
# 嘗試加入更多
# 20250702_002 >>
def extract_articles_batch(start_page, end_page, **context):
    articles = []
    page_nums = list(range(start_page, end_page + 1))
    # 控制最大 thread 數，建議不要超過 5~10，避免被 ban
    # max_workers = min(5, len(page_nums))
    max_workers = CONCURRENT_SIZE
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_page = {executor.submit(fetch_ptt_page, page): page for page in page_nums}
        for future in concurrent.futures.as_completed(future_to_page):
            page_articles = future.result()
            articles.extend(page_articles)
    context['ti'].xcom_push(key='articles', value=articles)
    print(f"Collected {len(articles)} articles from pages {start_page}-{end_page}")
# 20250702_002 <<

# Batch insert，一次 insert一整包而不要逐筆輸入，加速 insert
# 但因為瓶頸主要在爬蟲，沒有平行抓取資料下，batch insert的影響不大
def load_articles_to_temp(**context):
    articles = context['ti'].xcom_pull(key='articles')
    if not articles:
        return

    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    # 20250702_001 >>
    rows = [
        (article['title'], article['author'], article['date'], article['link'])
        for article in articles
    ]

    pg_hook.insert_rows(
        table="ptt_macshop_articles_temp",
        rows=rows,
        target_fields=["title", "author", "date", "link"]
    )
    # 20250702_001 <<

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

with DAG(
    "ptt_macshop_scraper_swap",
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
