# 20250630_001 - Pochun Hsu - Get all pages of PTT Macshop by sequential
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import requests
from bs4 import BeautifulSoup

BATCH_SIZE = 5
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

def extract_articles_batch(start_page, end_page, **context):
    articles = []
    cookies = {'over18': '1'}
    for page_num in range(start_page, end_page + 1):
        url = f"https://www.ptt.cc/bbs/{PTT_BOARD}/index{page_num}.html"
        res = requests.get(url, cookies=cookies)
        if not res.ok:
            print(f"Error fetching page {page_num}")
            continue
        soup = BeautifulSoup(res.text, 'html.parser')
        for entry in soup.select("div.r-ent"):
            try:
                title_div = entry.select_one("div.title")
                a_tag = title_div.select_one("a")
                title = title_div.text.strip()
                link = "https://www.ptt.cc" + a_tag["href"] if a_tag else None
                author = entry.select_one("div.author").text.strip()
                date = None
                # 抓詳細時間存進 date 欄位
                if link:
                    try:
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
                    "date": date,  # 這裡是 datetime 型態
                    "link": link
                })
            except Exception as e:
                print(f"Error parsing entry: {e}")
                continue
    context['ti'].xcom_push(key='articles', value=articles)
    print(f"Collected {len(articles)} articles from pages {start_page}-{end_page}")

def load_articles_to_postgres(**context):
    articles = context['ti'].xcom_pull(key='articles')
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS ptt_macshop_articles (
        id SERIAL PRIMARY KEY,
        title TEXT,
        author TEXT,
        date TIMESTAMP,
        link TEXT
    );
    """
    pg_hook.run(create_table_sql)

    for article in articles:
        insert_sql = """
        INSERT INTO ptt_macshop_articles (title, author, date, link)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
        """
        pg_hook.run(insert_sql, parameters=(
            article['title'],
            article['author'],
            article['date'],    # 這裡直接是 datetime
            article['link']
        ))

with DAG(
    "ptt_macshop_scraper_simple",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["ptt", "macshop", "postgres"],
) as dag:

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
            load_articles_to_postgres(**context)

    process_batches = PythonOperator(
        task_id='process_batches',
        python_callable=run_batch,
        provide_context=True,
    )

    gen_batches >> process_batches
