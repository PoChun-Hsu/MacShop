from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import requests
from bs4 import BeautifulSoup

default_args = {
    "start_date": datetime(2025, 5, 1),
}

with DAG(
    "ptt_macshop_scraper",
    default_args=default_args,
    schedule_interval="@daily",  # 每天執行一次
    catchup=False,
    tags=["ptt", "macshop", "postgres"],
) as dag:

    def extract_articles(**context):
        url = "https://www.ptt.cc/bbs/MacShop/index.html"
        cookies = {'over18': '1'}  # PTT 需要滿18 cookie
        res = requests.get(url, cookies=cookies)
        soup = BeautifulSoup(res.text, 'html.parser')

        articles = []
        for entry in soup.select("div.r-ent"):
            try:
                title = entry.select_one("div.title").text.strip()
                href = entry.select_one("div.title a")["href"]
                link = "https://www.ptt.cc" + href
                author = entry.select_one("div.author").text.strip()
                date = entry.select_one("div.date").text.strip()

                articles.append({
                    "title": title,
                    "author": author,
                    "date": date,
                    "link": link
                })
            except Exception as e:
                continue

        context['ti'].xcom_push(key='articles', value=articles)

    def load_articles_to_postgres(**context):
        articles = context['ti'].xcom_pull(task_ids='extract_ptt_articles', key='articles')
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')

        create_table_sql = """
        CREATE TABLE IF NOT EXISTS ptt_macshop_articles (
            id SERIAL PRIMARY KEY,
            title TEXT,
            author TEXT,
            date TEXT,
            link TEXT UNIQUE
        );
        """
        pg_hook.run(create_table_sql)

        for article in articles:
            insert_sql = """
            INSERT INTO ptt_macshop_articles (title, author, date, link)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (link) DO NOTHING;
            """
            pg_hook.run(insert_sql, parameters=(article['title'], article['author'], article['date'], article['link']))

    t1 = PythonOperator(
        task_id='extract_ptt_articles',
        python_callable=extract_articles,
    )

    t2 = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_articles_to_postgres,
    )

    t1 >> t2
