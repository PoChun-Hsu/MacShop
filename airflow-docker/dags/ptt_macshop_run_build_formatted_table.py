from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,  # 若需要重試次數可在此設定
}

with DAG(
    dag_id="trigger_build_formatted_table",
    default_args=default_args,
    # start_date=datetime(2025, 1, 1),
    # schedule_interval="*/15 * * * *",  # 每 15 分鐘排程一次
    schedule_interval=None,
    catchup=False
) as dag:
    trigger_spark_job = BashOperator(
        task_id="trigger_spark_job",
        bash_command="docker exec spark spark-submit /opt/spark-apps/build_formatted_table.py"
    )
