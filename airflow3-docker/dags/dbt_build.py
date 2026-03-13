# 20260313_001- PoChun Hsu - [Add]     DAG for updating DBT

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.datasets import Dataset

# 這裡要填「Airflow container 內」看得到的專案路徑
HOST_PROJECT_PATH = "/Users/sherryli/Desktop/MacShop/airflow3-docker"

# compose 檔名
COMPOSE_FILE = "docker-compose.yaml"

default_args = {
    "owner": "macshop",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=3),
}

# ===================== Dataset 定義 =====================
DATA_WAREHOUSE_UPDATED = Dataset("dataset://ptt_macshop/data_warehouse_updated")
DATA_MART_UPDATED      = Dataset("dataset://ptt_macshop/data_mart_updated")

with DAG(
    dag_id="dbt_build",
    description="Run dbt build after source tables are updated",
    default_args=default_args,
    start_date=datetime(2026, 3, 1),
    schedule=[DATA_WAREHOUSE_UPDATED],  # Dataset-based trigger
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "postgres", "mart"],
) as dag:

    start = EmptyOperator(task_id="start")

    check_dbt_container = BashOperator(
        task_id="check_dbt_container",
        bash_command=f"""
        set -euo pipefail
        cd {HOST_PROJECT_PATH}
        docker compose -f {COMPOSE_FILE} config --services | grep '^dbt$'
        """,
        execution_timeout=timedelta(minutes=2),
    )

    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=f"""
        set -euo pipefail
        cd {HOST_PROJECT_PATH}
        docker compose -f {COMPOSE_FILE} run --rm --no-deps dbt dbt debug
        """,
        execution_timeout=timedelta(minutes=5),
    )

    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"""
        set -euo pipefail
        cd {HOST_PROJECT_PATH}
        docker compose -f {COMPOSE_FILE} run --rm --no-deps dbt dbt source freshness
        """,
        execution_timeout=timedelta(minutes=10),
    )

    dbt_build_product_index = BashOperator(
        task_id="dbt_build_product_index",
        bash_command=f"""
        set -euo pipefail
        cd {HOST_PROJECT_PATH}
        docker compose -f {COMPOSE_FILE} run --rm --no-deps dbt \
          dbt build --select Mart_Log_Daily_Product_Index+ Mart_Log_Monthly_Product_Index+
        """,
        execution_timeout=timedelta(minutes=30),
    )

    dbt_docs_generate = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"""
        set -euo pipefail
        cd {HOST_PROJECT_PATH}
        docker compose -f {COMPOSE_FILE} run --rm dbt dbt docs generate
        """,
        execution_timeout=timedelta(minutes=15),
        trigger_rule=TriggerRule.ALL_SUCCESS,
        outlets=[DATA_MART_UPDATED]
    )

    finish = EmptyOperator(
        task_id="finish",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    start >> check_dbt_container >> dbt_debug >> dbt_source_freshness >> dbt_build_product_index >> dbt_docs_generate >> finish
