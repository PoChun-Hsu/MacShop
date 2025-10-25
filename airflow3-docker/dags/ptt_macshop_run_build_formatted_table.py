# -*- coding: utf-8 -*-
# DAG: trigger_build_formatted_table_2
# 需求重點：
# 1) 由 Dataset(dataset://ptt_macshop/raw_updated) 觸發
# 2) 手動觸發時可略過 debounce（避免 up_for_reschedule）
# 3) 正確處理 Branch，不讓 build_formatted_table 被誤判 skipped
# 4) build_formatted_table 受 Pool 控制：ptt_formatted_build_pool
# 5) 可用 Trigger DAG 時傳入 conf 覆寫行為：
#    - {"skip_debounce": true} 手動跳過等待
#    - {"debounce_seconds": 0} 或其他秒數，覆寫等待時間

# 20251010_001 - PoChun Hsu - [Add]     Dataset for trigger across DAGs.

import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.datasets import Dataset
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.latest_only import LatestOnlyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor
from airflow.task.trigger_rule import TriggerRule


FORMATTED_UPDATED = Dataset("dataset://ptt_macshop/formatted_updated") # 20251010_001

# --------------------------
# 基本設定
# --------------------------
DAG_ID = "trigger_build_formatted_table"
# Dataset 來源（與你 UI 圖一致）
RAW_UPDATED = Dataset("dataset://ptt_macshop/raw_updated")

# 預設 debounce 秒數（Dataset 觸發時避免過度頻繁重建）
DEFAULT_DEBOUNCE_SECONDS = 90

# Spark submit 指令（依你提供的語法）
SPARK_SUBMIT_CMD = "docker exec spark spark-submit /opt/spark-apps/build_formatted_table.py"

# Airflow Pool 名稱（請確保已建立）
POOL_NAME = "ptt_formatted_build_pool"


# --------------------------
# DAG 宣告
# --------------------------
with DAG(
    dag_id=DAG_ID,
    description="Dataset-driven trigger to build formatted table (PTT MacShop)",
    schedule=[RAW_UPDATED],             # 由 dataset 觸發
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Taipei"),
    catchup=False,                      # 只跑最新一次
    max_active_runs=1,                  # 避免重入
    tags=["dataset-driven", "formatted_table", "ptt_macshop"],
) as dag:

    # 僅讓「最新一次」生效（對 backfill/補跑會 skip；手動不受影響）
    latest_only = LatestOnlyOperator(task_id="latest_only")

    # 決定是否略過 debounce：
    # - 手動觸發（run_type == "manual"）或 conf={"skip_debounce": true} → 走 go_build
    # - 其餘（包含 Dataset 觸發） → 走 debounce 等待一段時間再建表
    def pick_next(**ctx) -> str:
        dag_run = ctx["dag_run"]
        conf = (dag_run.conf or {})
        manual_run = (dag_run.run_type == "manual")
        skip_debounce = bool(conf.get("skip_debounce", False))

        if manual_run or skip_debounce:
            target = "go_build"
        else:
            target = "debounce"

        # 立刻驗證：回傳目標必須是本節點的直接下游（避免「全 skipped」）
        downstream = ctx["task"].downstream_task_ids
        assert target in downstream, f"Branch target '{target}' is not a direct downstream: {downstream}"
        return target

    branch_on_manual = BranchPythonOperator(
        task_id="branch_on_manual",
        python_callable=pick_next,
            # [AF3 MIGRATION] provide_context removed; context auto-injected

        doc_md="""
        ### Branch 邏輯
        - 手動觸發或 `{"skip_debounce": true}` → go_build
        - 其他（Dataset 觸發） → debounce
        """,
    )

    # 直接開建表（手動觸發或明確要求略過 debounce）
    go_build = EmptyOperator(task_id="go_build")

    # 等待一段時間，避免 Dataset 短時間觸發多次
    # Mode 使用 reschedule：不佔住 worker slot，但外觀會看到 up_for_reschedule（合理）
    from airflow.operators.python import PythonOperator
    import time

    def do_debounce(**ctx):
        conf = ctx["dag_run"].conf or {}
        sec = int(conf.get("debounce_seconds", DEFAULT_DEBOUNCE_SECONDS))
        time.sleep(sec)  # 直接 sleep，等同於 debounce 效果

    debounce = PythonOperator(
        task_id="debounce",
        python_callable=do_debounce,
    )

    # 真正執行 Spark 建表（受 Pool 控制，避免多 Spark 作業互踩）
    build_formatted_table = BashOperator(
        task_id="build_formatted_table",
        bash_command=SPARK_SUBMIT_CMD,
        pool=POOL_NAME,
        # 匯合點：允許另一支路徑被 skipped，只要沒有 failed，且至少一支成功即可
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        retries=3,                          # 失敗時重試 3 次
        retry_delay=timedelta(seconds=10),  # 每次間隔 60 秒
        doc_md=f"""
        以 spark-submit 建表。  
        使用 Pool: `{POOL_NAME}` 避免並發互踩。
        指令：
        ```
        {SPARK_SUBMIT_CMD}
        ```
        """,
        outlets=[FORMATTED_UPDATED],
    )


    # 相依關係（**關鍵**：build_formatted_table 不直接掛 Branch 下）
    latest_only >> branch_on_manual
    branch_on_manual >> [go_build, debounce]
    [go_build, debounce] >> build_formatted_table
