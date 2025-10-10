from __future__ import annotations
# -*- coding: utf-8 -*-
"""
DAG：ptt_macshop_postgres_to_googlesheet
（Solution 1 + 序列化修正 + 自動擴尺寸 + 50,000 字元上限防呆）

錯誤說明：Google Sheets 單一儲存格最多 50,000 字元。若任何 cell 超出上限，
API 會回 400 並中止整批寫入。

本版修正：在寫入前，對每個 cell 做「長度防呆處理」：
- 轉成 JSON-safe 型別後，若是字串且長度 > 上限，依策略處理（預設 truncate）。
- 策略可用 Airflow Variables 控制：truncate / fail（直接丟錯）/ trim_whitespace。
建議：truncate（保底寫入成功）；真正完整內容請保留在 DB 或另存檔案後放連結。
"""

from datetime import datetime, date, timedelta
from decimal import Decimal
from typing import Iterable, List, Any

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from googleapiclient.discovery import build
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ===================== Airflow Variables（可於 UI 調整） =====================
SPREADSHEET_ID = Variable.get("GOOGLE_SHEETS_SPREADSHEET_ID", default_var="1MFwhTSKOc_RM8wKvTuHrj7doOxiArQiyOxFnAgXKrSQ")
SHEET_NAME     = Variable.get("GOOGLE_SHEETS_SHEET_NAME", default_var="sheet1")
SQL_QUERY      = Variable.get("PG_EXPORT_SQL", default_var="SELECT * FROM ptt_macshop_articles_product_detail ORDER BY created_date DESC")

# 連線 ID
PG_CONN_ID  = Variable.get("PG_CONN_ID", default_var="postgres_default")
GCP_CONN_ID = Variable.get("GCP_CONN_ID", default_var="google_cloud_default")

# 可調參數
MAX_CELLS_PER_CHUNK   = int(Variable.get("SHEETS_MAX_CELLS_PER_CHUNK", default_var="20000"))
REQUEST_TIMEOUT_SEC   = int(Variable.get("SHEETS_REQUEST_TIMEOUT_SEC", default_var="120"))
EXECUTE_NUM_RETRIES   = int(Variable.get("SHEETS_EXECUTE_NUM_RETRIES", default_var="5"))
DATETIME_FMT          = Variable.get("SHEETS_DATETIME_FMT", default_var="%Y-%m-%d %H:%M:%S")

# 自動擴表的額外 buffer
EXTRA_ROW_BUFFER      = int(Variable.get("SHEETS_EXTRA_ROW_BUFFER", default_var="100"))
EXTRA_COL_BUFFER      = int(Variable.get("SHEETS_EXTRA_COL_BUFFER", default_var="5"))

# 單一儲存格字元上限（Google Sheets 固定為 50000；保留成參數以便容錯/測試）
SHEETS_MAX_CELL_CHARS = int(Variable.get("SHEETS_MAX_CELL_CHARS", default_var="50000"))
# 超過上限時的策略：truncate / fail / trim_whitespace
SHEETS_OVERFLOW_POLICY = Variable.get("SHEETS_OVERFLOW_POLICY", default_var="truncate").lower()
# truncate 的尾註（提示被截斷）
TRUNCATION_SUFFIX = Variable.get("SHEETS_TRUNCATION_SUFFIX", default_var=" …[truncated]")

# ===================== 共用：execute 包裝 =====================
def execute_with_timeout(request, timeout: int = REQUEST_TIMEOUT_SEC, num_retries: int = EXECUTE_NUM_RETRIES):
    """在 execute 時設定 timeout / 重試（新版相容）。"""
    try:
        return request.execute(num_retries=num_retries, timeout=timeout)
    except TypeError:
        return request.execute(num_retries=num_retries)

# ===================== A1 / 分塊 =====================
def number_to_column_letters(n: int) -> str:
    s = ""
    n = int(n)
    while True:
        n, r = divmod(n, 26)
        s = chr(ord('A') + r) + s
        if n == 0:
            break
        n -= 1
    return s

def a1_range(start_row: int, start_col: int, rows: int, cols: int) -> str:
    start = f"{number_to_column_letters(start_col)}{start_row+1}"
    end   = f"{number_to_column_letters(start_col+cols-1)}{start_row+rows}"
    return f"{start}:{end}"

def chunk_by_cells(values: List[List[Any]], max_cells: int) -> Iterable[List[List[Any]]]:
    if not values:
        return
    cols = max(len(r) for r in values)
    chunk, rows_in_chunk = [], 0
    for row in values:
        if len(row) < cols:
            row = row + [None] * (cols - len(row))
        if rows_in_chunk and (rows_in_chunk + 1) * cols > max_cells:
            yield chunk
            chunk, rows_in_chunk = [], 0
        chunk.append(row)
        rows_in_chunk += 1
    if chunk:
        yield chunk

# ===================== 內容序列化 + 超長字元防呆 =====================
def enforce_cell_limit(text: str) -> str:
    """限制字串最大長度；依策略處理超標內容。"""
    if text is None:
        return ""
    if not isinstance(text, str):
        text = str(text)

    # 可選：先做基本清理
    if SHEETS_OVERFLOW_POLICY in ("trim_whitespace", "truncate"):
        text = text.strip()

    if len(text) <= SHEETS_MAX_CELL_CHARS:
        return text

    if SHEETS_OVERFLOW_POLICY == "fail":
        raise ValueError(f"Cell content exceeds {SHEETS_MAX_CELL_CHARS} characters")

    # truncate 策略（預設）：保留尾註，確保不超過上限
    suffix = TRUNCATION_SUFFIX or ""
    keep = max(0, SHEETS_MAX_CELL_CHARS - len(suffix))
    return (text[:keep] + suffix)[:SHEETS_MAX_CELL_CHARS]

def to_json_safe(v: Any) -> Any:
    """將非 JSON 型別轉成 Sheets 友善格式，並對字串套用長度限制。"""
    if isinstance(v, (datetime, date)):
        return enforce_cell_limit(v.strftime(DATETIME_FMT))
    if isinstance(v, Decimal):
        try:
            return float(v)
        except Exception:
            return enforce_cell_limit(str(v))
    if v is None:
        return ""
    if isinstance(v, (list, dict, set, tuple)):
        # 將複合型別序列化為字串（或自行 json.dumps），再套用長度限制
        return enforce_cell_limit(str(v))
    if isinstance(v, bytes):
        try:
            return enforce_cell_limit(v.decode("utf-8", errors="ignore"))
        except Exception:
            return enforce_cell_limit(str(v))
    if isinstance(v, str):
        return enforce_cell_limit(v)
    # 其他型別：轉字串後限制
    return enforce_cell_limit(str(v))

# ===================== 取數（含表頭） =====================
def fetch_data_from_postgres(sql: str, pg_conn_id: str) -> List[List[Any]]:
    hook = PostgresHook(pg_conn_id)
    conn = hook.get_conn()
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    headers = [desc[0] for desc in cur.description]
    cur.close(); conn.close()

    safe_rows = [[to_json_safe(cell) for cell in row] for row in rows]
    return [headers] + safe_rows

# ===================== 擴表：確保 rows/cols 足夠 =====================
def ensure_sheet_size(spreadsheet_id: str, sheet_name: str, rows_needed: int, cols_needed: int, gcp_conn_id: str) -> None:
    creds = GoogleBaseHook(gcp_conn_id=gcp_conn_id).get_credentials()
    service = build('sheets', 'v4', credentials=creds)

    meta_req = service.spreadsheets().get(
        spreadsheetId=spreadsheet_id,
        ranges=[],
        includeGridData=False,
        fields="sheets(properties(sheetId,title,gridProperties(rowCount,columnCount)))"
    )
    meta = execute_with_timeout(meta_req)
    target = None
    for s in meta.get("sheets", []):
        props = s.get("properties", {})
        if props.get("title") == sheet_name:
            target = props
            break
    if not target:
        raise ValueError(f"Sheet '{sheet_name}' 不存在，請先建立該分頁或確認名稱大小寫。")

    sheet_id = target["sheetId"]
    current_rows = target.get("gridProperties", {}).get("rowCount", 1000)
    current_cols = target.get("gridProperties", {}).get("columnCount", 26)

    new_rows = max(current_rows, rows_needed + EXTRA_ROW_BUFFER)
    new_cols = max(current_cols, cols_needed + EXTRA_COL_BUFFER)

    if new_rows > current_rows or new_cols > current_cols:
        req = service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={
                "requests": [{
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": sheet_id,
                            "gridProperties": {
                                "rowCount": new_rows,
                                "columnCount": new_cols
                            }
                        },
                        "fields": "gridProperties(rowCount,columnCount)"
                    }
                }]
            }
        )
        execute_with_timeout(req)

# ===================== 清空範圍（冪等） =====================
def clear_sheet(spreadsheet_id: str, sheet_name: str, gcp_conn_id: str) -> None:
    creds = GoogleBaseHook(gcp_conn_id=gcp_conn_id).get_credentials()
    service = build('sheets', 'v4', credentials=creds)
    request = service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=f"{sheet_name}!A1:ZZZ1048576"
    )
    execute_with_timeout(request)

# ===================== 分塊 + 批次寫入 =====================
def upload_values_in_batches(spreadsheet_id: str, sheet_name: str, values: List[List[Any]], gcp_conn_id: str) -> int:
    creds = GoogleBaseHook(gcp_conn_id=gcp_conn_id).get_credentials()
    service = build('sheets', 'v4', credentials=creds)
    sheets = service.spreadsheets().values()

    total_cells, row_cursor = 0, 0
    body_data = []

    for block in chunk_by_cells(values, MAX_CELLS_PER_CHUNK):
        rows = len(block)
        cols = len(block[0]) if rows else 0
        rng = a1_range(row_cursor, 0, rows, cols)
        body_data.append({"range": f"{sheet_name}!{rng}", "values": block})
        total_cells += rows * cols
        row_cursor += rows

        if len(body_data) >= 5:  # 避免單次 JSON 過大
            request = sheets.batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"valueInputOption": "RAW", "data": body_data}
            )
            execute_with_timeout(request)
            body_data = []

    if body_data:
        request = sheets.batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"valueInputOption": "RAW", "data": body_data}
        )
        execute_with_timeout(request)

    return total_cells

# ===================== 主任務 =====================
def task_export_and_upload(**context):
    values = fetch_data_from_postgres(SQL_QUERY, PG_CONN_ID)

    rows_needed = len(values)
    cols_needed = max((len(r) for r in values), default=0)

    ensure_sheet_size(SPREADSHEET_ID, SHEET_NAME, rows_needed, cols_needed, GCP_CONN_ID)
    clear_sheet(SPREADSHEET_ID, SHEET_NAME, GCP_CONN_ID)
    cells = upload_values_in_batches(SPREADSHEET_ID, SHEET_NAME, values, GCP_CONN_ID)

    return {"rows": rows_needed, "cols": cols_needed, "cells_written": cells}

# ===================== DAG 定義 =====================
with DAG(
    dag_id="ptt_macshop_postgres_to_googlesheet",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 0, "retry_delay": timedelta(minutes=3)},
    tags=["postgres", "sheets", "optimized", "solution1", "json-safe", "auto-resize", "maxcell-guard"],
) as dag:
    start = EmptyOperator(task_id="start")
    export_upload = PythonOperator(
        task_id="export_postgres_to_sheets_batch",
        python_callable=task_export_and_upload,
        provide_context=True,
    )
    done = EmptyOperator(task_id="done")
    start >> export_upload >> done
