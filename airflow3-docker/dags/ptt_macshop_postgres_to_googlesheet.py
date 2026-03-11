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

# 20251010_001 - PoChun Hsu - [Add]     Dataset for trigger across DAGs.

from datetime import datetime, date, timedelta
from decimal import Decimal
from typing import Iterable, List, Any

from airflow import DAG, Dataset
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from googleapiclient.discovery import build
from airflow.providers.postgres.hooks.postgres import PostgresHook


# ===================== Dataset 定義 =====================
FORMATTED_UPDATED = Dataset("dataset://ptt_macshop/formatted_updated")

# ===================== Airflow Variables（可於 UI 調整） =====================
# 原本主 spreadsheet
MAIN_SPREADSHEET_ID = Variable.get(
    "GOOGLE_SHEETS_SPREADSHEET_ID",
    default_var="1MFwhTSKOc_RM8wKvTuHrj7doOxiArQiyOxFnAgXKrSQ"
)
MAIN_SHEET_NAME = Variable.get(
    "GOOGLE_SHEETS_SHEET_NAME",
    default_var="sheet1"
)
MAIN_SQL_QUERY = Variable.get(
    "PG_EXPORT_SQL",
    default_var="SELECT * FROM ptt_macshop_articles_product_detail ORDER BY created_date DESC"
)

# daily mart spreadsheet
DAILY_PRODUCT_INDEX_SPREADSHEET_ID = Variable.get(
    "GOOGLE_SHEETS_DAILY_PRODUCT_INDEX_SPREADSHEET_ID",
    default_var="1AQchYJrMj7Q7H02r_krZclnnGx_8BsI9lXW7O3S5UcI"
)
DAILY_PRODUCT_INDEX_SHEET_NAME = Variable.get(
    "GOOGLE_SHEETS_DAILY_PRODUCT_INDEX_SHEET_NAME",
    default_var="sheet1"
)
DAILY_PRODUCT_INDEX_SQL = Variable.get(
    "PG_EXPORT_DAILY_PRODUCT_INDEX_SQL",
    default_var='SELECT * FROM analytics."Mart_Log_Daily_Product_Index" ORDER BY created_date DESC'
)

# monthly mart spreadsheet
MONTHLY_PRODUCT_INDEX_SPREADSHEET_ID = Variable.get(
    "GOOGLE_SHEETS_MONTHLY_PRODUCT_INDEX_SPREADSHEET_ID",
    default_var="1PjZITP2IZgQttWCblP3A2QuG_TQl520yw07bY-Eviok"
)
MONTHLY_PRODUCT_INDEX_SHEET_NAME = Variable.get(
    "GOOGLE_SHEETS_MONTHLY_PRODUCT_INDEX_SHEET_NAME",
    default_var="sheet1"
)
MONTHLY_PRODUCT_INDEX_SQL = Variable.get(
    "PG_EXPORT_MONTHLY_PRODUCT_INDEX_SQL",
    default_var='SELECT * FROM analytics."Mart_Log_Monthly_Product_Index" ORDER BY created_month DESC'
)

EXPORT_TARGETS = [
    {
        "task_id": "export_main_sheet",
        "spreadsheet_id": MAIN_SPREADSHEET_ID,
        "sheet_name": MAIN_SHEET_NAME,
        "sql": MAIN_SQL_QUERY,
    },
    {
        "task_id": "export_daily_product_index_sheet",
        "spreadsheet_id": DAILY_PRODUCT_INDEX_SPREADSHEET_ID,
        "sheet_name": DAILY_PRODUCT_INDEX_SHEET_NAME,
        "sql": DAILY_PRODUCT_INDEX_SQL,
    },
    {
        "task_id": "export_monthly_product_index_sheet",
        "spreadsheet_id": MONTHLY_PRODUCT_INDEX_SPREADSHEET_ID,
        "sheet_name": MONTHLY_PRODUCT_INDEX_SHEET_NAME,
        "sql": MONTHLY_PRODUCT_INDEX_SQL,
    },
]

# 連線 ID
PG_CONN_ID  = Variable.get("PG_CONN_ID", default_var="postgres_default")
GCP_CONN_ID = Variable.get("GCP_CONN_ID", default_var="google_cloud_default")

# 可調參數
# 每次寫入 google sheet的 cell數量上限
MAX_CELLS_PER_CHUNK   = int(Variable.get("SHEETS_MAX_CELLS_PER_CHUNK", default_var="20000"))
REQUEST_TIMEOUT_SEC   = int(Variable.get("SHEETS_REQUEST_TIMEOUT_SEC", default_var="120"))
EXECUTE_NUM_RETRIES   = int(Variable.get("SHEETS_EXECUTE_NUM_RETRIES", default_var="5"))
DATETIME_FMT          = Variable.get("SHEETS_DATETIME_FMT", default_var="%Y-%m-%d %H:%M:%S")

# 自動擴表的額外 buffer
# 每次自動往外新增 100 row or 5 column
EXTRA_ROW_BUFFER      = int(Variable.get("SHEETS_EXTRA_ROW_BUFFER", default_var="100"))
EXTRA_COL_BUFFER      = int(Variable.get("SHEETS_EXTRA_COL_BUFFER", default_var="5"))

# 單一儲存格字元上限（Google Sheets 固定為 50000；保留成參數以便容錯/測試）
SHEETS_MAX_CELL_CHARS = int(Variable.get("SHEETS_MAX_CELL_CHARS", default_var="50000"))
# 超過上限時的策略：truncate / fail / trim_whitespace
# 通常會超過是因為內文包含了太多留言，目前用不到留言內容，所以直接拋棄
SHEETS_OVERFLOW_POLICY = Variable.get("SHEETS_OVERFLOW_POLICY", default_var="truncate").lower()
# truncate 的尾註（提示被截斷）
TRUNCATION_SUFFIX = Variable.get("SHEETS_TRUNCATION_SUFFIX", default_var=" …[truncated]")


# ===================== 共用：execute 包裝 =====================
def execute_with_timeout(request, timeout: int = REQUEST_TIMEOUT_SEC, num_retries: int = EXECUTE_NUM_RETRIES):
    """在 execute 時設定 timeout / 重試（新版相容）。"""
    # 萬一不能帶 timeout參數時改傳沒有 timeout參數的執行指令
    try:
        return request.execute(num_retries=num_retries, timeout=timeout)
    except TypeError:
        return request.execute(num_retries=num_retries)


# ===================== A1 / 分塊 =====================
# 因為 google sheet 的 column是 A, B, ...
# 把第一欄轉為 A欄，第二欄轉為 B欄 . . .
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

# 定義 google sheet使用到的範圍
def a1_range(start_row: int, start_col: int, rows: int, cols: int) -> str:
    start = f"{number_to_column_letters(start_col)}{start_row+1}"
    end   = f"{number_to_column_letters(start_col+cols-1)}{start_row+rows}"
    return f"{start}:{end}"

# Google sheet API只接受二維陣列的格式（List[List[Any]]）
# 因為 fetch_data_from_postgres()會把值匯總成 List[List[Any]]
# 寫入時要把 List[List[Any]] 切成多個 List[Any], List[Any], ...
# List[Any] 的值塞進去 cell
def chunk_by_cells(values: List[List[Any]], max_cells: int) -> Iterable[List[List[Any]]]:
    """根據最大儲存格數量分塊，避免超大批次寫入。"""
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

# 把 Datetime, Decimal, list等 google sheet不接受的格式做適當轉換
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
        # 將複合型別序列化為字串
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
    cur.close()
    conn.close()

    # to_json_safe: 確保資料格式可被 google sheet接受
    safe_rows = [[to_json_safe(cell) for cell in row] for row in rows]
    return [headers] + safe_rows


# ===================== 擴表：確保 rows/cols 足夠 =====================
def ensure_sheet_size(spreadsheet_id: str, sheet_name: str, rows_needed: int, cols_needed: int, gcp_conn_id: str) -> None:
    creds = GoogleBaseHook(gcp_conn_id=gcp_conn_id).get_credentials()
    service = build('sheets', 'v4', credentials=creds)

    # 抓 spreadsheet metadata，只要 sheetId、title、row/column 數
    meta_req = service.spreadsheets().get(
        spreadsheetId=spreadsheet_id,
        includeGridData=False,
        fields="sheets(properties(sheetId,title,gridProperties(rowCount,columnCount)))"
    )
    meta = execute_with_timeout(meta_req)

    # 找出指定名稱的 sheet
    # 找不到直接中斷報 Error
    target = None
    for s in meta.get("sheets", []):
        props = s.get("properties", {})
        if props.get("title") == sheet_name:
            target = props
            break
    if not target:
        raise ValueError(f"Sheet '{sheet_name}' 不存在，請先建立該分頁或確認名稱大小寫。")

     # 取得指定 sheet 的 row / column 數
    sheet_id = target["sheetId"]
    current_rows = target.get("gridProperties", {}).get("rowCount", 1000)
    current_cols = target.get("gridProperties", {}).get("columnCount", 26)

    # 計算所需的 Row and Column數
    new_rows = max(current_rows, rows_needed + EXTRA_ROW_BUFFER)
    new_cols = max(current_cols, cols_needed + EXTRA_COL_BUFFER)

    # 目前 Row or Column數不夠的話要先擴增
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
# MAX_CELLS_PER_CHUNK ２萬，因此每次寫入 5個 block = 5*2萬 = 10萬
# 實務上最後一個 block可能沒有滿，因此是 <= 10萬
# 2025-12-16 時一個 row 34個欄位，10萬／34 = 一次大約是 2941筆資料
def upload_values_in_batches(spreadsheet_id: str, sheet_name: str, values: List[List[Any]], gcp_conn_id: str) -> int:
    creds = GoogleBaseHook(gcp_conn_id=gcp_conn_id).get_credentials()
    service = build('sheets', 'v4', credentials=creds)
    sheets = service.spreadsheets().values()

    # 紀錄實際寫入的 cell數，目前寫到第幾 row
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
    
    # 不滿 len(body_data) >= 5 最後要處理一次
    if body_data:
        request = sheets.batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"valueInputOption": "RAW", "data": body_data}
        )
        execute_with_timeout(request)

    return total_cells

# 20260311_001 >>
# ===================== 主任務 =====================
# ===================== 定義單一 sheet 寫入流程 =====================
def export_one_target_to_sheet(
    spreadsheet_id: str,
    sheet_name: str,
    sql: str,
    pg_conn_id: str,
    gcp_conn_id: str,
    **context
) -> dict:
    """單一 SQL -> 單一 Google Sheet 分頁"""

    # 1. 載入 Postgres的資料
    values = fetch_data_from_postgres(sql, pg_conn_id)

    # 2. 計算所需 column and row 數量
    rows_needed = len(values)
    cols_needed = max((len(r) for r in values), default=0)

    # 3. 如果 column or row數量不夠要擴增
    ensure_sheet_size(
        spreadsheet_id=spreadsheet_id,
        sheet_name=sheet_name,
        rows_needed=rows_needed,
        cols_needed=cols_needed,
        gcp_conn_id=gcp_conn_id,
    )

    # 4. 清空 google sheet
    clear_sheet(
        spreadsheet_id=spreadsheet_id,
        sheet_name=sheet_name,
        gcp_conn_id=gcp_conn_id,
    )

    # 5. 一次append 5*block，最多１０萬個 cells，約 2941筆資料到 google sheet
    cells = upload_values_in_batches(
        spreadsheet_id=spreadsheet_id,
        sheet_name=sheet_name,
        values=values,
        gcp_conn_id=gcp_conn_id,
    )

    return {
        "sheet_name": sheet_name,
        "rows": rows_needed,
        "cols": cols_needed,
        "cells_written": cells,
    }

def task_export_one_target(
    spreadsheet_id: str,
    sheet_name: str,
    sql: str,
    pg_conn_id: str,
    gcp_conn_id: str,
    **context,
) -> dict:
    return export_one_target_to_sheet(
        spreadsheet_id=spreadsheet_id,
        sheet_name=sheet_name,
        sql=sql,
        pg_conn_id=pg_conn_id,
        gcp_conn_id=gcp_conn_id,
    )
# 20260311_001 <<


# ===================== DAG 定義 =====================
with DAG(
    dag_id="ptt_macshop_postgßres_to_googlesheet",
    schedule=[FORMATTED_UPDATED],  # Dataset-based trigger
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 0, "retry_delay": timedelta(minutes=3)},
    tags=["postgres", "sheets", "optimized", "json-safe", "auto-resize", "maxcell-guard"],
) as dag:
    start = EmptyOperator(task_id="start")
    done = EmptyOperator(task_id="done")

    export_tasks = []

    for target in EXPORT_TARGETS:
        task = PythonOperator(
            task_id=target["task_id"],
            python_callable=task_export_one_target,
            op_kwargs={
                "spreadsheet_id": target["spreadsheet_id"],
                "sheet_name": target["sheet_name"],
                "sql": target["sql"],
                "pg_conn_id": PG_CONN_ID,
                "gcp_conn_id": GCP_CONN_ID,
            },
        )
        export_tasks.append(task)

    start >> export_tasks >> done
