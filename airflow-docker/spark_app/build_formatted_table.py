# -*- coding: utf-8 -*-
"""
PG → Spark → Transform → _temp → swap（整合版）

重點：
1) 依 **原 test.py** 的寫法處理 `primary_product_info` 與相關欄位：
   - 先建立 `title_norm`、`desc_norm`、`text`、`text_nocomma`
   - 透過 `MODEL_TO_SPEC_REGEX` 擷取 `[型號]..[規格]` 區間成為 `primary_product_info`，並修剪首尾破折號/冒號
   - `focus_text = coalesce( primary_product_info>0? primary_product_info : desc_norm : text )`
   - `search_text = title_norm + focus_text`（與原邏輯一致）
2) 價格抽取沿用/整合 **test2.py** 的進階邏輯（[售價] 區塊 + 相鄰兩行 + 多規則 + 啟發式），
   並且對「2萬8」這類中文混萬做優先處理。
3) 顏色、容量欄位對齊 **test.py**（`color_raw → color` 正規化、`storage_raw → storage_gb` 推導）。
4) 內建 `--selftest`（純 Python，不依賴 Spark）驗證價格抽取規則。

執行：
- spark-submit：
  docker compose exec spark spark-submit \\
    --packages org.postgresql:postgresql:42.7.3 \\
    /opt/spark-apps/pg_pipeline_merged_v2.py

- 自測：
  python pg_pipeline_merged_v2.py --selftest
"""

from __future__ import annotations

import sys
import traceback
from dataclasses import dataclass
from typing import Optional, Tuple

# ------------------------------
# Spark imports（動態載入以支援 --selftest）
# ------------------------------
def _has_spark() -> bool:
    try:
        import pyspark  # noqa
        return True
    except Exception:
        return False

if _has_spark():
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    from pyspark.sql import DataFrame
else:
    SparkSession = object
    F = None
    T = None
    DataFrame = object

# ------------------------------
# 設定
# ------------------------------
@dataclass(frozen=True)
class DBConfig:
    jdbc_url: str = (
        "jdbc:postgresql://postgres:5432/airflow?currentSchema=public&reWriteBatchedInserts=true"
    )
    src_table: str = "public.ptt_macshop_articles"
    dest_table: str = "ptt_macshop_articles_product_detail"
    temp_table: str = "ptt_macshop_articles_product_detail_temp"
    user: str = "airflow"
    password: str = "airflow"
    driver: str = "org.postgresql.Driver"

@dataclass(frozen=True)
class SparkWriteTuning:
    read_num_parts_max: int = 16
    read_fetchsize: str = "10000"
    write_coalesce: int = 2
    write_batchsize: str = "1000"
    write_isolation: str = "READ_COMMITTED"
    spark_timezone: str = "Asia/Taipei"
    repartition_target: Optional[int] = None

# ------------------------------
# 便利函式
# ------------------------------
def guard(msg: str, fn):
    """Run `fn()` with friendly logging, show stack trace on failure."""
    try:
        print(f"▶ {msg} ...")
        res = fn()
        print(f"✅ {msg} 完成")
        return res
    except Exception:
        print(f"❌ {msg} 失敗")
        traceback.print_exc()
        raise

def get_spark(app_name: str, tz: str):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", tz)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark

# ------------------------------
# Regex 與常數（含 test2 價格規則 + test.py 區塊萃取）
# ------------------------------
FULLWIDTH_DIGITS = "０１２３４５６７８９"
HALFWIDTH_DIGITS = "0123456789"

# 從 [型號] 到 [規格] 的區塊
MODEL_TO_SPEC_REGEX = r"(?s)\[型號\]\s*(.*?)\n\s*\[規格\]"

# [售價] 區塊 + 相鄰兩行
PRICE_BLOCK_REGEX = r"(?s)\[售價\]\s*(.+?)(?=\n\s*\[[^\]\\n]{1,20}\]|$)"
PRICE_LINE_AFTER  = r"(?i)^\s*\[售價\][^\\n]*\n([^\\n]{0,80})"
PRICE_NEXT_AFTER  = r"(?i)\[售價\][^\\n]*\n+[^\\n]*\n([^\\n]{0,80})"

# 價格候選與啟發式
PRICE_REGEX_PFX       = r"(?:NT\$|NTD|\$|台幣|新台幣|TWD)\s*([0-9][0-9,]{2,})"
PRICE_REGEX_PFX_LAST  = r".*(?:NT\$|NTD|\$|台幣|新台幣|TWD)\s*([0-9][0-9,]{2,})"
PRICE_REGEX_KW        = r"(?:售價|價格|價位)[^\d]{0,80}([1-9]\d{0,2}(?:,\d{3})+|[1-9]\d{3,6})(?:\s*[元塊])?"
PRICE_REGEX_KW_LOW    = r"(?:售價|價格|價位)[^\d]{0,80}([1-9]\d{2,3})(?=\s*[元塊])"
PRICE_REGEX_NUM       = r"(?<!\d)([1-9]\d{4,6})(?!\d)"
PRICE_REGEX_WAN       = r"(?i)([1-9]\d?(?:\.\d)?)\s*(萬|w|k)"
PRICE_REGEX_4_SUFFIX  = r"(?<!\d)([1-9]\d{3})(?=\s*[元塊])"
PRICE_REGEX_3_SUFFIX  = r"(?<!\d)([1-9]\d{2})(?=\s*[元塊])"

EXTRA_CTX_4_5_AFTER   = r"(?i)(?:售價|價格|價位|售|賣|出清|出售|讓售|處分)[^\d]{0,8}([1-9]\d{3,5})"
EXTRA_NUM_BEFORE_HINT = r"(?i)([1-9]\d{3,5})\s*(?:可議|可小議|即可|含|直上|換算|自取|面交)"
EXTRA_RANGE_NEAR      = r"(?i)(?:售價|價格|售|賣)[^\\n]{0,15}?([1-9]\d{3,5})\s*[/~\\-–—]\s*([1-9]\d{3,5})"
EXTRA_CHI_WAN_MIX     = r"(?i)\b([1-9]\d?(?:\.\d)?)\s*萬(?:\s*([1-9]?\d))?\b"

WTB_REGEX             = r"(徵求|我想要買|希望價格)"

# 產品屬性
COLOR_REGEX   = r"(太空灰|銀|金|玫瑰金|黑|白|藍|綠|紫|黃|紅|午夜|星光|原鈦|原色鈦(?:金屬)?|鈦金屬|Natural Titanium|Space Black|Silver|Gold|Space Gray|Midnight|Starlight|Blue|Green|Purple|Yellow|(PRODUCT)\s*RED|Graphite)"
SIZE_REGEX    = r"(?<![\dA-Za-z])((?:10|11|12|13|14|15|16|17|18|19|20|21|22|23|24)\.?\d{0,1})\s*(?:吋|\"|-inch)"
STORAGE_REGEX = r"(?i)(?<![\dA-Za-z])(64|128|256|512)\s*(?:gb|g)\b|(?<![\dA-Za-z])(1|2)\s*tb\b"
RAM_REGEX     = r"(?i)(?<![\dA-Za-z])(8|12|16|24|32)\s*(?:gb|g)\b"

IPHONE_REGEX  = r"(?i)\bi\s*phone\b"
PRODUCT_MAP = [
    ("iPad", r"(?i)\bi\s*pad\b"),
    ("MacBook", r"(?i)\bmac\s*book\b"),
    ("Apple Watch", r"(?i)\bapple\s*watch\b"),
    ("AirPods", r"(?i)\bair\s*pods\b"),
]

# ------------------------------
# JDBC 來源讀取（分區）
# ------------------------------
def get_id_bounds(spark, cfg: DBConfig) -> Optional[Tuple[int, int]]:
    row = (
        spark.read.format("jdbc")
        .option("url", cfg.jdbc_url)
        .option("dbtable", f"(SELECT MIN(id) AS min_id, MAX(id) AS max_id FROM {cfg.src_table}) t")
        .option("user", cfg.user).option("password", cfg.password).option("driver", cfg.driver)
        .load()
    ).first()
    if row is None or row["min_id"] is None or row["max_id"] is None:
        return None
    return int(row["min_id"]), int(row["max_id"])

def compute_num_partitions(min_id: int, max_id: int, max_parts: int) -> int:
    span = max(1, max_id - min_id + 1)
    return max(1, min(max_parts, span))

def read_src_jdbc(spark, cfg: DBConfig, tuning: SparkWriteTuning):
    bounds = get_id_bounds(spark, cfg)
    if not bounds:
        print("⚠️ 找不到 id 邊界，回退為非分區讀取")
        return (
            spark.read.format("jdbc")
            .option("url", cfg.jdbc_url)
            .option("dbtable", cfg.src_table)
            .option("user", cfg.user).option("password", cfg.password).option("driver", cfg.driver)
            .load()
        )

    min_id, max_id = bounds
    num_parts = compute_num_partitions(min_id, max_id, tuning.read_num_parts_max)
    print(f"ID 範圍：{min_id}~{max_id}，分區數：{num_parts}")

    return (
        spark.read.format("jdbc")
        .option("url", cfg.jdbc_url)
        .option("dbtable", cfg.src_table)
        .option("user", cfg.user).option("password", cfg.password).option("driver", cfg.driver)
        .option("partitionColumn", "id")
        .option("lowerBound", min_id)
        .option("upperBound", max_id + 1)
        .option("numPartitions", num_parts)
        .option("fetchsize", tuning.read_fetchsize)
        .load()
    )

# ------------------------------
# 轉換：標準化 + primary_product_info + focus_text/search_text（*依 test.py*）
# ------------------------------
def normalize_and_focus(df: DataFrame) -> DataFrame:
    """
    1) 標準化 `title`/`description`
    2) 組合 `text` 與 `text_nocomma`
    3) 從 `[型號]..[規格]` 擷取 `primary_product_info` 並修剪邊界符號
    4) `focus_text = coalesce( primary_product_info(if len>0), desc_norm, text )`
    5) `search_text = concat(title_norm, focus_text)`
    """
    assert _has_spark(), "normalize_and_focus 需在 Spark 環境執行"

    df1 = (
        df
        .withColumn(
            "title_norm",
            F.regexp_replace(
                F.translate(F.col("title"), FULLWIDTH_DIGITS, HALFWIDTH_DIGITS),
                u"[\u00A0\u3000]", " "
            )
        )
        .withColumn(
            "desc_norm",
            F.regexp_replace(
                F.translate(F.col("description"), FULLWIDTH_DIGITS, HALFWIDTH_DIGITS),
                u"[\u00A0\u3000]", " "
            )
        )
        # 原 test.py：text = title_norm + desc_norm
        .withColumn("text", F.concat_ws(" ", F.col("title_norm"), F.col("desc_norm")))
        .withColumn("text_nocomma", F.regexp_replace(F.col("text"), ",", ""))
        # 從 [型號] → [規格] 抽 primary_product_info 並修剪首尾 - : ：
        .withColumn(
            "primary_product_info",
            F.trim(
                F.regexp_replace(
                    F.regexp_extract(F.col("desc_norm"), MODEL_TO_SPEC_REGEX, 1),
                    r"^\s*[-:：]*\s*|\s*[-:：]*\s*$",
                    ""
                )
            )
        )
        # focus_text：優先 non-empty 的 primary_product_info，其次 desc_norm，再次 text
        .withColumn(
            "focus_text",
            F.coalesce(
                F.when(F.length(F.col("primary_product_info")) > 0, F.col("primary_product_info")),
                F.col("desc_norm"),
                F.col("text"),
            )
        )
        # search_text = title_norm + focus_text（對齊 test.py）
        .withColumn("search_text", F.concat_ws(" ", F.col("title_norm"), F.col("focus_text")))
    )
    return df1

# ------------------------------
# 價格：建立 price_focus（[售價] 區塊 + 相鄰兩行 + search_text）
# ------------------------------
def attach_price_focus(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("price_text", F.regexp_extract(F.col("desc_norm"), PRICE_BLOCK_REGEX, 1))
        .withColumn("price_text", F.regexp_replace(F.col("price_text"), r"違反者[^\\n]*", ""))
        .withColumn("price_sec_line", F.regexp_extract(F.col("desc_norm"), PRICE_LINE_AFTER, 1))
        .withColumn("price_sec_next", F.regexp_extract(F.col("desc_norm"), PRICE_NEXT_AFTER, 1))
        .withColumn("price_focus", F.concat_ws(" ", "price_text", "price_sec_line", "price_sec_next", "search_text"))
        .withColumn("price_focus_nc", F.regexp_replace(F.col("price_focus"), ",", ""))
    )

# ------------------------------
# 產品屬性解析：對齊 test.py 的欄位命名與部份規則
# ------------------------------
def extract_product_fields(df: DataFrame) -> DataFrame:
    # 分類
    cond = F.when(F.col("search_text").rlike(IPHONE_REGEX), F.lit("iPhone"))
    for name, pat in PRODUCT_MAP:
        cond = cond.when(F.col("search_text").rlike(pat), F.lit(name))

    # 先抽 raw，再做正規化/轉換
    df1 = (
        df.withColumn("product_category", cond.otherwise(F.lit(None)))
          .withColumn("color_raw", F.regexp_extract("focus_text", COLOR_REGEX, 1))
          .withColumn("size_inch", F.regexp_extract("focus_text", SIZE_REGEX, 1).cast("double"))
          .withColumn("storage_raw", F.regexp_extract("focus_text", STORAGE_REGEX, 0))
          .withColumn("ram_gb", F.regexp_extract("focus_text", RAM_REGEX, 1).cast("int"))
    )

    # 顏色正規化（擷取自 test.py 的 normalize_color 邏輯）
    normalize_color = (
        F.when(F.col("color_raw").isin("太空灰","Space Gray","Graphite"), "Space Gray")
         .when(F.col("color_raw").isin("銀","Silver"), "Silver")
         .when(F.col("color_raw").isin("金","Gold"), "Gold")
         .when(F.col("color_raw").isin("玫瑰金","Rose Gold"), "Rose Gold")
         .when(F.col("color_raw").isin("午夜","Midnight"), "Midnight")
         .when(F.col("color_raw").isin("星光","Starlight"), "Starlight")
         .when(F.col("color_raw").rlike("PRODUCT"), "PRODUCT RED")
         .when(F.col("color_raw").rlike("原鈦|原色鈦"), "Natural Titanium")
         .otherwise(F.col("color_raw"))
    )

    df2 = (
        df1.withColumn("color", normalize_color)
           .withColumn(
               "storage_gb",
               F.when(F.lower(F.col("storage_raw")).rlike("tb"),
                      F.regexp_extract("storage_raw", r"(1|2)\s*tb", 1).cast("int") * F.lit(1024))
                .otherwise(F.regexp_extract("storage_raw", r"(?i)(64|128|256|512)\s*(?:gb|g)", 1).cast("int"))
           )
    )
    return df2

# ------------------------------
# 價格（test2 增強版）主候選 + 啟發式 + 最終價
# ------------------------------
def extract_primary_price_candidates(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("cand_pfx_last", F.regexp_extract("price_focus", PRICE_REGEX_PFX_LAST, 1))
        .withColumn("cand_kw",       F.regexp_extract("price_focus_nc", PRICE_REGEX_KW, 1))
        .withColumn("cand_pfx",      F.regexp_extract("price_focus", PRICE_REGEX_PFX, 1))
        .withColumn("cand_kw_low",   F.regexp_extract("price_focus_nc", PRICE_REGEX_KW_LOW, 1))
        .withColumn("cand_4suffix",  F.regexp_extract("price_focus_nc", PRICE_REGEX_4_SUFFIX, 1))
        .withColumn("cand_3suffix",  F.regexp_extract("price_focus_nc", PRICE_REGEX_3_SUFFIX, 1))
        .withColumn("cand_num",      F.regexp_extract("price_focus_nc", PRICE_REGEX_NUM, 1))
        .withColumn("cand_wan_raw",  F.regexp_extract("price_focus_nc", PRICE_REGEX_WAN, 1))
        .withColumn("cand_wan_unit", F.regexp_extract("price_focus_nc", PRICE_REGEX_WAN, 2))
    )

def apply_extra_price_heuristics(df: DataFrame) -> DataFrame:
    df = df.withColumn("extra_range_a", F.regexp_extract("search_text", EXTRA_RANGE_NEAR, 1).cast("long"))
    df = df.withColumn("extra_range_b", F.regexp_extract("search_text", EXTRA_RANGE_NEAR, 2).cast("long"))
    df = df.withColumn("extra_range", F.greatest("extra_range_a", "extra_range_b"))

    df = df.withColumn("extra_ctx_4_5", F.regexp_extract("search_text", EXTRA_CTX_4_5_AFTER, 1).cast("long"))
    df = df.withColumn("extra_num_hint", F.regexp_extract("search_text", EXTRA_NUM_BEFORE_HINT, 1).cast("long"))

    df = df.withColumn("extra_wan_main", F.regexp_extract("search_text", EXTRA_CHI_WAN_MIX, 1))
    df = df.withColumn("extra_wan_tail", F.regexp_extract("search_text", EXTRA_CHI_WAN_MIX, 2))
    df = df.withColumn(
        "extra_wan_val",
        (F.col("extra_wan_main").cast("double") * F.lit(10000) +
         F.when(F.col("extra_wan_tail") != "", F.col("extra_wan_tail").cast("int") * F.lit(1000)).otherwise(F.lit(0)))
        .cast("long")
    )
    return df


def finalize_price(df: DataFrame) -> DataFrame:
    # 最高優先：以幣別前綴且位於最後出現的位置（pfx_last）
    pfx_last_val = F.regexp_replace(F.col("cand_pfx_last"), ",", "").cast("long")

    # 次優先群：kw / pfx / kw_low / 4suffix / 3suffix（不含純 num）
    mid_cluster = F.coalesce(
        F.regexp_replace(F.col("cand_kw"), ",", "").cast("long"),
        F.regexp_replace(F.col("cand_pfx"), ",", "").cast("long"),
        F.col("cand_kw_low").cast("long"),
        F.col("cand_4suffix").cast("long"),
        F.col("cand_3suffix").cast("long")
    )

    # 單位回退（萬/w/k）
    wan_fb = F.when(
        F.lower(F.col("cand_wan_unit")).isin("萬", "w"),
        (F.col("cand_wan_raw").cast("double") * F.lit(10000)).cast("long")
    ).otherwise((F.col("cand_wan_raw").cast("double") * F.lit(1000)).cast("long"))

    # 最終優先序：pfx_last → 中文混萬 → 單位回退 → 區間最大值 → 中間群 → 純數字 → 其他啟發式
    final_price = F.coalesce(
        pfx_last_val,
        F.col("extra_wan_val"),
        wan_fb,
        F.col("extra_range"),
        mid_cluster,
        F.col("cand_num").cast("long"),
        F.col("extra_ctx_4_5"),
        F.col("extra_num_hint")
    )

    rule = (
        F.when(F.col("cand_pfx_last") != "", F.lit("pfx_last"))
         .when(F.col("extra_wan_val").isNotNull(), F.lit("extra_wan_mix"))
         .when(F.col("cand_wan_raw") != "", F.lit("wan_unit"))
         .when(F.col("extra_range").isNotNull(), F.lit("extra_range_ctx"))
         .when(F.col("cand_kw") != "", F.lit("kw"))
         .when(F.col("cand_pfx") != "", F.lit("pfx"))
         .when(F.col("cand_kw_low") != "", F.lit("kw_low"))
         .when(F.col("cand_4suffix") != "", F.lit("4suffix"))
         .when(F.col("cand_3suffix") != "", F.lit("3suffix"))
         .when(F.col("cand_num") != "", F.lit("num"))
         .when(F.col("extra_ctx_4_5").isNotNull(), F.lit("extra_ctx_4_5"))
         .when(F.col("extra_num_hint").isNotNull(), F.lit("extra_num_hint"))
    )
    return df.withColumn("final_price_twd", final_price).withColumn("final_rule", rule)

# ------------------------------
# 其他衍生（保留掛點，需求時擴充）
# ------------------------------
def derive_misc(df: DataFrame) -> DataFrame:
    return df

# ------------------------------
# 主轉換流程
# ------------------------------
def transform_articles(df_src: DataFrame, repartition_target: Optional[int] = None) -> DataFrame:
    df = normalize_and_focus(df_src)
    df = attach_price_focus(df)

    df = extract_product_fields(df)

    df = extract_primary_price_candidates(df)
    df = apply_extra_price_heuristics(df)
    df = finalize_price(df)
    df = df.withColumn("price_twd", F.col("final_price_twd"))

    df = derive_misc(df)

    # 清理中間欄位
    drop_cols = [
        "text","text_nocomma","color_raw","storage_raw",
        "cand_kw_low","cand_4suffix","cand_3suffix","cand_pfx_last",
        "price_text","price_sec_line","price_sec_next","price_focus","price_focus_nc",
        "cand_pfx","cand_kw","cand_num","cand_wan_raw","cand_wan_unit",
        "extra_range_a","extra_range_b","extra_range","extra_ctx_4_5","extra_num_hint",
        "extra_wan_main","extra_wan_tail","extra_wan_val","final_price_twd"
    ]
    for c in drop_cols:
        if c in df.columns:
            df = df.drop(c)

    # 分區
    if repartition_target and isinstance(repartition_target, int) and repartition_target > 0:
        df_out = df.repartition(repartition_target)
    else:
        df_out = df.coalesce(4)

    # 輸出欄位（存在才選）
    write_cols = [
        "id","title","author","created_date","link","description","description_hash","updated_date",
        "product_category","size_inch","ram_gb","price_twd",
        "battery_health_pct","battery_cycles","model_number","model_identifier","sold_flag",
        "color","storage_gb","battery_health_bucket","design_cycle_target","health_status_hint","final_rule"
    ]
    return df_out.select(*[c for c in write_cols if c in df_out.columns])

# ------------------------------
# 建立 _temp → 寫入 → swap（沿用原流程）
# ------------------------------
def recreate_temp_from_df_schema(spark, df_out, cfg: DBConfig):
    url = cfg.jdbc_url
    tbl = f'public."{cfg.temp_table}"'

    ddl = f'''
    DROP TABLE IF EXISTS {tbl};
    CREATE UNLOGGED TABLE {tbl} (
      id BIGINT,
      title TEXT,
      author TEXT,
      created_date TIMESTAMP,
      link TEXT,
      description TEXT,
      description_hash TEXT,
      updated_date TIMESTAMP,
      product_category TEXT,
      size_inch DOUBLE PRECISION,
      ram_gb INTEGER,
      price_twd BIGINT,
      battery_health_pct DOUBLE PRECISION,
      battery_cycles INTEGER,
      model_number TEXT,
      model_identifier TEXT,
      sold_flag BOOLEAN,
      color TEXT,
      storage_gb INTEGER,
      battery_health_bucket TEXT,
      design_cycle_target INTEGER,
      health_status_hint TEXT,
      final_rule TEXT
    );
    '''
    conn = spark._sc._jvm.java.sql.DriverManager.getConnection(url, cfg.user, cfg.password)
    try:
        stmt = conn.createStatement()
        stmt.execute(ddl)
    finally:
        stmt.close()
        conn.close()

def write_temp_all(df_out, cfg: DBConfig, tuning: SparkWriteTuning):
    (df_out
        .coalesce(tuning.write_coalesce)
        .write
        .format("jdbc")
        .option("url", cfg.jdbc_url)
        .option("dbtable", f'public."{cfg.temp_table}"')
        .option("user", cfg.user).option("password", cfg.password).option("driver", cfg.driver)
        .option("batchsize", tuning.write_batchsize)
        .option("isolationLevel", tuning.write_isolation)
        .mode("append")
        .save()
    )

def swap_temp_to_final(spark, cfg: DBConfig):
    url = cfg.jdbc_url
    conn = spark._sc._jvm.java.sql.DriverManager.getConnection(url, cfg.user, cfg.password)
    try:
        conn.setAutoCommit(False)
        stmt = conn.createStatement()
        try:
            stmt.execute(f'DROP TABLE IF EXISTS public."{cfg.dest_table}"')
            stmt.execute(f'ALTER TABLE public."{cfg.temp_table}" RENAME TO "{cfg.dest_table}"')
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            stmt.close()
    finally:
        conn.close()

# ------------------------------
# 進入點
# ------------------------------
def main_spark():
    if not _has_spark():
        print("⚠️ 未偵測到 pyspark，請於 Spark 環境執行或改用 --selftest。")
        sys.exit(1)

    cfg = DBConfig()
    tuning = SparkWriteTuning()
    spark = get_spark("pg_pipeline_merged_v2", tuning.spark_timezone)

    try:
        df_src = guard("JDBC 讀取來源表", lambda: read_src_jdbc(spark, cfg, tuning))
        df_out = guard("轉換（正規化 + 價格抽取 + 欄位）", lambda: transform_articles(df_src, tuning.repartition_target))
        guard("建立 _temp 表（依 schema）", lambda: recreate_temp_from_df_schema(spark, df_out, cfg))
        guard("寫入 _temp（JDBC 批次）", lambda: write_temp_all(df_out, cfg, tuning))
        guard("交換 _temp → 正式表（交易內 rename swap）", lambda: swap_temp_to_final(spark, cfg))
        print("🎯 完成 Pipeline")
    finally:
        try:
            spark.stop()
        except Exception:
            pass

# ------------------------------
# 自動化自測（純 Python，不需 Spark）
# ------------------------------
def selftest():
    import re

    # 價格案例（涵蓋前綴、關鍵詞、萬/k、中文混萬、區間、可議等）
    cases = [
        ("AirPods Pro", "[售價] 單個 $700 / 4個 $2,790", 2790),
        ("台中 iPhone 14 Pro 256G 太空灰", "[規格] 256G / 8G RAM\n[售價] 28,900 元", 28900),
        ("MacBook Air M2", "誠可議，含配件。NT$30,500 可小議", 30500),
        ("iPad Pro 11", "[規格] 256G [售價] 3.9萬", 39000),
        ("iPad Air", "[售價] 25k 含盒", 25000),
        ("售 AirPods Pro", "價格 700 / 2790 兩種，最後決定 2790元", 2790),
        ("Apple Watch", "售價 9000元，配件齊", 9000),
        ("iPhone", "2萬8 幫老婆處分", 28000),
        ("iPhone", "賣 12800 可議", 12800),
        ("iPhone", "價格 27000~28900 可小議", 28900),
    ]

    def nocomma(s): return s.replace(",", "")

    OK = 0
    for ti, de, expect in cases:
        # 像 Spark 一樣構造 price_focus 欄位的來源字串
        price_text = re.search(PRICE_BLOCK_REGEX, de)
        price_text = price_text.group(1) if price_text else ""
        price_text = re.sub(r"違反者[^\\n]*", "", price_text)
        line = re.search(PRICE_LINE_AFTER, de)
        line = line.group(1) if line else ""
        nxt = re.search(PRICE_NEXT_AFTER, de)
        nxt = nxt.group(1) if nxt else ""
        search_text = f"{ti} {de}"  # 自測近似：title_norm + focus_text
        focus = " ".join([price_text, line, nxt, search_text])
        focus_nc = nocomma(focus)

        # 候選
        def _g(pat, text):
            m = re.search(pat, text)
            return m.group(1) if m else ""
        pfx_last = _g(PRICE_REGEX_PFX_LAST, focus)
        kw       = _g(PRICE_REGEX_KW, focus_nc)
        pfx      = _g(PRICE_REGEX_PFX, focus)
        kw_low   = _g(PRICE_REGEX_KW_LOW, focus_nc)
        s4       = _g(PRICE_REGEX_4_SUFFIX, focus_nc)
        s3       = _g(PRICE_REGEX_3_SUFFIX, focus_nc)
        num      = _g(PRICE_REGEX_NUM, focus_nc)
        m_wan    = re.search(PRICE_REGEX_WAN, focus_nc)
        wan_raw  = m_wan.group(1) if m_wan else ""
        wan_unit = m_wan.group(2) if m_wan else ""

        # extra
        def _gi(pat, text):
            m = re.search(pat, text)
            return int(m.group(1)) if m else None

        extra_range_m = re.search(EXTRA_RANGE_NEAR, search_text)
        extra_range = None
        if extra_range_m:
            a = int(extra_range_m.group(1))
            b = int(extra_range_m.group(2))
            extra_range = max(a, b)

        extra_ctx_4_5 = _gi(EXTRA_CTX_4_5_AFTER, search_text)
        extra_num_hint = _gi(EXTRA_NUM_BEFORE_HINT, search_text)

        extra_wan_mix = None
        m = re.search(EXTRA_CHI_WAN_MIX, search_text)
        if m:
            main = float(m.group(1))
            tail = m.group(2)
            extra_wan_mix = int(main * 10000 + (int(tail)*1000 if tail else 0))

        # 決策（與 finalize_price 順序一致）
        def _safe_int(s):
            try: return int(nocomma(s))
            except Exception: return None
        # 新優先序：pfx_last → 中文混萬 → 單位回退 → 區間最大值 → (kw/pfx/kw_low/4suffix/3suffix) → num → 其他
        mid_cluster = next((x for x in [_safe_int(kw), _safe_int(pfx), _safe_int(kw_low), _safe_int(s4), _safe_int(s3)] if x), None)
        price = _safe_int(pfx_last)
        if price is None and extra_wan_mix is not None:
            price = extra_wan_mix
        if price is None and wan_raw:
            if wan_unit.lower() in ("萬","w"): price = int(float(wan_raw)*10000)
            else: price = int(float(wan_raw)*1000)
        if price is None and extra_range:
            price = extra_range
        if price is None and mid_cluster is not None:
            price = mid_cluster
        if price is None and _safe_int(num):
            price = _safe_int(num)
        if price is None:
            price = extra_ctx_4_5 or extra_num_hint

        ok = (price == expect)
        OK += 1 if ok else 0
        print(f"[{'OK' if ok else 'NG'}] expect={expect}, got={price} | title={ti} desc={de}")

    print(f"\nSelftest passed {OK}/{len(cases)}")

if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        main_spark()
