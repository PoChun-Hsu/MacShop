
# -*- coding: utf-8 -*-
"""
Refactor: pg_to_pg_mirror_with_transforms.py

目的：
- 維持原功能（Postgres → Spark → 轉換 → _temp → 原子 swap）
- 代碼模組化、可讀性提升、註解清晰
- Regex 與欄位抽取邏輯「不改變語意與優先順序」

執行（範例，在容器內）：
docker compose exec spark spark-submit \
  --packages org.postgresql:postgresql:42.7.3 \
  /opt/spark-apps/pg_to_pg_mirror_with_transforms_refactor.py
"""
from __future__ import annotations

import traceback
from dataclasses import dataclass
from typing import Optional, Tuple

from pyspark.sql import SparkSession, functions as F
from pyspark.sql import DataFrame

# =========================
# Config
# =========================

@dataclass(frozen=True)
class DBConfig:
    jdbc_url: str = (
        "jdbc:postgresql://postgres:5432/airflow?currentSchema=public&reWriteBatchedInserts=true"
    )
    src_table: str = "public.ptt_macshop_articles"
    dest_table: str = "ptt_macshop_articles_product_detail"   # final table (no schema in SQL)
    temp_table: str = "ptt_macshop_articles_product_detail_temp"  # temp table (no schema)
    user: str = "airflow"
    password: str = "airflow"
    driver: str = "org.postgresql.Driver"

@dataclass(frozen=True)
class SparkWriteTuning:
    read_num_parts_max: int = 16
    read_fetchsize: str = "10000"
    write_coalesce: int = 2
    write_batchsize: str = "500"
    write_isolation: str = "READ_COMMITTED"
    spark_timezone: str = "Asia/Taipei"
    repartition_target: Optional[int] = None  # None: keep default

# =========================
# Utility
# =========================

def guard(msg: str, fn):
    """Run `fn` with console logs and stack trace on failure."""
    try:
        print(f"▶ {msg} ...")
        res = fn()
        print(f"✅ {msg} 完成")
        return res
    except Exception:
        print(f"❌ {msg} 失敗，堆疊：")
        traceback.print_exc()
        raise

def get_spark(app_name: str, tz: str) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", tz)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark

# =========================
# JDBC Bounds / Read
# =========================

def get_id_bounds(spark: SparkSession, cfg: DBConfig) -> Optional[Tuple[int, int]]:
    """Min/Max id from src table for partitioned JDBC read."""
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

def compute_num_partitions(spark: SparkSession, max_parts: int) -> int:
    return min(max(spark.sparkContext.defaultParallelism, 8), max_parts)

def read_src_jdbc(
    spark: SparkSession, cfg: DBConfig, tuning: SparkWriteTuning, lower: int, upper: int, num_parts: int
) -> DataFrame:
    return (
        spark.read.format("jdbc")
        .option("url", cfg.jdbc_url)
        .option("dbtable", cfg.src_table)
        .option("user", cfg.user).option("password", cfg.password).option("driver", cfg.driver)
        .option("fetchsize", tuning.read_fetchsize)
        .option("partitionColumn", "id")
        .option("lowerBound", str(lower))
        .option("upperBound", str(upper))
        .option("numPartitions", str(num_parts))
        .load()
    )

# =========================
# Regex (kept identical in meaning)
# =========================

# Text normalization
FULLWIDTH_DIGITS = "０１２３４５６７８９"
HALFWIDTH_DIGITS = "0123456789"

# Sections
MODEL_TO_SPEC_REGEX = r"(?s)\[型號\]\s*(.*?)\n\s*\[規格\]"

# Price block
PRICE_BLOCK_REGEX = r"(?s)\[售價\]\s*(.+?)(?=\n\s*\[[^\]\n]{1,20}\]|$)"

# Product fields
COLOR_REGEX   = r"(太空灰|銀|金|玫瑰金|黑|白|藍|綠|紫|黃|紅|午夜|星光|原鈦|原色鈦(?:金屬)?|Graphite|Silver|Gold|Rose Gold|Space Gray|Midnight|Starlight|Blue|Green|Purple|Yellow|(PRODUCT)\s*RED)"
SIZE_REGEX    = r"(?<![\dA-Za-z])((?:10|11|12|13|14|15|16|17|18|19|20|21|22|23|24)\.?\d{0,1})\s*(?:吋|\"|-inch)"
STORAGE_REGEX = r"(?i)(?<![\dA-Za-z])(64|128|256|512)\s*(?:gb|g)\b|(?<![\dA-Za-z])(1|2)\s*tb\b"
RAM_REGEX = r"(?i)(?<![\dA-Za-z])(8|16|24|32|64)\s*gb\s*(?:ram|記憶體)?\b"

# Price candidates
PRICE_REGEX_PFX       = r"(?:NT\$|NTD|\$|台幣|新台幣|TWD)\s*([0-9][0-9,]{2,})"
PRICE_REGEX_KW = r"(?:售價|價格|價位)[^\d]{0,80}([1-9]\d{0,2}(?:,\d{3})+|[1-9]\d{3,6})(?:\s*[元塊])?"
PRICE_REGEX_KW_LOW    = r"(?:售價|價格|價位)[^\d]{0,80}([1-9]\d{2,3})(?=\s*[元塊])"
PRICE_REGEX_NUM       = r"(?<!\d)([1-9]\d{4,6})(?!\d)"
PRICE_REGEX_WAN       = r"(?i)([1-9]\d?(?:\.\d)?)\s*(萬|w|k)"
PRICE_REGEX_4_SUFFIX  = r"(?<!\d)([1-9]\d{3})(?=\s*[元塊])"
PRICE_REGEX_3_SUFFIX  = r"(?<!\d)([1-9]\d{2})(?=\s*[元塊])"
PRICE_REGEX_PFX_LAST  = r".*(?:NT\$|NTD|\$|台幣|新台幣|TWD)\s*([0-9][0-9,]{2,})"

BATT_HEALTH_REGEX = r"(?:電池(?:健康)?(?:度)?|健康度|電池健康)[^\d%]{0,10}(\d{2,3})\s*%|\bBH\s*(\d{2,3})\s*%"
BATT_CYCLE_REGEX  = r"(\d{1,4})\s*(?:循環|cycles?|次)"
AMODEL_REGEX      = r"\bA\d{4}\b"
MODEL_ID_REGEX    = r"\b(?:iMac|MacBookPro|MacBookAir|Macmini|MacStudio|MacPro)\d{1,2},\d\b"
IPHONE_REGEX      = r"""(?ix)\bi\s*phone\s*(?:\d{1,2})?\s*(?:pro\s*max|pro|plus|mini)?\b"""

PRODUCT_MAP = [
    ("iPhone", IPHONE_REGEX),
    ("iPad", r"(?i)\bipad(?:\s*(pro|air|mini))?(?:\s*\d{1,2}(?:th|代|代目)?)?\b"),
    ("MacBook Pro", r"(?i)\bmac\s*book\s*pro\b|\bMBP\b"),
    ("MacBook Air", r"(?i)\bmac\s*book\s*air\b|\bMBA\b"),
    ("iMac",       r"(?i)\bimac\b"),
    ("Mac mini",   r"(?i)mac\s*mini"),
    ("Mac Studio", r"(?i)mac\s*studio"),
    ("Mac Pro",    r"(?i)mac\s*pro\b"),
    ("AirPods", r"(?i)\bair\s*pods?\s*(?:pro|max)?\s*(?:\d+|[一二三四五六七八九十]+代)?\b"),
    ("Apple Watch", r"(?i)\bapple\s*watch\b|\bAW\d+\b"),
    ("HomePod mini", r"(?i)\bhome\s*pod\s*mini\b"),
    ("HomePod",      r"(?i)\bhome\s*pod\b(?!\s*mini)"),
    ("AirTag", r"(?i)\bair\s*tags?\b"),
    ("Apple Pencil", r"(?i)\bapple\s*pencil\s*(?:\d+|[一二三四])?\s*代?\b"),
    ("Studio Display", r"(?i)\bstudio\s*display\b"),
    ("Pro Display XDR", r"(?i)\bpro\s*display\s*XDR\b"),
    ("Magic Keyboard", r"(?i)\bmagic\s*keyboard\b"),
    ("Magic Mouse",    r"(?i)\bmagic\s*mouse\b"),
    ("Magic Trackpad", r"(?i)\bmagic\s*trackpad\b"),
]

# =========================
# Transform
# =========================

def _nullif_empty(colname: str):
    return F.when((F.col(colname).isNull()) | (F.col(colname) == ""), F.lit(None)).otherwise(F.col(colname))

def _safe_int(col):
    return F.when((F.col(col).isNotNull()) & (F.col(col) != ""), F.col(col).cast("long"))

def normalize_and_focus(df: DataFrame) -> DataFrame:
    """標準化文字、產生 focus_text（primary_product_info > desc_norm > title+desc）。"""
    df1 = (
        df
        .withColumn("title_norm",
            F.regexp_replace(
                F.translate(F.col("title"), FULLWIDTH_DIGITS, HALFWIDTH_DIGITS),
                u"[\u00A0\u3000]", " "
            )
        )
        .withColumn("desc_norm",
            F.regexp_replace(
                F.translate(F.col("description"), FULLWIDTH_DIGITS, HALFWIDTH_DIGITS),
                u"[\u00A0\u3000]", " "
            )
        )
        .withColumn("text", F.concat_ws(" ", F.col("title_norm"), F.col("desc_norm")))
        .withColumn("text_nocomma", F.regexp_replace(F.col("text"), ",", ""))
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
        .withColumn(
            "focus_text",
            F.coalesce(
                F.when(F.length(F.col("primary_product_info")) > 0, F.col("primary_product_info")),
                F.col("desc_norm"),
                F.col("text")
            )
        )
        .withColumn("search_text", F.concat_ws(" ", F.col("title_norm"), F.col("focus_text")))
    )
    return df1

def attach_price_focus(df: DataFrame) -> DataFrame:
    """建立 price_focus（[售價] 區塊優先 + 相鄰兩行 + search_text）。"""
    return (
        df
        .withColumn("price_text", F.regexp_extract(F.col("desc_norm"), PRICE_BLOCK_REGEX, 1))
        .withColumn("price_text", F.regexp_replace(F.col("price_text"), r"違反者[^\n]*", ""))
        .withColumn("price_sec_line", F.regexp_extract(F.col("desc_norm"), r"(?i)^\s*\[售價\][^\n]*\n([^\n]{0,80})", 1))
        .withColumn("price_sec_next", F.regexp_extract(F.col("desc_norm"), r"(?i)\[售價\][^\n]*\n+[^\n]*\n([^\n]{0,80})", 1))
        .withColumn("price_focus", F.concat_ws(" ", "price_text", "price_sec_line", "price_sec_next", "search_text"))
        .withColumn("price_focus_nocomma", F.regexp_replace(F.col("price_focus"), ",", ""))
    )

def extract_product_fields(df: DataFrame) -> DataFrame:
    """從 focus_text/search_text 解析產品屬性、價格候選、電池資訊等。"""
    # product category
    cond = F.when(F.col("search_text").rlike(IPHONE_REGEX), F.lit("iPhone"))
    for name, pat in PRODUCT_MAP:
        cond = cond.when(F.col("search_text").rlike(pat), F.lit(name))

    df1 = (
        df.withColumn("product_category", cond.otherwise(F.lit(None)))
          .withColumn("color_raw", F.regexp_extract("focus_text", COLOR_REGEX, 1))
          .withColumn("size_inch", F.regexp_extract("focus_text", SIZE_REGEX, 1).cast("double"))
          .withColumn("storage_raw", F.regexp_extract("focus_text", STORAGE_REGEX, 0))
          .withColumn("ram_gb", F.regexp_extract("focus_text", RAM_REGEX, 1).cast("int"))

          .withColumn("price_pfx",      F.regexp_extract("price_focus", PRICE_REGEX_PFX, 1))
          .withColumn("price_kw",       F.regexp_extract("price_focus_nocomma", PRICE_REGEX_KW, 1))
          .withColumn("price_kw_low",   F.regexp_extract("price_focus", PRICE_REGEX_KW_LOW, 1))
          .withColumn("price_num",      F.regexp_extract("price_focus_nocomma", PRICE_REGEX_NUM, 1))
          .withColumn("price_wan_raw",  F.regexp_extract("price_focus_nocomma", PRICE_REGEX_WAN, 1))
          .withColumn("price_wan_unit", F.regexp_extract("price_focus_nocomma", PRICE_REGEX_WAN, 2))
          .withColumn("price_4suffix",  F.regexp_extract("price_focus_nocomma", PRICE_REGEX_4_SUFFIX, 1))
          .withColumn("price_3suffix",  F.regexp_extract("price_focus_nocomma", PRICE_REGEX_3_SUFFIX, 1))
          .withColumn("price_pfx_last", F.regexp_extract("price_focus", PRICE_REGEX_PFX_LAST, 1))

          .withColumn(
              "battery_health_pct",
              F.when(F.regexp_extract("search_text", BATT_HEALTH_REGEX, 1) != "",
                     F.regexp_extract("search_text", BATT_HEALTH_REGEX, 1).cast("int"))
               .otherwise(F.regexp_extract("search_text", BATT_HEALTH_REGEX, 2).cast("int"))
          )
          .withColumn("battery_cycles", F.regexp_extract("search_text", BATT_CYCLE_REGEX, 1).cast("int"))
          .withColumn("model_number", F.regexp_extract("search_text", AMODEL_REGEX, 0))
          .withColumn("model_identifier", F.regexp_extract("search_text", MODEL_ID_REGEX, 0))
          .withColumn(
              "sold_flag",
              F.when(F.coalesce(F.col("title_norm"), F.col("desc_norm")).rlike("已售|售出|賣掉|完售|收回|結案|sold|已在\\d+分鐘前.*取走"), F.lit(True))
               .when(F.coalesce(F.col("title_norm"), F.col("desc_norm")).rlike("徵求|收購"), F.lit(None).cast("boolean"))
               .otherwise(F.lit(False))
          )
    )
    # normalized color and storage
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

def compose_price(df: DataFrame) -> DataFrame:
    """依原先語意優先順序組合價格 + 萬/k fallback + 最大候選保險 + WTB 過濾。"""
    df3 = df.withColumn(
        "price_twd_raw",
        F.coalesce(
            _nullif_empty("price_pfx_last"),
            _nullif_empty("price_kw"),
            _nullif_empty("price_pfx"),
            _nullif_empty("price_kw_low"),
            _nullif_empty("price_4suffix"),
            _nullif_empty("price_3suffix"),
            _nullif_empty("price_num"),
            _nullif_empty("price_pfx_last")
        )
    ).withColumn("price_twd_raw", F.regexp_replace(F.col("price_twd_raw"), ",", ""))

    df3 = df3.withColumn(
        "price_candidates_max",
        F.greatest(
            _safe_int("price_pfx_last"), _safe_int("price_kw"), _safe_int("price_pfx"), _safe_int("price_kw_low"),
            _safe_int("price_4suffix"), _safe_int("price_3suffix"),
            _safe_int("price_num"), _safe_int("price_pfx_last")
        )
    )

    df3 = df3.withColumn(
        "price_twd_fallback",
        F.when(F.col("price_twd_raw").isNull() | (F.col("price_twd_raw") == ""),
            F.when(F.col("price_wan_raw") != "",
                F.when(F.lower(F.col("price_wan_unit")).isin("萬","w"),
                       (F.col("price_wan_raw").cast("double") * F.lit(10000)).cast("long"))
                 .otherwise((F.col("price_wan_raw").cast("double") * F.lit(1000)).cast("long"))
            ).otherwise(F.col("price_num").cast("long"))
        )
    )

    df3 = df3.withColumn(
        "price_twd",
        F.coalesce(
            F.col("price_twd_raw").cast("long"),
            F.col("price_candidates_max"),
            F.col("price_twd_fallback").cast("long")
        )
    )

    df3 = df3.withColumn(
        "price_twd",
        F.when(
            F.coalesce(F.col("title_norm"), F.col("desc_norm")).rlike("徵求|我想要買|希望價格"),
            F.lit(None).cast("long")
        ).otherwise(F.col("price_twd"))
    )
    return df3

def derive_misc(df: DataFrame) -> DataFrame:
    """電池 bucket / 設計循環目標 / 健康提示 等衍生欄位。"""
    return (
        df
        .withColumn(
            "battery_health_bucket",
            F.when(F.col("battery_health_pct") >= 90, F.lit(">=90"))
             .when((F.col("battery_health_pct") >= 80) & (F.col("battery_health_pct") < 90), F.lit("80-89"))
             .when(F.col("battery_health_pct") < 80, F.lit("<80"))
        )
        .withColumn(
            "design_cycle_target",
            F.when(
                F.coalesce(F.col("title"), F.col("description"))
                .rlike(r"(?i)\biphone\s*15(\s*pro(\s*max)?|\s*plus)?\b"),
                F.lit(1000)
            ).otherwise(F.lit(500))
        )
        .withColumn(
            "health_status_hint",
            F.when(F.col("battery_health_pct") < 80,
                   F.lit("建議更換（官方：14及前500循環/15及後1000循環≈80%）"))
             .otherwise(F.lit(None))
        )
    )

def transform_articles(df_src: DataFrame, repartition_target: Optional[int] = None) -> DataFrame:
    """封裝整體轉換邏輯，返回最終要寫回 PG 的 DataFrame。"""
    df = normalize_and_focus(df_src)
    df = attach_price_focus(df)
    df = extract_product_fields(df)
    df = compose_price(df)
    df = derive_misc(df)

    # drop temp helper columns
    df = df.drop(
        "text_nocomma","color_raw","storage_raw",
        "price_kw_low","price_4suffix","price_3suffix","price_pfx_last","price_candidates_max",
        "price_text","price_sec_line","price_sec_next","price_focus","price_focus_nocomma"
    )

    # partitions
    if repartition_target and isinstance(repartition_target, int) and repartition_target > 0:
        df_out = df.repartition(repartition_target)
    else:
        df_out = df.coalesce(4)

    write_cols = [
        "id","title","author","created_date","link","description","description_hash","updated_date",
        "product_category","size_inch","ram_gb","price_twd","battery_health_pct","battery_cycles",
        "model_number","model_identifier","sold_flag","color","storage_gb",
        "battery_health_bucket","design_cycle_target","health_status_hint"
    ]
    return df_out.select(*[c for c in write_cols if c in df_out.columns])

# =========================
# JDBC Write + Swap
# =========================

def recreate_temp_from_df_schema(spark: SparkSession, df_out: DataFrame, cfg: DBConfig):
    """Use JDBC via JVM to create an UNLOGGED temp table with explicit schema (text-friendly)."""
    DriverManager = spark._jvm.java.sql.DriverManager
    conn = DriverManager.getConnection(cfg.jdbc_url, cfg.user, cfg.password)
    conn.setAutoCommit(True)
    stmt = conn.createStatement()
    try:
        stmt.execute(f'DROP TABLE IF EXISTS public."{cfg.temp_table}";')
        create_sql = f"""
        CREATE UNLOGGED TABLE public."{cfg.temp_table}" (
          id INTEGER,
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
          battery_health_pct INTEGER,
          battery_cycles INTEGER,
          model_number TEXT,
          model_identifier TEXT,
          sold_flag BOOLEAN,
          color TEXT,
          storage_gb INTEGER,
          battery_health_bucket TEXT,
          design_cycle_target INTEGER,
          health_status_hint TEXT
        );
        """
        stmt.execute(create_sql)
    finally:
        stmt.close()
        conn.close()

def write_sample_append(df_out: DataFrame, cfg: DBConfig):
    """Write 1 row as early append test (type errors will surface fast)."""
    (
        df_out.limit(1).write.format("jdbc")
        .option("url", cfg.jdbc_url)
        .option("dbtable", f'public."{cfg.temp_table}"')
        .option("user", cfg.user).option("password", cfg.password).option("driver", cfg.driver)
        .option("batchsize", "1")
        .mode("append")
        .save()
    )

def write_temp_all(df_out: DataFrame, cfg: DBConfig, tuning: SparkWriteTuning):
    """Append full dataset into temp table with tuned JDBC options."""
    (
        df_out.write.format("jdbc")
        .option("url", cfg.jdbc_url)
        .option("dbtable", f'public."{cfg.temp_table}"')
        .option("user", cfg.user).option("password", cfg.password).option("driver", cfg.driver)
        .option("batchsize", tuning.write_batchsize)
        .option("isolationLevel", tuning.write_isolation)
        .option("stringtype", "unspecified")
        .option("truncate", "false")
        .mode("append")
        .save()
    )

def swap_temp_to_final(spark: SparkSession, cfg: DBConfig):
    """Atomic rename swap within a single transaction; analyze; drop old; (optional grants)."""
    DriverManager = spark._jvm.java.sql.DriverManager
    conn = None; stmt = None
    try:
        conn = DriverManager.getConnection(cfg.jdbc_url, cfg.user, cfg.password)
        conn.setAutoCommit(False)
        stmt = conn.createStatement()

        stmt.execute(f'DROP TABLE IF EXISTS public."{cfg.dest_table}_old";')
        stmt.execute(f'ALTER TABLE IF EXISTS public."{cfg.dest_table}" RENAME TO "{cfg.dest_table}_old";')
        stmt.execute(f'ALTER TABLE public."{cfg.temp_table}" RENAME TO "{cfg.dest_table}";')
        stmt.execute(f'ANALYZE public."{cfg.dest_table}";')
        # stmt.execute(f'GRANT SELECT ON public."{cfg.dest_table}" TO readonly;')
        stmt.execute(f'DROP TABLE IF EXISTS public."{cfg.dest_table}_old";')

        conn.commit()
        print(f'🎉 已原子替換：public."{cfg.dest_table}"')
    except Exception:
        if conn is not None:
            conn.rollback()
        raise
    finally:
        if stmt is not None: stmt.close()
        if conn is not None: conn.close()

# =========================
# Main
# =========================

def main():
    cfg = DBConfig()
    tuning = SparkWriteTuning()
    spark = get_spark("PG→Spark→PG mirror via _temp swap (refactor)", tuning.spark_timezone)

    bounds = guard("取得來源 id 範圍", lambda: get_id_bounds(spark, cfg))
    if bounds is None:
        print("⚠️ 來源表為空，結束")
        spark.stop(); return

    lower, upper = bounds
    print(f"📌 來源資料範圍：id {lower} ~ {upper}")

    num_parts = compute_num_partitions(spark, tuning.read_num_parts_max)
    print(f"⚙️ JDBC 讀取分區數：{num_parts}")

    df_src = guard("JDBC 分區讀取來源表",
                   lambda: read_src_jdbc(spark, cfg, tuning, lower, upper, num_parts))
    guard("來源表抽樣 count()", lambda: df_src.limit(1).count())

    df_out = guard("套用欄位抽取/正規化轉換",
                   lambda: transform_articles(df_src, tuning.repartition_target))

    # 控制回寫併發
    df_out = df_out.coalesce(tuning.write_coalesce)
    print("📐 df_out partitions:", df_out.rdd.getNumPartitions())

    guard("用 df_out schema 重建 _temp", lambda: recreate_temp_from_df_schema(spark, df_out, cfg))
    guard("小樣本 append 測試（1 筆）", lambda: write_sample_append(df_out, cfg))

    spark.sparkContext.setLogLevel("INFO")
    try:
        ui = spark.sparkContext.uiWebUrl
        print("Spark UI:", ui if ui else "(UI disabled)")
    except Exception as e:
        print("Spark UI unavailable:", str(e))

    guard("寫入 _temp（JDBC 批次）", lambda: write_temp_all(df_out, cfg, tuning))
    print(f"✅ 已寫入：public.\"{cfg.temp_table}\"")

    guard("交換 _temp → 正式表（交易內 rename swap）", lambda: swap_temp_to_final(spark, cfg))

    print("🎯 全流程完成：Postgres →（分區讀）→ Spark（轉換）→（批次寫）→ Postgres（_temp→swap）")
    spark.stop()

if __name__ == "__main__":
    main()
