# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType
from datetime import datetime
import math
import traceback

# ========= 使用者可調參數 =========
JDBC_URL = "jdbc:postgresql://postgres:5432/airflow?currentSchema=public"  # 加上 schema
SRC_TABLE = "ptt_macshop_articles"
DEST_TABLE = "ptt_macshop_articles_product_detail"  # 回寫目標表（鏡像表）
DB_PROPS = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver",
    "fetchsize": "10000",          # JDBC 流式抓取大小（Postgres 端生效）
}
PARQUET_COMPRESSION = "snappy"
PARQUET_BASE_DIR = "file:///opt/spark-output"
REPARTITION_TARGET = None         # 若想強制重分區數量，可設整數；None 代表用預設
JDBC_WRITE_BATCHSIZE = "5000"     # 回寫 Postgres 的批次大小
# =================================

# ========= Spark =========
spark = (
    SparkSession.builder
    .appName("PTT Export Optimized")
    # ↓↓↓↓↓ 這幾行是「關鍵」↓↓↓↓↓
    .config("spark.executor.cores", "1")          # 每個 Executor 只跑 1 個 task，避免單機併發吃爆
    .config("spark.cores.max", "2")               # 全 job 最多用 2 個 core（可視情況調到 3、4）
    .config("spark.executor.memory", "3g")        # Executor 堆內存（依你的主機情況）
    .config("spark.executor.memoryOverhead", "1g")# 堆外 + 序列化/Shuffle 的緩衝
    .config("spark.driver.memory", "3g")          # Driver 不用太大，3g 夠用
    # ↑↑↑↑↑ 這幾行是「關鍵」↑↑↑↑↑

    .config("spark.sql.shuffle.partitions", "32") # 降低 shuffle 併發，不要 200 那麼高
    .config("spark.sql.files.maxPartitionBytes", 64 * 1024 * 1024)  # 64MB，讓單批更小
    .config("spark.sql.parquet.compression.codec", PARQUET_COMPRESSION)
    .config("spark.jars", "/opt/spark/jars/postgresql-42.7.3.jar")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

def guard(msg, action):
    """小型診斷包：執行 action；失敗時印堆疊並拋出。"""
    try:
        print(f"▶ {msg} ...")
        out = action()
        print(f"✅ {msg} 完成")
        return out
    except Exception:
        print(f"❌ {msg} 失敗，堆疊：")
        traceback.print_exc()
        raise

# ========= 路徑 =========
run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
output_dir = f"{PARQUET_BASE_DIR}/{run_id}"
print(f"📂 輸出 Parquet 路徑：{output_dir}")

# ========= 先拿 min/max id =========
min_id_df = (spark.read.format("jdbc")
    .option("url", JDBC_URL)
    .option("dbtable", f"(SELECT MIN(id) AS min_id FROM {SRC_TABLE}) AS t")
    .option("user", DB_PROPS["user"])
    .option("password", DB_PROPS["password"])
    .option("driver", DB_PROPS["driver"])
    .load()
)

max_id_df = (spark.read.format("jdbc")
    .option("url", JDBC_URL)
    .option("dbtable", f"(SELECT MAX(id) AS max_id FROM {SRC_TABLE}) AS t")
    .option("user", DB_PROPS["user"])
    .option("password", DB_PROPS["password"])
    .option("driver", DB_PROPS["driver"])
    .load()
)

def get_scalar(df, col):
    row = df.select(F.col(col)).limit(1).collect()
    return None if not row else row[0][0]

min_id = get_scalar(min_id_df, "min_id")
max_id = get_scalar(max_id_df, "max_id")

if min_id is None or max_id is None:
    print("⚠️ 資料表為空，無需處理")
    spark.stop()
    raise SystemExit(0)

print(f"📌 資料範圍：ID {min_id} ~ {max_id}")

# ========= 估計並行度 =========
default_parallelism = spark.sparkContext.defaultParallelism
num_partitions = max(default_parallelism, 8)
print(f"⚙️ JDBC 讀取分區數：{num_partitions}")

# ========= 分區並行 JDBC 讀取（加診斷）=========
df = guard(
    "JDBC 分區讀取",
    lambda: (spark.read
             .format("jdbc")
             .option("url", JDBC_URL)
             .option("dbtable", SRC_TABLE)
             .option("user", DB_PROPS["user"])
             .option("password", DB_PROPS["password"])
             .option("driver", DB_PROPS["driver"])
             .option("fetchsize", DB_PROPS["fetchsize"])
             .option("partitionColumn", "id")
             .option("lowerBound", str(min_id))
             .option("upperBound", str(max_id))
             .option("numPartitions", str(num_partitions))
             .load())
)
guard("JDBC 讀回驗證 count()", lambda: df.limit(1).count())

# ========= 產品分類 / 欄位抽取（與你原版一致，小修 storage_raw 的 lower） =========
fullwidth_digits = "０１２３４５６７８９"
halfwidth_digits = "0123456789"

df_clean = (
    df
    .withColumn("title_norm",
        F.regexp_replace(
            F.translate(F.col("title"), fullwidth_digits, halfwidth_digits),
            u"[\u00A0\u3000]", " "
        )
    )
    .withColumn("desc_norm",
        F.regexp_replace(
            F.translate(F.col("description"), fullwidth_digits, halfwidth_digits),
            u"[\u00A0\u3000]", " "
        )
    )
    .withColumn("text", F.concat_ws(" ", F.col("title_norm"), F.col("desc_norm")))
    .withColumn("text_nocomma", F.regexp_replace(F.col("text"), ",", ""))
)

model_to_spec_regex = r"(?s)\[型號\]\s*(.*?)\n\s*\[規格\]"

df_clean = (
    df_clean
    .withColumn(
        "primary_product_info",
        F.trim(
            F.regexp_replace(
                F.regexp_extract(F.col("desc_norm"), model_to_spec_regex, 1),
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

color_regex   = r"(太空灰|銀|金|玫瑰金|黑|白|藍|綠|紫|黃|紅|午夜|星光|原鈦|原色鈦(?:金屬)?|Graphite|Silver|Gold|Rose Gold|Space Gray|Midnight|Starlight|Blue|Green|Purple|Yellow|(PRODUCT)\s*RED)"
size_regex    = r"(?<![\dA-Za-z])((?:10|11|12|13|14|15|16|17|18|19|20|21|22|23|24)\.?\d{0,1})\s*(?:吋|\"|-inch)"
storage_regex = r"(?i)\b(64|128|256|512)\s*(?:gb|g)\b|\b(1|2)\s*tb\b"
ram_regex     = r"(?i)\b(8|16|24|32|64)\s*gb\s*(?:ram|記憶體)?\b"

price_regex_pfx = r"(?:NT\$|NTD|\$|台幣|TWD)\s*([0-9][0-9,]{3,})"
price_regex_kw  = r"(?:售價|價格|價位)[^\d]{0,80}([1-9]\d{3,6})"
price_regex_num = r"(?<!\d)([1-9]\d{4,6})(?!\d)"
price_regex_wan = r"(?i)([1-9]\d?(?:\.\d)?)\s*(萬|w|k)"

batt_health_regex = r"(?:電池(?:健康)?(?:度)?|健康度|電池健康)[^\d%]{0,10}(\d{2,3})\s*%|\bBH\s*(\d{2,3})\s*%"
batt_cycle_regex  = r"(\d{1,4})\s*(?:循環|cycles?|次)"
amodel_regex      = r"\bA\d{4}\b"
model_id_regex    = r"\b(?:iMac|MacBookPro|MacBookAir|Macmini|MacStudio|MacPro)\d{1,2},\d\b"

iphone_regex = r"""(?ix)\bi\s*phone\s*(?:\d{1,2})?\s*(?:pro\s*max|pro|plus|mini)?\b"""

product_map = [
    ("iPhone", iphone_regex),
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

from functools import reduce
cond = F.when(F.col("search_text").rlike(iphone_regex), F.lit("iPhone"))
for name, pat in product_map:
    cond = cond.when(F.col("search_text").rlike(pat), F.lit(name))

df1 = (
    df_clean.withColumn("product_category", cond.otherwise(F.lit(None)))
    .withColumn("color_raw", F.regexp_extract("focus_text", color_regex, 1))
    .withColumn("size_inch", F.regexp_extract("focus_text", size_regex, 1).cast("double"))
    .withColumn("storage_raw", F.regexp_extract("focus_text", storage_regex, 0))
    .withColumn("ram_gb", F.regexp_extract("focus_text", ram_regex, 1).cast("int"))
    .withColumn("price_pfx", F.regexp_extract("search_text", price_regex_pfx, 1))
    .withColumn("price_kw", F.regexp_extract(F.regexp_replace("search_text", ",", ""), price_regex_kw, 1))
    .withColumn("price_num", F.regexp_extract(F.regexp_replace("search_text", ",", ""), price_regex_num, 1))
    .withColumn("price_wan_raw",  F.regexp_extract(F.regexp_replace("search_text", ",", ""), price_regex_wan, 1))
    .withColumn("price_wan_unit", F.regexp_extract(F.regexp_replace("search_text", ",", ""), price_regex_wan, 2))
    .withColumn(
        "battery_health_pct",
        F.when(F.regexp_extract("search_text", batt_health_regex, 1) != "", F.regexp_extract("search_text", batt_health_regex, 1).cast("int"))
         .otherwise(F.regexp_extract("search_text", batt_health_regex, 2).cast("int"))
    )
    .withColumn("battery_cycles", F.regexp_extract("search_text", batt_cycle_regex, 1).cast("int"))
    .withColumn("model_number", F.regexp_extract("search_text", amodel_regex, 0))
    .withColumn("model_identifier", F.regexp_extract("search_text", model_id_regex, 0))
    .withColumn(
        "sold_flag",
        F.when(F.coalesce(F.col("title_norm"), F.col("desc_norm")).rlike("已售|售出|賣掉|完售|收回|結案|sold|已在\\d+分鐘前.*取走"), F.lit(True))
         .when(F.coalesce(F.col("title_norm"), F.col("desc_norm")).rlike("徵求|收購"), F.lit(None).cast("boolean"))
         .otherwise(F.lit(False))
    )
)

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

def nullif_empty(colname: str):
    return F.when((F.col(colname).isNull()) | (F.col(colname) == ""), F.lit(None)).otherwise(F.col(colname))

df3 = df2.withColumn(
    "price_twd_raw",
    F.coalesce(
        nullif_empty("price_pfx"),
        nullif_empty("price_kw"),
        nullif_empty("price_num")
    )
).withColumn(
    "price_twd_raw", F.regexp_replace(F.col("price_twd_raw"), ",", "")
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
    F.coalesce(F.col("price_twd_raw").cast("long"), F.col("price_twd_fallback").cast("long"))
)

df3 = df3.withColumn(
    "battery_health_bucket",
    F.when(F.col("battery_health_pct") >= 90, F.lit(">=90"))
     .when((F.col("battery_health_pct") >= 80) & (F.col("battery_health_pct") < 90), F.lit("80-89"))
     .when(F.col("battery_health_pct") < 80, F.lit("<80"))
).withColumn(
    "design_cycle_target",
    F.when(
        F.coalesce(F.col("title"), F.col("description"))
         .rlike(r"(?i)\biphone\s*15(\s*pro(\s*max)?|\s*plus)?\b"),
        F.lit(1000)
    ).otherwise(F.lit(500))
).withColumn(
    "health_status_hint",
    F.when(F.col("battery_health_pct") < 80, F.lit("建議更換（官方：14及前500循環/15及後1000循環≈80%）")).otherwise(F.lit(None))
)

parsed_df = df3.drop(
    "text_nocomma","color_raw","storage_raw",
    "price_pfx","price_kw","price_num","price_wan_raw","price_wan_unit","price_twd_raw","price_twd_fallback"
)

# ========= 寫出 Parquet =========
# 明確控制分區數與單檔紀錄數，避免單 task/單檔太大
TARGET_WRITE_PARTS = 4  # 先用 4（不夠再降到 2）
df_to_write = parsed_df
if REPARTITION_TARGET and isinstance(REPARTITION_TARGET, int) and REPARTITION_TARGET > 0:
    df_to_write = parsed_df.repartition(REPARTITION_TARGET)

guard(
    "寫出 Parquet",
    lambda: (
        df_to_write.write
        .mode("overwrite")
        .option("compression", PARQUET_COMPRESSION)
        .option("parquet.block.size", 32 * 1024 * 1024)   # 32MB block
        .option("parquet.page.size",  1 * 1024 * 1024)    # 1MB page
        .option("maxRecordsPerFile",  200000)             # 限制單檔記錄數，避免大檔
        .parquet(output_dir)
    )
)
print("✅ 已寫出 Parquet（多檔、snappy 壓縮）")

# ========= 從 Parquet 讀回（保護 select 欄位）=========
parquet_df = guard("讀回 Parquet", lambda: spark.read.parquet(output_dir))

write_cols = [
    "id","title","author","created_date","link","description","description_hash","updated_date",
    "product_category","size_inch","ram_gb","price_twd","battery_health_pct","battery_cycles",
    "model_number","model_identifier","sold_flag","color","storage_gb",
    "battery_health_bucket","design_cycle_target","health_status_hint"
]
existing = parquet_df.columns
missing = [c for c in write_cols if c not in existing]
if missing:
    print(f"⚠️ 下列欄位在 DataFrame 中不存在，將跳過：{missing}")
parquet_df = parquet_df.select(*[c for c in write_cols if c in existing])
guard("Parquet 取樣 count()", lambda: parquet_df.limit(1).count())

# ========= 回寫 PostgreSQL（swap table: _temp -> 正式表） =========
TEMP_TABLE = f"{DEST_TABLE}_temp"

# 1) 先把資料寫到 _temp（不存在就建，存在就覆蓋）
jdbc_writer_temp = (
    parquet_df
    .coalesce(4)  # 視 DB 能力調整，避免一次開太多連線
    .write
    .format("jdbc")
    .option("url", JDBC_URL)
    .option("dbtable", TEMP_TABLE)
    .option("user", DB_PROPS["user"])
    .option("password", DB_PROPS["password"])
    .option("driver", DB_PROPS["driver"])
    .option("batchsize", JDBC_WRITE_BATCHSIZE)
    .option("isolationLevel", "READ_COMMITTED")
)

guard("寫入臨時表(_temp)", lambda: jdbc_writer_temp.mode("overwrite").save())
print(f"✅ 已寫入臨時表：{TEMP_TABLE}")

# 1.5) swap 前：確認 _temp 可以被 JDBC 讀到 & 有筆數
tmp_cnt = (spark.read.format("jdbc")
    .option("url", JDBC_URL)
    .option("dbtable", TEMP_TABLE)
    .option("user", DB_PROPS["user"])
    .option("password", DB_PROPS["password"])
    .option("driver", DB_PROPS["driver"])
    .load().limit(1).count())
print(f"🧪 _temp 可讀性檢查（至少 0/1 筆）：{tmp_cnt}")

# 2) 在同一個交易中完成 DROP + RENAME（原子替換）
DriverManager = spark._jvm.java.sql.DriverManager
conn = None
stmt = None
try:
    conn = DriverManager.getConnection(JDBC_URL, DB_PROPS["user"], DB_PROPS["password"])
    conn.setAutoCommit(False)  # 關閉自動提交，確保 drop+rename 原子性
    stmt = conn.createStatement()

    stmt.execute(f'DROP TABLE IF EXISTS "{DEST_TABLE}";')
    stmt.execute(f'ALTER TABLE "{TEMP_TABLE}" RENAME TO "{DEST_TABLE}";')

    conn.commit()
    print(f'✅ 已替換正式表：{DEST_TABLE}')
except Exception:
    print("❌ DROP/RENAME 失敗，回滾")
    traceback.print_exc()
    if conn is not None:
        conn.rollback()
    raise
finally:
    if stmt is not None:
        stmt.close()
    if conn is not None:
        conn.close()

# ========= 可選：統計筆數（注意：count() 會啟動全表掃描）=========
row_cnt = parquet_df.count()
print(f"📊 回寫筆數：{row_cnt}")

print("🎉 全流程完成：Postgres → Parquet → _temp → swap 成正式表")
spark.stop()