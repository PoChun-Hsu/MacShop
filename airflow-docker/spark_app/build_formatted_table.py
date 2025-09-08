# -*- coding: utf-8 -*-
# pg_to_pg_mirror_with_transforms.py
from pyspark.sql import SparkSession, functions as F
from datetime import datetime
import traceback

# ===== 連線與資料表 =====
JDBC_URL = (
    "jdbc:postgresql://postgres:5432/airflow"
    "?currentSchema=public"
    "&reWriteBatchedInserts=true"   # 批次合併為 multi-values insert
)
SRC_TABLE  = "public.ptt_macshop_articles"                 # 明確帶 schema
DEST_TABLE = "ptt_macshop_articles_product_detail"         # 目標正式表（不帶 schema）
TEMP_TABLE = f"{DEST_TABLE}_temp"                          # 臨時表名（不帶 schema）

DB_USER = "airflow"
DB_PASS = "airflow"
PG_DRIVER = "org.postgresql.Driver"

# ===== 參數（依 DB/資源微調）=====
READ_NUM_PARTS_MAX = 16           # JDBC 讀取最多分區數
READ_FETCHSIZE     = "10000"      # JDBC fetch size（Postgres 有效）
WRITE_COALESCE     = 8            # 控制回寫連線數（= 分區數）
WRITE_BATCHSIZE    = "5000"       # JDBC batch size（單次 roundtrip 筆數）
WRITE_ISOLATION    = "READ_COMMITTED"  # JDBC 交易隔離等級
SPARK_TZ           = "Asia/Taipei"     # 時區一致性
REPARTITION_TARGET = None         # 若想強制重分區數量，可設整數；None 代表用預設

# ===== SparkSession =====
spark = (
    SparkSession.builder
    .appName("PG→Spark→PG mirror via _temp swap (with transforms)")
    .config("spark.sql.session.timeZone", SPARK_TZ)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

def guard(msg, action):
    try:
        print(f"▶ {msg} ...")
        out = action()
        print(f"✅ {msg} 完成")
        return out
    except Exception:
        print(f"❌ {msg} 失敗，堆疊：")
        traceback.print_exc()
        raise

# ===== 取得 min/max id，決定 JDBC 讀取分區 =====
bounds = (
    spark.read.format("jdbc")
    .option("url", JDBC_URL)
    .option("dbtable", f"(SELECT MIN(id) AS min_id, MAX(id) AS max_id FROM {SRC_TABLE}) t")
    .option("user", DB_USER).option("password", DB_PASS).option("driver", PG_DRIVER)
    .load()
).first()

if bounds is None or bounds["min_id"] is None or bounds["max_id"] is None:
    print("⚠️ 來源表為空，結束")
    spark.stop(); raise SystemExit(0)

min_id, max_id = int(bounds["min_id"]), int(bounds["max_id"])
print(f"📌 來源資料範圍：id {min_id} ~ {max_id}")

read_num_parts = min(max(spark.sparkContext.defaultParallelism, 8), READ_NUM_PARTS_MAX)
print(f"⚙️ JDBC 讀取分區數：{read_num_parts}")

# ===== 讀：分區並行 + fetchsize =====
df_src = guard(
    "JDBC 分區讀取來源表",
    lambda: (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", SRC_TABLE)
        .option("user", DB_USER).option("password", DB_PASS).option("driver", PG_DRIVER)
        .option("fetchsize", READ_FETCHSIZE)
        .option("partitionColumn", "id")
        .option("lowerBound", str(min_id))
        .option("upperBound", str(max_id))
        .option("numPartitions", str(read_num_parts))
        .load()
    )
)
guard("來源表抽樣 count()", lambda: df_src.limit(1).count())

# ===== 轉換：把你舊版的抽取/正規化流程封裝成函式 =====
def transform_articles(df):
    """
    直接把你『原本的 code』裡「產品分類/欄位抽取 + 正規化」那一長段
    （從 df_clean / df1 / df2 / df3 / parsed_df 那段）原封不動貼進來。
    唯一差異：最後請 return 你要寫回 PG 的 DataFrame（等同於原本的 parquet_df/parsed_df）
    並且只保留要寫回的欄位（write_cols）。"""
    
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
    # df_to_write = parsed_df
    df_to_write = parsed_df.coalesce(TARGET_WRITE_PARTS)
    if REPARTITION_TARGET and isinstance(REPARTITION_TARGET, int) and REPARTITION_TARGET > 0:
        df_to_write = parsed_df.repartition(REPARTITION_TARGET)


    write_cols = [
        "id","title","author","created_date","link","description","description_hash","updated_date",
        "product_category","size_inch","ram_gb","price_twd","battery_health_pct","battery_cycles",
        "model_number","model_identifier","sold_flag","color","storage_gb",
        "battery_health_bucket","design_cycle_target","health_status_hint"
    ]
    # 假設 df 已完成所有正規化，這裡僅選欄位（實務請貼完整轉換）
    df_out = df_to_write.select(*[c for c in write_cols if c in df_to_write.columns])
    return df_out

df_out = guard("套用欄位抽取/正規化轉換", lambda: transform_articles(df_src))

# 控制回寫併發（避免一次開太多連線到 PG）
df_out = df_out.coalesce(WRITE_COALESCE)

print("📐 df_out partitions:", df_out.rdd.getNumPartitions())
df_out.printSchema()
df_out.show(5, truncate=False)

# ===== 用 df_out 的 schema 重建 _temp（確保欄位 1:1 對齊）=====
def recreate_temp_from_df_schema(df):
    # 先把舊的 _temp 丟掉，避免舊版 8 欄殘留
    DriverManager = spark._jvm.java.sql.DriverManager
    conn = None; stmt = None
    try:
        conn = DriverManager.getConnection(JDBC_URL, DB_USER, DB_PASS)
        conn.setAutoCommit(True)
        stmt = conn.createStatement()
        stmt.execute(f'DROP TABLE IF EXISTS public."{TEMP_TABLE}";')
    finally:
        if stmt is not None: stmt.close()
        if conn is not None: conn.close()

    # 用 0 筆資料把表「按 df_out 的 schema」建立起來
    (df.limit(0)
       .write.format("jdbc")
       .option("url", JDBC_URL)
       .option("dbtable", f'public."{TEMP_TABLE}"')
       .option("user", DB_USER).option("password", DB_PASS).option("driver", PG_DRIVER)
       .mode("overwrite")    # 建表用
       .save())

guard("用 df_out schema 重建 _temp", lambda: recreate_temp_from_df_schema(df_out))

# ===== 先用 append 寫 1 筆驗證「插入」路徑能不能通 =====
guard("小樣本 append 測試（1 筆）", lambda: (
    df_out.limit(1).write.format("jdbc")
    .option("url", JDBC_URL)
    .option("dbtable", f'public."{TEMP_TABLE}"')
    .option("user", DB_USER).option("password", DB_PASS).option("driver", PG_DRIVER)
    .option("batchsize", "1")   # 刻意設小，遇到型別錯誤會立刻拋
    .mode("append")
    .save()
))

spark.sparkContext.setLogLevel("INFO")
# 進 UI 看 active stage / task
try:
    ui = spark.sparkContext.uiWebUrl
    print("Spark UI:", ui if ui else "(UI disabled)")
except Exception as e:
    print("Spark UI unavailable:", str(e))


# ===== 寫：覆寫 _temp（存在就 TRUNCATE，不存在才 DROP+CREATE）=====
jdbc_writer = (
    df_out.write.format("jdbc")
    .option("url", JDBC_URL)
    .option("dbtable", f'public."{TEMP_TABLE}"')
    .option("user", DB_USER).option("password", DB_PASS).option("driver", PG_DRIVER)
    .option("batchsize", WRITE_BATCHSIZE)
    .option("isolationLevel", WRITE_ISOLATION)
)
guard("寫入 _temp（JDBC 批次）", lambda: jdbc_writer.mode("append").save())
print(f"✅ 已寫入：public.\"{TEMP_TABLE}\"")

# ===== swap：同一交易內 rename 成正式表，並 ANALYZE／可選 GRANT =====
def swap_temp_to_final():
    DriverManager = spark._jvm.java.sql.DriverManager
    conn = None; stmt = None
    try:
        conn = DriverManager.getConnection(JDBC_URL, DB_USER, DB_PASS)
        conn.setAutoCommit(False)
        stmt = conn.createStatement()

        # 先把舊正式表改名為 _old（若存在），再把 _temp 改名為正式表，最後丟棄 _old
        # 這樣整個替換是瞬時的，且正式表短暫不存在的時間 = 0
        stmt.execute(f'DROP TABLE IF EXISTS public."{DEST_TABLE}_old";')
        stmt.execute(f'ALTER TABLE IF EXISTS public."{DEST_TABLE}" RENAME TO "{DEST_TABLE}_old";')
        stmt.execute(f'ALTER TABLE public."{TEMP_TABLE}" RENAME TO "{DEST_TABLE}";')

        # 更新統計資訊，讓查詢規劃器立即有正確統計
        stmt.execute(f'ANALYZE public."{DEST_TABLE}";')

        # ✅ 若需要對外開權限，把 GRANT 放這（依你的需求調整）
        # stmt.execute(f'GRANT SELECT ON public."{DEST_TABLE}" TO readonly;')

        # 丟掉舊表
        stmt.execute(f'DROP TABLE IF EXISTS public."{DEST_TABLE}_old";')

        conn.commit()
        print(f'🎉 已原子替換：public."{DEST_TABLE}"')
    except Exception:
        if conn is not None: conn.rollback()
        raise
    finally:
        if stmt is not None: stmt.close()
        if conn is not None: conn.close()

guard("交換 _temp → 正式表（交易內 rename swap）", swap_temp_to_final)

print("🎯 全流程完成：Postgres →（分區讀）→ Spark（轉換）→（批次寫）→ Postgres（_temp→swap）")
spark.stop()
