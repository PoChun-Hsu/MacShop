# 20260105_001 - PoChun Hsu - [Add]     New Version.
# 20260110_001 - PoChun Hsu - [Add]     Batch size and bulk insert. 
# 20260110_002 - PoChun Hsu - [Add]     Add partition to increase connection and decrease insert quantity.

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    regexp_extract,
    regexp_replace,
    lower,
    when,
    lit,
    trim,
    length,
)
from pyspark.sql.types import IntegerType, BooleanType

# =========================================================
# Logging & Guard
# =========================================================
import logging

logging.basicConfig(
    level=logging.WARNING,  # silence root / spark logs
    format="%(asctime)s [%(levelname)s] %(message)s",
)

logging.getLogger("py4j").setLevel(logging.ERROR) # 關掉 py4j error message

logger = logging.getLogger("ptt-macshop")
logger.setLevel(logging.INFO)


def guard(msg: str, fn, *, action: bool = False):
    """
    Run fn() with structured logging.
    If action=True and fn returns DataFrame, trigger Spark action early.
    """
    logger.info(f"▶ [STEP] START {msg}")
    try:
        result = fn()

        if action and isinstance(result, DataFrame):
            logger.info(f"[STEP] Trigger Spark action for: {msg}")
            result.count()

        logger.info(f"✅ [STEP] SUCCESS {msg}")
        return result
    except Exception:
        logger.exception(f"❌ [STEP] FAILED {msg}")
        raise


# =========================================================
# Spark & JDBC Config
# =========================================================
spark = (
    SparkSession.builder
    .appName("PTT_Macshop_Product_Detail_v1_guard")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN") # 只顯示 WARN等級以上的 log, INFO不顯示

jdbc_url = "jdbc:postgresql://postgres:5432/airflow?currentSchema=public"
src_table = "public.ptt_macshop_articles"
dest_table = "ptt_macshop_articles_product_detail"

# rewriteBatchedInserts把多個 Insert改成一整包 insert
# -- 原本（慢）
# INSERT INTO t VALUES (...);
# INSERT INTO t VALUES (...);
# -- 改寫後（快）
# INSERT INTO t VALUES (...), (...), (...);

# 業界實務一個 batch約 500 ~ 2000
jdbc_props = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver",
    "batchsize": "1000",               # 20260110_001 
    #"rewriteBatchedInserts": "true",   # 20260110_001
}


# =========================================================
# Phase Functions
# =========================================================
def read_source() -> DataFrame:
    return (
        spark.read.jdbc(jdbc_url, src_table, properties=jdbc_props)
        .select("title", "created_date", "link", "description")
    )

# =========================================================
# helper
# =========================================================
# 清除前後空白 + 文字中所有空白與換行
def clean_wording(col_expr):
    return regexp_replace(
        trim(col_expr),
        r"[\s\r\n]+",
        "",
    )

# 判斷是買還是賣
# 標題中有 [販售] or [徵求]，可以快速分類，96%以上資料都可以分出來
# Regular Expression細節
# 0 → 整個被 regex match 到的字串
# 1 → 第一個括號 () 裡面 match 到的內容
# 沒 match → 回傳空字串 ""
def derive_trade_type(df: DataFrame) -> DataFrame:
    title = col("title")

    SELL_PATTERN      = r"^\[(販售|售出|已售|已售出)\]"
    BUY_PATTERN       = r"^\[徵求\]"
    EXCHANGE_PATTERN  = r"^\[交換\]"

    return df.withColumn(
        "trade_type",
        when(title.rlike(SELL_PATTERN), lit("sell"))
        .when(title.rlike(BUY_PATTERN), lit("buy"))
        .when(title.rlike(EXCHANGE_PATTERN), lit("exchange"))
        .otherwise(lit("unknown")),
    )
    # return df.filter(col("trade_type") != "unknown")

# 如果是公告文，警告文，不需要判斷商品內容，直接略過
def apply_announcement_guard(df: DataFrame) -> DataFrame:
    content = col("title")
    ANNOUNCEMENT_PATTERN = (
        r"\[公告\]"
        r"|\[版主\]"
        r"|\[板主\]"
        r"|\[心得\]"
        r"|\[情報\]"
        r"|\[黑名\]"
        r"|黑名單"
        r"|警告"
        r"|\[建議\]"
    )

    return (
        df.withColumn(
            "is_announcement",
            content.rlike(ANNOUNCEMENT_PATTERN)
        )
        .withColumn("is_announcement", col("is_announcement").cast(BooleanType()))
    )


# 一篇文章是否多個商品
# 多個商品需要特殊處理
# 判斷方式：
# 1. 型號中有 1.xxx 2.yyy
# 2. 售價中有 1.xxx 2.yyy
# 3. 內文中$出現超過 2次，這個條件相對強烈，但寧枉勿縱
# 如果是多個商品的情況下，後續都不處理
def apply_multi_product_guard(df: DataFrame) -> DataFrame:
    content = col("description")

    # 擷取區塊文字
    model_block = regexp_extract(
        content,
        r"\[型號\]([\s\S]*?)\[規格\]",
        1,
    )

    price_block = regexp_extract(
        content,
        r"\[售價\]([\s\S]*?)\[交易方式/地點\]",
        1,
    )

    return (
        df.withColumn(
            "is_multi_product",
            when(
                model_block.rlike(r"(?:^|\n)\s*[1-9]\."),
                lit(True),
            )
            .when(
                price_block.rlike(r"(?:^|\n)\s*[1-9]\."),
                lit(True),
            )
            .when(
                length(regexp_replace(price_block, r"[^\$]", "")) >= 2,
                lit(True),
            )
            .otherwise(lit(False)),
        )
        .withColumn("is_multi_product", col("is_multi_product").cast(BooleanType()))
    )

# 從內文特定區快擷取文字
# 目前不跨行，待修正
def extract_sections(df: DataFrame) -> DataFrame:
    content = col("description")
    should_parse_content = ( 
        ~col("is_multi_product") 
        & ~col("is_announcement") 
        & col("trade_type").isin(["sell", "buy"]) 
    )

 
    return (
        df.withColumn(
            "model_raw",
            when(
                should_parse_content,
                clean_wording(
                    regexp_extract(
                        content,
                        r"\[型號\]([\s\S]*?)\[規格\]",
                        1,
                    )
                ),
            ),
        )
        .withColumn(
            "spec_raw",
            when(
                should_parse_content,
                clean_wording(
                    regexp_extract(
                        content,
                        r"\[規格\]([\s\S]*?)\[保固\]",
                        1,
                    )
                ),
            ),
        )
        .withColumn(
            "warranty_raw",
            when(
                should_parse_content,
                clean_wording(
                    regexp_extract(
                        content,
                        r"\[保固\]([\s\S]*?)\[盒裝配件\]",
                        1,
                    )
                ),
            ),
        )
        .withColumn(
            "price_raw",
            when(
                should_parse_content,
                clean_wording(
                    regexp_extract(
                        content,
                        r"\[售價\]([\s\S]*?)\[交易方式/地點\]",
                        1,
                    )
                ),
            ),
        )
    )

# 從擷取的文字探究細節
# 商品類型(product_type) : iPhone, iPad, Mac, Airpods
# 型號數字(model_number) : 15, 16
# 型號細節(model_variant): pro, mini
# 保固(is_warranty_valid)
# 價格(price)
# 容量數字(capacity)     : 256, 512, 1
# 容量單位(capacity_unit): GB, TB
# 顏色(color)
def parse_product_fields(df: DataFrame) -> DataFrame:
    should_parse_content = ( 
        ~col("is_multi_product") 
        & ~col("is_announcement") 
        & col("trade_type").isin(["sell", "buy"]) 
    )
    
    return (
        df.withColumn(
            "product_type",
            when(~should_parse_content, lit(None))
            .when(lower(col("model_raw")).contains("iphone"), "iPhone")
            .when(lower(col("model_raw")).contains("ipad"), "iPad")
            .when(lower(col("model_raw")).contains("airpod"), "AirPods")
            .when(lower(col("model_raw")).contains("mac"), "Mac")
            .when(lower(col("model_raw")).contains("pencil"), "Apple Pencil")
            .when(lower(col("model_raw")).contains("appletv"), "Apple TV")
            .when(lower(col("model_raw")).contains("applewatch"), "Apple Watch")
            .when(lower(col("model_raw")).contains("homepod"), "HomePod")
            .when(lower(col("model_raw")).contains("earpod"), "EarPods")
            .when(lower(col("model_raw")).contains("airtag"), "AirTag")
            .otherwise(None),
        )
        .withColumn(
            "model_number",
            when(should_parse_content,
                 regexp_extract(lower(col("model_raw")),
                                r"(iphone|ipad|mac)\s*([0-9]{1,2})", 2)),
        )
        .withColumn(
            "model_variant",
            when(~should_parse_content, lit(None))
            # AirPods：只抓 pro / max（避免 airpods -> air）
            .when(col("product_type") == "AirPods",
                  regexp_extract(lower(col("model_raw")), r"(pro|max)", 1))
            # 其他：維持你原本的 variant 規則（air 不會影響 airpods 了）
            .otherwise(regexp_extract(lower(col("model_raw")), r"(promax|pro|plus|air|mini)", 1)),
        )
        .withColumn(
            "is_warranty_valid",
            when(~should_parse_content, lit(None))
            .when(col("warranty_raw").contains("過保"), lit(False))
            .when(col("warranty_raw") != "", lit(True))
            .otherwise(lit(None))
            .cast(BooleanType()),
        )
        .withColumn(
            "price",
            when(should_parse_content,
                 regexp_replace(col("price_raw"), r"[^\d]", ""))
            .cast(IntegerType()),
        )
        .withColumn(
            "capacity",
            when(should_parse_content,
                 regexp_extract(col("spec_raw"), r"([0-9]{2,4})\s*(GB|TB)", 1)),
        )
        .withColumn(
            "capacity_unit",
            when(should_parse_content,
                 regexp_extract(col("spec_raw"), r"([0-9]{2,4})\s*(GB|TB)", 2)),
        )
        .withColumn(
            "color",
            when(should_parse_content,
                 regexp_extract(col("spec_raw"),
                                r"(星光|午夜|藍|黑|白|紅|金|銀|紫|綠)", 1)),
        )
    )

# 補足內文的不足
# 當內文不足以判斷時，針對 null資料，嘗試用文章標題分辨
# product_type
# model_number
# capacity
# model_variant
# capacity_unit
# color
def apply_title_fallback(df: DataFrame) -> DataFrame:
    title = col("title")
    should_parse_content = ( 
        ~col("is_multi_product") 
        & ~col("is_announcement") 
        & col("trade_type").isin(["sell", "buy"]) 
    )

    return (
        df.withColumn(
            "product_type",
            when(should_parse_content & col("product_type").isNull()
                 & lower(title).contains("iphone"), "iPhone")
            .when(should_parse_content & col("product_type").isNull()
                  & lower(title).contains("ipad"), "iPad")
            .when(should_parse_content & col("product_type").isNull()
                  & lower(title).contains("airpod"), "AirPods")
            .when(should_parse_content & col("product_type").isNull()
                  & lower(title).contains("mac"), "Mac")
            .when(should_parse_content & col("product_type").isNull()
                  & lower(title).contains("pencil"), "Apple Pencil")
            .when(should_parse_content & col("product_type").isNull()
                  & lower(title).contains("apple tv"), "Apple TV")
            .when(should_parse_content & col("product_type").isNull()
                  & lower(title).contains("apple watch"), "Apple Watch")
            .when(should_parse_content & col("product_type").isNull()
                  & lower(title).contains("homepod"), "HomePod")
            .when(should_parse_content & col("product_type").isNull()
                  & lower(title).contains("earpod"), "EarPods")
            .when(should_parse_content & col("product_type").isNull()
                  & lower(title).contains("airtag"), "AirTag")
            .otherwise(col("product_type")),
        )
        .withColumn(
            "model_number",
            when(should_parse_content & (col("model_number") == ""),
                 regexp_extract(lower(title),
                                r"(iphone|ipad)\s*([0-9]{1,2})", 2))
            .otherwise(col("model_number")),
        )
        .withColumn(
            "model_variant",
            when(should_parse_content & (col("model_variant") == ""),
                 regexp_extract(lower(title),
                                r"(pro max|pro|plus| air |mini)", 1))
            .otherwise(col("model_variant")),
        )
        .withColumn(
            "capacity",
            when(should_parse_content & (col("capacity") == ""),
                 regexp_extract(title, r"([0-9]{2,4})\s*(GB|TB)", 1))
            .otherwise(col("capacity")),
        )
        .withColumn(
            "capacity_unit",
            when(should_parse_content & (col("capacity_unit") == ""),
                 regexp_extract(title, r"([0-9]{2,4})\s*(GB|TB)", 2))
            .otherwise(col("capacity_unit")),
        )
        .withColumn(
            "color",
            when(should_parse_content & (col("color") == ""),
                 regexp_extract(title,
                                r"(星光|午夜|藍|黑|白|紅|金|銀|紫|綠)", 1))
            .otherwise(col("color")),
        )
    )

from pyspark import TaskContext
import psycopg2
from psycopg2.extras import execute_batch


def write_partition_to_pg(rows):
    conn = None

    # === Spark Task Context（關鍵）===
    ctx = TaskContext.get()
    partition_id = ctx.partitionId() if ctx else -1
    attempt_id = ctx.attemptNumber() if ctx else -1

    print(
        f"[WRITE][EXECUTOR] START partition={partition_id}, attempt={attempt_id}",
        flush=True
    )

    try:
        conn = psycopg2.connect(
            host="postgres",
            port=5432,
            dbname="airflow",
            user="airflow",
            password="airflow",
            connect_timeout=10,
        )
        conn.autocommit = False
        cur = conn.cursor()

        sql = """
        INSERT INTO ptt_macshop_articles_product_detail (
            title, created_date, link, description,
            trade_type, is_multi_product, is_announcement,
            product_type, model_number, model_variant,
            capacity, capacity_unit, color,
            price, is_warranty_valid
        )
        VALUES (
            %(title)s, %(created_date)s, %(link)s, %(description)s,
            %(trade_type)s, %(is_multi_product)s, %(is_announcement)s,
            %(product_type)s, %(model_number)s, %(model_variant)s,
            %(capacity)s, %(capacity_unit)s, %(color)s,
            %(price)s, %(is_warranty_valid)s
        )
        ON CONFLICT (link) DO UPDATE SET
            title             = EXCLUDED.title,
            created_date      = EXCLUDED.created_date,
            description       = EXCLUDED.description,
            trade_type        = EXCLUDED.trade_type,
            is_multi_product  = EXCLUDED.is_multi_product,
            is_announcement   = EXCLUDED.is_announcement,
            product_type      = EXCLUDED.product_type,
            model_number      = EXCLUDED.model_number,
            model_variant     = EXCLUDED.model_variant,
            capacity          = EXCLUDED.capacity,
            capacity_unit     = EXCLUDED.capacity_unit,
            color             = EXCLUDED.color,
            price             = EXCLUDED.price,
            is_warranty_valid = EXCLUDED.is_warranty_valid
        """


        batch = []
        count = 0
        BATCH_SIZE = 1000

        for r in rows:
            batch.append(r.asDict())
            count += 1

            if count % BATCH_SIZE == 0:
                execute_batch(cur, sql, batch, page_size=BATCH_SIZE)
                conn.commit()

                print(
                    f"[WRITE][EXECUTOR] partition={partition_id} "
                    f"batch committed, total_rows={count}",
                    flush=True
                )
                batch.clear()

        # flush remaining rows
        if batch:
            execute_batch(cur, sql, batch, page_size=BATCH_SIZE)
            conn.commit()

            print(
                f"[WRITE][EXECUTOR] partition={partition_id} "
                f"final batch committed, total_rows={count}",
                flush=True
            )

        print(
            f"[WRITE][EXECUTOR] END partition={partition_id}, total_rows={count}",
            flush=True
        )

    except Exception as e:
        print(
            f"[WRITE][EXECUTOR][FAILED] partition={partition_id}, attempt={attempt_id}",
            flush=True
        )
        raise

    finally:
        if conn:
            conn.close()

from pyspark.sql.functions import col, when, trim

def normalize_int_columns(df, columns):
    for c in columns:
        df = df.withColumn(
            c,
            when(trim(col(c)) == "", None)
            .otherwise(col(c).cast("int"))
        )
    return df

def write_to_postgres(df: DataFrame):
    final_df = df.select(
        "title",
        "created_date",
        "link",
        "description",
        "trade_type",
        "is_multi_product",
        "is_announcement",
        "product_type",
        "model_number",
        "model_variant",
        "capacity",
        "capacity_unit",
        "color",
        "price",
        "is_warranty_valid",
    )

    final_df = normalize_int_columns(
        final_df,
        ["price", "capacity"]
    )

    row_count = final_df.count()
    logger.info(f"▶ [WRITE] rows={row_count}")

    # 1 個 partition，等於一次寫入全部資料（約8萬筆）
    # 8 個 partition可以分成 8個 connection來寫入

    final_df = final_df.repartition(8) # 20260110_002

    logger.info("[WRITE] start foreachPartition")

    (
        final_df
        .foreachPartition(write_partition_to_pg)
    )

    logger.info("[WRITE] all partitions completed")

    # final_df.write.jdbc(
    #     url=jdbc_url,
    #     table=dest_table,
    #     mode="overwrite",
    #     properties=jdbc_props,
    # )
    


# =========================================================
# Main Pipeline
# =========================================================
def main():
    df = guard("Read source", read_source, action=True)
    df = guard("Derive trade_type", lambda: derive_trade_type(df))
    df = guard("Apply announcement guard", lambda: apply_announcement_guard(df))
    df = guard("Apply multi-product guard", lambda: apply_multi_product_guard(df))
    df = guard("Extract sections", lambda: extract_sections(df))
    df = guard("Parse product fields", lambda: parse_product_fields(df))
    df = guard("Apply title fallback", lambda: apply_title_fallback(df))
    guard("Write to Postgres", lambda: write_to_postgres(df))


if __name__ == "__main__":
    try:
        main()
        logger.info("[JOB_STATUS] SUCCESS")
    except Exception:
        logger.error("[JOB_STATUS] FAILED")
        raise
    finally:
        spark.stop()
