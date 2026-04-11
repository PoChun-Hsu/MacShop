from __future__ import annotations

import argparse
import glob
import os
import sys
import traceback
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, to_timestamp, trim
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

ICEBERG_ARTICLE_TABLE = "iceberg.default.ptt_macshop_articles"
ICEBERG_PAGE_TABLE = "iceberg.default.ptt_macshop_page_dates"
SPARK_STAGE_ROOT = "/opt/spark-apps/staging"
AWS_REGION = "us-east-1"
AWS_ACCESS_KEY_ID = "admin"
AWS_SECRET_ACCESS_KEY = "admin123"
MINIO_ENDPOINT = "http://minio:9000"
ICEBERG_REST_URI = "http://iceberg-rest:8181"

ARTICLE_SCHEMA = StructType([
    StructField("Title", StringType(), True),
    StructField("Author", StringType(), True),
    StructField("Created_Date", StringType(), True),
    StructField("Link", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Description_Hash", StringType(), True),
    StructField("Updated_Date", StringType(), True),
])

PAGE_SCHEMA = StructType([
    StructField("Page_Num", IntegerType(), True),
    StructField("Url", StringType(), True),
    StructField("Min_Date", StringType(), True),
    StructField("Max_Date", StringType(), True),
    StructField("Updated_Date", StringType(), True),
])


def log(message: str) -> None:
    print(message, flush=True)


def fail(message: str, exit_code: int = 1) -> None:
    print(f"[ERROR] {message}", file=sys.stderr, flush=True)
    sys.exit(exit_code)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load staged PTT MacShop JSONL files into Iceberg safely."
    )
    parser.add_argument("--run-token", required=True, help="Folder name under staging root")
    parser.add_argument(
        "--article-glob",
        default=None,
        help="Optional override for article file glob",
    )
    parser.add_argument(
        "--page-glob",
        default=None,
        help="Optional override for page_dates file glob",
    )
    parser.add_argument(
        "--shuffle-partitions",
        type=int,
        default=4,
        help="Spark shuffle partitions for this job",
    )
    parser.add_argument(
        "--preview-rows",
        type=int,
        default=3,
        help="Rows to show for debug preview",
    )
    parser.add_argument(
        "--skip-maintenance",
        action="store_true",
        help="Skip rewrite_data_files maintenance",
    )
    return parser.parse_args()


def prepare_aws_env() -> None:
    # Some Iceberg + AWS SDK v2 paths read from environment / JVM defaults rather than
    # Spark catalog properties only. Set them early so both driver and libraries can see them.
    os.environ.setdefault("AWS_REGION", AWS_REGION)
    os.environ.setdefault("AWS_DEFAULT_REGION", AWS_REGION)
    os.environ.setdefault("AWS_ACCESS_KEY_ID", AWS_ACCESS_KEY_ID)
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", AWS_SECRET_ACCESS_KEY)


def build_spark(app_name: str, shuffle_partitions: int) -> SparkSession:
    prepare_aws_env()

    builder = (
        SparkSession.builder
        .master("local[*]")
        .appName(app_name)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "rest")
        .config("spark.sql.catalog.iceberg.uri", ICEBERG_REST_URI)
        .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.iceberg.s3.endpoint", MINIO_ENDPOINT)
        .config("spark.sql.catalog.iceberg.s3.path-style-access", "true")
        .config("spark.sql.catalog.iceberg.s3.access-key-id", AWS_ACCESS_KEY_ID)
        .config("spark.sql.catalog.iceberg.s3.secret-access-key", AWS_SECRET_ACCESS_KEY)
        # Keep the catalog-level region property as well. Some Iceberg stacks honor this directly.
        .config("spark.sql.catalog.iceberg.client.region", AWS_REGION)
        # Provide region/creds through Spark env + Hadoop env so AWS SDK v2 can resolve them.
        .config("spark.driverEnv.AWS_REGION", AWS_REGION)
        .config("spark.driverEnv.AWS_DEFAULT_REGION", AWS_REGION)
        .config("spark.driverEnv.AWS_ACCESS_KEY_ID", AWS_ACCESS_KEY_ID)
        .config("spark.driverEnv.AWS_SECRET_ACCESS_KEY", AWS_SECRET_ACCESS_KEY)
        .config("spark.executorEnv.AWS_REGION", AWS_REGION)
        .config("spark.executorEnv.AWS_DEFAULT_REGION", AWS_REGION)
        .config("spark.executorEnv.AWS_ACCESS_KEY_ID", AWS_ACCESS_KEY_ID)
        .config("spark.executorEnv.AWS_SECRET_ACCESS_KEY", AWS_SECRET_ACCESS_KEY)
        # Hadoop S3A configs are not used by S3FileIO directly, but setting them avoids surprises
        # if any path falls back to Hadoop/S3A behavior later.
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.endpoint.region", AWS_REGION)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def ensure_tables(spark: SparkSession) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_ARTICLE_TABLE} (
            title string,
            author string,
            created_date timestamp,
            link string,
            description string,
            description_hash string,
            updated_date timestamp
        )
        USING iceberg
        PARTITIONED BY (days(created_date))
        TBLPROPERTIES (
            'format-version'='2',
            'write.distribution-mode'='hash',
            'write.parquet.compression-codec'='snappy'
        )
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_PAGE_TABLE} (
            page_num int,
            url string,
            min_date timestamp,
            max_date timestamp,
            updated_date timestamp
        )
        USING iceberg
        PARTITIONED BY (days(max_date))
        TBLPROPERTIES (
            'format-version'='2',
            'write.distribution-mode'='hash',
            'write.parquet.compression-codec'='snappy'
        )
        """
    )


def resolve_files(pattern: str) -> list[str]:
    return sorted(glob.glob(pattern))


def read_json_file(spark: SparkSession, file_path: str, schema: StructType) -> DataFrame:
    return (
        spark.read
        .schema(schema)
        .option("multiLine", "false")
        .json(file_path)
    )


def transform_article_df(df: DataFrame) -> DataFrame:
    return (
        df.select(
            trim(col("Title")).alias("title"),
            trim(col("Author")).alias("author"),
            to_timestamp(col("Created_Date")).alias("created_date"),
            trim(col("Link")).alias("link"),
            col("Description").alias("description"),
            trim(col("Description_Hash")).alias("description_hash"),
            to_timestamp(col("Updated_Date")).alias("updated_date"),
        )
        .filter(col("link").isNotNull())
        .dropDuplicates(["link"])
    )


def transform_page_df(df: DataFrame) -> DataFrame:
    return (
        df.select(
            col("Page_Num").alias("page_num"),
            trim(col("Url")).alias("url"),
            to_timestamp(col("Min_Date")).alias("min_date"),
            to_timestamp(col("Max_Date")).alias("max_date"),
            to_timestamp(col("Updated_Date")).alias("updated_date"),
        )
        .filter(col("page_num").isNotNull())
        .dropDuplicates(["page_num"])
    )


def maybe_reduce_partitions(df: DataFrame, target_partitions: int = 1) -> DataFrame:
    return df.coalesce(max(1, target_partitions))


def debug_preview(df: DataFrame, name: str, preview_rows: int) -> None:
    log(f"[DEBUG] {name} preview start")
    df.show(preview_rows, truncate=False)
    log(f"[DEBUG] {name} preview done")


def merge_articles(spark: SparkSession, staged_df: DataFrame) -> None:
    staged_df.createOrReplaceTempView("stg_ptt_macshop_articles")
    log("[DEBUG] running MERGE article")
    spark.sql(
        f"""
        MERGE INTO {ICEBERG_ARTICLE_TABLE} t
        USING stg_ptt_macshop_articles s
        ON t.link = s.link
        WHEN MATCHED AND (
            coalesce(t.title, '') <> coalesce(s.title, '') OR
            coalesce(t.author, '') <> coalesce(s.author, '') OR
            coalesce(cast(t.created_date as string), '') <> coalesce(cast(s.created_date as string), '') OR
            coalesce(t.description_hash, '') <> coalesce(s.description_hash, '') OR
            coalesce(cast(t.updated_date as string), '') <> coalesce(cast(s.updated_date as string), '')
        ) THEN UPDATE SET
            t.title = s.title,
            t.author = s.author,
            t.created_date = s.created_date,
            t.description = s.description,
            t.description_hash = s.description_hash,
            t.updated_date = s.updated_date
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    spark.catalog.dropTempView("stg_ptt_macshop_articles")


def merge_pages(spark: SparkSession, staged_df: DataFrame) -> None:
    staged_df.createOrReplaceTempView("stg_ptt_macshop_page_dates")
    log("[DEBUG] running MERGE page")
    spark.sql(
        f"""
        MERGE INTO {ICEBERG_PAGE_TABLE} t
        USING stg_ptt_macshop_page_dates s
        ON t.page_num = s.page_num
        WHEN MATCHED AND (
            coalesce(t.url, '') <> coalesce(s.url, '') OR
            coalesce(cast(t.min_date as string), '') <> coalesce(cast(s.min_date as string), '') OR
            coalesce(cast(t.max_date as string), '') <> coalesce(cast(s.max_date as string), '') OR
            coalesce(cast(t.updated_date as string), '') <> coalesce(cast(s.updated_date as string), '')
        ) THEN UPDATE SET
            t.url = s.url,
            t.min_date = s.min_date,
            t.max_date = s.max_date,
            t.updated_date = s.updated_date
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    spark.catalog.dropTempView("stg_ptt_macshop_page_dates")


def process_article_files(
    spark: SparkSession,
    files: Iterable[str],
    preview_rows: int,
) -> int:
    processed_rows = 0

    for idx, file_path in enumerate(files, start=1):
        log(f"[INFO] article file {idx}: {file_path}")
        source_df = read_json_file(spark, file_path, ARTICLE_SCHEMA)
        staged_df = transform_article_df(source_df)

        debug_preview(staged_df, "article", preview_rows)

        log("[DEBUG] article count start")
        row_count = staged_df.count()
        log(f"[INFO] article rows after transform={row_count}")
        log("[DEBUG] article count done")

        if row_count == 0:
            continue

        staged_df = maybe_reduce_partitions(staged_df, target_partitions=1)

        log("[DEBUG] article merge start")
        merge_articles(spark, staged_df)
        log("[DEBUG] article merge done")

        processed_rows += row_count

    return processed_rows


def process_page_files(
    spark: SparkSession,
    files: Iterable[str],
    preview_rows: int,
) -> int:
    processed_rows = 0

    for idx, file_path in enumerate(files, start=1):
        log(f"[INFO] page file {idx}: {file_path}")
        source_df = read_json_file(spark, file_path, PAGE_SCHEMA)
        staged_df = transform_page_df(source_df)

        debug_preview(staged_df, "page", preview_rows)

        log("[DEBUG] page count start")
        row_count = staged_df.count()
        log(f"[INFO] page rows after transform={row_count}")
        log("[DEBUG] page count done")

        if row_count == 0:
            continue

        staged_df = maybe_reduce_partitions(staged_df, target_partitions=1)

        log("[DEBUG] page merge start")
        merge_pages(spark, staged_df)
        log("[DEBUG] page merge done")

        processed_rows += row_count

    return processed_rows


def run_maintenance(spark: SparkSession) -> None:
    log("[INFO] rewrite_data_files on article table")
    spark.sql(
        f"CALL iceberg.system.rewrite_data_files(table => '{ICEBERG_ARTICLE_TABLE}')"
    )

    log("[INFO] rewrite_data_files on page table")
    spark.sql(
        f"CALL iceberg.system.rewrite_data_files(table => '{ICEBERG_PAGE_TABLE}')"
    )


def main() -> None:
    args = parse_args()

    article_glob = args.article_glob or f"{SPARK_STAGE_ROOT}/{args.run_token}/articles/*.jsonl"
    page_glob = args.page_glob or f"{SPARK_STAGE_ROOT}/{args.run_token}/page_dates/*.jsonl"

    article_files = resolve_files(article_glob)
    page_files = resolve_files(page_glob)

    log(f"[INFO] run_token={args.run_token}")
    log(f"[INFO] article_glob={article_glob}")
    log(f"[INFO] page_glob={page_glob}")
    log(f"[INFO] article_files={len(article_files)}")
    log(f"[INFO] page_files={len(page_files)}")
    log(f"[INFO] AWS_REGION={os.environ.get('AWS_REGION', AWS_REGION)}")

    if not article_files:
        fail("No article files found")
    if not page_files:
        fail("No page files found")

    spark: SparkSession | None = None

    try:
        spark = build_spark(
            app_name="load-ptt-macshop-to-iceberg-safe",
            shuffle_partitions=args.shuffle_partitions,
        )
        ensure_tables(spark)

        article_rows = process_article_files(
            spark,
            article_files,
            preview_rows=args.preview_rows,
        )
        page_rows = process_page_files(
            spark,
            page_files,
            preview_rows=args.preview_rows,
        )

        if not args.skip_maintenance:
            run_maintenance(spark)

        log(f"[SUCCESS] article_rows_processed={article_rows}")
        log(f"[SUCCESS] page_rows_processed={page_rows}")

    except Exception:
        print("[ERROR] JOB FAILED", file=sys.stderr, flush=True)
        traceback.print_exc()
        sys.exit(1)
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
