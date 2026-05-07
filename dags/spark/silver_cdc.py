"""
Bronze Avro (Datastream CDC) → Iceberg silver.orders + silver.order_details

Reads Google Datastream Avro files from GCS Bronze, deduplicates change events,
and MERGE INTOs the Silver Iceberg tables so each row always reflects the
latest known state of an order / order-detail.

Bronze path layout:
    gs://{bucket}/{cdc_prefix}/{table_name}/YYYY/MM/DD/HH/mm/*.avro

Where cdc_prefix typically looks like:  bronze/cdc/HighlandsDB/dbo

Tables processed:
    Orders       → iceberg.silver.orders
    OrderDetails → iceberg.silver.order_details

Datastream metadata lives in a top-level struct field. The constants below
can be adjusted if your stream was created with a different schema version.

Usage:
    spark-submit silver_cdc.py \\
        --bucket    <name> \\
        --watermark <iso-datetime> \\
        --cdc-prefix bronze/cdc/HighlandsDB/dbo
"""

import argparse
import os
import sys
from functools import reduce

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

from common import build_session


# ── Datastream Avro top-level schema ──────────────────────────────────────────
# Confirmed from actual Avro files (printSchema output):
#   uuid, read_timestamp, source_timestamp (micros, long), object, read_method,
#   stream_name, schema_key, sort_keys, source_metadata (struct), payload (struct)
#
# source_metadata.change_type : "INSERT" | "UPDATE" | "DELETE"
# payload.*                   : actual row columns (order_id, store_id, ...)


# ── Table DDL ─────────────────────────────────────────────────────────────────

_CREATE_ORDERS_TABLE = """
    CREATE TABLE IF NOT EXISTS iceberg.silver.orders (
        order_id     STRING,
        store_id     INT,
        customer_id  INT,
        order_type   STRING,
        status       STRING,
        total_amount DOUBLE,
        created_at   TIMESTAMP,
        updated_at   TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (months(created_at))
"""

_CREATE_ORDER_DETAILS_TABLE = """
    CREATE TABLE IF NOT EXISTS iceberg.silver.order_details (
        detail_id  STRING,
        order_id   STRING,
        product_id INT,
        quantity   INT,
        unit_price DOUBLE,
        subtotal   DOUBLE
    )
    USING iceberg
"""

_CREATE_PRODUCTS_TABLE = """
    CREATE TABLE IF NOT EXISTS iceberg.silver.products (
        product_id INT,
        category   STRING,
        price      DOUBLE,
        is_active  INT,
        valid_from TIMESTAMP,
        valid_to   TIMESTAMP,
        is_current BOOLEAN
    )
    USING iceberg
"""

_CREATE_STORES_TABLE = """
    CREATE TABLE IF NOT EXISTS iceberg.silver.stores (
        store_id   INT,
        city_id    INT,
        is_active  INT,
        valid_from TIMESTAMP,
        valid_to   TIMESTAMP,
        is_current BOOLEAN
    )
    USING iceberg
"""


# ── Helpers ───────────────────────────────────────────────────────────────────

def _source_ts_col() -> F.Column:
    """Datastream source_timestamp is already stored as TIMESTAMP in Avro."""
    return F.col("source_timestamp")


def _to_ts(col: F.Column) -> F.Column:
    """
    Reconstruct a TIMESTAMP from Datastream's datetime STRUCT representation.

    Datastream stores SQL datetime columns as STRUCT<date: DATE, time: BIGINT>
    where `time` is microseconds since midnight UTC.
    Use datediff from epoch instead of casting DATE to LONG for reliability.

    Returns null if either struct field is null (null-safe).
    """
    days_since_epoch = F.datediff(col["date"], F.to_date(F.lit("1970-01-01"))).cast(T.LongType())
    micros_of_day    = col["time"]
    epoch_micros     = (days_since_epoch * F.lit(86_400_000_000)) + micros_of_day
    return F.when(
        days_since_epoch.isNotNull() & micros_of_day.isNotNull(),
        (epoch_micros / F.lit(1_000_000)).cast(T.TimestampType()),
    ).otherwise(F.lit(None).cast(T.TimestampType()))


def _path_exists(spark: SparkSession, gcs_path: str) -> bool:
    """Return True if the GCS path exists (file or non-empty directory)."""
    jvm  = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(gcs_path)
    fs   = path.getFileSystem(conf)
    return fs.exists(path)


def _read_avro(spark: SparkSession, path: str, watermark: str) -> DataFrame:
    """
    Load all Avro files under `path` (recursive), filter to records whose
    source_timestamp is newer than the watermark, and drop DELETE events.
    Returns an empty DataFrame (no schema) if the path does not yet exist.
    """
    if not _path_exists(spark, path):
        print(f"[silver_cdc] PATH NOT FOUND in GCS: {path} — Datastream chưa stream bảng này")
        return spark.createDataFrame([], T.StructType([]))

    return (
        spark.read.format("avro")
        .option("recursiveFileLookup", "true")
        .load(path)
        .filter(_source_ts_col() > F.lit(watermark).cast("timestamp"))
        .filter(F.col("source_metadata.change_type") != "DELETE")
    )


def _keep_latest(df: DataFrame, partition_col: str) -> DataFrame:
    """
    Deduplicate a CDC DataFrame (already flattened) by keeping the most-recent
    event per key. Expects a top-level `source_timestamp` long column (micros).
    """
    w = Window.partitionBy(partition_col).orderBy(F.col("source_timestamp").desc())
    return (
        df.withColumn("_rank", F.rank().over(w))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )


def _apply_scd2(
    spark: SparkSession,
    table: str,
    incoming: DataFrame,
    key_col: str,
    tracked_cols: list[str],
) -> int:
    """
    Apply SCD Type 2 to an Iceberg table.

    `incoming` must contain all business columns plus `source_timestamp` (TIMESTAMP).
    The function adds `valid_from`, `valid_to`, and `is_current` to inserted rows.

      - New key             → INSERT (valid_from=source_ts, valid_to=null, is_current=true)
      - Key exists, changed → expire old row + INSERT new version
      - Key exists, same    → skip
    """
    # ── Load currently-active rows for the keys we're about to process ─────────
    incoming_keys = incoming.select(key_col)
    active_current = (
        spark.sql(f"SELECT * FROM {table} WHERE is_current = true")
        .join(incoming_keys, on=key_col, how="inner")
    )

    # Rename tracked columns in the active snapshot so they don't collide after join
    active_vals = active_current.select(
        F.col(key_col),
        *[F.col(c).alias(f"_cur_{c}") for c in tracked_cols],
    )

    # ── Classify each incoming row ─────────────────────────────────────────────
    joined = incoming.join(active_vals, on=key_col, how="left")

    is_new     = F.col(f"_cur_{tracked_cols[0]}").isNull()
    is_changed = F.col(f"_cur_{tracked_cols[0]}").isNotNull() & reduce(
        lambda a, b: a | b,
        [F.col(c) != F.col(f"_cur_{c}") for c in tracked_cols],
    )

    business_cols = [c for c in incoming.columns if c != "source_timestamp"]

    to_expire = joined.filter(is_changed).select(
        F.col(key_col),
        F.col("source_timestamp").alias("new_valid_from"),
    )

    to_insert = (
        joined.filter(is_new | is_changed)
        .select(
            *[F.col(c) for c in business_cols],
            F.col("source_timestamp").alias("valid_from"),
            F.lit(None).cast(T.TimestampType()).alias("valid_to"),
            F.lit(True).alias("is_current"),
        )
    )

    count = to_insert.count()
    if count == 0:
        print(f"[silver_cdc] No changes detected for {table}")
        return 0

    # ── Expire old versions of changed records ─────────────────────────────────
    if not to_expire.isEmpty():
        to_expire.createOrReplaceTempView("_scd2_expire")
        spark.sql(f"""
            MERGE INTO {table} AS t
            USING _scd2_expire AS s
            ON t.{key_col} = s.{key_col} AND t.is_current = true
            WHEN MATCHED THEN UPDATE SET
                t.valid_to   = s.new_valid_from,
                t.is_current = false
        """)

    # ── Insert new versions ────────────────────────────────────────────────────
    to_insert.writeTo(table).append()

    print(f"[silver_cdc] SCD2 wrote {count} new version(s) to {table}")
    return count


# ── Per-table ETL ─────────────────────────────────────────────────────────────

def _process_orders(spark: SparkSession, bucket: str, cdc_prefix: str, watermark: str) -> int:
    spark.sql(_CREATE_ORDERS_TABLE)

    path = f"gs://{bucket}/{cdc_prefix}/dbo_Orders"
    raw  = _read_avro(spark, path, watermark)

    if raw.isEmpty():
        print("[silver_cdc] No new Order events since watermark.")
        return 0

    # DEBUG: print schema and sample raw payload to verify datetime field types
    print("[silver_cdc][DEBUG] raw schema:")
    raw.printSchema()
    print("[silver_cdc][DEBUG] sample raw payload (created_at struct):")
    raw.select("payload.order_id", "payload.created_at", "payload.updated_at").show(3, truncate=False)

    # Flatten payload struct before dedup so window can partition by payload.order_id
    flattened = raw.select(
        F.col("payload.order_id").alias("order_id"),
        F.col("payload.store_id").cast(T.IntegerType()).alias("store_id"),
        F.col("payload.customer_id").cast(T.IntegerType()).alias("customer_id"),
        F.col("payload.order_type").alias("order_type"),
        F.col("payload.status").alias("status"),
        F.col("payload.total_amount").cast(T.DoubleType()).alias("total_amount"),
        _to_ts(F.col("payload.created_at")).alias("created_at"),
        _to_ts(F.col("payload.updated_at")).alias("updated_at"),
        F.col("source_timestamp"),   # kept for _keep_latest window
    )

    deduped = _keep_latest(flattened, "order_id").drop("source_timestamp")

    count = deduped.count()
    deduped.createOrReplaceTempView("_new_orders")

    spark.sql("""
        MERGE INTO iceberg.silver.orders AS t
        USING _new_orders AS s
        ON t.order_id = s.order_id
        WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET
            t.status       = s.status,
            t.total_amount = s.total_amount,
            t.updated_at   = s.updated_at
        WHEN NOT MATCHED THEN INSERT *
    """)

    print(f"[silver_cdc] Merged {count} Order events into iceberg.silver.orders")
    return count


def _process_order_details(spark: SparkSession, bucket: str, cdc_prefix: str, watermark: str) -> int:
    spark.sql(_CREATE_ORDER_DETAILS_TABLE)

    path = f"gs://{bucket}/{cdc_prefix}/dbo_OrderDetails"
    raw  = _read_avro(spark, path, watermark)

    if raw.isEmpty():
        print("[silver_cdc] No new OrderDetail events since watermark.")
        return 0

    flattened = raw.select(
        F.col("payload.detail_id").alias("detail_id"),
        F.col("payload.order_id").alias("order_id"),
        F.col("payload.product_id").cast(T.IntegerType()).alias("product_id"),
        F.col("payload.quantity").cast(T.IntegerType()).alias("quantity"),
        F.col("payload.unit_price").cast(T.DoubleType()).alias("unit_price"),
        F.col("payload.subtotal").cast(T.DoubleType()).alias("subtotal"),
        F.col("source_timestamp"),
    )

    deduped = _keep_latest(flattened, "detail_id").drop("source_timestamp")

    count = deduped.count()
    deduped.createOrReplaceTempView("_new_order_details")

    spark.sql("""
        MERGE INTO iceberg.silver.order_details AS t
        USING _new_order_details AS s
        ON t.detail_id = s.detail_id
        WHEN NOT MATCHED THEN INSERT *
    """)

    print(f"[silver_cdc] Merged {count} OrderDetail events into iceberg.silver.order_details")
    return count


def _process_products(spark: SparkSession, bucket: str, cdc_prefix: str, watermark: str) -> int:
    spark.sql(_CREATE_PRODUCTS_TABLE)

    path = f"gs://{bucket}/{cdc_prefix}/dbo_Products"
    raw  = _read_avro(spark, path, watermark)

    if raw.isEmpty():
        print("[silver_cdc] No new Product events since watermark.")
        return 0

    flattened = _keep_latest(
        raw.select(
            F.col("payload.product_id").cast(T.IntegerType()).alias("product_id"),
            F.col("payload.category").alias("category"),
            F.col("payload.price").cast(T.DoubleType()).alias("price"),
            F.col("payload.is_active").cast(T.IntegerType()).alias("is_active"),
            F.col("source_timestamp"),
        ),
        partition_col="product_id",
    )

    return _apply_scd2(
        spark,
        table        = "iceberg.silver.products",
        incoming     = flattened,
        key_col      = "product_id",
        tracked_cols = ["category", "price", "is_active"],
    )


def _process_stores(spark: SparkSession, bucket: str, cdc_prefix: str, watermark: str) -> int:
    spark.sql(_CREATE_STORES_TABLE)

    path = f"gs://{bucket}/{cdc_prefix}/dbo_Stores"
    raw  = _read_avro(spark, path, watermark)

    if raw.isEmpty():
        print("[silver_cdc] No new Store events since watermark.")
        return 0

    flattened = _keep_latest(
        raw.select(
            F.col("payload.store_id").cast(T.IntegerType()).alias("store_id"),
            F.col("payload.city_id").cast(T.IntegerType()).alias("city_id"),
            F.col("payload.is_active").cast(T.IntegerType()).alias("is_active"),
            F.col("source_timestamp"),
        ),
        partition_col="store_id",
    )

    return _apply_scd2(
        spark,
        table        = "iceberg.silver.stores",
        incoming     = flattened,
        key_col      = "store_id",
        tracked_cols = ["city_id", "is_active"],
    )


# ── ETL ───────────────────────────────────────────────────────────────────────

def run(spark: SparkSession, bucket: str, cdc_prefix: str, watermark: str) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.silver")

    _process_orders(spark, bucket, cdc_prefix, watermark)
    _process_order_details(spark, bucket, cdc_prefix, watermark)
    _process_products(spark, bucket, cdc_prefix, watermark)
    _process_stores(spark, bucket, cdc_prefix, watermark)


# ── Entry point ───────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Bronze CDC → Silver: orders + order_details")
    p.add_argument("--bucket",     required=True, help="GCS bucket name (no gs:// prefix)")
    p.add_argument("--watermark",  required=True, help="ISO datetime — process records after this timestamp")
    p.add_argument("--cdc-prefix", required=True, help="GCS path prefix for Datastream output, e.g. bronze/cdc/HighlandsDB/dbo")
    return p.parse_args()


def main() -> None:
    args  = _parse_args()
    spark = build_session("silver.cdc", args.bucket)
    try:
        run(spark, args.bucket, args.cdc_prefix, args.watermark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
