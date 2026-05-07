"""
Bronze Parquet → Iceberg silver.weather

Reads raw weather Parquet files written by the three Bronze DAGs (openweather,
openmeteo, tomorrowio), parses the per-source raw_json, normalises into a
unified schema, and appends new records to iceberg.silver.weather.

Bronze path layout (Hive-partitioned):
    gs://{bucket}/bronze/weather/{source}/year={Y}/month={MM}/day={DD}/{file}.parquet

Usage:
    spark-submit silver_weather.py --bucket <name> --watermark <iso-datetime>
"""

import argparse
import os
import sys
from functools import reduce

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from common import build_session


# ── JSON schemas for from_json() ──────────────────────────────────────────────
# Each source stores a different JSON structure in the raw_json column.
# Defining explicit schemas avoids schema-inference overhead and makes the
# field mapping self-documenting.

_OPENWEATHER_SCHEMA = T.StructType([
    T.StructField("main", T.StructType([
        T.StructField("temp",     T.DoubleType()),
        T.StructField("humidity", T.IntegerType()),
        T.StructField("pressure", T.DoubleType()),
    ])),
    T.StructField("wind",    T.StructType([T.StructField("speed", T.DoubleType())])),
    T.StructField("weather", T.ArrayType(T.StructType([T.StructField("main", T.StringType())]))),
])

_OPENMETEO_SCHEMA = T.StructType([
    T.StructField("current", T.StructType([
        T.StructField("temperature_2m",       T.DoubleType()),
        T.StructField("relative_humidity_2m", T.IntegerType()),
        T.StructField("surface_pressure",     T.DoubleType()),
        T.StructField("wind_speed_10m",       T.DoubleType()),  # km/h → converted to m/s below
        T.StructField("weather_code",         T.IntegerType()),
    ])),
])

_TOMORROWIO_SCHEMA = T.StructType([
    T.StructField("data", T.StructType([
        T.StructField("values", T.StructType([
            T.StructField("temperature",          T.DoubleType()),
            T.StructField("humidity",             T.DoubleType()),
            T.StructField("pressureSurfaceLevel", T.DoubleType()),
            T.StructField("windSpeed",            T.DoubleType()),  # m/s
            T.StructField("weatherCode",          T.IntegerType()),
        ])),
    ])),
])


# ── Weather-code → condition mappings ─────────────────────────────────────────
# Mirrors the same mappings used in the Bronze fetchers so Silver stays
# consistent with what CurrentWeatherState already contains.

_WMO_CODE_MAP: dict[int, str] = {
    0: "Clear",       1: "Clear",       2: "Clouds",      3: "Clouds",
    45: "Fog",       48: "Fog",
    51: "Drizzle",   53: "Drizzle",    55: "Drizzle",
    61: "Rain",      63: "Rain",       65: "Rain",
    71: "Snow",      73: "Snow",       75: "Snow",
    80: "Rain",      81: "Rain",       82: "Rain",
    95: "Thunderstorm", 96: "Thunderstorm", 99: "Thunderstorm",
}

_TIO_CODE_MAP: dict[int, str] = {
    1000: "Clear",       1001: "Clouds",  1100: "Clear",   1101: "Clouds",  1102: "Clouds",
    2000: "Fog",         2100: "Fog",
    4000: "Drizzle",     4001: "Rain",    4200: "Rain",    4201: "Rain",
    5000: "Snow",        5001: "Snow",    5100: "Snow",    5101: "Snow",
    8000: "Thunderstorm",
}


def _code_to_condition(col: F.Column, code_map: dict[int, str]) -> F.Column:
    """Translate an integer weather-code column to a condition string via map lookup."""
    mapping = F.create_map([F.lit(x) for pair in code_map.items() for x in pair])
    return F.coalesce(mapping[col], F.lit("Clouds"))


# ── Per-source normalisation ───────────────────────────────────────────────────

def _normalize_openweather(df: DataFrame) -> DataFrame:
    p = F.from_json(F.col("raw_json"), _OPENWEATHER_SCHEMA)
    return df.select(
        F.col("city_id"),
        F.col("city_name"),
        F.col("source"),
        F.to_timestamp("fetched_at").alias("fetched_at"),
        p["main"]["temp"].cast(T.DoubleType()).alias("temperature"),
        p["main"]["humidity"].cast(T.IntegerType()).alias("humidity"),
        p["main"]["pressure"].cast(T.DoubleType()).alias("pressure"),
        p["wind"]["speed"].cast(T.DoubleType()).alias("wind_speed"),     # already m/s
        p["weather"][0]["main"].alias("condition"),
        F.current_timestamp().alias("ingested_at"),
    )


def _normalize_openmeteo(df: DataFrame) -> DataFrame:
    current = F.from_json(F.col("raw_json"), _OPENMETEO_SCHEMA)["current"]
    return df.select(
        F.col("city_id"),
        F.col("city_name"),
        F.col("source"),
        F.to_timestamp("fetched_at").alias("fetched_at"),
        current["temperature_2m"].cast(T.DoubleType()).alias("temperature"),
        current["relative_humidity_2m"].cast(T.IntegerType()).alias("humidity"),
        current["surface_pressure"].cast(T.DoubleType()).alias("pressure"),
        (current["wind_speed_10m"] / F.lit(3.6)).alias("wind_speed"),   # km/h → m/s
        _code_to_condition(current["weather_code"], _WMO_CODE_MAP).alias("condition"),
        F.current_timestamp().alias("ingested_at"),
    )


def _normalize_tomorrowio(df: DataFrame) -> DataFrame:
    values = F.from_json(F.col("raw_json"), _TOMORROWIO_SCHEMA)["data"]["values"]
    return df.select(
        F.col("city_id"),
        F.col("city_name"),
        F.col("source"),
        F.to_timestamp("fetched_at").alias("fetched_at"),
        values["temperature"].cast(T.DoubleType()).alias("temperature"),
        values["humidity"].cast(T.IntegerType()).alias("humidity"),
        values["pressureSurfaceLevel"].cast(T.DoubleType()).alias("pressure"),
        values["windSpeed"].cast(T.DoubleType()).alias("wind_speed"),    # already m/s
        _code_to_condition(values["weatherCode"], _TIO_CODE_MAP).alias("condition"),
        F.current_timestamp().alias("ingested_at"),
    )


_NORMALIZERS: dict[str, callable] = {
    "openweather": _normalize_openweather,
    "openmeteo":   _normalize_openmeteo,
    "tomorrowio":  _normalize_tomorrowio,
}


# ── Table DDL ─────────────────────────────────────────────────────────────────

_CREATE_WEATHER_TABLE = """
    CREATE TABLE IF NOT EXISTS iceberg.silver.weather (
        city_id     INT,
        city_name   STRING,
        source      STRING,
        fetched_at  TIMESTAMP,
        temperature DOUBLE,
        humidity    INT,
        pressure    DOUBLE,
        wind_speed  DOUBLE,
        condition   STRING,
        ingested_at TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (source, days(fetched_at))
"""


# ── ETL ───────────────────────────────────────────────────────────────────────

def run(spark: SparkSession, bucket: str, watermark: str) -> None:
    bronze_path = f"gs://{bucket}/bronze/weather/*/*/*/*/*.parquet"

    raw = (
        spark.read.parquet(bronze_path)
        .filter(F.to_timestamp("fetched_at") > F.lit(watermark).cast("timestamp"))
    )

    if raw.isEmpty():
        print("[silver_weather] No new records since watermark. Nothing to do.")
        return

    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.silver")
    spark.sql(_CREATE_WEATHER_TABLE)

    parts = [
        _NORMALIZERS[src](raw.filter(F.col("source") == src))
        for src in _NORMALIZERS
        if not raw.filter(F.col("source") == src).isEmpty()
    ]

    if not parts:
        print("[silver_weather] No records matched a known source.")
        return

    normalized = reduce(DataFrame.unionByName, parts)
    count = normalized.count()
    normalized.writeTo("iceberg.silver.weather").append()
    print(f"[silver_weather] Wrote {count} records to iceberg.silver.weather")


# ── Entry point ───────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Bronze → Silver: weather")
    p.add_argument("--bucket",    required=True, help="GCS bucket name (no gs:// prefix)")
    p.add_argument("--watermark", required=True, help="ISO datetime — process records after this timestamp")
    return p.parse_args()


def main() -> None:
    args  = _parse_args()
    spark = build_session("silver.weather", args.bucket)
    try:
        run(spark, args.bucket, args.watermark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
