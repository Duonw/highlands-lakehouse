"""
Shared SparkSession builder for all Silver jobs.

Spark JAR dependencies (gcs-connector, iceberg) are loaded via spark-submit --jars
and --driver-class-path — they must be present BEFORE the JVM starts.
This module only handles runtime Spark *configuration* (catalog, GCS auth).
"""

import os

from pyspark.sql import SparkSession


def build_session(app_name: str, bucket: str) -> SparkSession:
    """
    Create a SparkSession pre-configured for GCS access and Iceberg catalog.

    Expects the environment variable GCS_SA_KEYFILE to be set to the path of
    the service-account JSON key file inside the container.

    Args:
        app_name: Spark application name shown in UI / logs.
        bucket:   GCS bucket name (without gs:// prefix).
    """
    keyfile = os.environ["GCS_SA_KEYFILE"]

    return (
        SparkSession.builder
        .appName(app_name)
        # ── Iceberg catalog ────────────────────────────────────────────────
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type",      "hadoop")
        .config("spark.sql.catalog.iceberg.warehouse", f"gs://{bucket}/iceberg")
        # ── GCS authentication ─────────────────────────────────────────────
        .config(
            "spark.hadoop.fs.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
        )
        .config(
            "spark.hadoop.fs.AbstractFileSystem.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
        )
        .config("spark.hadoop.google.cloud.auth.service.account.enable",       "true")
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", keyfile)
        .getOrCreate()
    )
