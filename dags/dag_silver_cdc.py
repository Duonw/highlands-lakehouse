"""
DAG: silver_cdc — Bronze Avro (Datastream) → Iceberg silver.orders + silver.order_details

Schedule: mỗi 10 phút — bám sát order_simulator_dag (*/5) để Silver luôn gần
real-time nhưng không tranh cùng slot với Bronze.

Flow:
    get_config → submit_spark_job → update_watermark

Watermark: Airflow Variable SILVER_CDC_WATERMARK (ISO datetime string).

Airflow Variable cần có:
    GCS_BRONZE_BUCKET       — tên bucket, vd "highlands-lakehouse-2026"
    DATASTREAM_CDC_PREFIX   — path prefix dưới bucket, vd "bronze/cdc/HighlandsDB/dbo"
    SILVER_CDC_WATERMARK    — tự tạo lần đầu từ _WATERMARK_DEFAULT nếu chưa có
"""

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from airflow.sdk import dag, task, Variable
from pipeline_datasets import SILVER_CDC

# ── Container paths ────────────────────────────────────────────────────────────
_JARS_DIR    = "/opt/airflow/jars"
_SCRIPTS_DIR = "/opt/airflow/dags/spark"
_SA_KEYFILE  = "/opt/airflow/config/highlands-lakehouse-76b9d007fb7f.json"

_ICEBERG_JAR = f"{_JARS_DIR}/iceberg-spark-runtime-3.5_2.12-1.7.1.jar"
_GCS_JAR     = f"{_JARS_DIR}/gcs-connector-hadoop3-latest.jar"

# ── Airflow Variables ──────────────────────────────────────────────────────────
_WATERMARK_VAR     = "SILVER_CDC_WATERMARK"
_WATERMARK_DEFAULT = "2026-01-01T00:00:00"

# ── spark-submit base command ──────────────────────────────────────────────────
# spark-avro is resolved automatically from Maven Central on first run,
# then cached in ~/.ivy2/ inside the container.
_SPARK_BASE = (
    "spark-submit"
    " --master local[2]"
    " --driver-memory 1g"
    " --conf spark.sql.shuffle.partitions=4"
    f" --driver-class-path {_GCS_JAR}"
    f" --jars {_ICEBERG_JAR},{_GCS_JAR}"
    " --packages org.apache.spark:spark-avro_2.12:3.5.5"
)


@dag(
    start_date=datetime(2026, 4, 26),
    schedule="25 * * * *",
    catchup=False,
    default_args={
        "retries":      1,
        "retry_delay":  timedelta(minutes=3),
    },
    tags=["silver", "cdc", "orders"],
)
def silver_cdc_dag():

    @task
    def get_config(**context) -> dict:
        return {
            "bucket":     Variable.get("GCS_BRONZE_BUCKET"),
            "watermark":  Variable.get(_WATERMARK_VAR, default=_WATERMARK_DEFAULT),
            "cdc_prefix": Variable.get("DATASTREAM_CDC_PREFIX"),
            "run_end":    context["data_interval_end"].isoformat(),
        }

    config = get_config()

    submit_spark_job = BashOperator(
        task_id="submit_spark_job",
        bash_command=(
            f"{_SPARK_BASE}"
            f" {_SCRIPTS_DIR}/silver_cdc.py"
            " --bucket      \"{{ task_instance.xcom_pull('get_config')['bucket'] }}\""
            " --watermark   \"{{ task_instance.xcom_pull('get_config')['watermark'] }}\""
            " --cdc-prefix  \"{{ task_instance.xcom_pull('get_config')['cdc_prefix'] }}\""
        ),
        env={"GCS_SA_KEYFILE": _SA_KEYFILE},
        append_env=True,
    )

    @task
    def update_watermark(cfg: dict) -> None:
        Variable.set(_WATERMARK_VAR, cfg["run_end"])

    @task
    def sync_bigquery(cfg: dict) -> None:
        from bq_sync import sync_bq_external_table
        for table in ["orders", "order_details", "products", "stores"]:
            sync_bq_external_table(
                bucket         = cfg["bucket"],
                iceberg_prefix = f"iceberg/silver/{table}",
                bq_table       = table,
                keyfile        = _SA_KEYFILE,
            )

    @task(outlets=[SILVER_CDC])
    def mark_silver_cdc_done() -> None:
        """Báo hiệu SILVER_CDC dataset đã cập nhật → trigger gold_dbt_dag."""
        pass

    config >> submit_spark_job >> [update_watermark(config), sync_bigquery(config)] >> mark_silver_cdc_done()


silver_cdc_dag()
