"""
DAG: silver_weather — Bronze Parquet → Iceberg silver.weather

Schedule: 30 phút sau mỗi giờ (Bronze weather DAGs chạy @hourly, Silver
chạy sau 30 phút để chắc chắn Bronze đã xong).

Flow:
    get_config → submit_spark_job → update_watermark

Watermark: Airflow Variable SILVER_WEATHER_WATERMARK (ISO datetime string).
    - Được đọc trước khi job chạy để lọc Bronze records mới.
    - Được cập nhật thành data_interval_end sau khi job chạy thành công.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from datetime import datetime, timedelta, timezone

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task, Variable
from airflow.timetables.datasets import DatasetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable
from pipeline_datasets import BRONZE_OPENWEATHER, BRONZE_OPENMETEO, BRONZE_TOMORROWIO, SILVER_WEATHER

# ── Container paths ────────────────────────────────────────────────────────────
_JARS_DIR    = "/opt/airflow/jars"
_SCRIPTS_DIR = "/opt/airflow/dags/spark"
_SA_KEYFILE  = "/opt/airflow/config/highlands-lakehouse-76b9d007fb7f.json"

_ICEBERG_JAR = f"{_JARS_DIR}/iceberg-spark-runtime-3.5_2.12-1.7.1.jar"
_GCS_JAR     = f"{_JARS_DIR}/gcs-connector-hadoop3-latest.jar"

# ── Airflow Variables ──────────────────────────────────────────────────────────
_WATERMARK_VAR     = "SILVER_WEATHER_WATERMARK"
_WATERMARK_DEFAULT = "2026-01-01T00:00:00"

# ── spark-submit base command ──────────────────────────────────────────────────
# --driver-class-path loads the GCS connector into the JVM before SparkContext
# starts so the gs:// filesystem is registered at startup.
_SPARK_BASE = (
    "spark-submit"
    " --master local[2]"
    " --driver-memory 1g"
    " --conf spark.sql.shuffle.partitions=4"
    f" --driver-class-path {_GCS_JAR}"
    f" --jars {_ICEBERG_JAR},{_GCS_JAR}"
)


@dag(
    start_date=datetime(2026, 4, 26),
    # DatasetOrTimeSchedule: trigger khi cả 3 bronze dataset update (happy path)
    # fallback: chạy lúc :55 mỗi giờ nếu có API nào fail mà không recover trong chu kỳ
    # → :55 để tránh đụng silver_cdc (cron :25, chạy ~10 phút → xong ~:35)
    schedule=DatasetOrTimeSchedule(
        timetable=CronTriggerTimetable("55 * * * *", timezone="UTC"),
        datasets=[BRONZE_OPENWEATHER, BRONZE_OPENMETEO, BRONZE_TOMORROWIO],
    ),
    catchup=False,
    default_args={
        "retries":      1,
        "retry_delay":  timedelta(minutes=5),
    },
    tags=["silver", "weather"],
)
def silver_weather_dag():

    @task
    def get_config(**context) -> dict:
        run_end = context.get("data_interval_end")
        if run_end is None:
            run_end = datetime.now(tz=timezone.utc)
        return {
            "bucket":    Variable.get("GCS_BRONZE_BUCKET"),
            "watermark": Variable.get(_WATERMARK_VAR, default=_WATERMARK_DEFAULT),
            "run_end":   run_end.isoformat(),
        }

    config = get_config()

    submit_spark_job = BashOperator(
        task_id="submit_spark_job",
        bash_command=(
            f"{_SPARK_BASE}"
            f" {_SCRIPTS_DIR}/silver_weather.py"
            " --bucket    \"{{ task_instance.xcom_pull('get_config')['bucket'] }}\""
            " --watermark \"{{ task_instance.xcom_pull('get_config')['watermark'] }}\""
        ),
        env={"GCS_SA_KEYFILE": _SA_KEYFILE},
        append_env=True,    # inherit PATH, JAVA_HOME, etc. from container environment
    )

    @task
    def update_watermark(cfg: dict) -> None:
        Variable.set(_WATERMARK_VAR, cfg["run_end"])

    @task
    def sync_bigquery(cfg: dict) -> None:
        from bq_sync import sync_bq_external_table
        sync_bq_external_table(
            bucket         = cfg["bucket"],
            iceberg_prefix = "iceberg/silver/weather",
            bq_table       = "weather",
            keyfile        = _SA_KEYFILE,
        )

    @task(outlets=[SILVER_WEATHER])
    def mark_silver_weather_done() -> None:
        """Báo hiệu SILVER_WEATHER dataset đã cập nhật → trigger gold_dbt_dag."""
        pass

    config >> submit_spark_job >> [update_watermark(config), sync_bigquery(config)] >> mark_silver_weather_done()


silver_weather_dag()
