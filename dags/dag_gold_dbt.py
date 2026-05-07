"""
DAG: gold_dbt — chạy dbt để build Gold layer từ Silver External Tables

Flow:
    dbt_run → dbt_test (optional, chỉ warn không fail DAG)

Schedule: mỗi 30 phút — chạy sau Silver CDC (*/10) và Silver Weather (30 * * * *)
    đủ thời gian để cả 2 Silver DAG xong trước khi Gold refresh.

dbt project:
    /opt/airflow/dbt/              ← project root
    /opt/airflow/dbt/profiles.yml  ← BigQuery connection (SA keyfile)
    /opt/airflow/dbt/models/gold/  ← 2 model: weather_consensus, fact_sales_weather

Output:
    highlands-lakehouse.gold.weather_consensus   (BQ native table)
    highlands-lakehouse.gold.fact_sales_weather  (BQ native table)
"""

from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag
from pipeline_datasets import SILVER_WEATHER, SILVER_CDC

_DBT_DIR = "/opt/airflow/dbt"


@dag(
    start_date=datetime(2026, 4, 26),
    # Event-driven: chạy khi CẢ 2 silver dataset đều có update trong cùng 1 chu kỳ
    # silver_weather (event-based) + silver_cdc (:25 của mỗi giờ)
    # → gold sẽ run mỗi giờ sau khi cả 2 pipeline xong, không cần đoán giờ cụ thể
    schedule=[SILVER_WEATHER, SILVER_CDC],
    catchup=False,
    default_args={
        "retries":     1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["gold", "dbt"],
)
def gold_dbt_dag():

    # dbt run: đọc silver_ext.* → ghi gold.*
    # --profiles-dir chỉ cho dbt biết tìm profiles.yml ở đâu (không dùng ~/.dbt/)
    # --project-dir chỉ thư mục chứa dbt_project.yml
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {_DBT_DIR} && "
            f"dbt run --profiles-dir {_DBT_DIR} --project-dir {_DBT_DIR}"
        ),
        append_env=True,
    )

    # dbt test: kiểm tra not_null, unique trên Gold tables
    # trigger_rule=all_done → chạy dù dbt_run có warn, nhưng nếu run failed thì test cũng failed
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {_DBT_DIR} && "
            f"dbt test --profiles-dir {_DBT_DIR} --project-dir {_DBT_DIR}"
        ),
        append_env=True,
    )

    dbt_run >> dbt_test


gold_dbt_dag()
