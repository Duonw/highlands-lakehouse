import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from airflow.sdk import dag, task
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from datetime import datetime, timedelta

from weather.fetcher import OpenMeteoFetcher
from weather.loader import GcsBronzeLoader
from weather.repository import MetadataRepository
from pipeline_datasets import BRONZE_OPENMETEO

_MSSQL_HIGHLANDS_ID = "mssql_highlands"
_OPENMETEO_CODE     = "openmeteo"


def _get_highlands_engine():
    return MsSqlHook(mssql_conn_id=_MSSQL_HIGHLANDS_ID).get_sqlalchemy_engine()


@dag(
    start_date=datetime(2026, 4, 26),
    schedule="@hourly",
    catchup=False,
    default_args={
        "retries":                    2,
        "retry_delay":                timedelta(minutes=2),
        "retry_exponential_backoff":  True,
    },
    tags=["weather", "etl", "openmeteo"],
)
def weather_openmeteo_pipeline():

    @task
    def fetch_weather_data() -> list[dict]:
        repo    = MetadataRepository(_get_highlands_engine())
        regions = repo.get_cities()
        # fetch_all_with_raw() trả về thêm: condition (từ WMO code), raw_response, city_name
        return OpenMeteoFetcher().fetch_all_with_raw(regions)

    @task(outlets=[BRONZE_OPENMETEO])
    def push_to_gcs_bronze(raw_data: list[dict]) -> list[str]:
        """Ghi raw JSON response lên GCS Bronze.
        outlets=[BRONZE_OPENMETEO] → báo hiệu cho silver_weather_dag.
        """
        loader = GcsBronzeLoader()
        uris = []
        for row in raw_data:
            uri = loader.upload_weather(
                source    = _OPENMETEO_CODE,
                city_id   = row["city_id"],
                city_name = row["city_name"],
                raw_data  = row["raw_response"],
            )
            uris.append(uri)
        return uris

    @task
    def update_current_weather(raw_data: list[dict]) -> None:
        """Cập nhật CurrentWeatherState trong HighlandsDB (hot cache cho order simulator)."""
        repo = MetadataRepository(_get_highlands_engine())
        for row in raw_data:
            repo.upsert_weather_state(
                city_id     = row["city_id"],
                condition   = row["condition"],
                temperature = row["temp"],
                wind_speed  = row["wind"],
                humidity    = int(row["humidity"]),
            )

    raw = fetch_weather_data()

    push_to_gcs_bronze(raw)      # nhánh 1: raw → GCS Bronze
    update_current_weather(raw)  # nhánh 2: cập nhật CurrentWeatherState


weather_openmeteo_pipeline()
