import os
import sys

# Thêm thư mục chứa file này vào Python path để import package `weather`
sys.path.insert(0, os.path.dirname(__file__))

from airflow.sdk import dag, task
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from datetime import datetime, timedelta

from weather.fetcher import OpenWeatherFetcher
from weather.loader import GcsBronzeLoader
from weather.repository import MetadataRepository
from pipeline_datasets import BRONZE_OPENWEATHER

_MSSQL_HIGHLANDS_ID = "mssql_highlands"
_OPENWEATHER_CODE   = "openweather"


def _get_highlands_engine():
    return MsSqlHook(mssql_conn_id=_MSSQL_HIGHLANDS_ID).get_sqlalchemy_engine()


@dag(
    start_date=datetime(2026, 4, 26),
    schedule="@hourly",
    catchup=False,
    # Retry cho task gọi API bên ngoài: timeout/rate-limit/5xx là chuyện bình thường.
    # retries=2: thử lại tối đa 2 lần sau khi fail
    # retry_delay: chờ 2 phút trước mỗi lần retry (tránh spam API)
    # retry_exponential_backoff=True: lần 1 chờ 2p, lần 2 chờ 4p (tránh thundering herd)
    default_args={
        "retries":                    2,
        "retry_delay":                timedelta(minutes=2),
        "retry_exponential_backoff":  True,
    },
    tags=["weather", "etl", "openweather"],
)
def weather_openweather_pipeline():

    @task
    def fetch_weather_data() -> list[dict]:
        api_key = os.environ.get("OPENWEATHER_API_KEY")
        if not api_key:
            raise ValueError("OPENWEATHER_API_KEY không tìm thấy trong environment variables")

        repo    = MetadataRepository(_get_highlands_engine())
        regions = repo.get_cities()
        # fetch_all_with_raw() trả về thêm: condition, raw_response, city_name
        return OpenWeatherFetcher(api_key).fetch_all_with_raw(regions)

    @task(outlets=[BRONZE_OPENWEATHER])
    def push_to_gcs_bronze(raw_data: list[dict]) -> list[str]:
        """
        Ghi raw JSON response lên GCS Bronze.
        Trả về list GCS URI để log / audit trail.
        outlets=[BRONZE_OPENWEATHER] → Airflow đánh dấu dataset này đã update
        → trigger silver_weather_dag khi cả 3 bronze dataset đều done.
        """
        loader = GcsBronzeLoader()
        uris = []
        for row in raw_data:
            uri = loader.upload_weather(
                source    = _OPENWEATHER_CODE,
                city_id   = row["city_id"],
                city_name = row["city_name"],
                raw_data  = row["raw_response"],
            )
            uris.append(uri)
        return uris

    @task
    def update_current_weather(raw_data: list[dict]) -> None:
        """
        Cập nhật CurrentWeatherState trong HighlandsDB để order simulator
        biết thời tiết hiện tại của từng thành phố.

        Lưu ý: đây là bảng hot cache (chỉ lưu trạng thái mới nhất),
        KHÔNG liên quan đến pipeline Bronze/Silver/Gold.
        """
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

    # 2 nhánh chạy song song sau khi fetch xong:
    push_to_gcs_bronze(raw)      # nhánh 1: raw → GCS Bronze (outlet → trigger silver_weather)
    update_current_weather(raw)  # nhánh 2: cập nhật CurrentWeatherState


weather_openweather_pipeline()
