import io
import json
from datetime import datetime, timezone

import pandas as pd

from airflow.models import Variable


class GcsBronzeLoader:
    """
    Ghi raw weather API response lên GCS Bronze dưới dạng Parquet.

    Mỗi lần Airflow fetch 1 thành phố từ 1 source → 1 file Parquet.

    Path convention (Hive partitioning):
        bronze/weather/{source}/year={Y}/month={MM}/day={DD}/{city}_{HHmmss}.parquet

    Hive partitioning = mỗi subfolder là 1 partition.
    Khi PySpark / BigQuery đọc, chúng tự biết chỉ scan đúng ngày cần
    thay vì đọc toàn bộ bucket → nhanh hơn nhiều lần.

    Column raw_json giữ nguyên toàn bộ API response dưới dạng string
    → Bronze không bao giờ mất data gốc dù schema API thay đổi.
    """

    def __init__(self) -> None:
        from airflow.providers.google.cloud.hooks.gcs import GCSHook
        # Dùng Hook của Airflow — tự lấy credentials từ Connection "google_cloud_default"
        self._gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
        # Tên bucket lấy từ Airflow Variable, không hardcode
        self._bucket_name = Variable.get("GCS_BRONZE_BUCKET")

    def upload_weather(self, source: str, city_id: int, city_name: str,
                       raw_data: dict) -> str:
        """
        Ghi 1 API response lên GCS Bronze.

        Args:
            source:    tên source, vd "openweather", "openmeteo", "tomorrowio"
            city_id:   city_id trong HighlandsDB
            city_name: tên thành phố, vd "Hanoi"
            raw_data:  dict trả về từ API (chưa transform)

        Returns:
            GCS URI, vd "gs://highlands-lakehouse-2026/bronze/weather/..."
        """
        now = datetime.now(timezone.utc)

        # Hive-partitioned path: "Ho Chi Minh" → "ho_chi_minh"
        slug = city_name.lower().replace(" ", "_")
        blob_path = (
            f"bronze/weather/{source}/"
            f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
            f"{slug}_{now.strftime('%H%M%S')}.parquet"
        )

        df = pd.DataFrame([{
            "city_id":    city_id,
            "city_name":  city_name,
            "source":     source,
            "fetched_at": now.isoformat(),
            "raw_json":   json.dumps(raw_data, ensure_ascii=False),
            # ensure_ascii=False → giữ UTF-8, không encode tiếng Việt thành \uXXXX
        }])

        # Convert DataFrame → Parquet bytes trong RAM (không cần ghi file tạm ra disk)
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False, engine="pyarrow")
        buffer.seek(0)  # reset con trỏ về đầu buffer trước khi upload

        self._gcs_hook.upload(
            bucket_name=self._bucket_name,
            object_name=blob_path,
            data=buffer.getvalue(),
            mime_type="application/octet-stream"
        )

        return f"gs://{self._bucket_name}/{blob_path}"