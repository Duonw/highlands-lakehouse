# pipeline_datasets.py
# ─────────────────────────────────────────────────────────────────────────────
# Khai báo tập trung tất cả Airflow Datasets dùng trong pipeline.
#
# Dataset trong Airflow là một "mỏ neo" logic — không nhất thiết là file thật,
# mà là tín hiệu "data ở đây đã được cập nhật".
#
# Cách dùng:
#   - DAG Producer: khai báo outlets=[DATASET] trên task cuối cùng
#     → Airflow ghi nhận "dataset này có data mới" khi task success
#   - DAG Consumer: khai báo schedule=[DATASET_A, DATASET_B]
#     → Airflow chỉ trigger DAG khi TẤT CẢ các dataset trong list đều có update
#
# Naming convention: URI dùng scheme "ds://" để phân biệt với GCS URI thật.

from airflow.datasets import Dataset

# ── Bronze layer — 3 nguồn API thời tiết ─────────────────────────────────────
BRONZE_OPENWEATHER  = Dataset("ds://bronze/weather/openweather")
BRONZE_OPENMETEO    = Dataset("ds://bronze/weather/openmeteo")
BRONZE_TOMORROWIO   = Dataset("ds://bronze/weather/tomorrowio")

# ── Silver layer — 2 pipeline ─────────────────────────────────────────────────
SILVER_WEATHER  = Dataset("ds://silver/weather")   # producer: silver_weather_dag
SILVER_CDC      = Dataset("ds://silver/cdc")       # producer: silver_cdc_dag
