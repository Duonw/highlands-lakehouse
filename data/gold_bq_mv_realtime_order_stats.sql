-- ============================================================
-- BigQuery Materialized View: gold.realtime_order_stats
-- ============================================================
-- MỤC ĐÍCH:
--   Dashboard real-time đếm đơn hàng theo giờ, store, status.
--   Không JOIN SCD2 → BQ native MV hỗ trợ được.
--   BQ tự động refresh mỗi 5 phút khi silver_ext.orders có data mới.
--
-- KHÁC với dbt table (fact_sales_weather):
--   dbt table    → chạy 1 lần/giờ do Airflow trigger, có SCD2 + weather JOIN
--   MV này       → BQ tự refresh mỗi 5 phút, chỉ 1 bảng source, gần real-time
--
-- CÁCH CHẠY:
--   Paste SQL này vào BQ Console → Run (1 lần duy nhất)
--   Sau đó BQ tự maintain, không cần Airflow hay dbt.
--
-- LƯU Ý:
--   BQ MV trên External Table (Iceberg) chỉ hỗ trợ aggregation đơn giản:
--   COUNT, SUM, MIN, MAX — không hỗ trợ JOIN nhiều bảng external.
-- ============================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS `highlands-lakehouse.gold.realtime_order_stats`
OPTIONS (
  -- BQ tự refresh khi detect data mới trong source table
  -- Tần suất tối thiểu: 1 phút, mặc định: 5 phút
  enable_refresh = true,
  refresh_interval_minutes = 5
)
AS
SELECT
  -- Grain: (giờ, store, order_type, status)
  TIMESTAMP_TRUNC(created_at, HOUR)  AS order_hour,
  DATE(created_at)                   AS order_date,
  store_id,
  order_type,                         -- 'Delivery' | 'Dine-in'
  status,                             -- 'Pending' | 'Preparing' | 'Completed' | 'Cancelled'

  COUNT(*)                            AS order_count,
  SUM(total_amount)                   AS total_revenue,
  AVG(total_amount)                   AS avg_order_value

FROM `highlands-lakehouse.silver_ext.orders`
WHERE created_at IS NOT NULL

GROUP BY 1, 2, 3, 4, 5;
