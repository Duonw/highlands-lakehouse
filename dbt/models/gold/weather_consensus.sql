-- Model: weather_consensus
-- Mục đích: Tổng hợp dữ liệu thời tiết từ nhiều source (openweather, openmeteo, tomorrowio)
-- thành 1 bản ghi duy nhất cho mỗi (city, giờ).
--
-- Tại sao cần model này:
--   Silver có 3 bản ghi mỗi giờ mỗi thành phố (1 per source).
--   fact_sales_weather cần join 1-1 theo (city_id, date_hour) → cần aggregate trước.
--
-- APPROX_TOP_COUNT: đếm giá trị xuất hiện nhiều nhất trong condition (majority vote).
--   Nếu openweather nói "Clear", openmeteo nói "Clear", tomorrowio nói "Cloudy"
--   → majority_condition = "Clear"

SELECT
    city_id,
    city_name,
    TIMESTAMP_TRUNC(fetched_at, HOUR)            AS date_hour,
    AVG(temperature)                             AS avg_temperature,
    AVG(humidity)                                AS avg_humidity,
    AVG(pressure)                                AS avg_pressure,
    AVG(wind_speed)                              AS avg_wind_speed,
    APPROX_TOP_COUNT(condition, 1)[OFFSET(0)].value AS majority_condition,
    COUNT(DISTINCT source)                       AS source_count,
    STDDEV(temperature)                          AS temperature_stddev
FROM {{ source('silver_ext', 'weather') }}
WHERE fetched_at IS NOT NULL
GROUP BY city_id, city_name, date_hour
