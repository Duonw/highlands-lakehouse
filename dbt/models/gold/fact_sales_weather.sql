{{ config(materialized='table') }}

-- Model: fact_sales_weather
-- Grain: (date_hour, order_type, product_category, store_id, city_id)

WITH orders AS (
    SELECT * FROM {{ source('silver_ext', 'orders') }}
    WHERE status != 'Cancelled'  -- loại đơn hủy, chỉ tính doanh thu thực
),

order_details AS (
    SELECT * FROM {{ source('silver_ext', 'order_details') }}
),

products AS (
    SELECT * FROM {{ source('silver_ext', 'products') }}
),

stores AS (
    SELECT * FROM {{ source('silver_ext', 'stores') }}
),

weather AS (
    SELECT * FROM {{ ref('weather_consensus') }}
)

SELECT
    -- Thời gian
    TIMESTAMP_TRUNC(o.created_at, HOUR) AS date_hour,
    DATE(o.created_at)                  AS order_date,

    -- Địa điểm
    st.city_id,
    o.store_id,

    -- Thời tiết
    w.majority_condition                AS weather_condition,
    w.avg_temperature                   AS temperature_c,

    -- Dimensions
    o.order_type,
    p.category                          AS product_category,

    -- Metrics
    COUNT(DISTINCT o.order_id)          AS total_orders,
    SUM(od.quantity)                    AS total_items_sold,
    SUM(od.subtotal)                    AS total_revenue

FROM orders o
JOIN order_details od
    ON o.order_id = od.order_id

-- SCD2: lấy phiên bản product đúng tại thời điểm order
JOIN products p
    ON od.product_id = p.product_id
    AND o.created_at >= p.valid_from
    AND (p.valid_to IS NULL OR o.created_at < p.valid_to)

-- SCD2: lấy phiên bản store đúng tại thời điểm order (để lấy city_id lịch sử)
JOIN stores st
    ON o.store_id = st.store_id
    AND o.created_at >= st.valid_from
    AND (st.valid_to IS NULL OR o.created_at < st.valid_to)

-- Thời tiết theo giờ và thành phố
LEFT JOIN weather w
    ON st.city_id = w.city_id
    AND TIMESTAMP_TRUNC(o.created_at, HOUR) = w.date_hour

WHERE w.majority_condition IS NOT NULL  -- loại orders không có weather data

GROUP BY 1, 2, 3, 4, 5, 6, 7, 8

