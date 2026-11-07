
WITH seller_orders AS (
    SELECT
        seller_id,
        order_id,
        COUNT(*) AS items_sold,
        SUM(price) AS merchandise_value,
        SUM(freight_value) AS freight_value,
        MAX(CASE WHEN is_late_delivery = TRUE THEN 1 ELSE 0 END) AS is_late,
        MAX(CASE WHEN is_late_delivery = FALSE THEN 1 ELSE 0 END) AS is_on_time
    FROM {{ source('gold', 'fact_order_items') }}
    GROUP BY seller_id, order_id
),

seller_metrics AS (
    SELECT
        seller_id,
        COUNT(*) AS total_orders,
        SUM(items_sold) AS total_items_sold,
        ROUND(SUM(merchandise_value), 2) AS total_merchandise_value,
        ROUND(SUM(freight_value), 2) AS total_freight_value,
        SUM(is_late) AS late_orders,
        SUM(is_on_time) AS on_time_orders,
        ROUND(
            100.0 * SUM(is_on_time)
            / NULLIF(SUM(is_late) + SUM(is_on_time), 0),
            2
        ) AS on_time_delivery_rate_pct
    FROM seller_orders
    GROUP BY seller_id
)

SELECT
    s.seller_id,
    s.seller_city,
    s.seller_state,
    COALESCE(m.total_orders, 0) AS total_orders,
    COALESCE(m.total_items_sold, 0) AS total_items_sold,
    COALESCE(m.total_merchandise_value, 0) AS total_merchandise_value,
    COALESCE(m.total_freight_value, 0) AS total_freight_value,
    COALESCE(m.late_orders, 0) AS late_orders,
    COALESCE(m.on_time_orders, 0) AS on_time_orders,
    m.on_time_delivery_rate_pct
FROM {{ source('gold', 'dim_sellers') }} s
LEFT JOIN seller_metrics m
    ON s.seller_id = m.seller_id

