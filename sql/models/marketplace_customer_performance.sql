
WITH item_totals AS (
    SELECT
        order_id,
        SUM(price) AS merchandise_value,
        SUM(freight_value) AS freight_value
    FROM gold.fact_order_items
    GROUP BY order_id
),

customer_orders AS (
    SELECT
        o.customer_unique_id,
        o.order_id,
        o.order_status,
        o.order_purchase_timestamp,
        o.is_late_delivery,
        i.merchandise_value,
        i.freight_value
    FROM gold.fact_orders o
    LEFT JOIN item_totals i
        ON o.order_id = i.order_id
),

customer_metrics AS (
    SELECT
        customer_unique_id,
        COUNT(*) AS total_orders,
        COUNT(*) FILTER (
            WHERE order_status = 'delivered'
        ) AS delivered_orders,
        MIN(order_purchase_timestamp) AS first_order_date,
        MAX(order_purchase_timestamp) AS last_order_date,
        ROUND(SUM(merchandise_value), 2) AS total_merchandise_value,
        ROUND(SUM(freight_value), 2) AS total_freight_value,
        ROUND(AVG(merchandise_value), 2) AS avg_order_merchandise_value,
        COUNT(*) FILTER (
            WHERE is_late_delivery = TRUE
        ) AS late_orders,
        COUNT(*) FILTER (
            WHERE is_late_delivery = FALSE
        ) AS on_time_orders
    FROM customer_orders
    GROUP BY customer_unique_id
)

SELECT
    customer_unique_id,
    total_orders,
    delivered_orders,
    first_order_date,
    last_order_date,
    DATE_DIFF(
        'day',
        CAST(first_order_date AS DATE),
        CAST(last_order_date AS DATE)
    ) AS customer_lifespan_days,
    total_merchandise_value,
    total_freight_value,
    avg_order_merchandise_value,
    late_orders,
    on_time_orders,
    CASE
        WHEN total_orders > 1 THEN 'Repeat Customer'
        ELSE 'One-Time Customer'
    END AS customer_segment
FROM customer_metrics;
