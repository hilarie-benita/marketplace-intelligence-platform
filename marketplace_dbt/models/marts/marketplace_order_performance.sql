
SELECT
    DATE_TRUNC('month', order_purchase_timestamp)
        AS order_month,

    COUNT(*) AS total_orders,

    COUNT(*) FILTER (
        WHERE order_status = 'delivered'
    ) AS delivered_orders,

    COUNT(*) FILTER (
        WHERE order_status = 'canceled'
    ) AS canceled_orders,

    COUNT(*) FILTER (
        WHERE order_status = 'unavailable'
    ) AS unavailable_orders,

    COUNT(*) FILTER (
        WHERE is_late_delivery = TRUE
    ) AS late_deliveries,

    COUNT(*) FILTER (
        WHERE is_late_delivery = FALSE
    ) AS on_time_deliveries,

    COUNT(*) FILTER (
        WHERE is_late_delivery IS NULL
    ) AS unknown_delivery_status_orders,

    ROUND(
        100.0 * COUNT(*) FILTER (
            WHERE is_late_delivery = FALSE
        )
        / NULLIF(
            COUNT(*) FILTER (
                WHERE is_late_delivery IS NOT NULL
            ),
            0
        ),
        2
    ) AS on_time_delivery_rate_pct,

    ROUND(
        AVG(delivery_days) FILTER (
            WHERE order_status = 'delivered'
        ),
        2
    ) AS avg_delivery_days

FROM {{ source('gold', 'fact_orders') }}

GROUP BY 1
ORDER BY 1
