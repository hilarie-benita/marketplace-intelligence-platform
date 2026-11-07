
SELECT
    o.order_id,
    o.customer_id,
    o.purchase_date_key,
    CAST(o.order_purchase_timestamp AS DATE) AS purchase_date,
    EXTRACT('month' FROM o.order_purchase_timestamp) AS purchase_month,
    EXTRACT('dow' FROM o.order_purchase_timestamp) AS purchase_day_of_week,
    EXTRACT('hour' FROM o.order_purchase_timestamp) AS purchase_hour,
    c.customer_state,
    c.customer_city,
    o.order_estimated_delivery_date,
    DATE_DIFF(
        'day',
        CAST(o.order_purchase_timestamp AS DATE),
        CAST(o.order_estimated_delivery_date AS DATE)
    ) AS promised_delivery_days,
    CAST(o.is_late_delivery AS INTEGER) AS is_late_delivery
FROM gold.fact_orders o
INNER JOIN gold.dim_customers c
    ON o.customer_id = c.customer_id
WHERE o.is_late_delivery IS NOT NULL
  AND o.order_purchase_timestamp IS NOT NULL
  AND o.order_estimated_delivery_date IS NOT NULL;
