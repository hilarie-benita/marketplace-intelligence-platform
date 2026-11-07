
WITH customer_orders AS (
    SELECT
        customer_unique_id,
        order_id,
        CAST(
            DATE_TRUNC('month', order_purchase_timestamp)
            AS DATE
        ) AS order_month
    FROM {{ source('gold', 'fact_orders') }}
    WHERE order_purchase_timestamp IS NOT NULL
),

first_purchase AS (
    SELECT
        customer_unique_id,
        MIN(order_month) AS cohort_month
    FROM customer_orders
    GROUP BY customer_unique_id
),

cohort_activity AS (
    SELECT DISTINCT
        o.customer_unique_id,
        f.cohort_month,
        o.order_month,
        DATE_DIFF(
            'month',
            f.cohort_month,
            o.order_month
        ) AS months_since_first_purchase
    FROM customer_orders o
    INNER JOIN first_purchase f
        ON o.customer_unique_id = f.customer_unique_id
),

cohort_sizes AS (
    SELECT
        cohort_month,
        COUNT(*) AS cohort_size
    FROM first_purchase
    GROUP BY cohort_month
),

retention_metrics AS (
    SELECT
        cohort_month,
        months_since_first_purchase,
        COUNT(*) AS active_customers
    FROM cohort_activity
    GROUP BY
        cohort_month,
        months_since_first_purchase
)

SELECT
    r.cohort_month,
    r.months_since_first_purchase,
    c.cohort_size,
    r.active_customers,
    ROUND(
        100.0 * r.active_customers
        / NULLIF(c.cohort_size, 0),
        2
    ) AS retention_rate_pct
FROM retention_metrics r
INNER JOIN cohort_sizes c
    ON r.cohort_month = c.cohort_month
