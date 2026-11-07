
WITH reference_date AS (
    SELECT
        MAX(CAST(order_purchase_timestamp AS DATE)) AS snapshot_date
    FROM gold.fact_orders
),

customer_rfm AS (
    SELECT
        c.customer_unique_id,
        c.total_orders AS frequency,
        COALESCE(c.total_merchandise_value, 0) AS monetary_value,
        DATE_DIFF(
            'day',
            CAST(c.last_order_date AS DATE),
            r.snapshot_date
        ) AS recency_days
    FROM analytics.marketplace_customer_performance c
    CROSS JOIN reference_date r
),

rfm_scores AS (
    SELECT
        *,
        NTILE(5) OVER (
            ORDER BY recency_days DESC, customer_unique_id
        ) AS recency_score,
        CASE
            WHEN frequency >= 5 THEN 5
            WHEN frequency = 4 THEN 4
            WHEN frequency = 3 THEN 3
            WHEN frequency = 2 THEN 2
            ELSE 1
        END AS frequency_score,
        NTILE(5) OVER (
            ORDER BY monetary_value ASC, customer_unique_id
        ) AS monetary_score
    FROM customer_rfm
)

SELECT
    customer_unique_id,
    recency_days,
    frequency,
    monetary_value,
    recency_score,
    frequency_score,
    monetary_score,
    CASE
        WHEN frequency > 1
             AND recency_score >= 4
             AND monetary_score >= 4
            THEN 'High-Value Repeat'
        WHEN frequency > 1
            THEN 'Repeat Customer'
        WHEN recency_score >= 4
            THEN 'Recently Active'
        WHEN recency_score <= 2
            THEN 'Long-Inactive'
        ELSE 'Occasional Customer'
    END AS rfm_segment
FROM rfm_scores;
