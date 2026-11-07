
WITH review_metrics AS (
    SELECT
        order_id,
        COUNT(*) AS review_count,
        AVG(review_score) AS avg_review_score,
        COUNT(*) FILTER (
            WHERE review_score <= 2
        ) AS negative_reviews,
        COUNT(*) FILTER (
            WHERE review_score >= 4
        ) AS positive_reviews
    FROM {{ source('gold', 'fact_reviews') }}
    GROUP BY order_id
),

order_experience AS (
    SELECT
        o.order_id,
        o.order_status,
        o.is_late_delivery,
        o.delivery_days,
        o.delivery_delay_days,
        r.review_count,
        r.avg_review_score,
        r.negative_reviews,
        r.positive_reviews
    FROM {{ source('gold', 'fact_orders') }} o
    LEFT JOIN review_metrics r
        ON o.order_id = r.order_id
)

SELECT
    CASE
        WHEN is_late_delivery = TRUE THEN 'Late Delivery'
        WHEN is_late_delivery = FALSE THEN 'On-Time Delivery'
        ELSE 'Unknown Delivery Status'
    END AS delivery_category,
    COUNT(*) AS total_orders,
    COUNT(*) FILTER (
        WHERE review_count IS NOT NULL
    ) AS reviewed_orders,
    SUM(COALESCE(review_count, 0)) AS total_reviews,
    ROUND(AVG(avg_review_score), 2) AS avg_order_review_score,
    ROUND(AVG(delivery_days), 2) AS avg_delivery_days,
    ROUND(AVG(delivery_delay_days), 2) AS avg_delivery_delay_days,
    SUM(COALESCE(negative_reviews, 0)) AS negative_reviews,
    SUM(COALESCE(positive_reviews, 0)) AS positive_reviews
FROM order_experience
GROUP BY delivery_category
