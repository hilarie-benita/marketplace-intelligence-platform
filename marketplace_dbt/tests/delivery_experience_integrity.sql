
SELECT *
FROM {{ ref('marketplace_delivery_experience') }}
WHERE total_orders < 0
   OR reviewed_orders < 0
   OR total_reviews < 0
   OR negative_reviews < 0
   OR positive_reviews < 0
   OR reviewed_orders > total_orders
   OR total_reviews < reviewed_orders
   OR negative_reviews + positive_reviews > total_reviews
   OR (
       avg_order_review_score IS NOT NULL
       AND (
           avg_order_review_score < 1
           OR avg_order_review_score > 5
       )
   )
   OR (
       avg_delivery_days IS NOT NULL
       AND avg_delivery_days < 0
   )
