
SELECT *
FROM {{ ref('marketplace_customer_rfm') }}
WHERE recency_days < 0
   OR frequency < 1
   OR monetary_value < 0
   OR recency_score IS NULL
   OR frequency_score IS NULL
   OR monetary_score IS NULL
   OR recency_score NOT BETWEEN 1 AND 5
   OR frequency_score NOT BETWEEN 1 AND 5
   OR monetary_score NOT BETWEEN 1 AND 5
   OR frequency_score <> CASE
       WHEN frequency >= 5 THEN 5
       WHEN frequency = 4 THEN 4
       WHEN frequency = 3 THEN 3
       WHEN frequency = 2 THEN 2
       ELSE 1
   END
   OR rfm_segment <> CASE
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
   END
