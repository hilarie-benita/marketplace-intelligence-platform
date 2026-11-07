
SELECT *
FROM {{ ref('marketplace_delivery_risk_features') }}
WHERE purchase_date_key IS NULL
   OR purchase_month IS NULL
   OR purchase_day_of_week IS NULL
   OR purchase_hour IS NULL
   OR customer_state IS NULL
   OR customer_city IS NULL
   OR order_estimated_delivery_date IS NULL
   OR promised_delivery_days <= 0
   OR purchase_month NOT BETWEEN 1 AND 12
   OR purchase_day_of_week NOT BETWEEN 0 AND 6
   OR purchase_hour NOT BETWEEN 0 AND 23
   OR CAST(order_estimated_delivery_date AS DATE) < purchase_date
   OR promised_delivery_days <> DATE_DIFF(
       'day',
       purchase_date,
       CAST(order_estimated_delivery_date AS DATE)
   )
   OR is_late_delivery NOT IN (0, 1)
