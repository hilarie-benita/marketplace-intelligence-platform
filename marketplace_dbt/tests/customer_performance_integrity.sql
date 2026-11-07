
SELECT *
FROM {{ ref('marketplace_customer_performance') }}
WHERE total_orders < 1
   OR delivered_orders < 0
   OR delivered_orders > total_orders
   OR late_orders < 0
   OR on_time_orders < 0
   OR late_orders + on_time_orders > total_orders
   OR customer_lifespan_days < 0
   OR first_order_date > last_order_date
   OR COALESCE(total_merchandise_value, 0) < 0
   OR COALESCE(total_freight_value, 0) < 0
   OR COALESCE(avg_order_merchandise_value, 0) < 0
   OR (
       total_orders = 1
       AND customer_segment <> 'One-Time Customer'
   )
   OR (
       total_orders > 1
       AND customer_segment <> 'Repeat Customer'
   )
