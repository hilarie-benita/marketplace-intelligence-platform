
SELECT *
FROM {{ ref('marketplace_seller_performance') }}
WHERE total_orders < 0
   OR total_items_sold < 0
   OR total_merchandise_value < 0
   OR total_freight_value < 0
   OR late_orders < 0
   OR on_time_orders < 0
   OR late_orders > total_orders
   OR on_time_orders > total_orders
   OR (
       on_time_delivery_rate_pct IS NOT NULL
       AND (
           on_time_delivery_rate_pct < 0
           OR on_time_delivery_rate_pct > 100
       )
   )
   OR (
       total_orders = 0
       AND total_items_sold > 0
   )
