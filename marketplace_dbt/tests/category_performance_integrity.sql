
SELECT *
FROM {{ ref('marketplace_category_performance') }}
WHERE total_items_sold < 0
   OR total_orders < 0
   OR distinct_products < 0
   OR distinct_sellers < 0
   OR merchandise_value < 0
   OR freight_value < 0
   OR avg_item_price < 0
   OR late_items < 0
   OR on_time_items < 0
   OR total_orders > total_items_sold
   OR distinct_products > total_items_sold
   OR distinct_sellers > total_items_sold
   OR late_items + on_time_items > total_items_sold
   OR (
       on_time_delivery_rate_pct IS NOT NULL
       AND (
           on_time_delivery_rate_pct < 0
           OR on_time_delivery_rate_pct > 100
       )
   )
