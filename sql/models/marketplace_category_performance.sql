
WITH category_items AS (
    SELECT
        COALESCE(p.category_name_english, 'Unknown') AS product_category,
        i.order_id,
        i.product_id,
        i.seller_id,
        i.price,
        i.freight_value,
        i.is_late_delivery
    FROM gold.fact_order_items i
    LEFT JOIN gold.dim_products p
        ON i.product_id = p.product_id
),

category_metrics AS (
    SELECT
        product_category,
        COUNT(*) AS total_items_sold,
        COUNT(DISTINCT order_id) AS total_orders,
        COUNT(DISTINCT product_id) AS distinct_products,
        COUNT(DISTINCT seller_id) AS distinct_sellers,
        ROUND(SUM(price), 2) AS merchandise_value,
        ROUND(SUM(freight_value), 2) AS freight_value,
        ROUND(AVG(price), 2) AS avg_item_price,
        COUNT(*) FILTER (
            WHERE is_late_delivery = TRUE
        ) AS late_items,
        COUNT(*) FILTER (
            WHERE is_late_delivery = FALSE
        ) AS on_time_items
    FROM category_items
    GROUP BY product_category
)

SELECT
    product_category,
    total_items_sold,
    total_orders,
    distinct_products,
    distinct_sellers,
    merchandise_value,
    freight_value,
    avg_item_price,
    late_items,
    on_time_items,
    ROUND(
        100.0 * on_time_items
        / NULLIF(late_items + on_time_items, 0),
        2
    ) AS on_time_delivery_rate_pct
FROM category_metrics;
