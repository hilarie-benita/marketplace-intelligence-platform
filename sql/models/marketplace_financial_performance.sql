
WITH item_totals AS (
    SELECT
        order_id,
        SUM(price) AS merchandise_value,
        SUM(freight_value) AS freight_value,
        SUM(price + freight_value) AS item_total_value
    FROM gold.fact_order_items
    GROUP BY order_id
),

payment_totals AS (
    SELECT
        order_id,
        SUM(payment_value) AS payment_value
    FROM gold.fact_payments
    GROUP BY order_id
),

order_financials AS (
    SELECT
        o.order_id,
        o.order_status,
        DATE_TRUNC('month', o.order_purchase_timestamp) AS order_month,
        i.merchandise_value,
        i.freight_value,
        i.item_total_value,
        p.payment_value,
        CASE
            WHEN i.order_id IS NULL THEN 'Missing item records'
            WHEN p.order_id IS NULL THEN 'Missing payment records'
            WHEN ABS(
                ROUND(p.payment_value - i.item_total_value, 2)
            ) <= 0.01
                    THEN 'Reconciled'
            ELSE 'Discrepancy'
        END AS reconciliation_status
    FROM gold.fact_orders o
    LEFT JOIN item_totals i
        ON o.order_id = i.order_id
    LEFT JOIN payment_totals p
        ON o.order_id = p.order_id
)

SELECT
    order_month,
    COUNT(*) AS total_orders,
    ROUND(SUM(merchandise_value), 2) AS merchandise_value,
    ROUND(SUM(freight_value), 2) AS freight_value,
    ROUND(SUM(item_total_value), 2) AS item_total_value,
    ROUND(SUM(payment_value), 2) AS payment_value,
    COUNT(*) FILTER (
        WHERE reconciliation_status = 'Reconciled'
    ) AS reconciled_orders,
    COUNT(*) FILTER (
        WHERE reconciliation_status = 'Discrepancy'
    ) AS discrepancy_orders,
    COUNT(*) FILTER (
        WHERE reconciliation_status = 'Missing item records'
    ) AS missing_item_orders,
    COUNT(*) FILTER (
        WHERE reconciliation_status = 'Missing payment records'
    ) AS missing_payment_orders
FROM order_financials
GROUP BY order_month
ORDER BY order_month;
