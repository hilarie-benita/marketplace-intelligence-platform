
SELECT *
FROM {{ ref('marketplace_financial_performance') }}
WHERE reconciled_orders < 0
   OR discrepancy_orders < 0
   OR missing_item_orders < 0
   OR missing_payment_orders < 0
   OR reconciled_orders
      + discrepancy_orders
      + missing_item_orders
      + missing_payment_orders > total_orders
