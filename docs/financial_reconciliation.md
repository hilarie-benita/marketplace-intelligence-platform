# Financial Reconciliation — Olist Marketplace

## Objective

Reconcile order-item merchandise and freight values against recorded payment values, identify discrepancies, and evaluate their relationship with order status.

## Data Sources

- Gold `fact_orders`
- Gold `fact_order_items`
- Gold `fact_payments`

**Reconciliation grain:** One row per order (`order_id`).

## Financial Summary

| Metric | Value (BRL) |
|---|---:|
| Merchandise value | 13,591,643.70 |
| Freight value | 2,251,909.54 |
| Merchandise + freight | 15,843,553.24 |
| Recorded payment value | 16,008,872.12 |
| Net difference | 165,318.88 |

## Reconciliation Results

| Classification | Orders | Net difference (BRL) |
|---|---:|---:|
| Matched within R$0.01 | 98,362 | -0.67 |
| Missing item records | 775 | 162,591.95 |
| Payments exceed items | 264 | 3,070.14 |
| Items exceed payments | 39 | -199.08 |
| Missing payment records | 1 | -143.46 |

Approximately 98.91% of orders reconcile within the configured one-cent tolerance.

## Key Findings

### Missing Item Records

Of the 775 orders with payment records but no order-item records:

- 603 are unavailable, representing R$124,339.02.
- 164 are canceled, representing R$37,337.87.
- 5 are created, representing R$688.10.
- 2 are invoiced, representing R$149.23.
- 1 is shipped, representing R$77.73.

Canceled and unavailable orders account for approximately 99.44% of the payment value associated with missing item records.

### Delivered Order Discrepancies

Among delivered orders:

- 260 have payment values exceeding item-plus-freight values.
- 39 have item-plus-freight values exceeding payment values.
- 1 has no payment record.

These exceptions require further investigation before their causes can be established.

### Minor Differences

Among orders classified as matched, 273 have differences of exactly one cent:

- 170 orders: -R$0.01
- 103 orders: +R$0.01

Rounding or numerical precision is a possible explanation, but has not been confirmed.

## Analytical Decisions

1. Preserve original financial amounts rather than forcing reconciliation.
2. Maintain separate merchandise, freight, and payment measures.
3. Report reconciliation exceptions independently.
4. Provide order-status filters for financial and operational analysis.
5. Avoid describing recorded payment values as confirmed collected revenue.
6. Do not infer refunds or settlements without supporting transaction data.

## Limitations

The dataset does not provide sufficient information to establish refund completion, payment settlement, or formal revenue recognition.

The reconciliation identifies differences and their associations with order status; it does not prove the underlying financial cause.

## Conclusion

Most of the net discrepancy is associated with orders that have recorded payments but no item records, particularly unavailable and canceled orders.

The results demonstrate the importance of validating business meaning and transaction coverage before reporting marketplace financial KPIs.