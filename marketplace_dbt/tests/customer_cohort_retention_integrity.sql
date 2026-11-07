
WITH cohort_checks AS (
    SELECT
        *,
        COUNT(*) OVER (
            PARTITION BY cohort_month, months_since_first_purchase
        ) AS cohort_period_count,
        MIN(cohort_size) OVER (
            PARTITION BY cohort_month
        ) AS minimum_cohort_size,
        MAX(cohort_size) OVER (
            PARTITION BY cohort_month
        ) AS maximum_cohort_size
    FROM {{ ref('marketplace_customer_cohort_retention') }}
)

SELECT *
FROM cohort_checks
WHERE months_since_first_purchase < 0
   OR cohort_size <= 0
   OR active_customers <= 0
   OR active_customers > cohort_size
   OR retention_rate_pct < 0
   OR retention_rate_pct > 100
   OR cohort_period_count > 1
   OR minimum_cohort_size <> maximum_cohort_size
   OR ABS(
       retention_rate_pct
       - ROUND(100.0 * active_customers / cohort_size, 2)
   ) > 0.01
   OR (
       months_since_first_purchase = 0
       AND (
           active_customers <> cohort_size
           OR retention_rate_pct <> 100
       )
   )
