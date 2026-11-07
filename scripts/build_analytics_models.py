
from pathlib import Path
import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATABASE_PATH = PROJECT_ROOT / "data" / "warehouse" / "marketplace.duckdb"
SQL_MODELS_DIR = PROJECT_ROOT / "sql" / "models"

EXPECTED_COUNTS = {
    "marketplace_customer_performance": 96096,
    "marketplace_financial_performance": 25,
    "marketplace_order_performance": 25,
    "marketplace_seller_performance": 3095,
}

print("\n" + "=" * 60)
print("MARKETPLACE INTELLIGENCE PLATFORM")
print("ANALYTICS MODEL BUILD")
print("=" * 60)

if not DATABASE_PATH.exists():
    raise FileNotFoundError(f"Database not found: {DATABASE_PATH}")

if not SQL_MODELS_DIR.exists():
    raise FileNotFoundError(f"SQL directory not found: {SQL_MODELS_DIR}")

sql_files = sorted(SQL_MODELS_DIR.glob("*.sql"))

if not sql_files:
    raise FileNotFoundError("No SQL models found.")

con = duckdb.connect(str(DATABASE_PATH))

try:
    con.execute("CREATE SCHEMA IF NOT EXISTS analytics")

    print("\nBUILDING ANALYTICAL MODELS")
    print("-" * 60)

    for sql_file in sql_files:
        model_name = sql_file.stem

        if not model_name.isidentifier():
            raise ValueError(f"Invalid model name: {model_name}")

        sql_query = sql_file.read_text(encoding="utf-8").strip().rstrip(";")

        if not sql_query:
            raise ValueError(f"Empty SQL model: {model_name}")

        con.execute(
            f"""
            CREATE OR REPLACE VIEW analytics.{model_name} AS
            {sql_query}
            """
        )

        row_count = con.execute(
            f"SELECT COUNT(*) FROM analytics.{model_name}"
        ).fetchone()[0]

        if model_name in EXPECTED_COUNTS:
            assert row_count == EXPECTED_COUNTS[model_name], (
                f"Unexpected row count for {model_name}: {row_count}"
            )

        print(f"PASS | analytics.{model_name}: {row_count:,} rows")

    print("\nVALIDATING ORDER PERFORMANCE")
    print("-" * 60)

    order_result = con.execute(
        """
        SELECT
            SUM(total_orders),
            SUM(delivered_orders),
            SUM(late_deliveries),
            SUM(on_time_deliveries),
            SUM(unknown_delivery_status_orders)
        FROM analytics.marketplace_order_performance
        """
    ).fetchone()

    assert order_result == (99441, 96478, 6535, 89941, 2965)

    print("PASS | Order performance reconciles to Gold")

    print("\nVALIDATING SELLER PERFORMANCE")
    print("-" * 60)

    seller_result = con.execute(
        """
        SELECT
            COUNT(*),
            SUM(total_items_sold),
            SUM(total_merchandise_value)
        FROM analytics.marketplace_seller_performance
        """
    ).fetchone()

    assert seller_result[0] == 3095
    assert seller_result[1] == 112650
    assert abs(float(seller_result[2]) - 13591643.70) < 0.01

    print("PASS | Seller performance reconciles to Gold")

    print("\nVALIDATING FINANCIAL PERFORMANCE")
    print("-" * 60)

    financial_result = con.execute(
        """
        SELECT
            SUM(total_orders),
            SUM(merchandise_value),
            SUM(freight_value),
            SUM(item_total_value),
            SUM(payment_value),
            SUM(reconciled_orders),
            SUM(discrepancy_orders),
            SUM(missing_item_orders),
            SUM(missing_payment_orders)
        FROM analytics.marketplace_financial_performance
        """
    ).fetchone()

    expected_financials = [
        99441,
        13591643.70,
        2251909.54,
        15843553.24,
        16008872.12,
        98362,
        303,
        775,
        1,
    ]

    for actual, expected in zip(financial_result, expected_financials):
        assert actual is not None
        assert abs(float(actual) - expected) < 0.01, (
            f"Financial mismatch: {actual} != {expected}"
        )

    print("PASS | Financial performance reconciles to Gold")

    print("\nVALIDATING CUSTOMER PERFORMANCE")
    print("-" * 60)

    customer_result = con.execute(
        """
        SELECT
            COUNT(*),
            COUNT(DISTINCT customer_unique_id),
            SUM(total_orders),
            SUM(delivered_orders),
            SUM(total_merchandise_value),
            SUM(total_freight_value),
            COUNT(*) FILTER (
                WHERE customer_segment = 'Repeat Customer'
            ),
            COUNT(*) FILTER (
                WHERE customer_segment = 'One-Time Customer'
            )
        FROM analytics.marketplace_customer_performance
        """
    ).fetchone()

    expected_customer_counts = [
        96096,
        96096,
        99441,
        96478,
        2997,
        93099,
    ]

    actual_customer_counts = [
        customer_result[0],
        customer_result[1],
        customer_result[2],
        customer_result[3],
        customer_result[6],
        customer_result[7],
    ]

    assert actual_customer_counts == expected_customer_counts, (
        "Customer counts do not reconcile!"
    )

    assert abs(float(customer_result[4]) - 13591643.70) < 0.01
    assert abs(float(customer_result[5]) - 2251909.54) < 0.01

    repeat_rate = 100 * customer_result[6] / customer_result[0]

    print(f"Unique customers: {customer_result[0]:,}")
    print(f"Repeat customers: {customer_result[6]:,}")
    print(f"One-time customers: {customer_result[7]:,}")
    print(f"Repeat-customer rate: {repeat_rate:.2f}%")
    print("PASS | Customer performance reconciles to Gold")

    print("\nVALIDATING CATEGORY PERFORMANCE")
    print("-" * 60)

    category_result = con.execute(
        """
        SELECT
            COUNT(*),
            COUNT(DISTINCT product_category),
            SUM(total_items_sold),
            SUM(merchandise_value),
            SUM(freight_value),
            SUM(late_items),
            SUM(on_time_items)
        FROM analytics.marketplace_category_performance
        """
    ).fetchone()

    assert category_result[0] == 74
    assert category_result[1] == 74
    assert category_result[2] == 112650
    assert abs(float(category_result[3]) - 13591643.70) < 0.01
    assert abs(float(category_result[4]) - 2251909.54) < 0.01
    assert category_result[5] == 7265
    assert category_result[6] == 102931

    print(f"Categories: {category_result[0]:,}")
    print(f"Items: {int(category_result[2]):,}")
    print(f"Late items: {int(category_result[5]):,}")
    print(f"On-time items: {int(category_result[6]):,}")
    print("PASS | Category performance reconciles to Gold")

    print("\nVALIDATING CUSTOMER RFM")
    print("-" * 60)

    rfm_result = con.execute(
        """
        SELECT
            COUNT(*),
            COUNT(DISTINCT customer_unique_id),
            COUNT(*) FILTER (
                WHERE recency_days IS NULL OR recency_days < 0
            ),
            COUNT(*) FILTER (
                WHERE frequency IS NULL OR frequency < 1
            ),
            COUNT(*) FILTER (
                WHERE recency_score IS NULL
                   OR frequency_score IS NULL
                   OR monetary_score IS NULL
                   OR recency_score NOT BETWEEN 1 AND 5
                   OR frequency_score NOT BETWEEN 1 AND 5
                   OR monetary_score NOT BETWEEN 1 AND 5
            ),
            COUNT(*) FILTER (
                WHERE frequency_score != LEAST(frequency, 5)
            ),
            SUM(frequency),
            SUM(monetary_value)
        FROM analytics.marketplace_customer_rfm
        """
    ).fetchone()

    assert rfm_result[:7] == (
        96096,
        96096,
        0,
        0,
        0,
        0,
        99441,
    ), f"RFM validation failed: {rfm_result}"

    assert abs(float(rfm_result[7]) - 13591643.70) < 0.01

    print(f"Customers: {rfm_result[0]:,}")
    print(f"Orders represented: {int(rfm_result[6]):,}")
    print(f"Invalid recency values: {rfm_result[2]}")
    print(f"Invalid frequency values: {rfm_result[3]}")
    print(f"Invalid scores: {rfm_result[4]}")
    print(f"Incorrect frequency scores: {rfm_result[5]}")
    print("PASS | Customer RFM reconciles to Gold")

    print("\nVALIDATING CUSTOMER COHORT RETENTION")
    print("-" * 60)

    cohort_result = con.execute(
        """
        SELECT
            COUNT(*),
            COUNT(DISTINCT cohort_month),
            COUNT(*) FILTER (
                WHERE months_since_first_purchase = 0
            ),
            SUM(active_customers) FILTER (
                WHERE months_since_first_purchase = 0
            ),
            COUNT(*) FILTER (
                WHERE months_since_first_purchase = 0
                  AND (
                      active_customers != cohort_size
                      OR retention_rate_pct != 100
                  )
            ),
            COUNT(*) FILTER (
                WHERE retention_rate_pct < 0
                   OR retention_rate_pct > 100
                   OR months_since_first_purchase < 0
                   OR active_customers > cohort_size
                   OR active_customers < 1
            ),
            COUNT(*) - COUNT(DISTINCT (
                cohort_month,
                months_since_first_purchase
            ))
        FROM analytics.marketplace_customer_cohort_retention
        """
    ).fetchone()

    assert cohort_result == (
        226,
        25,
        25,
        96096,
        0,
        0,
        0,
    ), f"Cohort validation failed: {cohort_result}"

    print(f"Cohorts: {cohort_result[1]:,}")
    print(f"Cohort-period records: {cohort_result[0]:,}")
    print(f"Month-zero customers: {int(cohort_result[3]):,}")
    print(f"Invalid month-zero records: {cohort_result[4]}")
    print(f"Invalid retention records: {cohort_result[5]}")
    print(f"Duplicate cohort periods: {cohort_result[6]}")
    print("PASS | Customer cohort retention reconciles to Gold")


    print("\nVALIDATING DELIVERY EXPERIENCE")
    print("-" * 60)

    delivery_result = con.execute(
        """
        SELECT
            COUNT(*),
            SUM(total_orders),
            SUM(reviewed_orders),
            SUM(total_reviews),
            SUM(negative_reviews),
            SUM(positive_reviews)
        FROM analytics.marketplace_delivery_experience
        """
    ).fetchone()

    assert delivery_result == (
        3,
        99441,
        98673,
        99224,
        14575,
        76470,
    ), f"Delivery experience validation failed: {delivery_result}"

    delivery_breakdown = con.execute(
        """
        SELECT
            delivery_category,
            total_orders,
            reviewed_orders,
            total_reviews,
            avg_order_review_score
        FROM analytics.marketplace_delivery_experience
        """
    ).fetchall()

    expected_delivery_orders = {
        "On-Time Delivery": 89941,
        "Late Delivery": 6535,
        "Unknown Delivery Status": 2965,
    }

    for category, orders, reviewed, reviews, score in delivery_breakdown:
        assert category in expected_delivery_orders
        assert orders == expected_delivery_orders[category]
        assert 0 <= reviewed <= orders
        assert reviews >= reviewed
        assert score is not None and 1 <= score <= 5

    print(f"Delivery categories: {delivery_result[0]}")
    print(f"Orders: {int(delivery_result[1]):,}")
    print(f"Reviewed orders: {int(delivery_result[2]):,}")
    print(f"Review records: {int(delivery_result[3]):,}")
    print(f"Negative reviews: {int(delivery_result[4]):,}")
    print(f"Positive reviews: {int(delivery_result[5]):,}")
    print("PASS | Delivery experience reconciles to Gold")


    print("\nVALIDATING DELIVERY RISK FEATURES")
    print("-" * 60)

    risk_result = con.execute(
        """
        SELECT
            COUNT(*),
            COUNT(DISTINCT order_id),
            COUNT(*) FILTER (
                WHERE is_late_delivery = 1
            ),
            COUNT(*) FILTER (
                WHERE is_late_delivery = 0
            ),
            COUNT(*) FILTER (
                WHERE is_late_delivery NOT IN (0, 1)
                   OR is_late_delivery IS NULL
            ),
            COUNT(*) FILTER (
                WHERE promised_delivery_days < 0
                   OR promised_delivery_days IS NULL
            ),
            COUNT(*) FILTER (
                WHERE customer_state IS NULL
                   OR purchase_month IS NULL
                   OR purchase_day_of_week IS NULL
                   OR purchase_hour IS NULL
            )
        FROM analytics.marketplace_delivery_risk_features
        """
    ).fetchone()

    assert risk_result == (
        96476,
        96476,
        6535,
        89941,
        0,
        0,
        0,
    ), f"Delivery risk validation failed: {risk_result}"

    print(f"Total orders: {risk_result[0]:,}")
    print(f"Unique orders: {risk_result[1]:,}")
    print(f"Late deliveries: {risk_result[2]:,}")
    print(f"On-time deliveries: {risk_result[3]:,}")
    print(f"Invalid labels: {risk_result[4]}")
    print(f"Invalid promised delivery windows: {risk_result[5]}")
    print(f"Missing required features: {risk_result[6]}")
    print("PASS | Delivery risk features reconcile to Gold")

    print("\n" + "=" * 60)
    print("ALL ANALYTICS MODELS BUILT AND VALIDATED")
    print("=" * 60)
    print(f"Total models built: {len(sql_files)}")

finally:
    con.close()
