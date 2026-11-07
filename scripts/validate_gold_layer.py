
import os
import sys
import json
from pathlib import Path
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, sum as spark_sum

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

ROOT = Path(__file__).resolve().parent.parent
GOLD = ROOT / "data" / "gold"
REPORTS = ROOT / "reports" / "data_quality"

spark = (
    SparkSession.builder
    .appName("MarketplaceGoldValidation")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

EXPECTED = {
    "dim_customers": (99441, ["customer_id"]),
    "dim_products": (32951, ["product_id"]),
    "dim_sellers": (3095, ["seller_id"]),
    "dim_dates": (774, ["date_key"]),
    "fact_orders": (99441, ["order_id"]),
    "fact_order_items": (112650, ["order_id", "order_item_id"]),
    "fact_payments": (103886, ["order_id", "payment_sequential"]),
    "fact_reviews": (99224, ["review_record_key"]),
}

results = []

def record(check, actual, expected, passed):
    results.append({
        "check": check,
        "actual": actual,
        "expected": expected,
        "status": "PASS" if passed else "FAIL",
    })
    print(f"{'PASS' if passed else 'FAIL'} | {check}: {actual}")

try:
    print("\n" + "=" * 65)
    print("MARKETPLACE INTELLIGENCE PLATFORM")
    print("COMPREHENSIVE GOLD LAYER VALIDATION")
    print("=" * 65)

    tables = {}

    for name, (expected_count, keys) in EXPECTED.items():
        df = spark.read.parquet(str(GOLD / name))
        tables[name] = df

        actual_count = df.count()

        record(
            f"{name} row count",
            actual_count,
            expected_count,
            actual_count == expected_count,
        )

        null_keys = df.filter(
            " OR ".join(f"`{key}` IS NULL" for key in keys)
        ).count()

        record(
            f"{name} null primary keys",
            null_keys,
            0,
            null_keys == 0,
        )

        duplicate_keys = (
            df.groupBy(*keys)
            .agg(count("*").alias("n"))
            .filter(col("n") > 1)
            .count()
        )

        record(
            f"{name} duplicate primary keys",
            duplicate_keys,
            0,
            duplicate_keys == 0,
        )

    print("\nFOREIGN KEY VALIDATION")
    print("-" * 65)

    relationships = [
        ("fact_orders", "customer_id", "dim_customers", "customer_id"),
        ("fact_orders", "purchase_date_key", "dim_dates", "date_key"),
        ("fact_order_items", "order_id", "fact_orders", "order_id"),
        ("fact_order_items", "product_id", "dim_products", "product_id"),
        ("fact_order_items", "seller_id", "dim_sellers", "seller_id"),
        ("fact_payments", "order_id", "fact_orders", "order_id"),
        ("fact_reviews", "order_id", "fact_orders", "order_id"),
    ]

    for child, child_key, parent, parent_key in relationships:
        child_df = tables[child].select(
            col(child_key).alias("fk")
        )

        parent_df = tables[parent].select(
            col(parent_key).alias("pk")
        )

        missing = (
            child_df.join(
                parent_df,
                child_df.fk == parent_df.pk,
                "left_anti",
            ).count()
        )

        record(
            f"{child}.{child_key} -> {parent}.{parent_key}",
            missing,
            0,
            missing == 0,
        )

    print("\nFINANCIAL VALIDATION")
    print("-" * 65)

    item_total = tables["fact_order_items"].agg(
        spark_sum("item_total_amount")
    ).first()[0]

    payment_total = tables["fact_payments"].agg(
        spark_sum("payment_value")
    ).first()[0]

    difference = round(payment_total - item_total, 2)

    record(
    "Financial difference matches investigation",
    float(difference),
    165318.88,
    abs(float(difference) - 165318.88) < 0.01,
)

    print("\nREVIEW KEY VALIDATION")
    print("-" * 65)

    review_df = tables["fact_reviews"]

    unique_review_keys = review_df.agg(
        countDistinct("review_record_key")
    ).first()[0]

    record(
        "Unique review keys",
        unique_review_keys,
        99224,
        unique_review_keys == 99224,
    )

    print("\nFINAL VALIDATION SUMMARY")
    print("-" * 65)

    passed = sum(r["status"] == "PASS" for r in results)
    failed = sum(r["status"] == "FAIL" for r in results)

    print(f"Total checks: {len(results)}")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")

    REPORTS.mkdir(parents=True, exist_ok=True)

    report_path = REPORTS / "gold_validation.json"

    report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "total_checks": len(results),
        "passed": passed,
        "failed": failed,
        "results": results,
    }

    with open(report_path, "w") as f:
        json.dump(report, f, indent=2)

    print(f"\nValidation report: {report_path}")

    if failed > 0:
        raise ValueError(
            f"Gold validation failed: {failed} checks"
        )

    print("\nALL GOLD LAYER VALIDATIONS PASSED")

finally:
    spark.stop()
