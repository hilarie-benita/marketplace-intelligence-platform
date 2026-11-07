
import os
import sys
import json
from pathlib import Path
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SILVER_ROOT = PROJECT_ROOT / "data" / "silver"
REPORT_PATH = (
    PROJECT_ROOT
    / "reports"
    / "data_quality"
    / "silver_relationships.json"
)

spark = (
    SparkSession.builder
    .appName("MarketplaceSilverRelationships")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    print("\n" + "=" * 55)
    print("MARKETPLACE INTELLIGENCE PLATFORM")
    print("SILVER LAYER — RELATIONSHIP VALIDATION")
    print("=" * 55)

    datasets = {}

    for name in [
        "orders",
        "customers",
        "payments",
        "order_items",
        "products",
        "sellers",
        "reviews",
        "category_translation",
    ]:
        datasets[name] = spark.read.parquet(
            str(SILVER_ROOT / name)
        )

    relationships = [
        (
            "Orders → Customers",
            "orders",
            "customer_id",
            "customers",
            "customer_id",
        ),
        (
            "Payments → Orders",
            "payments",
            "order_id",
            "orders",
            "order_id",
        ),
        (
            "Order Items → Orders",
            "order_items",
            "order_id",
            "orders",
            "order_id",
        ),
        (
            "Order Items → Products",
            "order_items",
            "product_id",
            "products",
            "product_id",
        ),
        (
            "Order Items → Sellers",
            "order_items",
            "seller_id",
            "sellers",
            "seller_id",
        ),
        (
            "Reviews → Orders",
            "reviews",
            "order_id",
            "orders",
            "order_id",
        ),
        (
            "Products → Category Translation",
            "products",
            "product_category_name",
            "category_translation",
            "product_category_name",
        ),
    ]

    results = []

    print("\nRELATIONSHIP CHECKS")
    print("-" * 55)

    for (
        relationship_name,
        source_name,
        source_key,
        target_name,
        target_key,
    ) in relationships:

        source_df = datasets[source_name]
        target_df = datasets[target_name]

        target_keys = (
            target_df
            .select(col(target_key).alias("_target_key"))
            .filter(col("_target_key").isNotNull())
            .distinct()
        )

        missing_references = (
            source_df
            .filter(col(source_key).isNotNull())
            .join(
                target_keys,
                col(source_key) == col("_target_key"),
                "left_anti",
            )
        )

        orphan_count = missing_references.count()

        null_key_count = source_df.filter(
            col(source_key).isNull()
        ).count()

        status = (
            "PASS"
            if orphan_count == 0
            else "REVIEW_REQUIRED"
        )

        print(f"\n{relationship_name}")
        print(f"  Missing references: {orphan_count:,}")
        print(f"  Null source keys: {null_key_count:,}")
        print(f"  Status: {status}")

        results.append({
            "relationship": relationship_name,
            "source_dataset": source_name,
            "target_dataset": target_name,
            "missing_references": orphan_count,
            "null_source_keys": null_key_count,
            "status": status,
        })

    print("\nKEY UNIQUENESS CHECKS")
    print("-" * 55)

    uniqueness_checks = [
        ("orders", ["order_id"]),
        ("customers", ["customer_id"]),
        ("products", ["product_id"]),
        ("sellers", ["seller_id"]),
        (
            "order_items",
            ["order_id", "order_item_id"],
        ),
        (
            "payments",
            ["order_id", "payment_sequential"],
        ),
        (
            "category_translation",
            ["product_category_name"],
        ),
    ]

    uniqueness_results = []

    for dataset_name, keys in uniqueness_checks:

        duplicate_groups = (
            datasets[dataset_name]
            .groupBy(*keys)
            .agg(count("*").alias("_row_count"))
            .filter(col("_row_count") > 1)
        )

        duplicate_count = duplicate_groups.count()

        status = (
            "PASS"
            if duplicate_count == 0
            else "REVIEW_REQUIRED"
        )

        print(
            f"{dataset_name} ({', '.join(keys)}): "
            f"{duplicate_count:,} duplicate key groups "
            f"[{status}]"
        )

        uniqueness_results.append({
            "dataset": dataset_name,
            "keys": keys,
            "duplicate_key_groups": duplicate_count,
            "status": status,
        })

    report = {
        "generated_at_utc": (
            datetime.now(timezone.utc).isoformat()
        ),
        "relationship_checks": results,
        "uniqueness_checks": uniqueness_results,
    }

    REPORT_PATH.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    with open(REPORT_PATH, "w", encoding="utf-8") as file:
        json.dump(report, file, indent=2)

    print("\n" + "=" * 55)
    print(f"Report saved to: {REPORT_PATH}")
    print("SILVER RELATIONSHIP VALIDATION COMPLETED")
    print("=" * 55)

finally:
    spark.stop()
