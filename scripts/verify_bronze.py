
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"

EXPECTED_COUNTS = {
    "olist_customers_dataset": 99441,
    "olist_geolocation_dataset": 1000163,
    "olist_order_items_dataset": 112650,
    "olist_order_payments_dataset": 103886,
    "olist_order_reviews_dataset": 99224,
    "olist_orders_dataset": 99441,
    "olist_products_dataset": 32951,
    "olist_sellers_dataset": 3095,
    "product_category_name_translation": 71,
}

spark = (
    SparkSession.builder
    .appName("BronzeDataVerification")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    print("\nBRONZE DATA RECONCILIATION")
    print("-" * 75)

    all_passed = True

    for dataset, expected_count in EXPECTED_COUNTS.items():

        path = BRONZE_DIR / dataset

        df = spark.read.parquet(str(path))
        actual_count = df.count()

        status = (
            "PASS"
            if actual_count == expected_count
            else "FAIL"
        )

        if status == "FAIL":
            all_passed = False

        print(
            f"{dataset:<38} "
            f"Expected: {expected_count:>8,} "
            f"Actual: {actual_count:>8,} "
            f"{status}"
        )

    print("-" * 75)

    if all_passed:
        print("SUCCESS: All Bronze row counts match!")
    else:
        print("WARNING: One or more datasets failed reconciliation.")

finally:
    spark.stop()
