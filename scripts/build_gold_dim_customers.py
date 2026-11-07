
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    upper,
    lpad,
    count,
)

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

PROJECT_ROOT = Path(__file__).resolve().parent.parent

SILVER_PATH = (
    PROJECT_ROOT / "data" / "silver" / "customers"
)

GOLD_PATH = (
    PROJECT_ROOT / "data" / "gold" / "dim_customers"
)

spark = (
    SparkSession.builder
    .appName("MarketplaceGoldDimCustomers")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    print("\n" + "=" * 55)
    print("MARKETPLACE INTELLIGENCE PLATFORM")
    print("GOLD LAYER — CUSTOMER DIMENSION")
    print("=" * 55)

    customers = spark.read.parquet(str(SILVER_PATH))

    source_count = customers.count()

    print(f"\nSilver customer records: {source_count:,}")

    dim_customers = (
        customers
        .select(
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
            "customer_quality_status",
        )
        .withColumn(
            "customer_zip_code_prefix",
            lpad(
                trim(col("customer_zip_code_prefix")),
                5,
                "0",
            ),
        )
        .withColumn(
            "customer_state",
            upper(trim(col("customer_state"))),
        )
        .withColumn(
            "is_customer_valid",
            col("customer_quality_status") == "VALID",
        )
    )

    gold_count = dim_customers.count()

    if gold_count != source_count:
        raise ValueError(
            "Gold customer row count does not match Silver!"
        )

    duplicate_count = (
        dim_customers
        .groupBy("customer_id")
        .agg(count("*").alias("row_count"))
        .filter(col("row_count") > 1)
        .count()
    )

    if duplicate_count > 0:
        raise ValueError(
            f"Duplicate customer IDs detected: {duplicate_count}"
        )

    missing_ids = dim_customers.filter(
        col("customer_id").isNull()
        | (trim(col("customer_id")) == "")
    ).count()

    if missing_ids > 0:
        raise ValueError(
            f"Missing customer IDs detected: {missing_ids}"
        )

    dim_customers.write.mode("overwrite").parquet(
        str(GOLD_PATH)
    )

    gold_df = spark.read.parquet(str(GOLD_PATH))

    print(f"\nGold customer records: {gold_df.count():,}")
    print(f"Duplicate customer IDs: {duplicate_count:,}")
    print(f"Missing customer IDs: {missing_ids:,}")

    unique_customers = gold_df.select(
        "customer_unique_id"
    ).distinct().count()

    print(f"Distinct customer_unique_id values: {unique_customers:,}")

    print("\nCUSTOMER QUALITY SUMMARY")
    print("-" * 55)

    gold_df.groupBy(
        "customer_quality_status"
    ).count().show(truncate=False)

    print("\nTOP 10 CUSTOMER STATES")
    print("-" * 55)

    gold_df.groupBy(
        "customer_state"
    ).count().orderBy(
        col("count").desc()
    ).show(10, truncate=False)

    print("\nSAMPLE GOLD CUSTOMERS")
    print("-" * 55)

    gold_df.show(10, truncate=False)

    print(f"\nGold dimension written to: {GOLD_PATH}")
    print("\nGOLD CUSTOMER DIMENSION COMPLETED")

finally:
    spark.stop()
