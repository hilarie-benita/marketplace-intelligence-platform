
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    lower,
    upper,
    when,
    lit,
    count,
)

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

PROJECT_ROOT = Path(__file__).resolve().parent.parent

BRONZE_PATH = (
    PROJECT_ROOT / "data" / "bronze" / "olist_customers_dataset"
)

SILVER_PATH = (
    PROJECT_ROOT / "data" / "silver" / "customers"
)

spark = (
    SparkSession.builder
    .appName("MarketplaceSilverCustomers")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    print("\n" + "=" * 55)
    print("MARKETPLACE INTELLIGENCE PLATFORM")
    print("SILVER LAYER — CUSTOMERS")
    print("=" * 55)

    df = spark.read.parquet(str(BRONZE_PATH))

    bronze_count = df.count()

    print(f"\nBronze records: {bronze_count:,}")

    df = (
        df.withColumn(
            "customer_id",
            trim(col("customer_id"))
        )
        .withColumn(
            "customer_unique_id",
            trim(col("customer_unique_id"))
        )
        .withColumn(
            "customer_zip_code_prefix",
            trim(col("customer_zip_code_prefix"))
        )
        .withColumn(
            "customer_city",
            lower(trim(col("customer_city")))
        )
        .withColumn(
            "customer_state",
            upper(trim(col("customer_state")))
        )
    )

    df = df.withColumn(
        "missing_customer_id",
        col("customer_id").isNull()
        | (col("customer_id") == "")
    )

    df = df.withColumn(
        "missing_customer_unique_id",
        col("customer_unique_id").isNull()
        | (col("customer_unique_id") == "")
    )

    df = df.withColumn(
        "missing_zip_code",
        col("customer_zip_code_prefix").isNull()
        | (col("customer_zip_code_prefix") == "")
    )

    df = df.withColumn(
        "missing_city",
        col("customer_city").isNull()
        | (col("customer_city") == "")
    )

    df = df.withColumn(
        "missing_state",
        col("customer_state").isNull()
        | (col("customer_state") == "")
    )

    duplicate_ids = (
        df.groupBy("customer_id")
        .agg(count("*").alias("id_count"))
        .filter(col("id_count") > 1)
        .select("customer_id")
        .withColumn("duplicate_customer_id", lit(True))
    )

    df = df.join(
        duplicate_ids,
        on="customer_id",
        how="left"
    )

    df = df.fillna({
        "duplicate_customer_id": False
    })

    df = df.withColumn(
        "customer_quality_status",
        when(
            col("missing_customer_id")
            | col("missing_customer_unique_id")
            | col("missing_zip_code")
            | col("missing_city")
            | col("missing_state")
            | col("duplicate_customer_id"),
            lit("REVIEW_REQUIRED")
        ).otherwise(lit("VALID"))
    )

    df.write.mode("overwrite").parquet(
        str(SILVER_PATH)
    )

    silver_df = spark.read.parquet(str(SILVER_PATH))

    silver_count = silver_df.count()

    print(f"\nSilver records: {silver_count:,}")

    if bronze_count != silver_count:
        raise ValueError(
            "Row count mismatch between Bronze and Silver!"
        )

    print("\nCustomer quality summary:")

    silver_df.groupBy(
        "customer_quality_status"
    ).count().show(truncate=False)

    print("\nDATA QUALITY CHECKS")
    print("-" * 55)

    checks = [
        "missing_customer_id",
        "missing_customer_unique_id",
        "missing_zip_code",
        "missing_city",
        "missing_state",
        "duplicate_customer_id",
    ]

    for check in checks:
        count_flagged = silver_df.filter(
            col(check)
        ).count()

        print(f"{check}: {count_flagged:,}")

    print("-" * 55)
    print("\nSILVER CUSTOMERS PIPELINE COMPLETED")

finally:
    spark.stop()
