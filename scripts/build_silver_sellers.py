
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
    PROJECT_ROOT / "data" / "bronze" / "olist_sellers_dataset"
)

SILVER_PATH = (
    PROJECT_ROOT / "data" / "silver" / "sellers"
)

spark = (
    SparkSession.builder
    .appName("MarketplaceSilverSellers")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    print("\n" + "=" * 55)
    print("MARKETPLACE INTELLIGENCE PLATFORM")
    print("SILVER LAYER — SELLERS")
    print("=" * 55)

    df = spark.read.parquet(str(BRONZE_PATH))
    bronze_count = df.count()

    print(f"\nBronze records: {bronze_count:,}")

    df = (
        df.withColumn(
            "seller_id",
            trim(col("seller_id"))
        )
        .withColumn(
            "seller_zip_code_prefix",
            trim(col("seller_zip_code_prefix"))
        )
        .withColumn(
            "seller_city",
            lower(trim(col("seller_city")))
        )
        .withColumn(
            "seller_state",
            upper(trim(col("seller_state")))
        )
    )

    df = df.withColumn(
        "missing_seller_id",
        col("seller_id").isNull()
        | (col("seller_id") == "")
    )

    df = df.withColumn(
        "missing_zip_code",
        col("seller_zip_code_prefix").isNull()
        | (col("seller_zip_code_prefix") == "")
    )

    df = df.withColumn(
        "missing_city",
        col("seller_city").isNull()
        | (col("seller_city") == "")
    )

    df = df.withColumn(
        "missing_state",
        col("seller_state").isNull()
        | (col("seller_state") == "")
    )

    duplicate_ids = (
        df.groupBy("seller_id")
        .agg(count("*").alias("id_count"))
        .filter(col("id_count") > 1)
        .select("seller_id")
        .withColumn("duplicate_seller_id", lit(True))
    )

    df = df.join(
        duplicate_ids,
        on="seller_id",
        how="left"
    )

    df = df.fillna({
        "duplicate_seller_id": False
    })

    df = df.withColumn(
        "seller_quality_status",
        when(
            col("missing_seller_id")
            | col("missing_zip_code")
            | col("missing_city")
            | col("missing_state")
            | col("duplicate_seller_id"),
            lit("REVIEW_REQUIRED")
        ).otherwise(lit("VALID"))
    )

    df.write.mode("overwrite").parquet(str(SILVER_PATH))

    silver_df = spark.read.parquet(str(SILVER_PATH))
    silver_count = silver_df.count()

    print(f"\nSilver records: {silver_count:,}")

    if bronze_count != silver_count:
        raise ValueError(
            "Row count mismatch between Bronze and Silver!"
        )

    print("\nSeller quality summary:")

    silver_df.groupBy(
        "seller_quality_status"
    ).count().show(truncate=False)

    print("\nDATA QUALITY CHECKS")
    print("-" * 55)

    checks = [
        "missing_seller_id",
        "missing_zip_code",
        "missing_city",
        "missing_state",
        "duplicate_seller_id",
    ]

    for check in checks:
        flagged_count = silver_df.filter(
            col(check)
        ).count()

        print(f"{check}: {flagged_count:,}")

    print("-" * 55)
    print("\nSILVER SELLERS PIPELINE COMPLETED")

finally:
    spark.stop()
