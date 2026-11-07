
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, upper, trim, lpad

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

PROJECT_ROOT = Path(__file__).resolve().parent.parent

SILVER_PATH = PROJECT_ROOT / "data" / "silver" / "sellers"
GOLD_PATH = PROJECT_ROOT / "data" / "gold" / "dim_sellers"

spark = (
    SparkSession.builder
    .appName("MarketplaceGoldDimSellers")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    print("\n" + "=" * 55)
    print("MARKETPLACE INTELLIGENCE PLATFORM")
    print("GOLD LAYER — SELLER DIMENSION")
    print("=" * 55)

    sellers = spark.read.parquet(str(SILVER_PATH))

    source_count = sellers.count()
    print(f"\nSilver seller records: {source_count:,}")

    dim_sellers = (
        sellers
        .select(
            "seller_id",
            "seller_zip_code_prefix",
            "seller_city",
            "seller_state",
            "seller_quality_status"
        )
        .withColumn(
            "seller_zip_code_prefix",
            lpad(
                trim(col("seller_zip_code_prefix")),
                5,
                "0"
            )
        )
        .withColumn(
            "seller_state",
            upper(trim(col("seller_state")))
        )
        .withColumn(
            "is_seller_valid",
            col("seller_quality_status") == "VALID"
        )
    )

    gold_count = dim_sellers.count()

    if gold_count != source_count:
        raise ValueError(
            "Gold seller row count does not match Silver!"
        )

    duplicate_count = (
        dim_sellers
        .groupBy("seller_id")
        .agg(count("*").alias("row_count"))
        .filter(col("row_count") > 1)
        .count()
    )

    if duplicate_count > 0:
        raise ValueError(
            f"Duplicate seller IDs detected: {duplicate_count}"
        )

    missing_ids = dim_sellers.filter(
        col("seller_id").isNull()
        | (trim(col("seller_id")) == "")
    ).count()

    if missing_ids > 0:
        raise ValueError(
            f"Missing seller IDs detected: {missing_ids}"
        )

    dim_sellers.write.mode("overwrite").parquet(
        str(GOLD_PATH)
    )

    gold_df = spark.read.parquet(str(GOLD_PATH))

    print(f"\nGold seller records: {gold_df.count():,}")
    print(f"Duplicate seller IDs: {duplicate_count:,}")
    print(f"Missing seller IDs: {missing_ids:,}")

    print("\nSELLER QUALITY SUMMARY")
    print("-" * 55)

    gold_df.groupBy(
        "seller_quality_status"
    ).count().show(truncate=False)

    print("\nTOP 10 SELLER STATES")
    print("-" * 55)

    gold_df.groupBy(
        "seller_state"
    ).count().orderBy(
        col("count").desc()
    ).show(10, truncate=False)

    print("\nSAMPLE GOLD SELLERS")
    print("-" * 55)

    gold_df.show(10, truncate=False)

    print(f"\nGold dimension written to: {GOLD_PATH}")
    print("\nGOLD SELLER DIMENSION COMPLETED")

finally:
    spark.stop()
