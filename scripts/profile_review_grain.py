
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    struct,
)

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

ROOT = Path(__file__).resolve().parent.parent

spark = (
    SparkSession.builder
    .appName("MarketplaceReviewGrainProfile")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    reviews = spark.read.parquet(
        str(ROOT / "data" / "silver" / "reviews")
    )

    print("\n" + "=" * 60)
    print("REVIEW GRAIN AND IDENTIFIER ANALYSIS")
    print("=" * 60)

    reviews.agg(
        count("*").alias("review_records"),
        countDistinct("review_id").alias("distinct_review_ids"),
        countDistinct("order_id").alias("distinct_orders"),
        countDistinct(
            struct("review_id", "order_id")
        ).alias("distinct_review_order_pairs"),
    ).show(truncate=False)

    print("\nREPEATED REVIEW IDS")
    print("-" * 60)

    repeated_ids = (
        reviews.groupBy("review_id")
        .agg(
            count("*").alias("records"),
            countDistinct("order_id").alias("distinct_orders"),
        )
        .filter(col("records") > 1)
    )

    repeated_ids.agg(
        count("*").alias("repeated_review_ids"),
        count("*").alias("groups"),
    ).show(truncate=False)

    repeated_ids.groupBy(
        "distinct_orders"
    ).count().orderBy(
        "distinct_orders"
    ).show(truncate=False)

    print("\nREPEATED REVIEW-ORDER PAIRS")
    print("-" * 60)

    repeated_pairs = (
        reviews.groupBy("review_id", "order_id")
        .count()
        .filter(col("count") > 1)
    )

    print(f"Repeated pairs: {repeated_pairs.count():,}")

    print("\nORDERS WITH MULTIPLE REVIEW RECORDS")
    print("-" * 60)

    reviews.groupBy("order_id").count().filter(
        col("count") > 1
    ).groupBy("count").count().orderBy(
        "count"
    ).show(truncate=False)

    print("\nREVIEW GRAIN ANALYSIS COMPLETED")

finally:
    spark.stop()
