
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    avg,
    round as spark_round,
    sha2,
    concat_ws,
    to_date,
    date_format,
)

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SILVER_ROOT = PROJECT_ROOT / "data" / "silver"
GOLD_ROOT = PROJECT_ROOT / "data" / "gold"

spark = (
    SparkSession.builder
    .appName("MarketplaceGoldFactReviews")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    print("\n" + "=" * 60)
    print("MARKETPLACE INTELLIGENCE PLATFORM")
    print("GOLD LAYER — REVIEWS FACT TABLE")
    print("=" * 60)

    reviews = spark.read.parquet(
        str(SILVER_ROOT / "reviews")
    )

    orders = spark.read.parquet(
        str(GOLD_ROOT / "fact_orders")
    ).select(
        "order_id",
        "customer_id",
        "purchase_date_key",
        "order_status",
        "delivery_days",
        "delivery_delay_days",
        "is_late_delivery",
    )

    source_count = reviews.count()

    print(f"\nSilver review records: {source_count:,}")

    missing_source_keys = reviews.filter(
        col("review_id").isNull()
        | col("order_id").isNull()
    ).count()

    if missing_source_keys > 0:
        raise ValueError(
            f"Reviews with missing source keys: {missing_source_keys}"
        )

    duplicate_source_pairs = (
        reviews
        .groupBy("review_id", "order_id")
        .agg(count("*").alias("row_count"))
        .filter(col("row_count") > 1)
        .count()
    )

    if duplicate_source_pairs > 0:
        raise ValueError(
            f"Duplicate review-order pairs: {duplicate_source_pairs}"
        )

    reviews_with_id = reviews.withColumn(
        "review_record_key",
        sha2(
            concat_ws(
                "|",
                col("review_id"),
                col("order_id"),
            ),
            256,
        ),
    )

    fact_reviews = (
        reviews_with_id
        .join(
            orders,
            on="order_id",
            how="left",
        )
        .withColumn(
            "review_creation_date",
            to_date("review_creation_date"),
        )
        .withColumn(
            "review_creation_date_key",
            date_format(
                col("review_creation_date"),
                "yyyyMMdd",
            ).cast("int"),
        )
        .withColumn(
            "is_positive_review",
            col("review_score") >= 4,
        )
        .withColumn(
            "is_negative_review",
            col("review_score") <= 2,
        )
    )

    fact_reviews = fact_reviews.select(
        "review_record_key",
        "review_id",
        "order_id",
        "customer_id",
        "purchase_date_key",
        "order_status",
        "review_score",
        "review_comment_title",
        "review_comment_message",
        "review_creation_date",
        "review_creation_date_key",
        "review_answer_timestamp",
        "delivery_days",
        "delivery_delay_days",
        "is_late_delivery",
        "is_positive_review",
        "is_negative_review",
    )

    fact_count = fact_reviews.count()

    if fact_count != source_count:
        raise ValueError(
            f"Gold review count mismatch: "
            f"Silver={source_count}, Gold={fact_count}"
        )

    duplicate_keys = (
        fact_reviews
        .groupBy("review_record_key")
        .agg(count("*").alias("row_count"))
        .filter(col("row_count") > 1)
        .count()
    )

    if duplicate_keys > 0:
        raise ValueError(
            f"Duplicate review record keys: {duplicate_keys}"
        )

    missing_orders = fact_reviews.filter(
        col("customer_id").isNull()
    ).count()

    if missing_orders > 0:
        raise ValueError(
            f"Reviews without matching orders: {missing_orders}"
        )

    missing_generated_keys = fact_reviews.filter(
        col("review_record_key").isNull()
    ).count()

    if missing_generated_keys > 0:
        raise ValueError(
            f"Missing generated review keys: {missing_generated_keys}"
        )

    output_path = GOLD_ROOT / "fact_reviews"

    fact_reviews.write.mode("overwrite").parquet(
        str(output_path)
    )

    gold_df = spark.read.parquet(str(output_path))

    print(f"\nGold review records: {gold_df.count():,}")
    print(f"Missing source keys: {missing_source_keys:,}")
    print(f"Duplicate review-order pairs: {duplicate_source_pairs:,}")
    print(f"Duplicate review record keys: {duplicate_keys:,}")
    print(f"Missing generated keys: {missing_generated_keys:,}")
    print(f"Reviews without matching orders: {missing_orders:,}")

    print("\nREVIEW SCORE DISTRIBUTION")
    print("-" * 60)

    gold_df.groupBy(
        "review_score"
    ).count().orderBy(
        "review_score"
    ).show(truncate=False)

    print("\nOVERALL CUSTOMER SATISFACTION")
    print("-" * 60)

    gold_df.agg(
        spark_round(
            avg("review_score"), 2
        ).alias("average_review_score"),
        countDistinct("order_id").alias(
            "orders_with_reviews"
        ),
    ).show(truncate=False)

    print("\nDELIVERY PERFORMANCE VS REVIEW SCORE")
    print("-" * 60)

    gold_df.filter(
        col("is_late_delivery").isNotNull()
    ).groupBy(
        "is_late_delivery"
    ).agg(
        count("*").alias("review_records"),
        spark_round(
            avg("review_score"), 2
        ).alias("average_review_score"),
    ).orderBy(
        "is_late_delivery"
    ).show(truncate=False)

    print("\nREVIEW IDENTIFIER VALIDATION")
    print("-" * 60)

    gold_df.agg(
        count("*").alias("total_review_records"),
        countDistinct(
            "review_record_key"
        ).alias("unique_review_keys"),
        countDistinct(
            "review_id"
        ).alias("unique_review_ids"),
        countDistinct(
            "order_id"
        ).alias("unique_orders"),
    ).show(truncate=False)

    print("\nSAMPLE GOLD REVIEWS")
    print("-" * 60)

    gold_df.select(
        "review_record_key",
        "review_id",
        "order_id",
        "review_score",
        "delivery_days",
        "is_late_delivery",
        "is_positive_review",
    ).show(10, truncate=False)

    print(f"\nGold fact written to: {output_path}")
    print("\nGOLD REVIEWS FACT TABLE COMPLETED")

finally:
    spark.stop()

