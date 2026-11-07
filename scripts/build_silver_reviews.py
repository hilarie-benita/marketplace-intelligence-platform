
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    when,
    lit,
    expr,
    length,
)

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

PROJECT_ROOT = Path(__file__).resolve().parent.parent

BRONZE_PATH = (
    PROJECT_ROOT / "data" / "bronze" / "olist_order_reviews_dataset"
)

SILVER_PATH = (
    PROJECT_ROOT / "data" / "silver" / "reviews"
)

spark = (
    SparkSession.builder
    .appName("MarketplaceSilverReviews")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    print("\n" + "=" * 55)
    print("MARKETPLACE INTELLIGENCE PLATFORM")
    print("SILVER LAYER — REVIEWS")
    print("=" * 55)

    df = spark.read.parquet(str(BRONZE_PATH))
    bronze_count = df.count()

    print(f"\nBronze records: {bronze_count:,}")

    df = (
        df.withColumn("review_id", trim(col("review_id")))
          .withColumn("order_id", trim(col("order_id")))
          .withColumn(
              "_raw_review_score",
              col("review_score")
          )
          .withColumn(
              "_raw_review_creation_date",
              col("review_creation_date")
          )
          .withColumn(
              "_raw_review_answer_timestamp",
              col("review_answer_timestamp")
          )
    )

    df = (
        df.withColumn(
            "review_score",
            expr("try_cast(review_score AS INT)")
        )
        .withColumn(
            "review_creation_date",
            expr(
                "try_to_timestamp("
                "`_raw_review_creation_date`, "
                "'yyyy-MM-dd HH:mm:ss')"
            )
        )
        .withColumn(
            "review_answer_timestamp",
            expr(
                "try_to_timestamp("
                "`_raw_review_answer_timestamp`, "
                "'yyyy-MM-dd HH:mm:ss')"
            )
        )
    )

    df = df.withColumn(
        "review_comment_title",
        trim(col("review_comment_title"))
    )

    df = df.withColumn(
        "review_comment_message",
        trim(col("review_comment_message"))
    )

    df = df.withColumn(
        "missing_review_id",
        col("review_id").isNull()
        | (col("review_id") == "")
    )

    df = df.withColumn(
        "missing_order_id",
        col("order_id").isNull()
        | (col("order_id") == "")
    )

    df = df.withColumn(
        "invalid_review_score",
        col("review_score").isNull()
        | (col("review_score") < 1)
        | (col("review_score") > 5)
    )

    df = df.withColumn(
        "invalid_review_creation_date",
        col("review_creation_date").isNull()
    )

    df = df.withColumn(
        "invalid_review_answer_timestamp",
        col("review_answer_timestamp").isNull()
    )

    df = df.withColumn(
        "invalid_review_timeline",
        (
            col("review_creation_date").isNotNull()
            & col("review_answer_timestamp").isNotNull()
            & (
                col("review_answer_timestamp")
                < col("review_creation_date")
            )
        )
    )

    df = df.withColumn(
        "has_review_comment",
        col("review_comment_message").isNotNull()
        & (length(col("review_comment_message")) > 0)
    )

    df = df.withColumn(
        "review_quality_status",
        when(
            col("missing_review_id")
            | col("missing_order_id")
            | col("invalid_review_score")
            | col("invalid_review_creation_date")
            | col("invalid_review_answer_timestamp")
            | col("invalid_review_timeline"),
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

    print("\nReview quality summary:")

    silver_df.groupBy(
        "review_quality_status"
    ).count().show(truncate=False)

    print("\nDATA QUALITY CHECKS")
    print("-" * 55)

    checks = [
        "missing_review_id",
        "missing_order_id",
        "invalid_review_score",
        "invalid_review_creation_date",
        "invalid_review_answer_timestamp",
        "invalid_review_timeline",
    ]

    for check in checks:
        flagged_count = silver_df.filter(
            col(check)
        ).count()

        print(f"{check}: {flagged_count:,}")

    print("-" * 55)

    comments_count = silver_df.filter(
        col("has_review_comment")
    ).count()

    print(f"Reviews containing comments: {comments_count:,}")
    print("\nSILVER REVIEWS PIPELINE COMPLETED")

finally:
    spark.stop()
