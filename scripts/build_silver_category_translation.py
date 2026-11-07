
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    lower,
    when,
    lit,
    count,
)

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

PROJECT_ROOT = Path(__file__).resolve().parent.parent

BRONZE_PATH = (
    PROJECT_ROOT
    / "data"
    / "bronze"
    / "product_category_name_translation"
)

SILVER_PATH = (
    PROJECT_ROOT
    / "data"
    / "silver"
    / "category_translation"
)

spark = (
    SparkSession.builder
    .appName("MarketplaceSilverCategoryTranslation")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    print("\n" + "=" * 55)
    print("MARKETPLACE INTELLIGENCE PLATFORM")
    print("SILVER LAYER — CATEGORY TRANSLATION")
    print("=" * 55)

    df = spark.read.parquet(str(BRONZE_PATH))
    bronze_count = df.count()

    print(f"\nBronze records: {bronze_count:,}")

    df = (
        df.withColumn(
            "product_category_name",
            lower(trim(col("product_category_name")))
        )
        .withColumn(
            "product_category_name_english",
            lower(trim(col("product_category_name_english")))
        )
    )

    df = df.withColumn(
        "missing_category_name",
        col("product_category_name").isNull()
        | (col("product_category_name") == "")
    )

    df = df.withColumn(
        "missing_english_translation",
        col("product_category_name_english").isNull()
        | (col("product_category_name_english") == "")
    )

    duplicate_categories = (
        df.groupBy("product_category_name")
        .agg(count("*").alias("category_count"))
        .filter(col("category_count") > 1)
        .select("product_category_name")
        .withColumn("duplicate_category_name", lit(True))
    )

    df = df.join(
        duplicate_categories,
        on="product_category_name",
        how="left"
    )

    df = df.fillna({
        "duplicate_category_name": False
    })

    df = df.withColumn(
        "translation_quality_status",
        when(
            col("missing_category_name")
            | col("missing_english_translation")
            | col("duplicate_category_name"),
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

    print("\nCategory translation quality summary:")

    silver_df.groupBy(
        "translation_quality_status"
    ).count().show(truncate=False)

    print("\nDATA QUALITY CHECKS")
    print("-" * 55)

    checks = [
        "missing_category_name",
        "missing_english_translation",
        "duplicate_category_name",
    ]

    for check in checks:
        flagged_count = silver_df.filter(
            col(check)
        ).count()

        print(f"{check}: {flagged_count:,}")

    print("-" * 55)
    print("\nSILVER CATEGORY TRANSLATION PIPELINE COMPLETED")

finally:
    spark.stop()
