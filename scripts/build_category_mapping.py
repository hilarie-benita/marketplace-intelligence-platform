
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    lower,
    lit,
    when,
    count,
)

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

PROJECT_ROOT = Path(__file__).resolve().parent.parent

SILVER_ROOT = PROJECT_ROOT / "data" / "silver"

OVERRIDES_PATH = (
    PROJECT_ROOT
    / "data"
    / "reference"
    / "category_translation_overrides.csv"
)

OUTPUT_PATH = (
    PROJECT_ROOT
    / "data"
    / "silver"
    / "category_mapping"
)

spark = (
    SparkSession.builder
    .appName("MarketplaceCategoryMapping")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    print("\n" + "=" * 55)
    print("MARKETPLACE INTELLIGENCE PLATFORM")
    print("CATEGORY TRANSLATION MAPPING")
    print("=" * 55)

    original = (
        spark.read.parquet(
            str(SILVER_ROOT / "category_translation")
        )
        .select(
            "product_category_name",
            "product_category_name_english"
        )
        .withColumn(
            "translation_source",
            lit("OLIST")
        )
    )

    overrides = (
        spark.read.option("header", True)
        .option("mode", "FAILFAST")
        .csv(str(OVERRIDES_PATH))
        .select(
            "product_category_name",
            "product_category_name_english"
        )
        .withColumn(
            "product_category_name",
            lower(trim(col("product_category_name")))
        )
        .withColumn(
            "product_category_name_english",
            lower(trim(col("product_category_name_english")))
        )
        .withColumn(
            "translation_source",
            lit("CUSTOM_OVERRIDE")
        )
    )

    mapping = original.unionByName(overrides)

    duplicate_keys = (
        mapping.groupBy("product_category_name")
        .agg(count("*").alias("key_count"))
        .filter(col("key_count") > 1)
    )

    duplicate_count = duplicate_keys.count()

    if duplicate_count > 0:
        duplicate_keys.show(truncate=False)
        raise ValueError(
            "Duplicate category keys detected in mapping!"
        )

    missing_values = mapping.filter(
        col("product_category_name").isNull()
        | (trim(col("product_category_name")) == "")
        | col("product_category_name_english").isNull()
        | (trim(col("product_category_name_english")) == "")
    ).count()

    if missing_values > 0:
        raise ValueError(
            "Missing category names or translations detected!"
        )

    products = spark.read.parquet(
        str(SILVER_ROOT / "products")
    )

    unmatched = (
        products
        .filter(
            col("product_category_name").isNotNull()
            & (trim(col("product_category_name")) != "")
        )
        .join(
            mapping.select("product_category_name"),
            on="product_category_name",
            how="left_anti"
        )
    )

    unmatched_count = unmatched.count()

    print(f"\nOriginal translations: {original.count():,}")
    print(f"Custom translations: {overrides.count():,}")
    print(f"Combined translations: {mapping.count():,}")
    print(f"Duplicate category keys: {duplicate_count:,}")
    print(f"Unmatched non-empty product categories: {unmatched_count:,}")

    if unmatched_count > 0:
        unmatched.groupBy(
            "product_category_name"
        ).count().show(truncate=False)

        raise ValueError(
            "Some product categories remain unmatched!"
        )

    mapping.write.mode("overwrite").parquet(
        str(OUTPUT_PATH)
    )

    print("\nTranslation sources:")

    mapping.groupBy(
        "translation_source"
    ).count().show(truncate=False)

    print(f"\nCategory mapping written to: {OUTPUT_PATH}")

    print("\nCATEGORY MAPPING COMPLETED")

finally:
    spark.stop()
