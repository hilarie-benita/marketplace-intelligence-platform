
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    coalesce,
    lit,
    when,
    count,
    round as spark_round,
)

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

PROJECT_ROOT = Path(__file__).resolve().parent.parent

SILVER_ROOT = PROJECT_ROOT / "data" / "silver"
GOLD_ROOT = PROJECT_ROOT / "data" / "gold"
OUTPUT_PATH = GOLD_ROOT / "dim_products"

spark = (
    SparkSession.builder
    .appName("MarketplaceGoldDimProducts")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    print("\n" + "=" * 55)
    print("MARKETPLACE INTELLIGENCE PLATFORM")
    print("GOLD LAYER — PRODUCT DIMENSION")
    print("=" * 55)

    products = spark.read.parquet(
        str(SILVER_ROOT / "products")
    )

    category_mapping = spark.read.parquet(
        str(SILVER_ROOT / "category_mapping")
    )

    source_count = products.count()

    print(f"\nSilver product records: {source_count:,}")

    dim_products = products.join(
        category_mapping.select(
            "product_category_name",
            "product_category_name_english",
            "translation_source",
        ),
        on="product_category_name",
        how="left",
    )

    dim_products = (
        dim_products
        .withColumn(
            "category_name_english",
            coalesce(
                col("product_category_name_english"),
                lit("Unknown"),
            ),
        )
        .withColumn(
            "category_translation_source",
            coalesce(
                col("translation_source"),
                lit("MISSING_CATEGORY"),
            ),
        )
        .withColumn(
            "product_weight_kg",
            spark_round(
                col("product_weight_g") / lit(1000.0),
                3,
            ),
        )
        .withColumn(
            "product_volume_cm3",
            (
                col("product_length_cm")
                * col("product_height_cm")
                * col("product_width_cm")
            ),
        )
        .withColumn(
            "is_product_valid",
            col("product_quality_status") == "VALID",
        )
    )

    dim_products = dim_products.select(
        "product_id",
        "product_category_name",
        "category_name_english",
        "category_translation_source",
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_weight_kg",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
        "product_volume_cm3",
        "product_quality_status",
        "is_product_valid",
    )

    gold_count = dim_products.count()

    if gold_count != source_count:
        raise ValueError(
            "Gold product row count does not match Silver!"
        )

    duplicate_count = (
        dim_products
        .groupBy("product_id")
        .agg(count("*").alias("row_count"))
        .filter(col("row_count") > 1)
        .count()
    )

    if duplicate_count > 0:
        raise ValueError(
            f"Duplicate product IDs detected: {duplicate_count}"
        )

    dim_products.write.mode("overwrite").parquet(
        str(OUTPUT_PATH)
    )

    gold_df = spark.read.parquet(str(OUTPUT_PATH))

    print(f"\nGold product records: {gold_df.count():,}")
    print(f"Duplicate product IDs: {duplicate_count:,}")

    print("\nCATEGORY TRANSLATION COVERAGE")
    print("-" * 55)

    gold_df.groupBy(
        "category_translation_source"
    ).count().orderBy(
        "category_translation_source"
    ).show(truncate=False)

    print("\nPRODUCT QUALITY SUMMARY")
    print("-" * 55)

    gold_df.groupBy(
        "product_quality_status"
    ).count().show(truncate=False)

    unknown_count = gold_df.filter(
        col("category_name_english") == "Unknown"
    ).count()

    print(f"\nProducts with Unknown category: {unknown_count:,}")

    print("\nSAMPLE GOLD PRODUCTS")
    print("-" * 55)

    gold_df.select(
        "product_id",
        "category_name_english",
        "product_weight_kg",
        "product_volume_cm3",
        "is_product_valid",
    ).show(10, truncate=False)

    print(f"\nGold dimension written to: {OUTPUT_PATH}")
    print("\nGOLD PRODUCT DIMENSION COMPLETED")

finally:
    spark.stop()
