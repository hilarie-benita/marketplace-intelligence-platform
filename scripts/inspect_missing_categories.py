
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SILVER_ROOT = PROJECT_ROOT / "data" / "silver"

spark = (
    SparkSession.builder
    .appName("InspectMissingCategories")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    products = spark.read.parquet(
        str(SILVER_ROOT / "products")
    )

    translations = spark.read.parquet(
        str(SILVER_ROOT / "category_translation")
    )

    unmatched = (
        products
        .filter(
            col("product_category_name").isNotNull()
        )
        .join(
            translations.select("product_category_name"),
            on="product_category_name",
            how="left_anti"
        )
    )

    print("\n" + "=" * 55)
    print("MARKETPLACE INTELLIGENCE PLATFORM")
    print("UNMATCHED PRODUCT CATEGORY INVESTIGATION")
    print("=" * 55)

    unmatched.groupBy(
        "product_category_name"
    ).agg(
        count("*").alias("product_count")
    ).orderBy(
        col("product_count").desc()
    ).show(100, truncate=False)

    print(
        f"\nTotal affected products: "
        f"{unmatched.count():,}"
    )

    print("\nCATEGORY INVESTIGATION COMPLETED")

finally:
    spark.stop()
