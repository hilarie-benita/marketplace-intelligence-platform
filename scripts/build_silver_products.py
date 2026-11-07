
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
    expr,
    count,
)

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

PROJECT_ROOT = Path(__file__).resolve().parent.parent

BRONZE_PATH = (
    PROJECT_ROOT / "data" / "bronze" / "olist_products_dataset"
)

SILVER_PATH = (
    PROJECT_ROOT / "data" / "silver" / "products"
)

spark = (
    SparkSession.builder
    .appName("MarketplaceSilverProducts")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    print("\n" + "=" * 55)
    print("MARKETPLACE INTELLIGENCE PLATFORM")
    print("SILVER LAYER — PRODUCTS")
    print("=" * 55)

    df = spark.read.parquet(str(BRONZE_PATH))
    bronze_count = df.count()

    print(f"\nBronze records: {bronze_count:,}")

    df = (
        df.withColumn(
            "product_id",
            trim(col("product_id"))
        )
        .withColumn(
            "product_category_name",
            lower(trim(col("product_category_name")))
        )
    )

    numeric_columns = [
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    ]

    for column_name in numeric_columns:
        df = df.withColumn(
            f"_raw_{column_name}",
            col(column_name)
        )

        df = df.withColumn(
            column_name,
            expr(
                f"try_cast(`{column_name}` AS INT)"
            )
        )

        df = df.withColumn(
            f"invalid_{column_name}",
            (
                col(f"_raw_{column_name}").isNotNull()
                & (trim(col(f"_raw_{column_name}")) != "")
                & col(column_name).isNull()
            )
        )

    df = df.withColumn(
        "missing_product_id",
        col("product_id").isNull()
        | (col("product_id") == "")
    )

    df = df.withColumn(
        "missing_category",
        col("product_category_name").isNull()
        | (col("product_category_name") == "")
    )

    duplicate_ids = (
        df.groupBy("product_id")
        .agg(count("*").alias("id_count"))
        .filter(col("id_count") > 1)
        .select("product_id")
        .withColumn("duplicate_product_id", lit(True))
    )

    df = df.join(
        duplicate_ids,
        on="product_id",
        how="left"
    )

    df = df.fillna({
        "duplicate_product_id": False
    })

    invalid_numeric_condition = col(
        f"invalid_{numeric_columns[0]}"
    )

    for column_name in numeric_columns[1:]:
        invalid_numeric_condition = (
            invalid_numeric_condition
            | col(f"invalid_{column_name}")
        )

    df = df.withColumn(
        "has_invalid_numeric",
        invalid_numeric_condition
    )

    df = df.withColumn(
        "invalid_weight",
        col("product_weight_g").isNotNull()
        & (col("product_weight_g") <= 0)
    )

    df = df.withColumn(
        "invalid_dimensions",
        (
            (
                col("product_length_cm").isNotNull()
                & (col("product_length_cm") <= 0)
            )
            | (
                col("product_height_cm").isNotNull()
                & (col("product_height_cm") <= 0)
            )
            | (
                col("product_width_cm").isNotNull()
                & (col("product_width_cm") <= 0)
            )
        )
    )

    df = df.withColumn(
        "product_quality_status",
        when(
            col("missing_product_id")
            | col("missing_category")
            | col("duplicate_product_id")
            | col("has_invalid_numeric")
            | col("invalid_weight")
            | col("invalid_dimensions"),
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

    print("\nProduct quality summary:")

    silver_df.groupBy(
        "product_quality_status"
    ).count().show(truncate=False)

    print("\nDATA QUALITY CHECKS")
    print("-" * 55)

    checks = [
        "missing_product_id",
        "missing_category",
        "duplicate_product_id",
        "has_invalid_numeric",
        "invalid_weight",
        "invalid_dimensions",
    ]

    for check in checks:
        flagged_count = silver_df.filter(
            col(check)
        ).count()

        print(f"{check}: {flagged_count:,}")

    print("-" * 55)
    print("\nSILVER PRODUCTS PIPELINE COMPLETED")

finally:
    spark.stop()
