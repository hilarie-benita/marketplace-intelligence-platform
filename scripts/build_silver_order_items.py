
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
)

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

PROJECT_ROOT = Path(__file__).resolve().parent.parent

BRONZE_PATH = (
    PROJECT_ROOT / "data" / "bronze" / "olist_order_items_dataset"
)

SILVER_PATH = (
    PROJECT_ROOT / "data" / "silver" / "order_items"
)

spark = (
    SparkSession.builder
    .appName("MarketplaceSilverOrderItems")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    print("\n" + "=" * 55)
    print("MARKETPLACE INTELLIGENCE PLATFORM")
    print("SILVER LAYER — ORDER ITEMS")
    print("=" * 55)

    df = spark.read.parquet(str(BRONZE_PATH))

    bronze_count = df.count()

    print(f"\nBronze records: {bronze_count:,}")

    df = (
        df.withColumn("order_id", trim(col("order_id")))
          .withColumn("product_id", trim(col("product_id")))
          .withColumn("seller_id", trim(col("seller_id")))
          .withColumn("_raw_order_item_id", col("order_item_id"))
          .withColumn("_raw_price", col("price"))
          .withColumn("_raw_freight_value", col("freight_value"))
          .withColumn(
              "_raw_shipping_limit_date",
              col("shipping_limit_date")
          )
    )

    df = (
        df.withColumn(
            "order_item_id",
            expr("try_cast(order_item_id AS INT)")
        )
        .withColumn(
            "price",
            expr("try_cast(price AS DECIMAL(18,2))")
        )
        .withColumn(
            "freight_value",
            expr("try_cast(freight_value AS DECIMAL(18,2))")
        )
        .withColumn(
            "shipping_limit_date",
            expr(
                "try_to_timestamp("
                "`_raw_shipping_limit_date`, "
                "'yyyy-MM-dd HH:mm:ss')"
            )
        )
    )

    df = df.withColumn(
        "missing_order_id",
        col("order_id").isNull() | (col("order_id") == "")
    )

    df = df.withColumn(
        "missing_product_id",
        col("product_id").isNull() | (col("product_id") == "")
    )

    df = df.withColumn(
        "missing_seller_id",
        col("seller_id").isNull() | (col("seller_id") == "")
    )

    df = df.withColumn(
        "invalid_order_item_id",
        col("order_item_id").isNull()
        | (col("order_item_id") <= 0)
    )

    df = df.withColumn(
        "invalid_price",
        col("price").isNull() | (col("price") < 0)
    )

    df = df.withColumn(
        "invalid_freight_value",
        col("freight_value").isNull()
        | (col("freight_value") < 0)
    )

    df = df.withColumn(
        "invalid_shipping_limit_date",
        col("shipping_limit_date").isNull()
    )

    df = df.withColumn(
        "order_item_quality_status",
        when(
            col("missing_order_id")
            | col("missing_product_id")
            | col("missing_seller_id")
            | col("invalid_order_item_id")
            | col("invalid_price")
            | col("invalid_freight_value")
            | col("invalid_shipping_limit_date"),
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

    print("\nOrder item quality summary:")

    silver_df.groupBy(
        "order_item_quality_status"
    ).count().show(truncate=False)

    print("\nDATA QUALITY CHECKS")
    print("-" * 55)

    checks = [
        "missing_order_id",
        "missing_product_id",
        "missing_seller_id",
        "invalid_order_item_id",
        "invalid_price",
        "invalid_freight_value",
        "invalid_shipping_limit_date",
    ]

    for check in checks:
        flagged_count = silver_df.filter(col(check)).count()
        print(f"{check}: {flagged_count:,}")

    print("-" * 55)
    print("\nSILVER ORDER ITEMS PIPELINE COMPLETED")

finally:
    spark.stop()
