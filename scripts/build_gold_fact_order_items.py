
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    count,
    sum as spark_sum,
    round as spark_round,
)

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SILVER_ROOT = PROJECT_ROOT / "data" / "silver"
GOLD_ROOT = PROJECT_ROOT / "data" / "gold"

spark = (
    SparkSession.builder
    .appName("MarketplaceGoldFactOrderItems")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    print("\n" + "=" * 55)
    print("MARKETPLACE INTELLIGENCE PLATFORM")
    print("GOLD LAYER — ORDER ITEMS FACT TABLE")
    print("=" * 55)

    items = spark.read.parquet(
        str(SILVER_ROOT / "order_items")
    )

    orders = spark.read.parquet(
        str(GOLD_ROOT / "fact_orders")
    ).select(
        "order_id",
        "customer_id",
        "purchase_date_key",
        "order_status",
        "is_delivered",
        "is_late_delivery",
    )

    products = spark.read.parquet(
        str(GOLD_ROOT / "dim_products")
    ).select("product_id")

    sellers = spark.read.parquet(
        str(GOLD_ROOT / "dim_sellers")
    ).select("seller_id")

    source_count = items.count()

    print(f"\nSilver order item records: {source_count:,}")

    fact_items = (
        items
        .join(orders, on="order_id", how="left")
        .join(products, on="product_id", how="left")
        .join(sellers, on="seller_id", how="left")
        .withColumn(
            "item_total_amount",
            spark_round(
                col("price") + col("freight_value"),
                2,
            ),
        )
        .withColumn(
            "item_quantity",
            lit(1),
        )
    )

    fact_items = fact_items.select(
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "customer_id",
        "purchase_date_key",
        "order_status",
        "is_delivered",
        "is_late_delivery",
        "price",
        "freight_value",
        "item_total_amount",
        "item_quantity",
    )

    fact_count = fact_items.count()

    if fact_count != source_count:
        raise ValueError(
            "Gold item row count does not match Silver!"
        )

    duplicate_count = (
        fact_items
        .groupBy("order_id", "order_item_id")
        .agg(count("*").alias("row_count"))
        .filter(col("row_count") > 1)
        .count()
    )

    if duplicate_count > 0:
        raise ValueError(
            f"Duplicate order item keys detected: {duplicate_count}"
        )

    missing_dimensions = fact_items.filter(
        col("customer_id").isNull()
        | col("purchase_date_key").isNull()
    ).count()

    if missing_dimensions > 0:
        raise ValueError(
            f"Order items missing order attributes: {missing_dimensions}"
        )

    output_path = GOLD_ROOT / "fact_order_items"

    fact_items.write.mode("overwrite").parquet(
        str(output_path)
    )

    gold_df = spark.read.parquet(str(output_path))

    print(f"\nGold order item records: {gold_df.count():,}")
    print(f"Duplicate order item keys: {duplicate_count:,}")
    print(f"Missing order attributes: {missing_dimensions:,}")

    print("\nMERCHANDISE AND FREIGHT SUMMARY")
    print("-" * 55)

    gold_df.agg(
        spark_round(
            spark_sum("price"), 2
        ).alias("merchandise_value_brl"),
        spark_round(
            spark_sum("freight_value"), 2
        ).alias("freight_value_brl"),
        spark_round(
            spark_sum("item_total_amount"), 2
        ).alias("items_plus_freight_brl"),
        spark_sum("item_quantity").alias("units_sold"),
    ).show(truncate=False)

    print("\nTOP ORDER STATUSES BY ITEM COUNT")
    print("-" * 55)

    gold_df.groupBy("order_status").count().orderBy(
        col("count").desc()
    ).show(truncate=False)

    print("\nSAMPLE GOLD ORDER ITEMS")
    print("-" * 55)

    gold_df.show(10, truncate=False)

    print(f"\nGold fact written to: {output_path}")
    print("\nGOLD ORDER ITEMS FACT TABLE COMPLETED")

finally:
    spark.stop()
