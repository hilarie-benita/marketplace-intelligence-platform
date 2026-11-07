
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    date_format,
    datediff,
    unix_timestamp,
    when,
    lit,
    count,
    trim,
)

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SILVER_ROOT = PROJECT_ROOT / "data" / "silver"
GOLD_ROOT = PROJECT_ROOT / "data" / "gold"

spark = (
    SparkSession.builder
    .appName("MarketplaceGoldFactOrders")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    print("\n" + "=" * 55)
    print("MARKETPLACE INTELLIGENCE PLATFORM")
    print("GOLD LAYER — ORDER FACT TABLE")
    print("=" * 55)

    orders = spark.read.parquet(
        str(SILVER_ROOT / "orders")
    )

    customers = spark.read.parquet(
        str(GOLD_ROOT / "dim_customers")
    ).select(
        "customer_id",
        "customer_unique_id"
    )

    source_count = orders.count()

    print(f"\nSilver order records: {source_count:,}")

    fact_orders = (
        orders
        .join(
            customers,
            on="customer_id",
            how="left"
        )
        .withColumn(
            "purchase_date_key",
            date_format(
                to_date("order_purchase_timestamp"),
                "yyyyMMdd"
            ).cast("int")
        )
        .withColumn(
            "purchase_date",
            to_date("order_purchase_timestamp")
        )
        .withColumn(
            "approved_date",
            to_date("order_approved_at")
        )
        .withColumn(
            "carrier_date",
            to_date("order_delivered_carrier_date")
        )
        .withColumn(
            "delivered_date",
            to_date("order_delivered_customer_date")
        )
        .withColumn(
            "estimated_delivery_date",
            to_date("order_estimated_delivery_date")
        )
        .withColumn(
            "delivery_days",
            datediff(
                col("delivered_date"),
                col("purchase_date")
            )
        )
        .withColumn(
            "delivery_delay_days",
            datediff(
                col("delivered_date"),
                col("estimated_delivery_date")
            )
        )
        .withColumn(
            "is_delivered",
            col("order_status") == "delivered"
        )
        .withColumn(
            "is_late_delivery",
            when(
                col("delivered_date").isNotNull(),
                col("delivered_date")
                > col("estimated_delivery_date")
            ).otherwise(lit(None).cast("boolean"))
        )
        .withColumn(
            "is_order_valid",
            col("order_quality_status") == "VALID"
        )
    )

    fact_orders = fact_orders.select(
        "order_id",
        "customer_id",
        "customer_unique_id",
        "order_status",
        "purchase_date_key",
        "purchase_date",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
        "approved_date",
        "carrier_date",
        "delivered_date",
        "estimated_delivery_date",
        "delivery_days",
        "delivery_delay_days",
        "is_delivered",
        "is_late_delivery",
        "order_quality_status",
        "is_order_valid"
    )

    fact_count = fact_orders.count()

    if fact_count != source_count:
        raise ValueError(
            "Fact order row count does not match Silver!"
        )

    duplicate_count = (
        fact_orders
        .groupBy("order_id")
        .agg(count("*").alias("row_count"))
        .filter(col("row_count") > 1)
        .count()
    )

    if duplicate_count > 0:
        raise ValueError(
            f"Duplicate order IDs detected: {duplicate_count}"
        )

    missing_customers = fact_orders.filter(
        col("customer_unique_id").isNull()
    ).count()

    if missing_customers > 0:
        raise ValueError(
            f"Orders without matching customers: {missing_customers}"
        )

    missing_dates = fact_orders.filter(
        col("purchase_date_key").isNull()
    ).count()

    if missing_dates > 0:
        raise ValueError(
            f"Orders without purchase dates: {missing_dates}"
        )

    output_path = GOLD_ROOT / "fact_orders"

    fact_orders.write.mode("overwrite").parquet(
        str(output_path)
    )

    gold_df = spark.read.parquet(
        str(output_path)
    )

    print(f"\nGold order records: {gold_df.count():,}")
    print(f"Duplicate order IDs: {duplicate_count:,}")
    print(f"Orders without customers: {missing_customers:,}")
    print(f"Orders without purchase dates: {missing_dates:,}")

    print("\nORDER STATUS SUMMARY")
    print("-" * 55)

    gold_df.groupBy(
        "order_status"
    ).count().orderBy(
        col("count").desc()
    ).show(truncate=False)

    print("\nORDER QUALITY SUMMARY")
    print("-" * 55)

    gold_df.groupBy(
        "order_quality_status"
    ).count().show(truncate=False)

    print("\nDELIVERY PERFORMANCE")
    print("-" * 55)

    gold_df.groupBy(
        "is_late_delivery"
    ).count().show(truncate=False)

    print("\nSAMPLE GOLD ORDERS")
    print("-" * 55)

    gold_df.select(
        "order_id",
        "order_status",
        "purchase_date",
        "delivered_date",
        "estimated_delivery_date",
        "delivery_days",
        "delivery_delay_days",
        "is_late_delivery"
    ).show(10, truncate=False)

    print(f"\nGold fact written to: {output_path}")
    print("\nGOLD ORDER FACT TABLE COMPLETED")

finally:
    spark.stop()
