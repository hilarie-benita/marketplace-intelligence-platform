
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
    PROJECT_ROOT / "data" / "bronze" / "olist_orders_dataset"
)

SILVER_PATH = (
    PROJECT_ROOT / "data" / "silver" / "orders"
)

spark = (
    SparkSession.builder
    .appName("MarketplaceSilverOrders")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    print("\n" + "=" * 55)
    print("MARKETPLACE INTELLIGENCE PLATFORM")
    print("SILVER LAYER — ORDERS")
    print("=" * 55)

    df = spark.read.parquet(str(BRONZE_PATH))

    bronze_count = df.count()

    print(f"\nBronze records: {bronze_count:,}")

    df = (
        df.withColumn("order_id", trim(col("order_id")))
          .withColumn("customer_id", trim(col("customer_id")))
          .withColumn("order_status", trim(col("order_status")))
    )

    timestamp_columns = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]

    for column_name in timestamp_columns:

        raw_column = f"_raw_{column_name}"

        df = df.withColumn(
            raw_column,
            col(column_name)
        )

        df = df.withColumn(
            column_name,
            expr(
                f"try_to_timestamp(`{raw_column}`, "
                "'yyyy-MM-dd HH:mm:ss')"
            )
        )

        df = df.withColumn(
            f"invalid_{column_name}",
            (
                col(raw_column).isNotNull()
                & (trim(col(raw_column)) != "")
                & col(column_name).isNull()
            )
        )

    df = df.withColumn(
        "missing_order_id",
        col("order_id").isNull()
        | (col("order_id") == "")
    )

    df = df.withColumn(
        "missing_customer_id",
        col("customer_id").isNull()
        | (col("customer_id") == "")
    )

    df = df.withColumn(
        "missing_delivery_date",
        (
            (col("order_status") == "delivered")
            & col("order_delivered_customer_date").isNull()
        )
    )

    invalid_timestamp_columns = [
        f"invalid_{column_name}"
        for column_name in timestamp_columns
    ]

    invalid_timestamp_condition = col(
        invalid_timestamp_columns[0]
    )

    for flag_column in invalid_timestamp_columns[1:]:
        invalid_timestamp_condition = (
            invalid_timestamp_condition | col(flag_column)
        )

    df = df.withColumn(
        "has_invalid_timestamp",
        invalid_timestamp_condition
    )

    df = df.withColumn(
        "order_quality_status",
        when(
            col("missing_order_id")
            | col("missing_customer_id")
            | col("missing_delivery_date")
            | col("has_invalid_timestamp"),
            lit("REVIEW_REQUIRED")
        ).otherwise(lit("VALID"))
    )

    df.write.mode("overwrite").parquet(
        str(SILVER_PATH)
    )

    print(f"\nSilver dataset written to: {SILVER_PATH}")

    silver_df = spark.read.parquet(str(SILVER_PATH))

    silver_count = silver_df.count()

    print(f"\nSilver records: {silver_count:,}")

    if bronze_count != silver_count:
        raise ValueError(
            "Row count mismatch between Bronze and Silver!"
        )

    print("\nData quality summary:")

    silver_df.groupBy(
        "order_quality_status"
    ).count().show(truncate=False)

    invalid_deliveries = silver_df.filter(
        col("missing_delivery_date")
    ).count()

    invalid_timestamps = silver_df.filter(
        col("has_invalid_timestamp")
    ).count()

    missing_order_ids = silver_df.filter(
        col("missing_order_id")
    ).count()

    missing_customer_ids = silver_df.filter(
        col("missing_customer_id")
    ).count()

    print("\nDATA QUALITY CHECKS")
    print("-" * 55)
    print(f"Missing order IDs: {missing_order_ids:,}")
    print(f"Missing customer IDs: {missing_customer_ids:,}")
    print(f"Delivered orders missing delivery dates: {invalid_deliveries:,}")
    print(f"Records with invalid timestamps: {invalid_timestamps:,}")
    print("-" * 55)

    print("\nSILVER ORDERS PIPELINE COMPLETED")

finally:
    spark.stop()
