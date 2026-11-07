
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
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
    .appName("MarketplaceGoldFactPayments")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    print("\n" + "=" * 55)
    print("MARKETPLACE INTELLIGENCE PLATFORM")
    print("GOLD LAYER — PAYMENTS FACT TABLE")
    print("=" * 55)

    payments = spark.read.parquet(
        str(SILVER_ROOT / "payments")
    )

    orders = spark.read.parquet(
        str(GOLD_ROOT / "fact_orders")
    ).select(
        "order_id",
        "customer_id",
        "purchase_date_key",
        "order_status",
    )

    source_count = payments.count()

    print(f"\nSilver payment records: {source_count:,}")

    fact_payments = payments.join(
        orders,
        on="order_id",
        how="left",
    )

    fact_payments = fact_payments.select(
        "order_id",
        "payment_sequential",
        "customer_id",
        "purchase_date_key",
        "order_status",
        "payment_type",
        "payment_installments",
        "payment_value",
    )

    fact_count = fact_payments.count()

    if fact_count != source_count:
        raise ValueError(
            "Gold payment row count does not match Silver!"
        )

    duplicate_count = (
        fact_payments
        .groupBy("order_id", "payment_sequential")
        .agg(count("*").alias("row_count"))
        .filter(col("row_count") > 1)
        .count()
    )

    if duplicate_count > 0:
        raise ValueError(
            f"Duplicate payment keys detected: {duplicate_count}"
        )

    missing_orders = fact_payments.filter(
        col("customer_id").isNull()
        | col("purchase_date_key").isNull()
    ).count()

    if missing_orders > 0:
        raise ValueError(
            f"Payments without order attributes: {missing_orders}"
        )

    output_path = GOLD_ROOT / "fact_payments"

    fact_payments.write.mode("overwrite").parquet(
        str(output_path)
    )

    gold_df = spark.read.parquet(str(output_path))

    print(f"\nGold payment records: {gold_df.count():,}")
    print(f"Duplicate payment keys: {duplicate_count:,}")
    print(f"Payments missing order attributes: {missing_orders:,}")

    print("\nPAYMENT VALUE SUMMARY")
    print("-" * 55)

    gold_df.agg(
        spark_round(
            spark_sum("payment_value"), 2
        ).alias("total_payment_value_brl"),
    ).show(truncate=False)

    print("\nPAYMENT TYPES")
    print("-" * 55)

    gold_df.groupBy(
        "payment_type"
    ).agg(
        count("*").alias("payment_records"),
        spark_round(
            spark_sum("payment_value"), 2
        ).alias("payment_value_brl"),
    ).orderBy(
        col("payment_value_brl").desc()
    ).show(truncate=False)

    print("\nPAYMENT INSTALLMENTS")
    print("-" * 55)

    gold_df.groupBy(
        "payment_installments"
    ).count().orderBy(
        "payment_installments"
    ).show(15, truncate=False)

    print("\nSAMPLE GOLD PAYMENTS")
    print("-" * 55)

    gold_df.show(10, truncate=False)

    print(f"\nGold fact written to: {output_path}")
    print("\nGOLD PAYMENTS FACT TABLE COMPLETED")

finally:
    spark.stop()
