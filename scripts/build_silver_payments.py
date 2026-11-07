
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
)

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

PROJECT_ROOT = Path(__file__).resolve().parent.parent

BRONZE_PATH = (
    PROJECT_ROOT / "data" / "bronze" / "olist_order_payments_dataset"
)

SILVER_PATH = (
    PROJECT_ROOT / "data" / "silver" / "payments"
)

spark = (
    SparkSession.builder
    .appName("MarketplaceSilverPayments")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    print("\n" + "=" * 55)
    print("MARKETPLACE INTELLIGENCE PLATFORM")
    print("SILVER LAYER — PAYMENTS")
    print("=" * 55)

    df = spark.read.parquet(str(BRONZE_PATH))
    bronze_count = df.count()

    print(f"\nBronze records: {bronze_count:,}")

    df = (
        df.withColumn("order_id", trim(col("order_id")))
          .withColumn("payment_type", lower(trim(col("payment_type"))))
          .withColumn("_raw_payment_sequential", col("payment_sequential"))
          .withColumn("_raw_payment_installments", col("payment_installments"))
          .withColumn("_raw_payment_value", col("payment_value"))
    )

    df = (
        df.withColumn(
            "payment_sequential",
            expr("try_cast(payment_sequential AS INT)")
        )
        .withColumn(
            "payment_installments",
            expr("try_cast(payment_installments AS INT)")
        )
        .withColumn(
            "payment_value",
            expr("try_cast(payment_value AS DECIMAL(18,2))")
        )
    )

    df = df.withColumn(
        "missing_order_id",
        col("order_id").isNull() | (col("order_id") == "")
    )

    df = df.withColumn(
        "invalid_payment_type",
        col("payment_type").isNull() | (col("payment_type") == "")
    )

    df = df.withColumn(
        "invalid_payment_sequential",
        col("payment_sequential").isNull()
        | (col("payment_sequential") <= 0)
    )

    df = df.withColumn(
        "invalid_payment_installments",
        col("payment_installments").isNull()
        | (col("payment_installments") < 0)
    )

    df = df.withColumn(
        "invalid_payment_value",
        col("payment_value").isNull()
        | (col("payment_value") < 0)
    )

    df = df.withColumn(
        "payment_quality_status",
        when(
            col("missing_order_id")
            | col("invalid_payment_type")
            | col("invalid_payment_sequential")
            | col("invalid_payment_installments")
            | col("invalid_payment_value"),
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

    print("\nPayment quality summary:")
    silver_df.groupBy(
        "payment_quality_status"
    ).count().show(truncate=False)

    print("\nDATA QUALITY CHECKS")
    print("-" * 55)

    checks = [
        "missing_order_id",
        "invalid_payment_type",
        "invalid_payment_sequential",
        "invalid_payment_installments",
        "invalid_payment_value",
    ]

    for check in checks:
        flagged_count = silver_df.filter(col(check)).count()
        print(f"{check}: {flagged_count:,}")

    print("-" * 55)
    print("\nSILVER PAYMENTS PIPELINE COMPLETED")

finally:
    spark.stop()
