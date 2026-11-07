
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum as spark_sum,
    round as spark_round,
    abs as spark_abs,
    when,
)

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

PROJECT_ROOT = Path(__file__).resolve().parent.parent

REPORT_PATH = (
    PROJECT_ROOT
    / "reports"
    / "data_quality"
    / "financial_reconciliation"
)

spark = (
    SparkSession.builder
    .appName("MarketplaceFinancialDiscrepancyAnalysis")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    print("\n" + "=" * 60)
    print("MARKETPLACE INTELLIGENCE PLATFORM")
    print("FINANCIAL DISCREPANCY ROOT CAUSE ANALYSIS")
    print("=" * 60)

    df = spark.read.parquet(str(REPORT_PATH))

    print("\nMISSING ITEMS BY ORDER STATUS")
    print("-" * 60)

    df.filter(
        col("reconciliation_status") == "MISSING_ITEMS"
    ).groupBy("order_status").agg(
        count("*").alias("orders"),
        spark_round(
            spark_sum("payment_total"), 2
        ).alias("payment_value_brl"),
    ).orderBy(
        col("orders").desc()
    ).show(truncate=False)

    print("\nRECONCILIATION BY ORDER STATUS")
    print("-" * 60)

    df.groupBy(
        "order_status",
        "reconciliation_status",
    ).agg(
        count("*").alias("orders"),
        spark_round(
            spark_sum("difference_brl"), 2
        ).alias("net_difference_brl"),
    ).orderBy(
        "order_status",
        "reconciliation_status",
    ).show(100, truncate=False)

    print("\nDISCREPANCY SUMMARY")
    print("-" * 60)

    df.groupBy("reconciliation_status").agg(
        count("*").alias("orders"),
        spark_round(
            spark_sum("difference_brl"), 2
        ).alias("net_difference_brl"),
        spark_round(
            spark_sum(
                spark_abs(col("difference_brl"))
            ), 2
        ).alias("absolute_difference_brl"),
    ).orderBy(
        col("absolute_difference_brl").desc()
    ).show(truncate=False)

    print("\nMATCHED ORDERS WITH NONZERO DIFFERENCES")
    print("-" * 60)

    df.filter(
        (col("reconciliation_status") == "MATCHED")
        & (col("difference_brl") != 0)
    ).groupBy("difference_brl").agg(
        count("*").alias("orders")
    ).orderBy(
        "difference_brl"
    ).show(truncate=False)

    print("\nFINANCIAL DISCREPANCY ANALYSIS COMPLETED")

finally:
    spark.stop()
