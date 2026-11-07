
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    sum as spark_sum,
    count,
    round as spark_round,
    coalesce,
    abs as spark_abs,
    when,
)

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

PROJECT_ROOT = Path(__file__).resolve().parent.parent
GOLD_ROOT = PROJECT_ROOT / "data" / "gold"
REPORT_ROOT = PROJECT_ROOT / "reports" / "data_quality"

spark = (
    SparkSession.builder
    .appName("MarketplaceFinancialReconciliation")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    print("\n" + "=" * 60)
    print("MARKETPLACE INTELLIGENCE PLATFORM")
    print("ORDER-LEVEL FINANCIAL RECONCILIATION")
    print("=" * 60)

    orders = spark.read.parquet(
        str(GOLD_ROOT / "fact_orders")
    ).select("order_id", "order_status")

    items = spark.read.parquet(
        str(GOLD_ROOT / "fact_order_items")
    )

    payments = spark.read.parquet(
        str(GOLD_ROOT / "fact_payments")
    )

    item_totals = items.groupBy("order_id").agg(
        spark_sum("price").alias("merchandise_value"),
        spark_sum("freight_value").alias("freight_value"),
        spark_sum("item_total_amount").alias("item_total"),
        count("*").alias("item_count"),
    )

    payment_totals = payments.groupBy("order_id").agg(
        spark_sum("payment_value").alias("payment_total"),
        count("*").alias("payment_count"),
    )

    reconciliation = (
        orders
        .join(item_totals, on="order_id", how="left")
        .join(payment_totals, on="order_id", how="left")
        .withColumn(
            "has_items",
            col("item_count").isNotNull(),
        )
        .withColumn(
            "has_payments",
            col("payment_count").isNotNull(),
        )
        .withColumn(
            "item_total",
            coalesce(col("item_total"), lit(0.0)),
        )
        .withColumn(
            "payment_total",
            coalesce(col("payment_total"), lit(0.0)),
        )
        .withColumn(
            "difference_brl",
            spark_round(
                col("payment_total") - col("item_total"),
                2,
            ),
        )
        .withColumn(
            "reconciliation_status",
            when(
                ~col("has_items") & ~col("has_payments"),
                "MISSING_BOTH",
            )
            .when(
                ~col("has_items"),
                "MISSING_ITEMS",
            )
            .when(
                ~col("has_payments"),
                "MISSING_PAYMENTS",
            )
            .when(
                spark_abs(col("difference_brl")) <= 0.01,
                "MATCHED",
            )
            .when(
                col("difference_brl") > 0.01,
                "PAYMENT_EXCEEDS_ITEMS",
            )
            .otherwise("ITEMS_EXCEED_PAYMENT"),
        )
    )

    order_count = orders.count()
    reconciliation_count = reconciliation.count()

    if reconciliation_count != order_count:
        raise ValueError(
            "Reconciliation row count does not match orders!"
        )

    REPORT_ROOT.mkdir(parents=True, exist_ok=True)

    output_path = REPORT_ROOT / "financial_reconciliation"

    reconciliation.write.mode("overwrite").parquet(
        str(output_path)
    )

    print(f"\nOrders reconciled: {reconciliation_count:,}")

    print("\nRECONCILIATION STATUS")
    print("-" * 60)

    reconciliation.groupBy(
        "reconciliation_status"
    ).agg(
        count("*").alias("orders"),
        spark_round(
            spark_sum("difference_brl"), 2
        ).alias("net_difference_brl"),
    ).orderBy(
        col("orders").desc()
    ).show(truncate=False)

    print("\nFINANCIAL TOTALS")
    print("-" * 60)

    reconciliation.agg(
        spark_round(
            spark_sum("item_total"), 2
        ).alias("item_total_brl"),
        spark_round(
            spark_sum("payment_total"), 2
        ).alias("payment_total_brl"),
        spark_round(
            spark_sum("difference_brl"), 2
        ).alias("net_difference_brl"),
    ).show(truncate=False)

    print("\nTOP 10 LARGEST DISCREPANCIES")
    print("-" * 60)

    reconciliation.filter(
        col("reconciliation_status") != "MATCHED"
    ).orderBy(
        spark_abs(col("difference_brl")).desc()
    ).select(
        "order_id",
        "order_status",
        "item_count",
        "payment_count",
        "item_total",
        "payment_total",
        "difference_brl",
        "reconciliation_status",
    ).show(10, truncate=False)

    print(f"\nReport written to: {output_path}")
    print("\nFINANCIAL RECONCILIATION COMPLETED")

finally:
    spark.stop()
