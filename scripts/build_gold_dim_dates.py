
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    min as spark_min,
    max as spark_max,
    explode,
    sequence,
    expr,
    year,
    quarter,
    month,
    dayofmonth,
    dayofweek,
    dayofyear,
    weekofyear,
    date_format,
    when,
    count,
    lit,
)

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

PROJECT_ROOT = Path(__file__).resolve().parent.parent

SILVER_PATH = PROJECT_ROOT / "data" / "silver" / "orders"
GOLD_PATH = PROJECT_ROOT / "data" / "gold" / "dim_dates"

spark = (
    SparkSession.builder
    .appName("MarketplaceGoldDimDates")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    print("\n" + "=" * 55)
    print("MARKETPLACE INTELLIGENCE PLATFORM")
    print("GOLD LAYER — DATE DIMENSION")
    print("=" * 55)

    orders = spark.read.parquet(str(SILVER_PATH))

    purchase_dates = orders.select(
        to_date(
            col("order_purchase_timestamp")
        ).alias("purchase_date")
    )

    date_bounds = purchase_dates.agg(
        spark_min("purchase_date").alias("start_date"),
        spark_max("purchase_date").alias("end_date"),
    ).first()

    start_date = date_bounds["start_date"]
    end_date = date_bounds["end_date"]

    if start_date is None or end_date is None:
        raise ValueError("No valid purchase dates found!")

    print(f"\nFirst purchase date: {start_date}")
    print(f"Last purchase date: {end_date}")

    date_range = spark.sql(
        f"""
        SELECT explode(
            sequence(
                DATE '{start_date}',
                DATE '{end_date}',
                INTERVAL 1 DAY
            )
        ) AS full_date
        """
    )

    dim_dates = (
        date_range
        .withColumn(
            "date_key",
            date_format(
                col("full_date"),
                "yyyyMMdd"
            ).cast("int")
        )
        .withColumn("year", year("full_date"))
        .withColumn("quarter", quarter("full_date"))
        .withColumn("month_number", month("full_date"))
        .withColumn(
            "month_name",
            date_format("full_date", "MMMM")
        )
        .withColumn(
            "month_short_name",
            date_format("full_date", "MMM")
        )
        .withColumn(
            "year_month",
            date_format("full_date", "yyyy-MM")
        )
        .withColumn(
            "day_of_month",
            dayofmonth("full_date")
        )
        .withColumn(
            "day_of_week",
            dayofweek("full_date")
        )
        .withColumn(
            "day_name",
            date_format("full_date", "EEEE")
        )
        .withColumn(
            "day_of_year",
            dayofyear("full_date")
        )
        .withColumn(
            "week_of_year",
            weekofyear("full_date")
        )
        .withColumn(
            "is_weekend",
            col("day_of_week").isin(1, 7)
        )
        .withColumn(
            "year_quarter",
            expr(
                "concat(year(full_date), '-Q', quarter(full_date))"
            )
        )
        .select(
            "date_key",
            "full_date",
            "year",
            "quarter",
            "year_quarter",
            "month_number",
            "month_name",
            "month_short_name",
            "year_month",
            "week_of_year",
            "day_of_month",
            "day_of_week",
            "day_name",
            "day_of_year",
            "is_weekend",
        )
    )

    date_count = dim_dates.count()

    duplicate_count = (
        dim_dates
        .groupBy("date_key")
        .agg(count("*").alias("row_count"))
        .filter(col("row_count") > 1)
        .count()
    )

    expected_days = (end_date - start_date).days + 1

    if date_count != expected_days:
        raise ValueError(
            f"Expected {expected_days} dates, found {date_count}"
        )

    if duplicate_count > 0:
        raise ValueError(
            f"Duplicate date keys detected: {duplicate_count}"
        )

    dim_dates.write.mode("overwrite").parquet(
        str(GOLD_PATH)
    )

    gold_df = spark.read.parquet(str(GOLD_PATH))

    print(f"\nGold date records: {gold_df.count():,}")
    print(f"Expected calendar days: {expected_days:,}")
    print(f"Duplicate date keys: {duplicate_count:,}")

    print("\nDATES BY YEAR")
    print("-" * 55)

    gold_df.groupBy("year").count().orderBy(
        "year"
    ).show(truncate=False)

    print("\nSAMPLE DATE DIMENSION")
    print("-" * 55)

    gold_df.orderBy("full_date").show(
        10,
        truncate=False
    )

    print(f"\nGold dimension written to: {GOLD_PATH}")
    print("\nGOLD DATE DIMENSION COMPLETED")

finally:
    spark.stop()
