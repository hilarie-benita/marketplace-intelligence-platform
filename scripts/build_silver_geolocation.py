
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    lower,
    upper,
    when,
    lit,
    expr,
    count,
)

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

PROJECT_ROOT = Path(__file__).resolve().parent.parent

BRONZE_PATH = (
    PROJECT_ROOT / "data" / "bronze" / "olist_geolocation_dataset"
)

SILVER_PATH = (
    PROJECT_ROOT / "data" / "silver" / "geolocation"
)

spark = (
    SparkSession.builder
    .appName("MarketplaceSilverGeolocation")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    print("\n" + "=" * 55)
    print("MARKETPLACE INTELLIGENCE PLATFORM")
    print("SILVER LAYER — GEOLOCATION")
    print("=" * 55)

    df = spark.read.parquet(str(BRONZE_PATH))

    bronze_count = df.count()

    print(f"\nBronze records: {bronze_count:,}")

    df = (
        df.withColumn(
            "geolocation_zip_code_prefix",
            trim(col("geolocation_zip_code_prefix"))
        )
        .withColumn(
            "geolocation_city",
            lower(trim(col("geolocation_city")))
        )
        .withColumn(
            "geolocation_state",
            upper(trim(col("geolocation_state")))
        )
        .withColumn(
            "_raw_geolocation_lat",
            col("geolocation_lat")
        )
        .withColumn(
            "_raw_geolocation_lng",
            col("geolocation_lng")
        )
    )

    df = (
        df.withColumn(
            "geolocation_lat",
            expr(
                "try_cast(geolocation_lat AS DOUBLE)"
            )
        )
        .withColumn(
            "geolocation_lng",
            expr(
                "try_cast(geolocation_lng AS DOUBLE)"
            )
        )
    )

    df = df.withColumn(
        "missing_zip_code",
        col("geolocation_zip_code_prefix").isNull()
        | (col("geolocation_zip_code_prefix") == "")
    )

    df = df.withColumn(
        "missing_city",
        col("geolocation_city").isNull()
        | (col("geolocation_city") == "")
    )

    df = df.withColumn(
        "missing_state",
        col("geolocation_state").isNull()
        | (col("geolocation_state") == "")
    )

    df = df.withColumn(
        "invalid_latitude",
        col("geolocation_lat").isNull()
        | (col("geolocation_lat") < -90)
        | (col("geolocation_lat") > 90)
    )

    df = df.withColumn(
        "invalid_longitude",
        col("geolocation_lng").isNull()
        | (col("geolocation_lng") < -180)
        | (col("geolocation_lng") > 180)
    )

    df = df.withColumn(
        "geolocation_quality_status",
        when(
            col("missing_zip_code")
            | col("missing_city")
            | col("missing_state")
            | col("invalid_latitude")
            | col("invalid_longitude"),
            lit("REVIEW_REQUIRED")
        ).otherwise(lit("VALID"))
    )

    df.write.mode("overwrite").parquet(
        str(SILVER_PATH)
    )

    silver_df = spark.read.parquet(
        str(SILVER_PATH)
    )

    silver_count = silver_df.count()

    print(f"\nSilver records: {silver_count:,}")

    if bronze_count != silver_count:
        raise ValueError(
            "Row count mismatch between Bronze and Silver!"
        )

    print("\nGeolocation quality summary:")

    silver_df.groupBy(
        "geolocation_quality_status"
    ).count().show(truncate=False)

    print("\nDATA QUALITY CHECKS")
    print("-" * 55)

    checks = [
        "missing_zip_code",
        "missing_city",
        "missing_state",
        "invalid_latitude",
        "invalid_longitude",
    ]

    for check in checks:
        flagged_count = silver_df.filter(
            col(check)
        ).count()

        print(f"{check}: {flagged_count:,}")

    print("-" * 55)

    unique_zip_codes = silver_df.select(
        "geolocation_zip_code_prefix"
    ).distinct().count()

    print(f"Distinct ZIP code prefixes: {unique_zip_codes:,}")

    print("\nSILVER GEOLOCATION PIPELINE COMPLETED")

finally:
    spark.stop()

