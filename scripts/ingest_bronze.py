
import os
import sys
from pathlib import Path
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

PROJECT_ROOT = Path(__file__).resolve().parent.parent

RAW_DIR = PROJECT_ROOT / "data" / "raw"
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"

EXPECTED_FILES = {
    "olist_customers_dataset.csv",
    "olist_geolocation_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv",
    "product_category_name_translation.csv",
}

spark = (
    SparkSession.builder
    .appName("MarketplaceBronzeIngestion")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:

    actual_files = {
        file.name for file in RAW_DIR.glob("*.csv")
    }

    missing_files = EXPECTED_FILES - actual_files

    if missing_files:
        raise FileNotFoundError(
            f"Missing source files: {sorted(missing_files)}"
        )

    ingestion_timestamp = datetime.now(
        timezone.utc
    ).isoformat()

    print("\n" + "=" * 50)
    print("MARKETPLACE INTELLIGENCE PLATFORM")
    print("BRONZE DATA INGESTION")
    print("=" * 50)

    for filename in sorted(EXPECTED_FILES):

        source_path = RAW_DIR / filename
        dataset_name = source_path.stem

        print(f"\nIngesting: {dataset_name}")

        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .option("multiLine", True)
            .option("quote", '"')
            .option("escape", '"')
            .option("mode", "FAILFAST")
            .csv(str(source_path))
        )

        df = (
            df.withColumn(
                "_source_file",
                lit(filename)
            )
            .withColumn(
                "_ingested_at_utc",
                lit(ingestion_timestamp)
            )
        )

        destination = BRONZE_DIR / dataset_name

        df.write.mode("overwrite").parquet(
            str(destination)
        )

        print(
            f"Bronze dataset written: {destination}"
        )

    print("\n" + "=" * 50)
    print("BRONZE INGESTION COMPLETED")
    print("=" * 50)

finally:

    spark.stop()
