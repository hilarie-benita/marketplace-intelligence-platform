
# Getting Started

This guide explains how to reproduce the completed data engineering and analytics engineering components of the Marketplace Intelligence & AI Platform.

## 1. Prerequisites

- Python 3.11
- Java 17 (required by PySpark)
- Git
- Approximately 2 GB of free disk space for the source data and generated outputs, with additional space recommended for Spark processing

Install the project dependencies from the repository root:

```bash
python -m pip install -r requirements.txt
```

Configure Java 17 and ensure `JAVA_HOME` points to the correct installation.

## 2. Download the Dataset

This project uses the public Brazilian E-Commerce dataset by Olist, available on Kaggle:

https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

Download and extract the dataset.

Place these nine CSV files inside `data/raw/`:

- olist_customers_dataset.csv
- olist_geolocation_dataset.csv
- olist_order_items_dataset.csv
- olist_order_payments_dataset.csv
- olist_order_reviews_dataset.csv
- olist_orders_dataset.csv
- olist_products_dataset.csv
- olist_sellers_dataset.csv
- product_category_name_translation.csv

The raw data is excluded from Git.

## 3. Build the Bronze Layer

From the repository root:

```bash
python scripts/ingest_bronze.py
```

This ingests the source CSVs into Parquet and adds source-file and ingestion-time metadata.

## 4. Build the Silver Layer

Run the cleaning and standardization scripts:

```bash
python scripts/build_silver_category_translation.py
python scripts/build_silver_customers.py
python scripts/build_silver_geolocation.py
python scripts/build_silver_order_items.py
python scripts/build_silver_orders.py
python scripts/build_silver_payments.py
python scripts/build_silver_products.py
python scripts/build_silver_reviews.py
python scripts/build_silver_sellers.py
```

These scripts create standardized Silver datasets and perform data-quality checks.

## 5. Build the Gold Layer

First, build the dimensions:

```bash
python scripts/build_gold_dim_customers.py
python scripts/build_gold_dim_products.py
python scripts/build_gold_dim_sellers.py
python scripts/build_gold_dim_dates.py
```

Then build the order fact:

```bash
python scripts/build_gold_fact_orders.py
```

Finally, build the remaining facts:

```bash
python scripts/build_gold_fact_order_items.py
python scripts/build_gold_fact_payments.py
python scripts/build_gold_fact_reviews.py
```

Validate and reconcile the Gold layer:

```bash
python scripts/validate_gold_layer.py
python scripts/reconcile_gold_financials.py
```

The financial reconciliation distinguishes order-item values from payment amounts and reports discrepancies without treating payments as recognized revenue.

## 6. Initialize the DuckDB Warehouse

```bash
python scripts/setup_duckdb.py
```

This creates `data/warehouse/marketplace.duckdb` and registers the eight Gold Parquet datasets as views under the `gold` schema.

## 7. Configure dbt

Copy the example profile to your local dbt configuration directory:

```bash
mkdir -p ~/.dbt
cp marketplace_dbt/profiles.yml.example ~/.dbt/profiles.yml
```

Edit `~/.dbt/profiles.yml` so the DuckDB `path` points to the database created in Step 6.

For reliable execution, use the absolute path to `data/warehouse/marketplace.duckdb` on your machine.

## 8. Build and Test the Analytics Models

From the repository root:

```bash
cd marketplace_dbt
dbt debug
dbt build
```

The completed project contains nine analytical models and 52 automated tests.

## 9. Generate the Data Lineage Documentation

From the `marketplace_dbt` directory:

```bash
dbt docs generate
dbt docs serve --port 8080
```

Open http://localhost:8080 to explore the dbt models, source dependencies, and tests.

## 10. Export Data for Power BI

Return to the repository root:

```bash
cd ..
python scripts/export_power_bi.py
```

The generated CSV files can be imported into Power BI Desktop for dashboard development.

The Power BI dashboard, machine learning, and AI features remain in development.

## Project Documentation

- [Medallion Architecture](medallion_architecture.md)
- [Financial Reconciliation](financial_reconciliation.md)
- [Main README](../README.md)