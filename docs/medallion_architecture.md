
# Medallion Architecture

## Overview

The Marketplace Intelligence & AI Platform implements a
Bronze–Silver–Gold Medallion Architecture using PySpark
and Apache Parquet.

The architecture separates raw ingestion, data cleaning,
and business-ready dimensional modeling into distinct layers.

## Bronze Layer — Raw Data Ingestion

**Implementation:** `scripts/ingest_bronze.py`

### Data Source

The Bronze layer ingests nine CSV files from the publicly
available Olist Brazilian E-Commerce dataset.

Expected source files:

- olist_customers_dataset.csv
- olist_geolocation_dataset.csv
- olist_order_items_dataset.csv
- olist_order_payments_dataset.csv
- olist_order_reviews_dataset.csv
- olist_orders_dataset.csv
- olist_products_dataset.csv
- olist_sellers_dataset.csv
- product_category_name_translation.csv

### Ingestion Process

1. Validate the presence of all nine expected CSV files.
2. Read source data using PySpark without automatic schema inference.
3. Apply fail-fast handling for malformed CSV records.
4. Add source filename and UTC ingestion timestamp metadata.
5. Write each dataset to the Bronze layer in Parquet format.

### Data Lineage Metadata

Each ingested record receives two additional columns:

- `_source_file`: Original source CSV filename.
- `_ingested_at_utc`: UTC timestamp of the ingestion run.

### Storage

Input directory: `data/raw/`

Output directory: `data/bronze/`

### Run Bronze Ingestion

From the project root:

```bash
python scripts/ingest_bronze.py
```

The ingestion process requires all nine source CSV files
to be present in `data/raw/`.


## Silver Layer — Cleaning, Standardization & Data Quality

**Implementation:** `scripts/build_silver_*.py`

The Silver layer transforms Bronze datasets into standardized,
typed, and quality-checked Parquet datasets using PySpark.

### Key Transformations

**1. Data Standardization**
- Trim whitespace from identifiers and text fields.
- Normalize city and category names to lowercase.
- Standardize state abbreviations to uppercase.

**2. Data Type Enforcement**
- Convert numeric identifiers and quantities to integers.
- Convert monetary fields to DECIMAL(18,2).
- Parse order, shipping, and review timestamps.
- Convert geographic coordinates to numeric values.

**3. Duplicate Detection**
- Identify duplicate customer identifiers.
- Identify duplicate product and seller identifiers.
- Flag duplicate category mappings.

**4. Data Quality Validation**
- Detect invalid or missing identifiers.
- Identify malformed numeric and timestamp values.
- Create data quality indicators for suspicious records.
- Report counts of records that fail validation checks.

**5. Data Preservation**
- Retain original values in selected fields to support
  validation and traceability.
- Apply quality flags rather than automatically removing
  every problematic record.

### Silver Datasets

The Silver layer processes nine marketplace datasets:

- Customers
- Geolocation
- Order items
- Orders
- Payments
- Products
- Reviews
- Sellers
- Product category translations

### Storage

Input directory: `data/bronze/`

Output directory: `data/silver/`

### Engineering Objective

The Silver layer provides standardized, traceable data
for downstream dimensional modeling while making data
quality issues visible for investigation.


## Gold Layer — Dimensional Modeling & Data Validation

**Implementation:** `scripts/build_gold_*.py`

The Gold layer transforms standardized Silver datasets into
business-ready fact and dimension tables using PySpark.

### Dimensional Model

The Gold layer contains eight tables organized into
four dimensions and four facts.

**Dimension Tables**

| Table | Purpose |
|---|---|
| dim_customers | Customer attributes and geographic information |
| dim_products | Product attributes and category information |
| dim_sellers | Seller attributes and geographic information |
| dim_dates | Calendar attributes for time-based analytics |

**Fact Tables**

| Table | Purpose |
|---|---|
| fact_orders | Order lifecycle and delivery performance |
| fact_order_items | Item-level sales, prices, and freight |
| fact_payments | Payment transactions and values |
| fact_reviews | Customer review information |

### Data Quality

The Gold layer includes 33 validation checks covering
data integrity and consistency.

Validation is implemented through:

`scripts/validate_gold_layer.py`

### Financial Reconciliation

The platform includes reconciliation logic to compare
order-item financial totals with payment amounts.

The reconciliation process identifies discrepancies
and missing financial records without assuming that
payment values represent recognized revenue.

See `docs/financial_reconciliation.md` for additional details.


### Gold Layer Execution Order

Gold tables must be built in dependency order.

**Step 1 — Build Dimensions**

```bash
python scripts/build_gold_dim_customers.py
python scripts/build_gold_dim_products.py
python scripts/build_gold_dim_sellers.py
python scripts/build_gold_dim_dates.py
```

**Step 2 — Build Order Fact**

```bash
python scripts/build_gold_fact_orders.py
```

The order fact depends on the customer dimension.

**Step 3 — Build Remaining Facts**

```bash
python scripts/build_gold_fact_order_items.py
python scripts/build_gold_fact_payments.py
python scripts/build_gold_fact_reviews.py
```

These fact tables depend on the completed order fact.
The order-items fact also references the product and
seller dimensions.

**Step 4 — Validate Gold Data**

```bash
python scripts/validate_gold_layer.py
```

The Gold validation script implements 33 data quality checks.

### Storage

Input directory: `data/silver/`

Output directory: `data/gold/`

### Downstream Consumption

Gold-layer Parquet datasets are exposed through DuckDB
for analytical modeling with dbt.

The resulting analytical models support marketplace
performance, customer segmentation, cohort retention,
seller performance, financial analysis, and delivery analytics.


## Analytical Warehouse — DuckDB

**Implementation:** `scripts/setup_duckdb.py`

DuckDB provides the SQL analytical layer between the
Gold Medallion layer and dbt.

### Warehouse Design

The warehouse uses DuckDB views over Gold-layer Parquet
datasets rather than duplicating the data into physical
warehouse tables.

The setup process:

1. Creates the local warehouse directory.
2. Initializes the DuckDB database.
3. Creates the `gold` schema.
4. Registers eight Gold datasets as SQL views.
5. Verifies view availability and row counts.
6. Executes a sample analytical SQL query.

### Initialize the Warehouse

From the project root:

```bash
python scripts/setup_duckdb.py
```

Database location:

`data/warehouse/marketplace.duckdb`

The database is generated locally and is not committed
to GitHub.

## Analytics Engineering — dbt Core

**Implementation:** `marketplace_dbt/`

dbt transforms the Gold-layer warehouse views into
business-oriented analytical models.

### Analytical Models

The project includes nine dbt models covering:

- Order performance
- Financial performance and reconciliation
- Seller performance
- Customer performance
- Product category performance
- Customer RFM segmentation
- Customer cohort retention
- Delivery experience
- Delivery-risk feature engineering

### Automated Testing

The dbt project contains 52 automated tests.

The validated build completed successfully:

- 9 analytical models
- 52 automated tests
- 61 successful resources
- 0 errors

### Data Lineage

dbt documentation provides a dependency graph connecting
Gold-layer sources, analytical models, and integrity tests.

The lineage graph is available in the main project README.

### Run dbt

After initializing DuckDB, navigate to the dbt directory:

```bash
cd marketplace_dbt
```

Run the models and tests:

```bash
dbt build
```

Generate documentation:

```bash
dbt docs generate
```

Launch the documentation website:

```bash
dbt docs serve --port 8080
```

The documentation will be accessible locally at:

`http://localhost:8080`

A local dbt profile must be configured before running
these commands. See `profiles.yml.example` for the
required connection settings.