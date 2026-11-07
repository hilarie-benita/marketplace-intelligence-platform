
from pathlib import Path
import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent

GOLD_ROOT = PROJECT_ROOT / "data" / "gold"
DATABASE_DIR = PROJECT_ROOT / "data" / "warehouse"
DATABASE_PATH = DATABASE_DIR / "marketplace.duckdb"

TABLES = [
    "dim_customers",
    "dim_products",
    "dim_sellers",
    "dim_dates",
    "fact_orders",
    "fact_order_items",
    "fact_payments",
    "fact_reviews",
]

DATABASE_DIR.mkdir(parents=True, exist_ok=True)

print("\n" + "=" * 60)
print("MARKETPLACE INTELLIGENCE PLATFORM")
print("DUCKDB ANALYTICAL WAREHOUSE SETUP")
print("=" * 60)

con = duckdb.connect(str(DATABASE_PATH))

try:
    con.execute("CREATE SCHEMA IF NOT EXISTS gold")

    print("\nREGISTERING GOLD LAYER TABLES")
    print("-" * 60)

    for table_name in TABLES:

        table_dir = GOLD_ROOT / table_name
        parquet_files = list(table_dir.glob("*.parquet"))

        if not parquet_files:
            raise FileNotFoundError(
                f"No Parquet files found for {table_name} "
                f"in {table_dir}"
            )

        parquet_path = table_dir / "*.parquet"

        safe_parquet_path = str(parquet_path).replace("'", "''")

        con.execute(
            f"""
            CREATE OR REPLACE VIEW gold.{table_name} AS
            SELECT *
            FROM read_parquet('{safe_parquet_path}')
            """
        )

        row_count = con.execute(
            f"SELECT COUNT(*) FROM gold.{table_name}"
        ).fetchone()[0]

        print(
            f"REGISTERED | gold.{table_name}: "
            f"{row_count:,} rows"
        )

    print("\nVERIFYING GOLD SCHEMA")
    print("-" * 60)

    views = con.execute(
        """
        SELECT table_name
        FROM information_schema.views
        WHERE table_schema = 'gold'
        ORDER BY table_name
        """
    ).fetchall()

    for (view_name,) in views:
        print(f"VIEW FOUND | gold.{view_name}")

    print("\nFIRST SQL ANALYTICAL QUERY")
    print("-" * 60)

    results = con.execute(
        """
        SELECT
            order_status,
            COUNT(*) AS total_orders
        FROM gold.fact_orders
        GROUP BY order_status
        ORDER BY total_orders DESC
        """
    ).fetchall()

    print(f"{'Order Status':<20} {'Total Orders':>15}")
    print("-" * 38)

    for status, total in results:
        print(f"{status:<20} {total:>15,}")

    print("\n" + "=" * 60)
    print("DUCKDB WAREHOUSE SETUP COMPLETED")
    print("=" * 60)

    print(f"Database: {DATABASE_PATH}")
    print(f"Gold tables registered: {len(TABLES)}")
    print(f"Gold views verified: {len(views)}")

finally:
    con.close()
