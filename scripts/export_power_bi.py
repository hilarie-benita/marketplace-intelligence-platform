
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATABASE_PATH = PROJECT_ROOT / "data/warehouse/marketplace.duckdb"
EXPORT_DIRECTORY = PROJECT_ROOT / "reports/power_bi/exports"

MODELS = {
    "marketplace_order_performance": "order_performance",
    "marketplace_financial_performance": "financial_performance",
    "marketplace_seller_performance": "seller_performance",
    "marketplace_customer_performance": "customer_performance",
    "marketplace_category_performance": "category_performance",
    "marketplace_customer_rfm": "customer_rfm",
    "marketplace_customer_cohort_retention": "customer_cohort_retention",
    "marketplace_delivery_experience": "delivery_experience",
    "marketplace_delivery_risk_features": "delivery_risk_features",
}


def main():
    EXPORT_DIRECTORY.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(DATABASE_PATH), read_only=True)

    try:
        for model_name, export_name in MODELS.items():
            output_path = EXPORT_DIRECTORY / f"{export_name}.csv"

            con.execute(
                f"""
                COPY (
                    SELECT *
                    FROM dbt_dev_marts.{model_name}
                )
                TO '{output_path.as_posix()}'
                (HEADER TRUE, DELIMITER ',')
                """
            )

            row_count = con.execute(
                f"SELECT COUNT(*) FROM dbt_dev_marts.{model_name}"
            ).fetchone()[0]

            print(f"{export_name}.csv: {row_count:,} rows")

        print("\nAll 9 Power BI datasets exported successfully.")

    finally:
        con.close()


if __name__ == "__main__":
    main()
