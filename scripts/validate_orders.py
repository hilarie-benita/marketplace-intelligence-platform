
from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
REPORT_DIR = PROJECT_ROOT / "reports" / "data_quality"

REPORT_DIR.mkdir(parents=True, exist_ok=True)

orders = pd.read_csv(RAW_DIR / "olist_orders_dataset.csv")

# DQ-001: Delivered orders must have a delivery timestamp
invalid_orders = orders[
    (orders["order_status"] == "delivered")
    & (orders["order_delivered_customer_date"].isna())
].copy()

invalid_orders["quality_rule"] = "DQ-001"
invalid_orders["quality_issue"] = "Delivered order missing delivery date"

output_file = REPORT_DIR / "dq001_invalid_delivered_orders.csv"
invalid_orders.to_csv(output_file, index=False)

print("DATA QUALITY VALIDATION")
print("-" * 45)
print("Rule: DQ-001")
print(f"Total orders checked: {len(orders):,}")
print(f"Violations detected: {len(invalid_orders)}")
print(f"Report saved: {output_file}")
