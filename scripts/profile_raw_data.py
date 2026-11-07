
from pathlib import Path
import json
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
REPORT_DIR = PROJECT_ROOT / "reports" / "data_quality"

REPORT_DIR.mkdir(parents=True, exist_ok=True)

csv_files = sorted(RAW_DIR.glob("*.csv"))

if not csv_files:
    raise FileNotFoundError(f"No CSV files found in {RAW_DIR}")

results = []

for file_path in csv_files:
    print(f"Profiling: {file_path.name}")

    df = pd.read_csv(file_path, low_memory=False)

    row_count = len(df)
    column_count = len(df.columns)
    duplicate_rows = int(df.duplicated().sum())

    for column in df.columns:
        missing_count = int(df[column].isna().sum())

        results.append({
            "dataset": file_path.name,
            "column": column,
            "data_type": str(df[column].dtype),
            "total_rows": row_count,
            "total_columns": column_count,
            "missing_count": missing_count,
            "missing_percentage": round(
                missing_count / row_count * 100, 2
            ) if row_count else 0,
            "distinct_values": int(df[column].nunique(dropna=True)),
            "duplicate_rows_in_dataset": duplicate_rows,
        })

profile_df = pd.DataFrame(results)

csv_report = REPORT_DIR / "raw_data_profile.csv"
json_report = REPORT_DIR / "raw_data_profile.json"

profile_df.to_csv(csv_report, index=False)

with open(json_report, "w", encoding="utf-8") as file:
    json.dump(
        results,
        file,
        indent=2,
        ensure_ascii=False
    )

print("\nDATA PROFILING COMPLETED")
print("-" * 45)
print(f"Datasets processed: {len(csv_files)}")
print(f"Columns profiled: {len(profile_df)}")
print(f"CSV report: {csv_report}")
print(f"JSON report: {json_report}")
