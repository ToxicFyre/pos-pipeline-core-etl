"""Example 2: Main Payments ETL Workflow

This example demonstrates how to build a comprehensive aggregated payments dataset
with one row per day per sucursal for a date range. At the end, you'll have a
DataFrame ready for analysis or forecasting.

Prerequisites:
- Set WS_BASE environment variable (for online extraction)
- Create utils/sucursales.json with branch configuration
- Ensure data/ directory structure exists (or modify paths below)

This example uses the high-level build_payments_dataset() API, which is the
recommended way to process payments data.
"""

from datetime import date, timedelta
from pathlib import Path

from pos_core.etl import PaymentsETLConfig, build_payments_dataset

# Calculate date range (3 years ago to today) - MODIFY AS NEEDED
end_date = date.today()
start_date = end_date - timedelta(days=3 * 365)

# Set up configuration - MODIFY PATHS AS NEEDED
data_root = Path("data")
sucursales_json = Path("utils/sucursales.json")

config = PaymentsETLConfig.from_data_root(
    data_root=data_root,
    sucursales_json=sucursales_json,
    chunk_size_days=180,  # Process in 6-month chunks
)

# Run the complete ETL pipeline
# This will:
# 1. Download missing payment reports from POS API
# 2. Clean the raw Excel files into normalized CSVs
# 3. Aggregate cleaned data into daily dataset
print(f"Running ETL for {start_date} to {end_date}...")
payments_df = build_payments_dataset(
    start_date=start_date.strftime("%Y-%m-%d"),
    end_date=end_date.strftime("%Y-%m-%d"),
    config=config,
    branches=None,  # Process all branches (or specify: ["Banana", "Queen"])
)

# The resulting DataFrame has one row per sucursal per day
print("\nETL Complete!")
print(f"Total rows: {len(payments_df)}")
print(f"Date range: {payments_df['fecha'].min()} to {payments_df['fecha'].max()}")
print(f"Branches: {payments_df['sucursal'].nunique()}")
print(f"\nColumns: {list(payments_df.columns)}")
print("\nFirst few rows:")
print(payments_df.head())

# Save to CSV for future use
output_path = data_root / "c_processed" / "payments" / "aggregated_payments_daily.csv"
output_path.parent.mkdir(parents=True, exist_ok=True)
payments_df.to_csv(output_path, index=False)
print(f"\nSaved to: {output_path}")

# Example: Filter for a specific branch
if len(payments_df) > 0:
    sample_branch = payments_df["sucursal"].iloc[0]
    branch_payments = payments_df[payments_df["sucursal"] == sample_branch]
    print(f"\n{sample_branch} payments: {len(branch_payments)} days")

# Example: Get summary statistics
summary = payments_df.groupby("sucursal").agg(
    {"ingreso_total": ["sum", "mean", "min", "max"], "fecha": ["min", "max", "count"]}
)
print("\nSummary by branch:")
print(summary)

print("\nThe resulting DataFrame contains columns such as:")
print("- sucursal: Branch name")
print("- fecha: Date (YYYY-MM-DD)")
print("- ingreso_efectivo: Cash income")
print("- ingreso_credito: Credit card income")
print("- ingreso_debito: Debit card income")
print("- ingreso_total: Total income")
print("- Additional payment method columns (AMEX, UberEats, Rappi, etc.)")
