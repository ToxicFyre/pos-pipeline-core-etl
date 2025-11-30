"""Example: Full Payments ETL Pipeline using new API

This example demonstrates how to run the complete payments ETL pipeline
using the new domain-oriented API. The API handles all stages automatically:
1. Download raw data from Wansoft API (Bronze)
2. Clean into fact_payments_ticket (Silver/Core)
3. Aggregate into mart_payments_daily (Gold/Mart)

Prerequisites:
- Set WS_BASE, WS_USER, WS_PASS environment variables
- Create utils/sucursales.json with branch configuration
- Ensure data/ directory structure exists (or modify paths below)
"""

from pathlib import Path

from pos_core import DataPaths
from pos_core.payments import get_payments
from pos_core.qa import run_payments_qa

# Set up configuration with new unified DataPaths
data_root = Path("data")
sucursales_json = Path("utils/sucursales.json")

paths = DataPaths.from_root(data_root, sucursales_json)

# Define date range
start_date = "2025-01-01"  # MODIFY AS NEEDED
end_date = "2025-01-31"  # MODIFY AS NEEDED

# Run full ETL pipeline with refresh=True to force all stages
print(f"Running full payments ETL for {start_date} to {end_date}...")

# Get payments at daily grain (the default mart)
df_daily = get_payments(
    paths=paths,
    start_date=start_date,
    end_date=end_date,
    grain="daily",  # Daily mart (default)
    refresh=True,  # Force re-run all stages
)

print(f"\nDaily mart (mart_payments_daily): {len(df_daily)} rows")
print(df_daily.head())

# You can also get the core fact (ticket grain) if needed
print("\nGetting ticket-level core fact...")
df_ticket = get_payments(
    paths=paths,
    start_date=start_date,
    end_date=end_date,
    grain="ticket",  # Core fact: ticket × payment method
    refresh=False,  # Use existing cleaned data
)

print(f"Ticket core fact (fact_payments_ticket): {len(df_ticket)} rows")
print(df_ticket.head())

# Run QA checks on the daily mart
print("\nRunning QA checks...")
qa_result = run_payments_qa(df_daily, level=4)

print("\nQA Summary:")
print(f"  - Total rows: {qa_result.summary['total_rows']}")
print(f"  - Total branches: {qa_result.summary['total_sucursales']}")
print(f"  - Has missing days: {qa_result.summary['has_missing_days']}")
print(f"  - Has duplicates: {qa_result.summary['has_duplicates']}")
print(f"  - Has anomalies: {qa_result.summary['has_zscore_anomalies']}")

if qa_result.missing_days is not None and not qa_result.missing_days.empty:
    print("\nMissing days detected:")
    print(qa_result.missing_days)

print("\n✓ ETL pipeline completed successfully!")

# Show data layer summary
print("\nData Layers Created:")
print(f"  - Bronze (raw): {paths.raw_payments}")
print(f"  - Silver (core): {paths.clean_payments}")
print(f"  - Gold (mart): {paths.mart_payments}")
