"""Example 1: Sales Detail ETL (Low-Level APIs)

This example demonstrates how to download sales detail reports for all sucursales
for a specific week, clean them, and aggregate by product group.

Prerequisites:
- Set WS_BASE environment variable (or modify base_url below)
- Create utils/sucursales.json with branch configuration
- Ensure data/ directory structure exists (or modify paths below)

Note: Unlike payments (which has the high-level build_payments_dataset() API),
sales details currently require using lower-level functions directly.
Payments is the primary public API; sales details are lower-level utilities.
"""

from pathlib import Path
from datetime import date
import os
import pandas as pd

from pos_core.etl.a_extract.HTTP_extraction import (
    make_session,
    login_if_needed,
    export_sales_report,
    build_out_name
)
from pos_core.etl.branch_config import load_branch_segments_from_json
from pos_core.etl.b_transform.pos_excel_sales_details_cleaner import (
    transform_detalle_ventas,
    output_name_for
)
from pos_core.etl.c_load.aggregate_sales_details_by_ticket import aggregate_by_ticket
from pos_core.etl.c_load.aggregate_sales_details_by_group import build_category_pivot

# Define the week (Monday to Sunday)
week_start = "2025-01-06"  # Monday - MODIFY AS NEEDED
week_end = "2025-01-12"    # Sunday - MODIFY AS NEEDED

# Set up paths - MODIFY AS NEEDED
data_root = Path("data")
raw_sales_dir = data_root / "a_raw" / "sales" / "batch"
clean_sales_dir = data_root / "b_clean" / "sales" / "batch"
sucursales_json = Path("utils/sucursales.json")

# Step 1: Download sales detail reports for all sucursales
raw_sales_dir.mkdir(parents=True, exist_ok=True)

# Get base URL from environment (or set explicitly)
base_url = os.environ.get("WS_BASE")
if not base_url:
    raise ValueError("WS_BASE environment variable must be set. "
                     "Set it in your environment or modify this script to set base_url directly.")

# Create session and authenticate
session = make_session()
login_if_needed(session, base_url=base_url, user=None, password=None)

# Load branch configuration
branch_segments = load_branch_segments_from_json(sucursales_json)
start_date = date.fromisoformat(week_start)
end_date = date.fromisoformat(week_end)

# Download reports for each branch
for branch_name, segments in branch_segments.items():
    for segment in segments:
        code = segment.code
        # Check if this code was valid during the week
        if segment.valid_from and segment.valid_from > end_date:
            continue
        if segment.valid_to and segment.valid_to < start_date:
            continue
        
        try:
            # Export the report
            suggested, blob = export_sales_report(
                s=session,
                base_url=base_url,
                report="Detail",
                subsidiary_id=code,
                start=start_date,
                end=end_date,
            )
            
            # Save file
            out_name = build_out_name("Detail", branch_name, start_date, end_date, suggested)
            out_path = raw_sales_dir / out_name
            out_path.write_bytes(blob)
            print(f"Downloaded: {out_path}")
        except Exception as e:
            print(f"Error downloading {branch_name} ({code}): {e}")

# Step 2: Clean Excel files to CSV
clean_sales_dir.mkdir(parents=True, exist_ok=True)

for xlsx_file in raw_sales_dir.glob("*.xlsx"):
    try:
        df = transform_detalle_ventas(xlsx_file)
        out_name = output_name_for(xlsx_file, df)
        out_path = clean_sales_dir / out_name
        out_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out_path, index=False, encoding="utf-8")
        print(f"Cleaned: {out_path} ({len(df)} rows)")
    except Exception as e:
        print(f"Error cleaning {xlsx_file}: {e}")

# Step 3: Aggregate by ticket
ticket_csv = data_root / "c_processed" / "sales" / f"sales_by_ticket_{week_start}_{week_end}.csv"
ticket_csv.parent.mkdir(parents=True, exist_ok=True)

ticket_df = aggregate_by_ticket(
    input_csv=str(clean_sales_dir / "*.csv"),
    output_csv=str(ticket_csv),
    recursive=True
)
print(f"Aggregated by ticket: {ticket_csv} ({len(ticket_df)} tickets)")

# Step 4: Aggregate by group (creates pivot table: groups × sucursales)
group_csv = data_root / "c_processed" / "sales" / f"sales_by_group_{week_start}_{week_end}.csv"
group_csv.parent.mkdir(parents=True, exist_ok=True)

group_pivot = build_category_pivot(
    input_csv=str(ticket_csv),
    output_csv=str(group_csv)
)
print(f"Aggregated by group: {group_csv}")
print(group_pivot)

print("\nThe final output (sales_by_group_*.csv) is a pivot table with:")
print("- Rows: Product groups (e.g., 'CAFE Y BEBIDAS CALIENTES', 'COMIDAS', 'PIZZA', etc.)")
print("- Columns: Sucursales (branches)")
print("- Values: Total sales amounts for each group-branch combination")

