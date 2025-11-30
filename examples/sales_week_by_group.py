"""Example: Sales Detail ETL using new API

This example demonstrates how to use the new domain-oriented API to get sales data
for a specific week, at different grains (item-line, ticket, group).

Prerequisites:
- Set WS_BASE environment variable
- Create utils/sucursales.json with branch configuration
- Ensure data/ directory structure exists (or modify paths below)
"""

from pathlib import Path

from pos_core import DataPaths
from pos_core.sales import get_sales

# Define the week (Monday to Sunday)
week_start = "2025-01-06"  # Monday - MODIFY AS NEEDED
week_end = "2025-01-12"  # Sunday - MODIFY AS NEEDED

# Set up configuration with new unified DataPaths
data_root = Path("data")
sucursales_json = Path("utils/sucursales.json")

paths = DataPaths.from_root(data_root, sucursales_json)

# Get sales data at item-line grain (core fact) - default grain
print(f"Getting sales item-line data for {week_start} to {week_end}...")
df_item = get_sales(
    paths=paths,
    start_date=week_start,
    end_date=week_end,
    grain="item",  # Core fact: one row per item/modifier line
    refresh=True,  # Force re-run all stages
)

print(f"Item-line core fact: {len(df_item)} rows")
print(df_item.head())
print("\nThis is the atomic grain - one row per item or modifier on a ticket.")

# Get sales data aggregated by ticket (mart)
print("\nGetting sales data aggregated by ticket (mart)...")
df_ticket = get_sales(
    paths=paths,
    start_date=week_start,
    end_date=week_end,
    grain="ticket",  # Mart: one row per ticket
    refresh=False,  # Use existing data if available
)

print(f"Ticket mart: {len(df_ticket)} rows")
print(df_ticket.head())

# Get sales data aggregated by group (pivot mart)
print("\nGetting sales data aggregated by group (pivot mart)...")
df_group = get_sales(
    paths=paths,
    start_date=week_start,
    end_date=week_end,
    grain="group",  # Mart: category pivot table
    refresh=False,  # Use existing data if available
)

print(f"Group mart (pivot): {len(df_group)} rows")
print(df_group.head())

print("\nGrain Summary:")
print(f"  - Item-line (core fact): {len(df_item)} rows - most granular")
print(f"  - Ticket (mart): {len(df_ticket)} rows - aggregated per ticket")
print(f"  - Group (mart): {len(df_group)} rows - pivot by category")

print("\nThe final group output (mart_sales_by_group_*.csv) is a pivot table with:")
print("- Rows: Product groups (e.g., 'CAFE Y BEBIDAS CALIENTES', 'COMIDAS', 'PIZZA', etc.)")
print("- Columns: Sucursales (branches)")
print("- Values: Total sales amounts for each group-branch combination")
