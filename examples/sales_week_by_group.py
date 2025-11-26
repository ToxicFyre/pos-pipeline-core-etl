"""Example: Sales Detail ETL using new query API

This example demonstrates how to use the new high-level query API to get sales data
for a specific week, aggregated by ticket and by product group.

Prerequisites:
- Set WS_BASE environment variable
- Create utils/sucursales.json with branch configuration
- Ensure data/ directory structure exists (or modify paths below)
"""

from pathlib import Path

from pos_core.etl import SalesETLConfig, get_sales

# Define the week (Monday to Sunday)
week_start = "2025-01-06"  # Monday - MODIFY AS NEEDED
week_end = "2025-01-12"  # Sunday - MODIFY AS NEEDED

# Set up configuration
data_root = Path("data")
sucursales_json = Path("utils/sucursales.json")

config = SalesETLConfig.from_root(data_root, sucursales_json)

# Get sales data aggregated by ticket (with refresh to ensure fresh data)
print(f"Getting sales data for {week_start} to {week_end}...")
df_ticket = get_sales(
    start_date=week_start,
    end_date=week_end,
    config=config,
    level="ticket",
    refresh=True,  # Force re-run all stages
)

print(f"Aggregated by ticket: {len(df_ticket)} tickets")
print(df_ticket.head())

# Get sales data aggregated by group (reuses existing data if available)
print("\nGetting sales data aggregated by group...")
df_group = get_sales(
    start_date=week_start,
    end_date=week_end,
    config=config,
    level="group",
    refresh=False,  # Use existing data if available
)

print(f"Aggregated by group: pivot table with {len(df_group)} groups")
print(df_group.head())

print("\nThe final output (sales_by_group_*.csv) is a pivot table with:")
print("- Rows: Product groups (e.g., 'CAFE Y BEBIDAS CALIENTES', 'COMIDAS', 'PIZZA', etc.)")
print("- Columns: Sucursales (branches)")
print("- Values: Total sales amounts for each group-branch combination")
