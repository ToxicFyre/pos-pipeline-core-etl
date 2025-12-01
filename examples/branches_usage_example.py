"""Simple example: Using branches parameter with load_group.

This demonstrates the key usage patterns for filtering branch columns
in the group mart pivot table.
"""

from pathlib import Path

from pos_core import DataPaths
from pos_core.sales import marts as sales_marts

# Setup
paths = DataPaths.from_root(Path("data"), Path("utils/sucursales.json"))
start_date = "2025-01-06"
end_date = "2025-01-12"

# Example 1: Load all branches (default)
print("Example 1: All branches")
print("-" * 60)
df_all = sales_marts.load_group(paths, start_date, end_date)
print(f"Columns: {list(df_all.columns)}")
print(f"Shape: {df_all.shape}\n")

# Example 2: Filter to specific branch using keyword argument
print("Example 2: Single branch (keyword argument)")
print("-" * 60)
df_kavia = sales_marts.load_group(
    paths=paths,
    start_date=start_date,
    end_date=end_date,
    branches=["Kavia"],  # ← This is the key parameter
)
print(f"Columns: {list(df_kavia.columns)}")
print(f"Shape: {df_kavia.shape}\n")

# Example 3: Multiple branches
print("Example 3: Multiple branches")
print("-" * 60)
df_multi = sales_marts.load_group(
    paths, start_date, end_date, branches=["Kavia", "Credi Club"]
)
print(f"Columns: {list(df_multi.columns)}")
print(f"Shape: {df_multi.shape}\n")

# Example 4: Using fetch_group (same behavior)
print("Example 4: Using fetch_group with branches")
print("-" * 60)
df_fetch = sales_marts.fetch_group(
    paths, start_date, end_date, branches=["Kavia"], mode="missing"
)
print(f"Columns: {list(df_fetch.columns)}")
print(f"Shape: {df_fetch.shape}\n")

print("Note: Partial matching means 'Kavia' matches 'Panem - Hotel Kavia N'")
