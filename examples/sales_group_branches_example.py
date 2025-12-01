"""Example: Using branches parameter with load_group and fetch_group.

This example demonstrates how to filter the group mart by specific branches.
The group mart is a pivot table where:
- Rows = Product categories (Grupo_Nuevo)
- Columns = Branch names (sucursales)
- Values = Sales amounts

When you specify branches, only the matching branch columns are returned.

Prerequisites:
- Set WS_BASE environment variable
- Create utils/sucursales.json with branch configuration
- Ensure data/ directory structure exists (or modify paths below)
"""

from pathlib import Path

from pos_core import DataPaths
from pos_core.sales import marts as sales_marts

# Define the date range
start_date = "2025-01-06"
end_date = "2025-01-12"

# Set up configuration
data_root = Path("data")
sucursales_json = Path("utils/sucursales.json")
paths = DataPaths.from_root(data_root, sucursales_json)

# Example 1: Load all branches (default behavior)
print("=" * 80)
print("Example 1: Loading group mart with ALL branches")
print("=" * 80)
df_all = sales_marts.load_group(
    paths=paths,
    start_date=start_date,
    end_date=end_date,
    # branches=None is the default - returns all branch columns
)

print(f"\nShape: {df_all.shape} (rows x columns)")
print(f"Columns (all branches): {list(df_all.columns)}")
print("\nFirst few rows:")
print(df_all.head())

# Example 2: Load specific branches using keyword argument
print("\n" + "=" * 80)
print("Example 2: Loading group mart with SPECIFIC branches")
print("=" * 80)
df_filtered = sales_marts.load_group(
    paths=paths,
    start_date=start_date,
    end_date=end_date,
    branches=["Kavia"],  # Only return columns matching "Kavia"
)

print(f"\nShape: {df_filtered.shape} (rows x columns)")
print(f"Columns (filtered): {list(df_filtered.columns)}")
print("\nFirst few rows (only Kavia columns):")
print(df_filtered.head())

# Example 3: Multiple branches
print("\n" + "=" * 80)
print("Example 3: Loading group mart with MULTIPLE branches")
print("=" * 80)
df_multi = sales_marts.load_group(
    paths=paths,
    start_date=start_date,
    end_date=end_date,
    branches=["Kavia", "Credi Club"],  # Return columns matching either branch
)

print(f"\nShape: {df_multi.shape} (rows x columns)")
print(f"Columns (filtered): {list(df_multi.columns)}")
print("\nFirst few rows:")
print(df_multi.head())

# Example 4: Using fetch_group with branches (builds if needed, then filters)
print("\n" + "=" * 80)
print("Example 4: Using fetch_group with branches")
print("=" * 80)
df_fetch = sales_marts.fetch_group(
    paths=paths,
    start_date=start_date,
    end_date=end_date,
    branches=["Kavia"],  # Filters the result after building/loading
    mode="missing",  # Use existing data if available
)

print(f"\nShape: {df_fetch.shape} (rows x columns)")
print(f"Columns (filtered): {list(df_fetch.columns)}")

# Example 5: Partial matching demonstration
print("\n" + "=" * 80)
print("Example 5: Understanding partial matching")
print("=" * 80)
print(
    """
The branches parameter uses case-insensitive partial matching.
This means:
- "Kavia" will match "Panem - Hotel Kavia N" (if that's the column name)
- "kavia" (lowercase) will also match "Kavia" (case-insensitive)
- "Credi Club" will match "Panem - Credi Club" or similar variations

This is useful because branch names in the pivot table columns might have
prefixes or variations (e.g., "Panem - Hotel Kavia N" instead of just "Kavia").
"""
)

# Example 6: Comparison - with vs without branches
print("\n" + "=" * 80)
print("Example 6: Comparison - All branches vs Filtered")
print("=" * 80)

# Get all branches
df_all_branches = sales_marts.load_group(paths, start_date, end_date)

# Get filtered branches
df_kavia_only = sales_marts.load_group(paths, start_date, end_date, branches=["Kavia"])

print(f"\nAll branches: {len(df_all_branches.columns)} columns")
print(f"Kavia only: {len(df_kavia_only.columns)} columns")
print(f"\nDifference: {len(df_all_branches.columns) - len(df_kavia_only.columns)} columns filtered out")

# Show a sample row comparison
if "Grupo_Nuevo" in df_all_branches.index.name or len(df_all_branches) > 0:
    sample_category = df_all_branches.index[0] if hasattr(df_all_branches.index, "__getitem__") else None
    if sample_category:
        print(f"\nSample category '{sample_category}':")
        print(f"  All branches total: {df_all_branches.loc[sample_category].sum():,.2f}")
        if len(df_kavia_only.columns) > 0:
            print(f"  Kavia only: {df_kavia_only.loc[sample_category].sum():,.2f}")

print("\n" + "=" * 80)
print("Summary")
print("=" * 80)
print(
    """
Key points:
1. branches=None (default): Returns all branch columns in the pivot table
2. branches=["Kavia"]: Returns only columns containing "Kavia" (case-insensitive)
3. branches=["Kavia", "Credi Club"]: Returns columns matching any of the specified branches
4. Partial matching: "Kavia" matches "Panem - Hotel Kavia N" and similar variations
5. Works the same way for both load_group() and fetch_group()
6. The group mart structure: Rows = categories, Columns = branches, Values = sales amounts
"""
)
