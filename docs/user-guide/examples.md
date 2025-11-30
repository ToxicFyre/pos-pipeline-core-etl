# Examples

This page provides complete runnable example scripts demonstrating POS Core ETL usage. All examples are self-contained and can be run directly.

## Prerequisites

Before running any example:

1. **Install the package**: `pip install -e .` (or `pip install pos-core-etl` for production)
2. **Create `utils/sucursales.json`**: Branch configuration file (see [Configuration](configuration.md))
3. **Set environment variables** (required for online extraction):
   - `WS_BASE`: Base URL of your POS instance
   - `WS_USER`: Username for authentication
   - `WS_PASS`: Password for authentication

   **Example:**
   ```bash
   export WS_BASE="https://your-pos-instance.com"
   export WS_USER="your_username"
   export WS_PASS="your_password"
   ```
4. **Create data directory structure** (or modify paths in scripts):
   ```
   data/
   ├── a_raw/
   ├── b_clean/
   └── c_processed/
   ```

## Example 1: Payments Daily Mart

**File**: `examples/payments_full_etl.py`

Demonstrates fetching payments data at the daily mart level:

```python
from pathlib import Path
from pos_core import DataPaths
from pos_core.payments import marts as payments_marts

# Configure paths
paths = DataPaths.from_root(Path("data"), Path("utils/sucursales.json"))

# Fetch daily mart (most common use case)
# This automatically handles extraction, transformation, and aggregation
payments_daily = payments_marts.fetch_daily(
    paths, 
    "2025-01-01", 
    "2025-01-31"
)

print(f"Retrieved {len(payments_daily)} rows")
print(payments_daily.head())

# Show summary statistics
print("\nSummary by branch:")
print(payments_daily.groupby("sucursal")["ingreso_total"].sum())
```

**Key Features:**
- Uses `fetch_daily()` which automatically handles all ETL stages
- Idempotent: safe to run multiple times
- Returns daily aggregations ready for analysis

## Example 2: Sales Core Fact

**File**: `examples/sales_week_by_group.py`

Demonstrates fetching sales data at different aggregation levels:

```python
from pathlib import Path
from pos_core import DataPaths
from pos_core.sales import core as sales_core
from pos_core.sales import marts as sales_marts

# Configure paths
paths = DataPaths.from_root(Path("data"), Path("utils/sucursales.json"))

# Get core fact (item-line grain)
sales_items = sales_core.fetch(paths, "2025-01-01", "2025-01-31")
print(f"Core fact: {len(sales_items)} item lines")

# Get ticket-level mart
sales_tickets = sales_marts.fetch_ticket(paths, "2025-01-01", "2025-01-31")
print(f"Ticket mart: {len(sales_tickets)} tickets")

# Get group-level mart (pivot table)
sales_groups = sales_marts.fetch_group(paths, "2025-01-01", "2025-01-31")
print(f"Group mart: {len(sales_groups)} rows")
print(sales_groups.head())
```

**Key Features:**
- Shows different aggregation levels available
- Demonstrates the relationship between core fact and marts
- Group mart creates a pivot table by category

## Example 3: Forecasting

**File**: `examples/payments_forecast.py`

Demonstrates generating forecasts for payment metrics:

```python
from pathlib import Path
from pos_core import DataPaths
from pos_core.payments import marts as payments_marts
from pos_core.forecasting import ForecastConfig, run_payments_forecast

# Configure paths
paths = DataPaths.from_root(Path("data"), Path("utils/sucursales.json"))

# Get historical data (need sufficient history for forecasting)
# ARIMA models require at least 30 days
payments_df = payments_marts.fetch_daily(
    paths, 
    "2022-01-01", 
    "2025-01-31"
)

# Configure forecast
config = ForecastConfig(horizon_days=91)  # 13 weeks

# Run forecast
result = run_payments_forecast(payments_df, config)

# Access results
print("Forecast Results:")
print(result.forecast.head(20))

print("\nDeposit Schedule:")
print(result.deposit_schedule.head())

print("\nMetadata:")
print(f"Branches: {result.metadata['branches']}")
print(f"Metrics: {result.metadata['metrics']}")
print(f"Horizon: {result.metadata['horizon_days']} days")
```

**Key Features:**
- Requires sufficient historical data (30+ days recommended)
- Generates forecasts per branch and per metric
- Includes deposit schedule for cash flow planning

### With Debug Information

For advanced debugging, enable debug mode:

```python
# Run forecast with debug information
result = run_payments_forecast(payments_df, config, debug=True)

# Access debug info for a specific model/branch/metric
if result.debug:
    naive_debug = result.debug["naive_last_week"]["Kavia"]["ingreso_efectivo"]
    print(f"Model: {naive_debug.model_name}")
    print(f"Source dates: {naive_debug.data['source_dates']}")
```

## Example 4: Quality Assurance

Demonstrates running QA checks on payments data:

```python
from pathlib import Path
from pos_core import DataPaths
from pos_core.payments import marts as payments_marts
from pos_core.qa import run_payments_qa

# Configure paths
paths = DataPaths.from_root(Path("data"), Path("utils/sucursales.json"))

# Get payments data
df = payments_marts.fetch_daily(paths, "2025-01-01", "2025-01-31")

# Run QA checks
result = run_payments_qa(df, level=4)

# Print summary
print("QA Summary:")
print(f"Total rows: {result.summary['total_rows']}")
print(f"Total branches: {result.summary['total_sucursales']}")
print(f"Date range: {result.summary['min_fecha']} to {result.summary['max_fecha']}")
print(f"Missing days: {result.summary['missing_days_count']}")
print(f"Duplicates: {result.summary['duplicate_days_count']}")
print(f"Anomalies: {result.summary['zscore_anomalies_count']}")

# Print detailed findings
if result.missing_days is not None:
    print("\nMissing Days:")
    print(result.missing_days)

if result.zscore_anomalies is not None:
    print("\nZ-Score Anomalies:")
    print(result.zscore_anomalies.head())
```

**Key Features:**
- Multiple QA levels available (0-4)
- Detects missing days, duplicates, and statistical anomalies
- Provides both summary and detailed findings

## Example 5: Filtering by Branch

Demonstrates processing specific branches:

```python
from pathlib import Path
from pos_core import DataPaths
from pos_core.payments import marts as payments_marts

# Configure paths
paths = DataPaths.from_root(Path("data"), Path("utils/sucursales.json"))

# Process only specific branches
branches = ["Banana", "Queen"]

payments_df = payments_marts.fetch_daily(
    paths,
    "2025-01-01",
    "2025-01-31",
    branches=branches
)

print(f"Processed {len(branches)} branches")
print(f"Retrieved {len(payments_df)} rows")
print(payments_df["sucursal"].unique())
```

## Example 6: Force Refresh

Demonstrates forcing a refresh of all ETL stages:

```python
from pathlib import Path
from pos_core import DataPaths
from pos_core.payments import marts as payments_marts

# Configure paths
paths = DataPaths.from_root(Path("data"), Path("utils/sucursales.json"))

# Force refresh all stages (useful after fixing bugs or refreshing from source)
payments_df = payments_marts.fetch_daily(
    paths,
    "2025-01-01",
    "2025-01-31",
    mode="force"  # Force re-run all stages
)

print("Data refreshed successfully")
```

**Use Cases:**
- After fixing a bug in transformation logic
- When you want to refresh data from source
- When debugging ETL issues

## Example 7: Load vs Fetch

Demonstrates the difference between `fetch()` and `load()`:

```python
from pathlib import Path
from pos_core import DataPaths
from pos_core.payments import marts as payments_marts

paths = DataPaths.from_root(Path("data"), Path("utils/sucursales.json"))

# Fetch: ensures data exists, runs ETL if needed
df1 = payments_marts.fetch_daily(paths, "2025-01-01", "2025-01-31")

# Load: read only (faster, but requires data to exist)
# Raises FileNotFoundError if data doesn't exist
try:
    df2 = payments_marts.load_daily(paths, "2025-01-01", "2025-01-31")
    print("Loaded existing data successfully")
except FileNotFoundError:
    print("Data doesn't exist, use fetch() instead")
```

**When to use:**
- **`fetch()`**: When you're not sure if data exists, or want automatic ETL
- **`load()`**: When you're certain data exists and want faster reads

## Example 8: Complete Workflow

Complete workflow combining ETL, forecasting, and QA:

```python
from pathlib import Path
from pos_core import DataPaths
from pos_core.payments import marts as payments_marts
from pos_core.forecasting import ForecastConfig, run_payments_forecast
from pos_core.qa import run_payments_qa

# Configure paths
paths = DataPaths.from_root(Path("data"), Path("utils/sucursales.json"))

# Step 1: Get historical data
print("Step 1: Fetching payments data...")
payments_df = payments_marts.fetch_daily(
    paths,
    "2022-01-01",
    "2025-01-31"
)
print(f"Retrieved {len(payments_df)} rows")

# Step 2: Run QA checks
print("\nStep 2: Running QA checks...")
qa_result = run_payments_qa(payments_df)
print(f"Missing days: {qa_result.summary['missing_days_count']}")
print(f"Anomalies: {qa_result.summary['zscore_anomalies_count']}")

# Step 3: Generate forecast
print("\nStep 3: Generating forecast...")
config = ForecastConfig(horizon_days=91)
forecast_result = run_payments_forecast(payments_df, config)
print(f"Generated forecast for {len(forecast_result.forecast)} rows")

# Step 4: Display results
print("\nStep 4: Forecast Summary:")
print(forecast_result.forecast.groupby("sucursal")["valor"].sum())
```

## Advanced: Using Raw Data Layer

For fine-grained control, you can work with raw data directly:

```python
from pathlib import Path
from pos_core import DataPaths
from pos_core.payments import raw

paths = DataPaths.from_root(Path("data"), Path("utils/sucursales.json"))

# Download raw data only
raw.fetch(paths, "2025-01-01", "2025-01-31", mode="force")

# Load raw Excel files
raw_df = raw.load(paths, "2025-01-01", "2025-01-31")
print(f"Loaded {len(raw_df)} raw rows")
```

## Next Steps

- **[Configuration](configuration.md)** - Learn about branch configuration and environment variables
- **[Concepts](concepts.md)** - Understand data layers, grains, and API design
- **[API Reference](../api-reference/etl.md)** - Detailed function documentation
- **[Quickstart](quickstart.md)** - Get started in minutes

## More Information

For detailed information about each example script, see the [examples README](https://github.com/ToxicFyre/pos-pipeline-core-etl/blob/main/examples/README.md) in the repository.
