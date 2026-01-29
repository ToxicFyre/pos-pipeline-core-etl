# Quickstart

Get started with POS Core ETL in minutes. This guide walks you through setting up and running your first ETL pipeline.

## Prerequisites

Before starting, ensure you have:

1. ✅ Installed POS Core ETL (see [Installation](installation.md))
2. ✅ Access to a Wansoft-style POS system (or existing raw data files)
3. ✅ Python 3.10+ installed

## Step 1: Create Branch Configuration

Create `utils/sucursales.json` with your branch configuration:

```json
{
  "Banana": {
    "code": "8888",
    "valid_from": "2024-02-21",
    "valid_to": null
  },
  "Queen": {
    "code": "6362",
    "valid_from": "2024-01-01",
    "valid_to": null
  }
}
```

**Key fields:**
- `code`: The POS system code for this branch
- `valid_from`: Start date when this code became active (YYYY-MM-DD format)
- `valid_to`: End date when this code became inactive (null = still active)

See [Configuration](configuration.md) for detailed information.

## Step 2: Set Environment Variables

**Required for downloading raw data** (skip if you already have raw data files):

```bash
export WS_BASE="https://your-pos-instance.com"
export WS_USER="your_username"
export WS_PASS="your_password"
```

**Note**: If you're working with already-downloaded files in `a_raw/`, these environment variables are not needed.

## Step 3: Create Data Directory Structure

The package expects a specific directory structure:

```
data/
├── a_raw/          # Bronze: Raw Wansoft exports (Excel files)
│   ├── payments/
│   ├── sales/
│   └── transfers/
├── b_clean/        # Silver: Core facts at atomic grain (CSV files)
│   ├── payments/
│   ├── sales/
│   └── transfers/
└── c_processed/    # Gold: Marts (aggregated tables)
    ├── payments/
    ├── sales/
    └── transfers/
```

Create these directories:

```bash
mkdir -p data/{a_raw,b_clean,c_processed}/{payments,sales,transfers}
```

## Step 4: Run Your First ETL

Create a Python script `quickstart.py`:

```python
from pathlib import Path
from pos_core import DataPaths
from pos_core.payments import marts as payments_marts
from pos_core.sales import core as sales_core
from pos_core.transfers import marts as transfers_marts

# Configure paths
paths = DataPaths.from_root(Path("data"), Path("utils/sucursales.json"))

# Payments: daily mart (most common use case)
print("Fetching payments daily mart...")
payments_daily = payments_marts.fetch_daily(paths, "2025-01-01", "2025-01-31")
print(f"Retrieved {len(payments_daily)} rows")
print(payments_daily.head())

# Sales: core item-line fact
print("\nFetching sales core fact...")
sales_items = sales_core.fetch(paths, "2025-01-01", "2025-01-31")
print(f"Retrieved {len(sales_items)} rows")
print(sales_items.head())

# Transfers: pivot mart (branch × category aggregation)
print("\nFetching transfers pivot mart...")
transfers_pivot = transfers_marts.fetch_pivot(paths, "2025-01-01", "2025-01-31")
print(f"Retrieved pivot table with shape {transfers_pivot.shape}")
print(transfers_pivot)
```

Run it:

```bash
python quickstart.py
```

## Step 5: Generate a Forecast

Add forecasting to your script:

```python
from pos_core.forecasting import ForecastConfig, run_payments_forecast

# Get historical data (need more data for forecasting)
payments_df = payments_marts.fetch_daily(paths, "2022-01-01", "2025-01-31")

# Run forecast
config = ForecastConfig(horizon_days=91)  # 13 weeks
result = run_payments_forecast(payments_df, config)

print("\nForecast Results:")
print(result.forecast.head())

print("\nDeposit Schedule:")
print(result.deposit_schedule.head())
```

## Step 6: Run Quality Assurance

Add QA checks:

```python
from pos_core.qa import run_payments_qa

df = payments_marts.fetch_daily(paths, "2025-01-01", "2025-01-31")
result = run_payments_qa(df)

print("\nQA Summary:")
print(f"Total rows: {result.summary['total_rows']}")
print(f"Missing days: {result.summary['missing_days_count']}")
print(f"Anomalies: {result.summary['zscore_anomalies_count']}")

if result.missing_days is not None:
    print("\nMissing Days:")
    print(result.missing_days)
```

## Understanding the Output

### Payments Daily Mart

The `payments.marts.fetch_daily()` function returns a DataFrame with:
- `sucursal`: Branch name
- `fecha`: Date
- `ingreso_efectivo`: Cash income
- `ingreso_credito`: Credit card income
- `ingreso_debito`: Debit card income
- `num_tickets`: Number of tickets
- Additional payment method columns

### Sales Core Fact

The `sales.core.fetch()` function returns a DataFrame with:
- `sucursal`: Branch name
- `operating_date`: Date of operation
- `order_id`: Ticket/order identifier
- `item_key`: Item identifier
- `group`: Product group/category
- `subtotal_item`: Item subtotal
- `total_item`: Item total

### Transfers Pivot Mart

The `transfers.marts.fetch_pivot()` function returns a pivot table with:
- **Rows**: Branch codes (K, N, C, Q, PV, HZ, CC, TOTAL)
- **Columns**: Product categories (NO-PROC, REFRICONGE, TOSTADOR, COMIDA SALADA, REPO, PAN DULCE Y SALADA, TOTAL)

This aggregates transfer costs from CEDIS warehouse to retail branches by product category.

## Common Patterns

### Fetch vs Load

- **`fetch()`**: Ensures data exists, runs ETL if needed
- **`load()`**: Reads existing data only (raises error if missing)

```python
# Fetch: runs ETL if needed
df = payments_marts.fetch_daily(paths, "2025-01-01", "2025-01-31")

# Load: read only (faster, but requires existing data)
df = payments_marts.load_daily(paths, "2025-01-01", "2025-01-31")
```

### Force Refresh

Force re-run ETL stages:

```python
# Force re-run all stages
df = payments_marts.fetch_daily(paths, "2025-01-01", "2025-01-31", mode="force")
```

### Filter by Branch

Process specific branches:

```python
df = payments_marts.fetch_daily(
    paths, 
    "2025-01-01", 
    "2025-01-31",
    branches=["Banana", "Queen"]
)
```

## Next Steps

- **[Explore Concepts](concepts.md)** - Understand data layers, grains, and API design
- **[See Examples](examples.md)** - Complete runnable example scripts
- **[Read API Reference](../api-reference/etl.md)** - Detailed function documentation
- **[Configure Advanced Settings](configuration.md)** - Branch codes, date ranges, and more

## Troubleshooting

**Authentication Errors**: Verify environment variables are set correctly:
```bash
echo $WS_BASE
echo $WS_USER
echo $WS_PASS
```

**Missing Data**: Use `mode="force"` to force re-extraction:
```python
df = payments_marts.fetch_daily(paths, "2025-01-01", "2025-01-31", mode="force")
```

**Insufficient Data for Forecasting**: ARIMA models require at least 30 days of historical data.
