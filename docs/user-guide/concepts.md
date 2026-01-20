# Concepts

This page explains key concepts and design decisions in POS Core ETL.

## Table of Contents

- [Data Layers](#data-layers-bronzesilvergold)
- [Data Grains](#data-grains)
- [API Design](#api-design)
- [Branch Code Windows](#branch-code-windows)
- [Metadata and Idempotence](#metadata-and-idempotence)
- [POS System Requirements](#pos-system-requirements)
- [Forecasting Model](#forecasting-model)

## Data Layers (Bronze/Silver/Gold)

The package follows industry-standard data engineering conventions with explicit data layers:

| Layer | Description | Data Directory | Format |
|-------|-------------|----------------|--------|
| **Bronze** | Raw Wansoft exports, unchanged | `data/a_raw/` | Excel files |
| **Silver (Core)** | Core facts at atomic grain | `data/b_clean/` | CSV files |
| **Gold (Marts)** | Aggregated tables for analysis | `data/c_processed/` | CSV files |

### Directory Convention

- **`a_raw/`**: Bronze - Data files downloaded from POS API (Excel files)
- **`b_clean/`**: Silver - Core facts at **atomic grain** (CSV files)
- **`c_processed/`**: Gold - Marts (aggregated datasets)

This convention makes it easy to:
- Identify which layer each file belongs to
- Re-run specific stages without re-processing everything
- Debug issues at each layer

### Layer Flow

```
┌─────────────┐     ┌─────────────────────────┐     ┌─────────────────┐
│   Bronze    │ ──▶ │    Silver (Core)        │ ──▶ │   Gold (Marts)  │
│             │     │                         │     │                 │
│ a_raw/      │     │ b_clean/                │     │ c_processed/    │
│ Excel files │     │ • fact_payments_ticket  │     │ • By ticket     │
│             │     │ • fact_sales_item_line  │     │ • By day        │
│             │     │                         │     │ • By category   │
└─────────────┘     └─────────────────────────┘     └─────────────────┘
```

## Data Grains

Understanding data grain is essential for working with this package. Each domain has a specific atomic grain that defines the most granular meaningful unit of data.

### Grain Definitions

| Domain | Core Fact | Grain | Key |
|--------|-----------|-------|-----|
| **Payments** | `fact_payments_ticket` | ticket × payment method | `(sucursal, operating_date, order_index, payment_method)` |
| **Sales** | `fact_sales_item_line` | item/modifier line | `(sucursal, operating_date, order_id, item_key)` |

### Key Rule

- **Sales**: The most granular meaningful unit is **item/modifier line**. Anything aggregated beyond this (ticket-level, day-level, group-level) is a **mart**, not core.

- **Payments**: The most granular meaningful unit is **ticket × payment method**. This IS the atomic fact, so it sits in the **silver/core** layer.

### Example: Sales Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ Raw (Bronze): Excel file with sales transactions                           │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ Silver (Core) = Core Fact: fact_sales_item_line                            │
│                                                                             │
│ Grain: One row per item/modifier line on a ticket                          │
│ Key: (sucursal, operating_date, order_id, item_key)                        │
│                                                                             │
│ Example rows for ticket #1001:                                              │
│   Row 1: Café Americano (item_key=CAFE01, group=CAFE)                       │
│   Row 2: Pan Dulce (item_key=PAN01, group=PAN DULCE)                        │
│   Row 3: Extra Leche (item_key=MOD01, is_modifier=True)                     │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ Gold (Marts): Aggregations built from core fact                            │
│                                                                             │
│ • mart_sales_by_ticket: One row per ticket (aggregates item-lines)         │
│ • mart_sales_by_group: Category pivot tables (aggregates by group)         │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Example: Payments Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ Raw (Bronze): Excel file with payment transactions                         │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ Silver (Core) = Core Fact: fact_payments_ticket                            │
│                                                                             │
│ Grain: One row per ticket × payment method                                 │
│ Key: (sucursal, operating_date, order_index, payment_method)               │
│                                                                             │
│ Example rows for a split payment (ticket #1001):                            │
│   Row 1: order_index=1001, payment_method=Efectivo, ticket_total=50.00     │
│   Row 2: order_index=1001, payment_method=Tarjeta Crédito, ticket_total=100│
└────────────────────────────────┬────────────────────────────────────────────┘
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ Gold (Marts): Aggregations built from core fact                            │
│                                                                             │
│ • mart_payments_daily: One row per sucursal × day                          │
│   Columns: ingreso_efectivo, ingreso_credito, propinas, num_tickets, etc.  │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Why This Matters

1. **Data Integrity**: Understanding the grain ensures you're not accidentally double-counting or losing data.
2. **Correct Joins**: When joining tables, you need to understand the grain to choose the right join keys.
3. **Query Efficiency**: Querying at the right grain level improves performance and accuracy.
4. **Debugging**: When data doesn't match expectations, checking the grain is often the first step.

## API Design

The package uses **domain + layer modules** to encode specificity:

### Module Structure

- **Modules define domain + layer**:
  - `pos_core.payments.core` → payments, silver (core fact)
  - `pos_core.payments.marts` → payments, gold (aggregates)
  - `pos_core.payments.raw` → payments, bronze (extraction)
  - `pos_core.sales.core` → sales, silver
  - `pos_core.sales.marts` → sales, gold
  - `pos_core.sales.raw` → sales, bronze
  - `pos_core.order_times.raw` → order times, bronze (extraction)

- **Functions define behavior**:
  - `fetch(...)` / `fetch_*`: Run ETL for missing partitions (or all if `mode="force"`)
  - `load(...)` / `load_*`: Read existing outputs only; never run ETL

### Configuration

```python
from pathlib import Path
from pos_core import DataPaths

paths = DataPaths.from_root(Path("data"), Path("utils/sucursales.json"))
```

### Payments API

```python
from pos_core.payments import core, marts

# Get daily mart (most common use case)
daily_df = marts.fetch_daily(paths, "2025-01-01", "2025-01-31")

# Get core fact (ticket × payment method grain)
fact_df = core.fetch(paths, "2025-01-01", "2025-01-31")

# Load existing data only (faster, but requires data to exist)
daily_df = marts.load_daily(paths, "2025-01-01", "2025-01-31")
```

### Sales API

```python
from pos_core.sales import core, marts

# Get core fact (item-line grain)
fact_df = core.fetch(paths, "2025-01-01", "2025-01-31")

# Get ticket mart
ticket_df = marts.fetch_ticket(paths, "2025-01-01", "2025-01-31")

# Get group mart
group_df = marts.fetch_group(paths, "2025-01-01", "2025-01-31")
```

### Order Times API

```python
from pos_core.order_times import raw

# Download raw order times data
raw.fetch(paths, "2025-01-01", "2025-01-31")

# Verify data exists
raw.load(paths, "2025-01-01", "2025-01-31")
```

### Processing Modes

- **`mode="missing"`** (default): Only runs ETL for date ranges that don't have completed outputs
- **`mode="force"`**: Forces re-run of all ETL stages for the given date range

```python
# Default: skip if data exists
df = marts.fetch_daily(paths, "2025-01-01", "2025-01-31")

# Force refresh
df = marts.fetch_daily(paths, "2025-01-01", "2025-01-31", mode="force")
```

## Branch Code Windows

Branches (sucursales) may change their codes over time. The package handles this through "validity windows" in `sucursales.json`.

Each branch entry can specify:
- `code`: The POS system code for this branch
- `valid_from`: When this code became active (YYYY-MM-DD format)
- `valid_to`: When this code became inactive (null if still active)

**Example**: If a branch changed codes on 2024-06-01:

```json
{
  "MyBranch": {
    "code": "5678",
    "valid_from": "2024-06-01",
    "valid_to": null
  },
  "MyBranch_OLD": {
    "code": "1234",
    "valid_from": "2020-01-01",
    "valid_to": "2024-05-31"
  }
}
```

Branch codes are resolved using `pos_core.branches.BranchRegistry`:

```python
from pos_core.branches import BranchRegistry

registry = BranchRegistry(paths)

# Get code for a specific date
code = registry.get_code_for_date("MyBranch", "2023-01-15")  # Returns "1234"
code = registry.get_code_for_date("MyBranch", "2024-07-01")  # Returns "5678"
```

## Metadata and Idempotence

The ETL pipeline uses metadata files to track stage completion and enable idempotent operations.

### Metadata Storage

Metadata files are stored in `_meta/` subdirectories within each stage directory:
- `a_raw/payments/batch/_meta/2025-01-01_2025-01-31.json`
- `b_clean/payments/batch/_meta/2025-01-01_2025-01-31.json`
- `c_processed/payments/_meta/2025-01-01_2025-01-31.json`

### Automatic Idempotence

`fetch()` functions automatically check metadata:
- If metadata exists and `status == "ok"`, skip the stage
- If `mode="force"`, force re-run all stages
- If `mode="missing"` (default), use existing data when available

This makes it safe to re-run queries without duplicating work.

### Fetch vs Load

- **`fetch()`**: May run ETL if data doesn't exist or if `mode="force"`
- **`load()`**: Never runs ETL; only reads existing data (raises error if missing)

```python
# Fetch: runs ETL if needed
df = marts.fetch_daily(paths, "2025-01-01", "2025-01-31")

# Load: read only (faster, but requires existing data)
df = marts.load_daily(paths, "2025-01-01", "2025-01-31")
```

## POS System Requirements

This package is designed for POS systems that:

1. **Expose HTTP exports** for payments, sales detail, and transfer reports
2. **Use Excel format** for exported reports
3. **Support authentication** via username/password (required for downloading raw data; optional if you already have raw data files)

The package is optimized for Wansoft-style POS systems.

## Forecasting Model

The forecasting module uses **ARIMA (AutoRegressive Integrated Moving Average)** models:

- **Log transformation**: Applied to handle non-negative values
- **Automatic hyperparameter selection**: Searches for optimal parameters
- **Per-branch, per-metric**: Separate models for each combination
- **Fallback models**: Uses naive models when ARIMA fails

Models require at least 30 days of historical data for reliable forecasts.

### Usage

```python
from pos_core.payments import marts as payments_marts
from pos_core.forecasting import ForecastConfig, run_payments_forecast

# Get historical data
payments_df = payments_marts.fetch_daily(paths, "2022-01-01", "2025-01-31")

# Run forecast
config = ForecastConfig(horizon_days=91)  # 13 weeks
result = run_payments_forecast(payments_df, config)

# Access results
print(result.forecast.head())  # Per-branch/metric forecasts
print(result.deposit_schedule.head())  # Cash-flow deposits
```

## Migration from v0.2.x

The API was refactored in v0.3.0 to use module namespaces. Key changes:

| Old API (v0.2.x) | New API (v0.3.x) |
|------------------|------------------|
| `from pos_core.payments import get_payments` | `from pos_core.payments import core, marts` |
| `get_payments(..., grain="ticket")` | `payments.core.fetch(...)` |
| `get_payments(..., grain="daily")` | `payments.marts.fetch_daily(...)` |
| `get_payments(..., refresh=True)` | `payments.core.fetch(..., mode="force")` |
| `from pos_core.sales import get_sales` | `from pos_core.sales import core, marts` |
| `get_sales(..., grain="item")` | `sales.core.fetch(...)` |
| `get_sales(..., grain="ticket")` | `sales.marts.fetch_ticket(...)` |
| `get_sales(..., grain="group")` | `sales.marts.fetch_group(...)` |

The new API:
- Uses **module namespaces** (`payments.core`, `payments.marts`, `sales.core`, `sales.marts`) to encode domain + layer
- Uses **short verb-based names** (`fetch`, `load`) for behavior
- Makes **bronze/silver/gold layers explicit** via module paths
- Supports **`mode="missing"`** (default) vs **`mode="force"`** for partition-aware ETL
- Provides **`load()`** functions that never run ETL (read-only)

## Next Steps

- **[Try Examples](examples.md)** - Complete runnable example scripts
- **[API Reference](../api-reference/etl.md)** - Detailed function documentation
- **[Quickstart](quickstart.md)** - Get started in minutes
