# Concepts

This page explains key concepts and design decisions in POS Core ETL.

## Branch Code Windows

Branches (sucursales) may change their codes over time. The package handles this through "validity windows" in `sucursales.json`.

Each branch entry can specify:
- `valid_from`: When this code became active
- `valid_to`: When this code became inactive (null if still active)

**Example**: If a branch changed codes on 2024-06-01:

```json
{
  "MyBranch": {
    "code": "5678",
    "valid_from": "2024-06-01",
    "valid_to": null
  }
}
```

## Data Layers (Bronze/Silver/Gold)

The package follows industry-standard data engineering conventions with explicit data layers:

| Layer | Description | Data Directory |
|-------|-------------|----------------|
| **Bronze** | Raw Wansoft exports, unchanged (Excel files) | `data/a_raw/` |
| **Silver (Core)** | Core facts at atomic grain (CSV files) | `data/b_clean/` |
| **Gold (Marts)** | Aggregated tables for analysis (CSV files) | `data/c_processed/` |

### Directory Convention

- **`a_raw/`**: Bronze - Data files downloaded from POS API (Excel files)
- **`b_clean/`**: Silver - Core facts at **atomic grain** (CSV files)
- **`c_processed/`**: Gold - Marts (aggregated datasets)

This convention makes it easy to:
- Identify which layer each file belongs to
- Re-run specific stages without re-processing everything
- Debug issues at each layer

## Grain and Layers

Understanding data grain is essential for working with this package. Each domain has a specific atomic grain that defines the most granular meaningful unit of data.

### Grain Definitions (Ground Truth)

| Domain | Core Fact | Grain | Key |
|--------|-----------|-------|-----|
| **Payments** | `fact_payments_ticket` | ticket × payment method | `(sucursal, operating_date, order_index, payment_method)` |
| **Sales** | `fact_sales_item_line` | item/modifier line | `(sucursal, operating_date, order_id, item_key)` |

### Key Rule

- **Sales**: The most granular meaningful unit is **item/modifier line**. Anything aggregated beyond this (ticket-level, day-level, group-level) is a **mart**, not core.

- **Payments**: The most granular meaningful unit is **ticket × payment method**. This IS the atomic fact, so it sits in the **staging/core** layer.

### Example: Sales Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ Raw (Bronze): Excel file with sales transactions                           │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ Staging (Silver) = Core Fact: fact_sales_item_line                         │
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
│ Marts (Gold): Aggregations built from core fact                            │
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
│ Staging (Silver) = Core Fact: fact_payments_ticket                         │
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
│ Marts (Gold): Aggregations built from core fact                            │
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

## Public API

The package provides a simple, domain-oriented API:

### Configuration

```python
from pos_core import DataPaths

paths = DataPaths.from_root("data", "utils/sucursales.json")
```

### Payments

```python
from pos_core.payments import get_payments

# Get daily mart (default)
df = get_payments(paths, "2025-01-01", "2025-01-31")

# Get core fact (ticket grain)
df = get_payments(paths, "2025-01-01", "2025-01-31", grain="ticket")
```

### Sales

```python
from pos_core.sales import get_sales

# Get core fact (item-line grain, default)
df = get_sales(paths, "2025-01-01", "2025-01-31")

# Get ticket mart
df = get_sales(paths, "2025-01-01", "2025-01-31", grain="ticket")

# Get group mart
df = get_sales(paths, "2025-01-01", "2025-01-31", grain="group")
```

### Forecasting

```python
from pos_core.payments import get_payments
from pos_core.forecasting import run_payments_forecast, ForecastConfig

# Get historical data
payments_df = get_payments(paths, "2022-01-01", "2025-01-31")

# Run forecast
config = ForecastConfig(horizon_days=91)
result = run_payments_forecast(payments_df, config)
```

### QA

```python
from pos_core.payments import get_payments
from pos_core.qa import run_payments_qa

df = get_payments(paths, "2025-01-01", "2025-01-31")
result = run_payments_qa(df)
```

## Metadata and Idempotence

The ETL pipeline uses metadata files to track stage completion and enable idempotent operations.

### Metadata Storage

Metadata files are stored in `_meta/` subdirectories within each stage directory:
- `a_raw/payments/batch/_meta/2025-01-01_2025-01-31.json`
- `b_clean/payments/batch/_meta/2025-01-01_2025-01-31.json`
- `c_processed/payments/_meta/2025-01-01_2025-01-31.json`

### Automatic Idempotence

Query functions automatically check metadata:
- If metadata exists and `status == "ok"`, skip the stage
- If `refresh=True`, force re-run all stages
- If `refresh=False`, use existing data when available

This makes it safe to re-run queries without duplicating work.

## POS System Requirements

This package is designed for POS systems that:

1. **Expose HTTP exports** for payments, sales detail, and transfer reports
2. **Use Excel format** for exported reports
3. **Support authentication** via username/password (optional)

The package is optimized for Wansoft-style POS systems.

## Forecasting Model

The forecasting module uses **ARIMA (AutoRegressive Integrated Moving Average)** models:

- **Log transformation**: Applied to handle non-negative values
- **Automatic hyperparameter selection**: Searches for optimal parameters
- **Per-branch, per-metric**: Separate models for each combination

Models require at least 30 days of historical data for reliable forecasts.

## Next Steps

- **Try Examples**: See [Examples](examples.md) for complete runnable scripts
- **API Reference**: Check [API Reference](../api-reference/etl.md) for detailed documentation
