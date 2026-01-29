# New API Design Plan

This document describes the clean, modern API structure for pos-core-etl.

## Design Principles

1. **Domain-oriented modules**: `pos_core.payments`, `pos_core.sales`, `pos_core.forecasting`, `pos_core.qa`
2. **Clear grain semantics**: Core facts at atomic grain, marts for aggregations
3. **Simple, opinionated API**: One obvious way to do common tasks
4. **No backward compatibility cruft**: Clean break from legacy naming

## Module Structure

```
src/pos_core/
├── __init__.py              # Top-level exports (version, key functions)
├── config.py                # Unified configuration
├── exceptions.py            # Custom exceptions
│
├── payments/                # Payments domain
│   ├── __init__.py          # Public: get_payments()
│   ├── extract.py           # Bronze: download from Wansoft
│   ├── transform.py         # Silver: clean Excel → CSV (fact_payments_ticket)
│   └── aggregate.py         # Gold: daily aggregation (mart_payments_daily)
│
├── sales/                   # Sales domain
│   ├── __init__.py          # Public: get_sales()
│   ├── extract.py           # Bronze: download from Wansoft
│   ├── transform.py         # Silver: clean Excel → CSV (fact_sales_item_line)
│   └── aggregate.py         # Gold: ticket/group aggregations
│
├── transfers/               # Transfers domain
│   ├── __init__.py          # Public: raw, core, marts
│   ├── extract.py           # Bronze: download from Wansoft (Inventory > Transfers > Issued)
│   ├── transform.py         # Silver: clean Excel → CSV (fact_transfers_line)
│   └── aggregate.py         # Gold: pivot aggregation (mart_transfers_pivot)
│
├── forecasting/             # Forecasting domain (mostly unchanged)
│   ├── __init__.py          # Public: run_payments_forecast, ForecastConfig, ForecastResult
│   └── ...
│
└── qa/                      # QA domain (mostly unchanged)
    ├── __init__.py          # Public: run_payments_qa, QAResult
    └── ...
```

## Data Layers and Grains

### Payments
- **Bronze**: `data/a_raw/payments/` - Raw Excel from Wansoft
- **Silver (Core Fact)**: `data/b_clean/payments/` - `fact_payments_ticket`
  - Grain: ticket × payment method
  - Key: `(sucursal, operating_date, order_index, payment_method)`
- **Gold (Mart)**: `data/c_processed/payments/` - `mart_payments_daily`
  - Grain: sucursal × date
  - Aggregates: income by payment type, tips, ticket counts

### Sales
- **Bronze**: `data/a_raw/sales/` - Raw Excel from Wansoft
- **Silver (Core Fact)**: `data/b_clean/sales/` - `fact_sales_item_line`
  - Grain: item/modifier line
  - Key: `(sucursal, operating_date, order_id, item_key, [modifier_cols])`
- **Gold (Marts)**: `data/c_processed/sales/`
  - `mart_sales_by_ticket`: One row per ticket
  - `mart_sales_by_group`: Category pivot table

### Transfers
- **Bronze**: `data/a_raw/transfers/` - Raw Excel from Wansoft (Inventory > Transfers > Issued)
- **Silver (Core Fact)**: `data/b_clean/transfers/` - `fact_transfers_line`
  - Grain: transfer line item
  - Key: `(orden, almacen_origen, sucursal_destino, producto)`
- **Gold (Marts)**: `data/c_processed/transfers/`
  - `mart_transfers_pivot`: Branch × category pivot table

## Public API

### Configuration
```python
from pos_core import DataPaths

paths = DataPaths.from_root(
    data_root="data",
    sucursales_json="utils/sucursales.json"
)
```

### Payments
```python
from pos_core.payments import get_payments

# Get daily mart (default, most common use case)
df = get_payments(paths, "2025-01-01", "2025-01-31")

# Get core fact (ticket grain)
df = get_payments(paths, "2025-01-01", "2025-01-31", grain="ticket")
```

### Sales
```python
from pos_core.sales import get_sales

# Get core fact (item-line grain, default)
df = get_sales(paths, "2025-01-01", "2025-01-31")

# Get ticket-level mart
df = get_sales(paths, "2025-01-01", "2025-01-31", grain="ticket")

# Get group pivot mart
df = get_sales(paths, "2025-01-01", "2025-01-31", grain="group")
```

### Transfers
```python
from pos_core.transfers import core, marts

# Get core fact (transfer line grain)
df = core.fetch(paths, "2025-01-01", "2025-01-31")

# Get pivot mart (branch × category)
df = marts.fetch_pivot(paths, "2025-01-01", "2025-01-31")
```

### Forecasting
```python
from pos_core.payments import get_payments
from pos_core.forecasting import run_payments_forecast, ForecastConfig

# Get historical data
payments_df = get_payments(paths, "2022-01-01", "2025-01-31")

# Run forecast
result = run_payments_forecast(payments_df, ForecastConfig(horizon_days=91))
print(result.forecast)
```

### QA
```python
from pos_core.payments import get_payments
from pos_core.qa import run_payments_qa

df = get_payments(paths, "2025-01-01", "2025-01-31")
result = run_payments_qa(df)
```

## Removed/Changed from Old API

| Old | New | Notes |
|-----|-----|-------|
| `PaymentsETLConfig` | `DataPaths` | Simplified, unified |
| `SalesETLConfig` | `DataPaths` | Same config for both |
| `get_sales(level="ticket")` | `get_sales(grain="ticket")` | Clearer parameter name |
| `get_payments_forecast()` | Use `get_payments()` + `run_payments_forecast()` | Explicit composition |
| `build_payments_dataset()` | `get_payments()` | Simplified |
| `download_payments`, `clean_payments`, `aggregate_payments` | Internal | Not part of public API |

## Migration Notes

The API has been simplified. Old entry points were intentionally removed to reduce
technical debt. The new API:
- Uses a unified `DataPaths` config for both payments and sales
- Uses `grain=` parameter instead of `level=` for clarity
- Exposes core facts and marts through the same `get_*` functions
- Keeps ETL implementation details internal
