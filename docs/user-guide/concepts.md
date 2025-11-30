# Concepts

This page explains key concepts and design decisions in POS Core ETL.

## Branch Code Windows

Branches (sucursales) may change their codes over time. The package handles this through "validity windows" in `sucursales.json`.

Each branch entry can specify:
- `valid_from`: When this code became active
- `valid_to`: When this code became inactive (null if still active)

During extraction, the package automatically selects the correct code for each date range based on these windows.

**Example**: If a branch changed codes on 2024-06-01, you'd have two entries:

```json
{
  "MyBranch": {
    "code": "5678",
    "valid_from": "2024-06-01",
    "valid_to": null
  }
}
```

**Note**: For branches with code changes over time, the package automatically selects the correct code based on validity windows. The current format supports a single code per branch entry. If you need to track multiple code changes, ensure your `sucursales.json` has separate entries or use the validity windows appropriately.

## Data Layers (Bronze/Silver/Gold)

The package follows industry-standard data engineering conventions with explicit data layers:

| Layer | Code Location | Data Directory | Description |
|-------|--------------|----------------|-------------|
| **Raw (Bronze)** | `pos_core.etl.raw/` | `data/a_raw/` | Direct Wansoft exports, unchanged. Excel files as received from the POS API. |
| **Staging (Silver)** | `pos_core.etl.staging/` | `data/b_clean/` | Cleaned and standardized tables containing **core facts** at their atomic grain. |
| **Core (Silver+)** | `pos_core.etl.core/` | `data/b_clean/` | Documents the grain definitions. The staging output IS the core fact. |
| **Marts (Gold)** | `pos_core.etl.marts/` | `data/c_processed/` | Aggregated semantic tables. All aggregations beyond core grain. |

### Directory Convention

The package uses a layered directory structure:

- **`a_raw/`**: Raw (Bronze) - Data files downloaded from POS API (Excel files)
- **`b_clean/`**: Staging (Silver) - Cleaned and normalized data (CSV files) at **atomic grain**
- **`c_processed/`**: Marts (Gold) - Aggregated datasets (CSV files)

This convention makes it easy to:
- Identify which layer each file belongs to
- Re-run specific stages without re-processing everything
- Debug issues at each layer
- Apply industry-standard data engineering practices

## Grain and Layers

Understanding data grain is essential for working with this package. Each domain has a specific atomic grain that defines the most granular meaningful unit of data.

### Grain Definitions (Ground Truth)

| Domain | Core Fact | Grain | Key | Description |
|--------|-----------|-------|-----|-------------|
| **Payments** | `fact_payments_ticket` | ticket × payment method | `(sucursal, operating_date, order_index, payment_method)` | The POS payments export does not expose item-level payment data. Ticket-level is the atomic fact. |
| **Sales** | `fact_sales_item_line` | item/modifier line | `(sucursal, operating_date, order_id, item_key, [modifier])` | Each row represents an item or modifier on a ticket. Multiple rows can share the same `ticket_id`. |

### Key Rule

- **Sales**: The most granular meaningful unit is **item/modifier line**. Each row in the cleaned sales details represents an item or modifier on a ticket. Anything aggregated beyond this (e.g., ticket-level, day-level, group-level) is a **mart**, not core.

- **Payments**: The most granular meaningful unit is **ticket × payment method**. The POS payments export does not expose deeper item-level fact. This IS the atomic fact, so it sits in the **staging/core** layer.

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
│ Key: (sucursal, operating_date, order_id, item_key, [modifier])            │
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
│ • mart_sales_daily: One row per sucursal × day (if implemented)            │
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

1. **Data Integrity**: Understanding the grain ensures you're not accidentally double-counting or losing data during aggregations.

2. **Correct Joins**: When joining tables, you need to understand the grain to choose the right join keys.

3. **Query Efficiency**: Querying at the right grain level improves performance and accuracy.

4. **Debugging**: When data doesn't match expectations, checking the grain is often the first step in troubleshooting.

## API Layers

The package provides three levels of APIs:

### Query Functions (Recommended)

The **query functions** are the recommended way to get data:

- **`get_payments()`**: Get payments data, running ETL stages only if needed
- **`get_sales()`**: Get sales data at specified level, running ETL stages only if needed
- **`get_payments_forecast()`**: Get payments forecast, automatically handling historical data

**Key features**:
- Automatic idempotence through metadata checks
- Only runs ETL stages when needed
- Simple, high-level interface
- Returns DataFrames directly

**Example**:
```python
from pathlib import Path
from pos_core.etl import PaymentsETLConfig, get_payments

config = PaymentsETLConfig.from_root(Path("data"), Path("utils/sucursales.json"))
df = get_payments("2025-01-01", "2025-01-31", config, refresh=False)
```

### Stage Functions (Fine-Grained Control)

**Stage functions** provide control over individual ETL stages:

- **Payments**: `download_payments()`, `clean_payments()`, `aggregate_payments()`
- **Sales**: `download_sales()`, `clean_sales()`, `aggregate_sales()`

**Use when**:
- You need to run specific stages independently
- You want to inspect intermediate results
- You're building custom workflows

**Example**:
```python
from pathlib import Path
from pos_core.etl import PaymentsETLConfig, download_payments, clean_payments

config = PaymentsETLConfig.from_root(Path("data"), Path("utils/sucursales.json"))
download_payments("2025-01-01", "2025-01-31", config)
clean_payments("2025-01-01", "2025-01-31", config)
```

### Low-Level Functions (Advanced)

**Low-level functions** in the layer subpackages provide direct access to extraction, transformation, and aggregation logic:

- `pos_core.etl.raw/` - HTTP extraction from Wansoft API
- `pos_core.etl.staging/` - Excel cleaning and normalization
- `pos_core.etl.core/` - Per-ticket granular aggregation
- `pos_core.etl.marts/` - Daily/category-level aggregated tables

**Use when**:
- You need to customize the ETL logic
- You're building custom pipelines
- You need access to internal implementation details

**Note**: These are considered internal APIs and may change between minor versions.

## Metadata and Idempotence

The ETL pipeline uses metadata files to track stage completion and enable idempotent operations.

### Metadata Storage

Metadata files are stored in `_meta/` subdirectories within each stage directory:
- `a_raw/payments/_meta/2025-01-01_2025-01-31.json`
- `b_clean/payments/_meta/2025-01-01_2025-01-31.json`
- `c_processed/payments/_meta/2025-01-01_2025-01-31.json`

### Metadata Contents

Each metadata file contains:
- `start_date`, `end_date`: Date range processed
- `branches`: List of branches processed
- `cleaner_version`: Version identifier for the cleaner (enables re-cleaning when logic changes)
- `last_run`: ISO timestamp of when the stage was run
- `status`: "ok", "failed", or "partial"

### Automatic Idempotence

Query functions automatically check metadata:
- If metadata exists and `status == "ok"` and `cleaner_version` matches, skip the stage
- If `refresh=True`, force re-run all stages
- If `refresh=False`, use existing data when available

This makes it safe to re-run queries without duplicating work.

## POS System Requirements

This package is designed for POS systems that:

1. **Expose HTTP exports** for:
   - Payment reports
   - Sales detail reports
   - Transfer reports

2. **Use Excel format** for exported reports

3. **Support authentication** via username/password (optional)

The package is currently optimized for Wansoft-style POS systems, but the architecture allows for future extension to other POS backends.

## Incremental Processing

The ETL pipeline is designed for incremental processing:

- **Smart date range chunking**: Automatically splits large date ranges into manageable chunks
- **Existing data discovery**: Skips downloading files that already exist
- **Metadata-based idempotence**: Tracks stage completion to avoid redundant work
- **Resumable**: Can be interrupted and resumed without losing progress

This makes it practical to process years of historical data.

## Forecasting Model

The forecasting module uses **ARIMA (AutoRegressive Integrated Moving Average)** models:

- **Log transformation**: Applied to handle non-negative values
- **Automatic hyperparameter selection**: Searches for optimal ARIMA parameters
- **Per-branch, per-metric**: Separate models for each combination

The models require at least 30 days of historical data to generate reliable forecasts.

## Next Steps

- **Explore API**: Check the [API Reference](../api-reference/etl.md) for detailed API documentation
- **Try Examples**: See [Examples](examples.md) for complete runnable scripts
