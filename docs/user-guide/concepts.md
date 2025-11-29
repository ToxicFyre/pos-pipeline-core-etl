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

## ETL Directory Convention

The package uses a three-stage directory structure:

- **`a_raw/`**: Raw data files downloaded from POS API (Excel files)
- **`b_clean/`**: Cleaned and normalized data (CSV files)
- **`c_processed/`**: Aggregated and processed datasets (CSV files)

This convention makes it easy to:
- Identify which stage each file belongs to
- Re-run specific stages without re-processing everything
- Debug issues at each stage

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

**Low-level functions** in `pos_core.etl.a_extract`, `pos_core.etl.b_transform`, and `pos_core.etl.c_load` provide direct access to extraction, transformation, and aggregation logic.

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
