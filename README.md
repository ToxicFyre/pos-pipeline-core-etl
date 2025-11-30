# POS Core ETL

A comprehensive Python package for Point of Sale (POS) data processing, forecasting, and quality assurance. This package provides tools to extract payment and sales data from POS systems, clean and transform it, aggregate it for analysis, generate forecasts using time series models, and perform automated quality checks.

> **Note**: This README assumes you're viewing the repo (e.g., on GitHub). References to `tests/` and `src/...` paths are relative to the repository root.
>
> **Documentation**: Full documentation is available at [https://toxicfyre.github.io/pos-pipeline-core-etl/](https://toxicfyre.github.io/pos-pipeline-core-etl/)

## POS System Compatibility

This package is designed for POS systems that expose **Wansoft-style HTTP exports** for payment reports, sales detail reports, and transfer reports. The package expects:

- HTTP endpoints for exporting reports (payments, detail, transfers)
- Excel format for exported reports
- Username/password authentication (required for downloading raw data; optional if you already have raw data files)

## Features

- **ETL Pipeline**: Extract, transform, and load POS payment and sales data
- **Time Series Forecasting**: Generate ARIMA-based forecasts for payment metrics
- **Quality Assurance**: Automated data validation and anomaly detection
- **Multi-Branch Support**: Handle multiple sucursales (branches) with code window tracking
- **Incremental Processing**: Smart date range chunking and existing data discovery

## Installation

### Production Install

```bash
pip install pos-core-etl
```

### Development Install

```bash
git clone https://github.com/ToxicFyre/pos-pipeline-core-etl.git
cd pos-pipeline-core-etl
pip install -e .[dev]
```

### Dependencies

The package requires Python 3.10+ and the following dependencies:
- pandas >= 1.3.0
- numpy >= 1.20.0
- requests >= 2.25.0
- beautifulsoup4 >= 4.9.0
- statsmodels >= 0.12.0
- openpyxl >= 3.0.0

## Quickstart

**Before running extraction**, set up your credentials:

```bash
export WS_BASE="https://your-pos-instance.com"
export WS_USER="your_username"
export WS_PASS="your_password"
```

Then run your ETL pipeline:

```python
from pathlib import Path
from pos_core import DataPaths
from pos_core import payments, sales
from pos_core.forecasting import ForecastConfig, run_payments_forecast

# Configure paths
paths = DataPaths.from_root(Path("data"), Path("utils/sucursales.json"))

# Payments: daily mart
payments_daily = payments.marts.fetch_daily(paths, "2025-01-01", "2025-01-31")

# Sales: core item-line fact
sales_items = sales.core.fetch(paths, "2025-01-01", "2025-01-31")

# Forecasting on payments daily mart
config = ForecastConfig(horizon_days=91)  # 13 weeks
result = run_payments_forecast(payments_daily, config)
print(result.forecast.head())
```

For more examples, see:
- **Runnable scripts**: Check the [`examples/`](examples/) directory
- **Documentation**: Full documentation at [https://toxicfyre.github.io/pos-pipeline-core-etl/](https://toxicfyre.github.io/pos-pipeline-core-etl/)

## Prerequisites

### Configuration Files

**sucursales.json** (required): Branch configuration file that maps branch names to codes with validity windows.

Default location: `utils/sucursales.json`

The file supports branch code changes over time using `valid_from` and `valid_to` fields:
- `code`: The POS system code for this branch (e.g., "8888")
- `valid_from`: Start date when this code became active (YYYY-MM-DD format)
- `valid_to`: End date when this code became inactive (null = still active)

Example:
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
  },
  "Kavia_OLD": {
    "code": "6161",
    "valid_from": "2022-11-01",
    "valid_to": "2024-02-20"
  }
}
```

Branch codes are resolved using `pos_core.branches.BranchRegistry`:
```python
from pathlib import Path
from pos_core import DataPaths
from pos_core.branches import BranchRegistry

paths = DataPaths.from_root(Path("data"), Path("utils/sucursales.json"))
registry = BranchRegistry(paths)

# Get code for a specific date
code = registry.get_code_for_date("Kavia", "2023-01-15")  # Returns "6161"
code = registry.get_code_for_date("Kavia", "2024-03-01")  # Returns "8777"
```

### Environment Variables

**Required for extraction** (downloading data from the POS API):

- `WS_BASE` (required): Base URL of your POS instance
- `WS_USER` (required): Username for authentication
- `WS_PASS` (required): Password for authentication

These credentials are required to authenticate with the POS system and download data. Set them before running any extraction operations.

**Example**:
```bash
export WS_BASE="https://your-pos-instance.com"
export WS_USER="your_username"
export WS_PASS="your_password"
```

If you only work with already-downloaded files in `a_raw`, these are not needed.

### Directory Structure

```
data/
├── a_raw/          # Bronze: Raw Wansoft exports (Excel files)
│   ├── payments/
│   └── sales/
├── b_clean/        # Silver: Core facts at atomic grain (CSV files)
│   ├── payments/   # fact_payments_ticket
│   └── sales/      # fact_sales_item_line
└── c_processed/    # Gold: Marts (aggregated tables)
    ├── payments/   # mart_payments_daily
    └── sales/      # mart_sales_by_ticket, mart_sales_by_group
```

## Data Layers and Grains

The ETL pipeline follows **bronze/silver/gold** data layer conventions:

| Layer | Description | Data Directory |
|-------|-------------|----------------|
| **Bronze** | Raw Wansoft exports, unchanged | `data/a_raw/` |
| **Silver (Core)** | Core facts at atomic grain | `data/b_clean/` |
| **Gold (Marts)** | Aggregated tables for analysis | `data/c_processed/` |

### Grain Definitions

| Domain | Core Fact | Grain | Key |
|--------|-----------|-------|-----|
| **Payments** | `fact_payments_ticket` | ticket × payment method | `(sucursal, operating_date, order_index, payment_method)` |
| **Sales** | `fact_sales_item_line` | item/modifier line | `(sucursal, operating_date, order_id, item_key)` |

**Key Rule:**
- For **sales**: anything aggregated beyond item/modifier line is **gold/mart**
- For **payments**: ticket × payment method is the atomic fact (silver/core)

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

## Module & Naming Philosophy

The package uses **domain + layer modules** to encode specificity:

- **Modules define domain + layer**:
  - `pos_core.payments.core` → payments, silver (core fact)
  - `pos_core.payments.marts` → payments, gold (aggregates)
  - `pos_core.sales.core` → sales, silver
  - `pos_core.sales.marts` → sales, gold
  - `pos_core.payments.raw` / `pos_core.sales.raw` → bronze (extraction)

- **Functions define behavior**:
  - `fetch(...)` / `fetch_*`: May run ETL for missing (or all, if `mode="force"`) partitions and return a DataFrame
  - `load(...)` / `load_*`: Read existing outputs only; never run ETL

## API Reference

### Configuration

```python
from pos_core import DataPaths

paths = DataPaths.from_root(Path("data"), Path("utils/sucursales.json"))
```

### Payments

#### Core Fact (Silver Layer)

```python
from pos_core.payments import core

# Fetch: ensures data exists, runs ETL if needed
df = core.fetch(paths, "2025-01-01", "2025-01-31")

# Load: read existing data only (raises error if missing)
df = core.load(paths, "2025-01-01", "2025-01-31")
```

**Parameters:**
- `paths`: DataPaths configuration
- `start_date`, `end_date`: Date range (YYYY-MM-DD format)
- `branches`: Optional list of branch names to filter
- `mode`: `"missing"` (default) or `"force"` (for `fetch` only)

Returns: DataFrame with `fact_payments_ticket` structure (ticket × payment method grain)

#### Daily Mart (Gold Layer)

```python
from pos_core.payments import marts

# Fetch: ensures core fact + mart exist, runs ETL if needed
df = marts.fetch_daily(paths, "2025-01-01", "2025-01-31")

# Load: read existing mart only (raises error if missing)
df = marts.load_daily(paths, "2025-01-01", "2025-01-31")
```

**Parameters:** Same as core, plus:
- `mode`: `"missing"` (default) or `"force"` (for `fetch_daily` only)

Returns: DataFrame with `mart_payments_daily` structure (sucursal × date grain)

### Sales

#### Core Fact (Silver Layer)

```python
from pos_core.sales import core

# Fetch: ensures data exists, runs ETL if needed
df = core.fetch(paths, "2025-01-01", "2025-01-31")

# Load: read existing data only (raises error if missing)
df = core.load(paths, "2025-01-01", "2025-01-31")
```

**Parameters:** Same as payments core

Returns: DataFrame with `fact_sales_item_line` structure (item/modifier line grain)

#### Ticket Mart (Gold Layer)

```python
from pos_core.sales import marts

# Fetch: ensures core fact + ticket mart exist
df = marts.fetch_ticket(paths, "2025-01-01", "2025-01-31")

# Load: read existing mart only
df = marts.load_ticket(paths, "2025-01-01", "2025-01-31")
```

Returns: DataFrame with `mart_sales_by_ticket` structure (one row per ticket)

#### Group Mart (Gold Layer)

```python
from pos_core.sales import marts

# Fetch: ensures ticket mart + group mart exist
df = marts.fetch_group(paths, "2025-01-01", "2025-01-31")

# Load: read existing mart only
df = marts.load_group(paths, "2025-01-01", "2025-01-31")
```

Returns: DataFrame with `mart_sales_by_group` structure (category pivot table)

### Processing Lifecycle

**`payments.core.fetch(...)`**:
1. Extract raw payments from Wansoft into `a_raw/` (only missing/force)
2. Transform into `b_clean/fact_payments_ticket`
3. Return the core fact DataFrame

**`payments.marts.fetch_daily(...)`**:
1. Ensure core fact exists (as above)
2. Build/update `c_processed/mart_payments_daily`
3. Return the daily mart DataFrame

**`sales.core.fetch(...)`**:
1. Extract raw sales from Wansoft into `a_raw/` (only missing/force)
2. Transform into `b_clean/fact_sales_item_line`
3. Return the core fact DataFrame

**`sales.marts.fetch_ticket(...)`**:
1. Ensure core fact exists (as above)
2. Build/update `c_processed/mart_sales_by_ticket`
3. Return the ticket mart DataFrame

### Capabilities Table

| Domain | Layer | Module | Function | Description |
|--------|-------|--------|----------|-------------|
| **Payments** | Silver (Core) | `payments.core` | `fetch()` / `load()` | Core fact: ticket × payment method |
| **Payments** | Gold (Mart) | `payments.marts` | `fetch_daily()` / `load_daily()` | Daily aggregations for forecasting |
| **Sales** | Silver (Core) | `sales.core` | `fetch()` / `load()` | Core fact: item/modifier line |
| **Sales** | Gold (Mart) | `sales.marts` | `fetch_ticket()` / `load_ticket()` | Ticket-level aggregations |
| **Sales** | Gold (Mart) | `sales.marts` | `fetch_group()` / `load_group()` | Group-level pivot table |

### Forecasting

```python
from pos_core.payments import marts as payments_marts
from pos_core.forecasting import run_payments_forecast, ForecastConfig

# Get historical data (daily mart)
payments_df = payments_marts.fetch_daily(paths, "2022-01-01", "2025-01-31")

# Run forecast
config = ForecastConfig(horizon_days=91)
result = run_payments_forecast(payments_df, config)

# Access results
print(result.forecast.head())  # Per-branch/metric forecasts
print(result.deposit_schedule.head())  # Cash-flow deposits
```

**With debug information:**
```python
result = run_payments_forecast(payments_df, config, debug=True)
if result.debug:
    debug_info = result.debug["naive_last_week"]["Kavia"]["ingreso_efectivo"]
    print(f"Source dates: {debug_info.data['source_dates']}")
```

### QA

```python
from pos_core.payments import marts as payments_marts
from pos_core.qa import run_payments_qa

df = payments_marts.fetch_daily(paths, "2025-01-01", "2025-01-31")
result = run_payments_qa(df)

print(result.summary)
if result.missing_days is not None:
    print(result.missing_days)
```

## Data Formats

### Date Format

All dates should be in `YYYY-MM-DD` format (e.g., `"2025-01-15"`).

### Payments DataFrame (Daily Mart)

- `sucursal` (str): Branch name
- `fecha` (date): Date of the record
- `ingreso_efectivo` (float): Cash income
- `ingreso_credito` (float): Credit card income
- `ingreso_debito` (float): Debit card income
- Additional payment method columns as available

### Sales DataFrame (Item-Line Core Fact)

- `sucursal` (str): Branch name
- `operating_date` (date): Date of operation
- `order_id` (str): Ticket/order identifier
- `item_key` (str): Item identifier
- `group` (str): Product group/category
- `subtotal_item` (float): Item subtotal
- `total_item` (float): Item total

## Security

**Never commit secrets or sensitive data to version control.**

- Environment variables should be stored in `secrets.env` or `.env` files (both are in `.gitignore`)
- Real data files (`.xlsx`, `.csv`) should not be committed

## Troubleshooting

1. **Missing sucursales.json**: Ensure the file exists at the expected location
2. **Authentication Errors**: Verify `WS_BASE`, `WS_USER`, and `WS_PASS` environment variables are set correctly. Credentials are required for extraction.
3. **Insufficient Data for Forecasting**: ARIMA models require at least 30 days of historical data
4. **Missing Date Ranges**: Use `mode="force"` in `fetch()` functions to force re-run ETL stages

## Development

### Code Quality Checks

```bash
# Fix linting issues
python3 -m ruff check --fix src/ tests/

# Format code
python3 -m ruff format src/ tests/

# Type checking
python3 -m mypy src/pos_core
```

### Testing

```bash
python -m pytest tests/
```

For development, install with dev dependencies:

```bash
pip install -e .[dev]
```

## Migration from v0.2.x

The API has been refactored in v0.3.0 to use Option A (module namespaces). Key changes:

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

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Contributing

When contributing:
- Follow the existing code structure and conventions
- Add type hints to all functions
- Include comprehensive docstrings
- Update tests for new features

## Support

- **Documentation**: [https://toxicfyre.github.io/pos-pipeline-core-etl/](https://toxicfyre.github.io/pos-pipeline-core-etl/)
- **Issues**: [GitHub Issues](https://github.com/ToxicFyre/pos-pipeline-core-etl/issues)
- **Source Code**: [`src/pos_core/`](src/pos_core/)
