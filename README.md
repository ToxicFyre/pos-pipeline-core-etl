# POS Core ETL

A comprehensive Python package for Point of Sale (POS) data processing, forecasting, and quality assurance. This package provides tools to extract payment and sales data from POS systems, clean and transform it, aggregate it for analysis, generate forecasts using time series models, and perform automated quality checks.

> **Note**: This README assumes you're viewing the repo (e.g., on GitHub). References to `tests/` and `src/...` paths are relative to the repository root.
>
> **Documentation**: Full documentation is available at [https://toxicfyre.github.io/pos-pipeline-core-etl/](https://toxicfyre.github.io/pos-pipeline-core-etl/)

## POS System Compatibility

This package is designed for POS systems that expose **Wansoft-style HTTP exports** for payment reports, sales detail reports, and transfer reports. The package expects:

- HTTP endpoints for exporting reports (payments, detail, transfers)
- Excel format for exported reports
- Optional username/password authentication

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
from pos_core.payments import get_payments
from pos_core.sales import get_sales
from pos_core.forecasting import ForecastConfig, run_payments_forecast

# Configure paths
paths = DataPaths.from_root(Path("data"), Path("utils/sucursales.json"))

# Get payments data (daily mart by default)
# This will automatically download data if needed
payments = get_payments(paths, "2025-01-01", "2025-01-31")

# Get sales data (item-line core fact by default)
sales = get_sales(paths, "2025-01-01", "2025-01-31")

# Generate forecast
config = ForecastConfig(horizon_days=91)  # 13 weeks
result = run_payments_forecast(payments, config)
print(result.forecast.head())
```

For more examples, see:
- **Runnable scripts**: Check the [`examples/`](examples/) directory
- **Documentation**: Full documentation at [https://toxicfyre.github.io/pos-pipeline-core-etl/](https://toxicfyre.github.io/pos-pipeline-core-etl/)

## Prerequisites

### Configuration Files

**sucursales.json** (required): Branch configuration file that maps branch names to codes.

Default location: `utils/sucursales.json`

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
  }
}
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

## API Reference

### Configuration

```python
from pos_core import DataPaths

paths = DataPaths.from_root(Path("data"), Path("utils/sucursales.json"))
```

### Payments

```python
from pos_core.payments import get_payments

# Get daily mart (default, most common use case)
df = get_payments(paths, "2025-01-01", "2025-01-31")

# Get core fact (ticket × payment method grain)
df = get_payments(paths, "2025-01-01", "2025-01-31", grain="ticket")
```

**Parameters:**
- `paths`: DataPaths configuration
- `start_date`, `end_date`: Date range (YYYY-MM-DD format)
- `grain`: `"ticket"` (core fact) or `"daily"` (mart, default)
- `branches`: Optional list of branch names to filter
- `refresh`: If True, force re-run all ETL stages

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

**Parameters:**
- `paths`: DataPaths configuration
- `start_date`, `end_date`: Date range (YYYY-MM-DD format)
- `grain`: `"item"` (core fact, default), `"ticket"` (mart), or `"group"` (mart)
- `branches`: Optional list of branch names to filter
- `refresh`: If True, force re-run all ETL stages

### Forecasting

```python
from pos_core.payments import get_payments
from pos_core.forecasting import run_payments_forecast, ForecastConfig

# Get historical data
payments_df = get_payments(paths, "2022-01-01", "2025-01-31")

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
from pos_core.payments import get_payments
from pos_core.qa import run_payments_qa

df = get_payments(paths, "2025-01-01", "2025-01-31")
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
4. **Missing Date Ranges**: Use `refresh=True` to force re-run ETL stages

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

## Migration from v0.1.x

The API has been simplified in v0.2.0. Key changes:

| Old API | New API |
|---------|---------|
| `PaymentsETLConfig.from_root(...)` | `DataPaths.from_root(...)` |
| `SalesETLConfig.from_root(...)` | `DataPaths.from_root(...)` |
| `from pos_core.etl import get_payments` | `from pos_core.payments import get_payments` |
| `from pos_core.etl import get_sales` | `from pos_core.sales import get_sales` |
| `get_sales(..., level="ticket")` | `get_sales(..., grain="ticket")` |
| `get_payments_forecast(...)` | `get_payments(...) + run_payments_forecast(...)` |
| `build_payments_dataset(...)` | `get_payments(...)` |

Old entry points were intentionally removed to reduce technical debt. The new API uses:
- A unified `DataPaths` config for both payments and sales
- `grain=` parameter instead of `level=` for clarity
- Explicit composition for forecasting (get data, then forecast)

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
