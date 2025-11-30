# POS Core ETL

A comprehensive Python package for Point of Sale (POS) data processing, forecasting, and quality assurance. Extract payment and sales data from POS systems, transform it through bronze/silver/gold layers, generate forecasts, and perform automated quality checks.

> **📚 Full Documentation**: [https://toxicfyre.github.io/pos-pipeline-core-etl/](https://toxicfyre.github.io/pos-pipeline-core-etl/)

## Features

- **ETL Pipeline**: Extract, transform, and load POS payment and sales data across bronze/silver/gold layers
- **Time Series Forecasting**: Generate ARIMA-based forecasts for payment metrics
- **Quality Assurance**: Automated data validation and anomaly detection
- **Multi-Branch Support**: Handle multiple sucursales (branches) with code window tracking
- **Incremental Processing**: Smart date range chunking and existing data discovery
- **Idempotent Operations**: Automatic skipping of already-completed work

## Quick Start

### 1. Installation

```bash
pip install pos-core-etl
```

For development:

```bash
git clone https://github.com/ToxicFyre/pos-pipeline-core-etl.git
cd pos-pipeline-core-etl
pip install -e .[dev]
```

### 2. Configuration

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

### 3. Set Environment Variables

**Required for downloading raw data** (optional if you already have raw data files):

```bash
export WS_BASE="https://your-pos-instance.com"
export WS_USER="your_username"
export WS_PASS="your_password"
```

### 4. Run Your First ETL

```python
from pathlib import Path
from pos_core import DataPaths
from pos_core.payments import marts as payments_marts
from pos_core.sales import core as sales_core
from pos_core.forecasting import ForecastConfig, run_payments_forecast

# Configure paths
paths = DataPaths.from_root(Path("data"), Path("utils/sucursales.json"))

# Payments: daily mart (most common use case)
payments_daily = payments_marts.fetch_daily(paths, "2025-01-01", "2025-01-31")

# Sales: core item-line fact
sales_items = sales_core.fetch(paths, "2025-01-01", "2025-01-31")

# Forecasting on payments daily mart
config = ForecastConfig(horizon_days=91)  # 13 weeks
result = run_payments_forecast(payments_daily, config)
print(result.forecast.head())
```

## Key Concepts

### Data Layers (Bronze/Silver/Gold)

The package follows industry-standard data engineering conventions:

| Layer | Description | Directory | Format |
|-------|-------------|-----------|--------|
| **Bronze** | Raw Wansoft exports, unchanged | `data/a_raw/` | Excel files |
| **Silver (Core)** | Core facts at atomic grain | `data/b_clean/` | CSV files |
| **Gold (Marts)** | Aggregated tables for analysis | `data/c_processed/` | CSV files |

### Data Grains

| Domain | Core Fact | Grain | Key |
|--------|-----------|-------|-----|
| **Payments** | `fact_payments_ticket` | ticket × payment method | `(sucursal, operating_date, order_index, payment_method)` |
| **Sales** | `fact_sales_item_line` | item/modifier line | `(sucursal, operating_date, order_id, item_key)` |

### API Philosophy

The package uses **domain + layer modules** to encode specificity:

- **Modules define domain + layer**:
  - `pos_core.payments.core` → payments, silver (core fact)
  - `pos_core.payments.marts` → payments, gold (aggregates)
  - `pos_core.sales.core` → sales, silver
  - `pos_core.sales.marts` → sales, gold

- **Functions define behavior**:
  - `fetch(...)` / `fetch_*`: Run ETL for missing partitions (or all if `mode="force"`)
  - `load(...)` / `load_*`: Read existing outputs only; never run ETL

## Common Usage Patterns

### Payments

```python
from pos_core.payments import core, marts

# Get daily mart (most common)
daily_df = marts.fetch_daily(paths, "2025-01-01", "2025-01-31")

# Get core fact (ticket × payment method)
fact_df = core.fetch(paths, "2025-01-01", "2025-01-31")
```

### Sales

```python
from pos_core.sales import core, marts

# Get core fact (item-line grain)
fact_df = core.fetch(paths, "2025-01-01", "2025-01-31")

# Get ticket-level mart
ticket_df = marts.fetch_ticket(paths, "2025-01-01", "2025-01-31")

# Get group pivot mart
group_df = marts.fetch_group(paths, "2025-01-01", "2025-01-31")
```

### Forecasting

```python
from pos_core.payments import marts as payments_marts
from pos_core.forecasting import ForecastConfig, run_payments_forecast

# Get historical data
payments_df = payments_marts.fetch_daily(paths, "2022-01-01", "2025-01-31")

# Run forecast
config = ForecastConfig(horizon_days=91)
result = run_payments_forecast(payments_df, config)

print(result.forecast.head())
print(result.deposit_schedule.head())
```

### Quality Assurance

```python
from pos_core.payments import marts as payments_marts
from pos_core.qa import run_payments_qa

df = payments_marts.fetch_daily(paths, "2025-01-01", "2025-01-31")
result = run_payments_qa(df)

print(result.summary)
if result.missing_days is not None:
    print(result.missing_days)
```

## POS System Compatibility

This package is designed for POS systems that expose **Wansoft-style HTTP exports**:

- HTTP endpoints for exporting reports (payments, detail, transfers)
- Excel format for exported reports
- Username/password authentication (required for downloading raw data; optional if you already have raw data files)

## Documentation

### User Guide

- **[Installation](https://toxicfyre.github.io/pos-pipeline-core-etl/user-guide/installation/)** - Setup and requirements
- **[Quickstart](https://toxicfyre.github.io/pos-pipeline-core-etl/user-guide/quickstart/)** - Get started in minutes
- **[Configuration](https://toxicfyre.github.io/pos-pipeline-core-etl/user-guide/configuration/)** - Branch config and environment variables
- **[Concepts](https://toxicfyre.github.io/pos-pipeline-core-etl/user-guide/concepts/)** - Key concepts and design decisions
- **[Examples](https://toxicfyre.github.io/pos-pipeline-core-etl/user-guide/examples/)** - Runnable example scripts

### API Reference

- **[ETL API](https://toxicfyre.github.io/pos-pipeline-core-etl/api-reference/etl/)** - ETL pipeline functions
- **[Forecasting API](https://toxicfyre.github.io/pos-pipeline-core-etl/api-reference/forecasting/)** - Time series forecasting
- **[QA API](https://toxicfyre.github.io/pos-pipeline-core-etl/api-reference/qa/)** - Quality assurance and validation
- **[Exceptions](https://toxicfyre.github.io/pos-pipeline-core-etl/api-reference/exceptions/)** - Error handling

## Requirements

- Python 3.10+
- pandas >= 1.3.0
- numpy >= 1.20.0
- requests >= 2.25.0
- beautifulsoup4 >= 4.9.0
- statsmodels >= 0.12.0
- openpyxl >= 3.0.0

## Troubleshooting

1. **Missing sucursales.json**: Ensure the file exists at the expected location (`utils/sucursales.json` by default)
2. **Authentication Errors**: Verify `WS_BASE`, `WS_USER`, and `WS_PASS` environment variables are set correctly. Required for extraction.
3. **Insufficient Data for Forecasting**: ARIMA models require at least 30 days of historical data
4. **Missing Date Ranges**: Use `mode="force"` in `fetch()` functions to force re-run ETL stages

## Development

```bash
# Install development dependencies
pip install -e .[dev]

# Run tests
python -m pytest tests/

# Fix linting issues
python3 -m ruff check --fix src/ tests/

# Format code
python3 -m ruff format src/ tests/

# Type checking
python3 -m mypy src/pos_core
```

## Migration from v0.2.x

The API was refactored in v0.3.0 to use module namespaces. See the [migration guide](https://toxicfyre.github.io/pos-pipeline-core-etl/user-guide/concepts/#migration-from-v02x) for details.

| Old API (v0.2.x) | New API (v0.3.x) |
|------------------|------------------|
| `get_payments(..., grain="ticket")` | `payments.core.fetch(...)` |
| `get_payments(..., grain="daily")` | `payments.marts.fetch_daily(...)` |
| `get_sales(..., grain="item")` | `sales.core.fetch(...)` |
| `get_sales(..., grain="ticket")` | `sales.marts.fetch_ticket(...)` |

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Support

- **Documentation**: [https://toxicfyre.github.io/pos-pipeline-core-etl/](https://toxicfyre.github.io/pos-pipeline-core-etl/)
- **Issues**: [GitHub Issues](https://github.com/ToxicFyre/pos-pipeline-core-etl/issues)
- **Source Code**: [`src/pos_core/`](src/pos_core/)
