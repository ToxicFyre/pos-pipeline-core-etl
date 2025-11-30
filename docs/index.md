# POS Core ETL Documentation

Welcome to the POS Core ETL documentation! This package provides a comprehensive solution for Point of Sale data processing, forecasting, and quality assurance.

## What is POS Core ETL?

POS Core ETL is a Python package designed for POS systems that expose Wansoft-style HTTP exports. It provides:

- **ETL Pipeline**: Extract, transform, and load POS payment and sales data across bronze/silver/gold layers
- **Time Series Forecasting**: Generate ARIMA-based forecasts for payment metrics
- **Quality Assurance**: Automated data validation and anomaly detection
- **Multi-Branch Support**: Handle multiple sucursales (branches) with code window tracking
- **Incremental Processing**: Smart date range chunking and existing data discovery

## Quick Start

```python
from pathlib import Path
from pos_core import DataPaths
from pos_core.payments import marts as payments_marts
from pos_core.forecasting import ForecastConfig, run_payments_forecast

# Configure paths
paths = DataPaths.from_root(Path("data"), Path("utils/sucursales.json"))

# Get payments daily mart
payments_daily = payments_marts.fetch_daily(paths, "2025-01-01", "2025-01-31")

# Generate forecast
config = ForecastConfig(horizon_days=91)
result = run_payments_forecast(payments_daily, config)
print(result.forecast.head())
```

## Documentation Structure

### 🚀 Getting Started

- **[Installation](user-guide/installation.md)** - Install the package and set up your environment
- **[Quickstart](user-guide/quickstart.md)** - Get started in minutes with a working example
- **[Configuration](user-guide/configuration.md)** - Configure branches, credentials, and paths
- **[Examples](user-guide/examples.md)** - Complete runnable example scripts

### 📚 Concepts

- **[Concepts](user-guide/concepts.md)** - Understand data layers, grains, API design, and key concepts

### 🔧 API Reference

- **[ETL API](api-reference/etl.md)** - ETL pipeline functions and configuration
- **[Forecasting API](api-reference/forecasting.md)** - Time series forecasting functions
- **[QA API](api-reference/qa.md)** - Quality assurance and validation functions
- **[Exceptions](api-reference/exceptions.md)** - Error handling and exceptions

### 🛠️ Development

- **[Development Notes](development/dev-notes.md)** - Information for contributors

## Key Concepts

### Data Layers

The package follows industry-standard **bronze/silver/gold** data layer conventions:

- **Bronze**: Raw Wansoft exports (Excel files in `a_raw/`)
- **Silver (Core)**: Core facts at atomic grain (CSV files in `b_clean/`)
- **Gold (Marts)**: Aggregated tables for analysis (CSV files in `c_processed/`)

### API Design

The package uses **domain + layer modules**:

- `pos_core.payments.core` → payments, silver (core fact)
- `pos_core.payments.marts` → payments, gold (aggregates)
- `pos_core.sales.core` → sales, silver
- `pos_core.sales.marts` → sales, gold

Functions:
- `fetch()` / `fetch_*`: Run ETL for missing partitions
- `load()` / `load_*`: Read existing outputs only

### Data Grains

| Domain | Core Fact | Grain |
|--------|-----------|-------|
| **Payments** | `fact_payments_ticket` | ticket × payment method |
| **Sales** | `fact_sales_item_line` | item/modifier line |

## Common Use Cases

### Payments ETL

```python
from pos_core.payments import core, marts

# Daily mart (most common)
daily_df = marts.fetch_daily(paths, "2025-01-01", "2025-01-31")

# Core fact
fact_df = core.fetch(paths, "2025-01-01", "2025-01-31")
```

### Sales ETL

```python
from pos_core.sales import core, marts

# Core fact
fact_df = core.fetch(paths, "2025-01-01", "2025-01-31")

# Ticket mart
ticket_df = marts.fetch_ticket(paths, "2025-01-01", "2025-01-31")

# Group mart
group_df = marts.fetch_group(paths, "2025-01-01", "2025-01-31")
```

### Forecasting

```python
from pos_core.forecasting import ForecastConfig, run_payments_forecast

config = ForecastConfig(horizon_days=91)
result = run_payments_forecast(payments_df, config)
```

### Quality Assurance

```python
from pos_core.qa import run_payments_qa

result = run_payments_qa(payments_df)
print(result.summary)
```

## Requirements

- Python 3.10+
- pandas >= 1.3.0
- numpy >= 1.20.0
- requests >= 2.25.0
- beautifulsoup4 >= 4.9.0
- statsmodels >= 0.12.0
- openpyxl >= 3.0.0

## Installation

```bash
pip install pos-core-etl
```

For development:

```bash
git clone https://github.com/ToxicFyre/pos-pipeline-core-etl.git
cd pos-pipeline-core-etl
pip install -e .[dev]
```

## Support

- **GitHub Issues**: [Report bugs or request features](https://github.com/ToxicFyre/pos-pipeline-core-etl/issues)
- **Source Code**: [`src/pos_core/`](https://github.com/ToxicFyre/pos-pipeline-core-etl/tree/main/src/pos_core)

## License

MIT License - see [LICENSE](https://github.com/ToxicFyre/pos-pipeline-core-etl/blob/main/LICENSE) file for details.
