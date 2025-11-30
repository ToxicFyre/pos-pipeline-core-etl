# POS Core ETL

A comprehensive Python package for Point of Sale (POS) data processing, forecasting, and quality assurance.

## Overview

POS Core ETL provides tools to extract payment and sales data from POS systems, clean and transform it, aggregate it for analysis, generate forecasts using time series models, and perform automated quality checks.

## Features

- **ETL Pipeline**: Extract, transform, and load POS payment and sales data
- **Time Series Forecasting**: Generate ARIMA-based forecasts for payment metrics
- **Quality Assurance**: Automated data validation and anomaly detection
- **Multi-Branch Support**: Handle multiple sucursales (branches) with code window tracking
- **Incremental Processing**: Smart date range chunking and existing data discovery
- **Automatic Idempotence**: Query functions skip work that's already been done

## Quick Start

```python
from pathlib import Path
from pos_core.etl import PaymentsETLConfig, get_payments, get_payments_forecast

# Configure and get payments data
config = PaymentsETLConfig.from_root(Path("data"), Path("utils/sucursales.json"))
payments = get_payments("2025-01-01", "2025-01-31", config)

# Generate forecast
forecast = get_payments_forecast("2025-01-31", horizon_weeks=1, config=config)
print(forecast.head())
```

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

## Documentation

### User Guide

- [Installation](user-guide/installation.md) - Setup and requirements
- [Quickstart](user-guide/quickstart.md) - Get started in minutes
- [Configuration](user-guide/configuration.md) - Branch config and environment variables
- [Concepts](user-guide/concepts.md) - Key concepts and design decisions
- [Examples](user-guide/examples.md) - Runnable example scripts

### API Reference

- [ETL API](api-reference/etl.md) - ETL pipeline functions and configuration
- [Forecasting API](api-reference/forecasting.md) - Time series forecasting
- [QA API](api-reference/qa.md) - Quality assurance and validation
- [Exceptions](api-reference/exceptions.md) - Error handling

## License

MIT License - see [LICENSE](https://github.com/ToxicFyre/pos-pipeline-core-etl/blob/main/LICENSE) file for details.

