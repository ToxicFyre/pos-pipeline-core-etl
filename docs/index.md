# POS Core ETL

A comprehensive Python package for Point of Sale (POS) data processing, forecasting, and quality assurance.

## Features

- **ETL Pipeline**: Extract, transform, and load POS payment and sales data
- **Time Series Forecasting**: Generate ARIMA-based forecasts for payment metrics
- **Quality Assurance**: Automated data validation and anomaly detection
- **Multi-Branch Support**: Handle multiple sucursales (branches) with code window tracking
- **Incremental Processing**: Smart date range chunking and existing data discovery

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
pip install -e .[dev]
```

## Documentation

- [User Guide](user-guide/installation.md) - Installation, configuration, and usage
- [API Reference](api-reference/etl.md) - Complete API documentation
- [Examples](user-guide/examples.md) - Runnable example scripts

## License

MIT License - see [LICENSE](https://github.com/ToxicFyre/pos-pipeline-core-etl/blob/main/LICENSE) file for details.

