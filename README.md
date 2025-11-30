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

The package is currently optimized for this specific POS architecture, but the design allows for future extension to other POS backends. See the [Concepts](docs/user-guide/concepts.md) documentation for more details.

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

For development and contributing:

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

Here's a minimal example to get started:

```python
from pathlib import Path
from pos_core.etl import PaymentsETLConfig, get_payments, get_payments_forecast

# Configure
config = PaymentsETLConfig.from_root(Path("data"), Path("utils/sucursales.json"))

# Get payments data (automatically runs ETL stages only if needed)
payments = get_payments("2025-01-01", "2025-01-31", config)

# Generate forecast
forecast = get_payments_forecast("2025-01-31", horizon_weeks=1, config=config)
print(forecast.head())
```

This gets payment data for the date range (running ETL stages only if needed), then generates a 7-day forecast. The query functions automatically handle idempotence - they skip work that's already been done. 

For more examples, see:
- **Runnable scripts**: Check the [`examples/`](examples/) directory for complete, runnable example scripts
- **Documentation**: Full documentation at [https://toxicfyre.github.io/pos-pipeline-core-etl/](https://toxicfyre.github.io/pos-pipeline-core-etl/)
- **Detailed examples**: See the "Usage Examples" section below

## Prerequisites

### Configuration Files

1. **sucursales.json** (required): Branch configuration file that maps branch names to codes and tracks validity windows. The same file is used by both ETL and sales examples.

   Default location: `utils/sucursales.json` (relative to your data root's parent directory)

   Example structure:
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

The following environment variables are **required for online extraction** (downloading data from the POS API). They are **not needed** if you only use the library on already-downloaded files:

- `WS_BASE` (required for extraction): Base URL of your POS instance
- `WS_USER` (optional): Username for authentication (if your POS instance requires login)
- `WS_PASS` (optional): Password for authentication (if your POS instance requires login)

If you only work with already-downloaded files in `a_raw`, you can ignore `WS_BASE/USER/PASS`.

In practice, these are often stored in a `secrets.env` file and loaded into the environment before running the pipeline (for example via a launcher script). You can also set these in a `.env` file or export them in your shell.

### Directory Structure

The package follows an ETL naming convention with industry-standard data layers:

```
data/
├── a_raw/          # Raw (Bronze) - Direct Wansoft exports, unchanged
│   ├── payments/
│   │   └── batch/
│   └── sales/
│       └── batch/
├── b_clean/        # Staging (Silver) - Cleaned and standardized tables
│   ├── payments/
│   │   └── batch/
│   └── sales/
│       └── batch/
└── c_processed/    # Core + Marts (Silver+/Gold) - Modeled and aggregated tables
    ├── payments/
    └── sales/
```

## Data Layers

The ETL pipeline follows industry-standard **bronze/silver/gold** data layer conventions, making it familiar to data engineers:

| Layer | Code Location | Data Directory | Description |
|-------|--------------|----------------|-------------|
| **Raw (Bronze)** | `pos_core.etl.raw/` | `data/a_raw/` | Direct Wansoft exports, unchanged. Excel files as received from the POS API. |
| **Staging (Silver)** | `pos_core.etl.staging/` | `data/b_clean/` | Cleaned and standardized tables containing **core facts** at their atomic grain. |
| **Core (Silver+)** | `pos_core.etl.core/` | `data/b_clean/` | Documents the grain definitions. The staging output IS the core fact. |
| **Marts (Gold)** | `pos_core.etl.marts/` | `data/c_processed/` | Aggregated semantic tables. All aggregations beyond core grain. |

### Grain Definitions (Ground Truth)

The most granular meaningful unit of data differs by domain:

| Domain | Core Fact | Grain | Key | Description |
|--------|-----------|-------|-----|-------------|
| **Payments** | `fact_payments_ticket` | ticket × payment method | `(sucursal, operating_date, order_index, payment_method)` | The POS payments export does not expose item-level payment data. Ticket-level is the atomic fact. |
| **Sales** | `fact_sales_item_line` | item/modifier line | `(sucursal, operating_date, order_id, item_key, [modifier])` | Each row represents an item or modifier on a ticket. Multiple rows can share the same `ticket_id`. |

**Key Rule:**
- For **sales**: anything aggregated beyond item/modifier line is **gold/mart**, not silver/core
- For **payments**: ticket × payment method is already the atomic fact (silver/core)

### Layer Flow

```
┌─────────────────┐     ┌─────────────────────────────┐     ┌─────────────────┐
│   Raw (Bronze)  │ ──▶ │ Staging (Silver) = Core     │ ──▶ │  Marts (Gold)   │
│                 │     │                             │     │                 │
│ a_raw/          │     │ b_clean/                    │     │ c_processed/    │
│ HTTP extraction │     │ Core facts at atomic grain: │     │ Aggregations:   │
│ Excel files     │     │ • fact_payments_ticket      │     │ • By ticket     │
│                 │     │ • fact_sales_item_line      │     │ • By day        │
│                 │     │                             │     │ • By category   │
└─────────────────┘     └─────────────────────────────┘     └─────────────────┘
```

### API and Layers

- **High-level functions** like `get_sales()` and `get_payments()` automatically orchestrate all layers, running only the stages that are needed.
- **`pos_core.forecasting`** and **`pos_core.qa`** are consumers of the ETL outputs (mart layer).
- The `level=` parameter in query functions controls aggregation level:
  - `level="ticket"` → **Mart**: aggregates item-lines to ticket level
  - `level="group"` → **Mart**: aggregates to category pivot tables
  - Core facts (item-line, ticket × payment method) are accessed via the staging layer output

## Usage Examples

For detailed examples, see:
- **[Quickstart Guide](docs/user-guide/quickstart.md)** - Get started in minutes
- **[Examples Guide](docs/user-guide/examples.md)** - Complete runnable examples
- **[Example Scripts](examples/)** - Runnable Python scripts in the repository

### Quick Examples

**Sales Data**:
```python
from pos_core.etl import SalesETLConfig, get_sales
config = SalesETLConfig.from_root(Path("data"), Path("utils/sucursales.json"))
df = get_sales("2025-01-01", "2025-01-31", config, level="group")
```

**Payments Data**:
```python
from pos_core.etl import PaymentsETLConfig, get_payments
config = PaymentsETLConfig.from_root(Path("data"), Path("utils/sucursales.json"))
payments = get_payments("2025-01-01", "2025-01-31", config)
```

**Forecasting**:
```python
from pos_core.etl import PaymentsETLConfig, get_payments_forecast
config = PaymentsETLConfig.from_root(Path("data"), Path("utils/sucursales.json"))
forecast = get_payments_forecast("2025-01-31", horizon_weeks=13, config=config)
```

**Forecasting with Debug Information**:
```python
from pos_core.forecasting import ForecastConfig, run_payments_forecast

config = ForecastConfig(horizon_days=7)
result = run_payments_forecast(payments_df, config=config, debug=True)

# Access model-specific debug information
if result.debug:
    naive_debug = result.debug["naive_last_week"]["Kavia"]["ingreso_efectivo"]
    print(f"Source dates mapping: {naive_debug.data['source_dates']}")
```

## API Reference

For complete API documentation, see:
- **[ETL API Reference](docs/api-reference/etl.md)** - ETL pipeline functions and configuration
- **[Forecasting API Reference](docs/api-reference/forecasting.md)** - Time series forecasting
- **[QA API Reference](docs/api-reference/qa.md)** - Quality assurance and validation
- **[Exceptions API Reference](docs/api-reference/exceptions.md)** - Error handling

### Quick API Overview

**Query Functions** (Recommended):
- `get_payments()` - Get payments data with automatic ETL stage execution
- `get_sales()` - Get sales data at specified aggregation level
- `get_payments_forecast()` - Generate forecasts with automatic data preparation

**Configuration**:
- `PaymentsETLConfig` - Payments ETL configuration
- `SalesETLConfig` - Sales ETL configuration
- `ForecastConfig` - Forecasting configuration

**Forecasting**:
- `run_payments_forecast()` - Main forecasting function (supports `debug=True` for model introspection)
- `ForecastResult` - Forecast results with deposit schedule, metadata, and optional debug info
- `ModelDebugInfo` - Generic container for model-specific debug information

**QA**:
- `run_payments_qa()` - Data quality validation
- `PaymentsQAResult` - QA results with detailed findings

## API Stability

**Public APIs**: Functions and dataclasses documented in the "API Reference" section above are considered stable and will follow semantic versioning. These include:

- All exports from `pos_core.etl`, `pos_core.forecasting`, and `pos_core.qa` modules (as defined in their `__all__` attributes)
- Exception classes in `pos_core.exceptions`
- The `__version__` export from `pos_core`

**Internal APIs**: Other modules, functions, and classes not explicitly exported are considered internal and may change between minor versions without notice. This includes:

- Functions in `pos_core.etl.raw`, `pos_core.etl.staging`, `pos_core.etl.core`, `pos_core.etl.marts`
- Internal helper functions and utilities
- Any module or function not listed in a package's `__all__` attribute

When in doubt, only use the documented public APIs. If you need access to internal functionality, please open an issue to discuss making it part of the public API.

## Configuration

For detailed configuration options, see the [Configuration Guide](docs/user-guide/configuration.md).

### Quick Configuration Examples

**Payments ETL**:
```python
from pos_core.etl import PaymentsETLConfig
config = PaymentsETLConfig.from_root(Path("data"), Path("utils/sucursales.json"))
```

**Sales ETL**:
```python
from pos_core.etl import SalesETLConfig
config = SalesETLConfig.from_root(Path("data"), Path("utils/sucursales.json"))
```

**Forecasting**:
```python
from pos_core.forecasting import ForecastConfig
config = ForecastConfig(horizon_days=14, metrics=["ingreso_efectivo", "ingreso_total"])
```

## Data Formats

### Date Formats

All dates should be in `YYYY-MM-DD` format (e.g., `"2025-01-15"`).

### Payments DataFrame

The aggregated payments DataFrame contains:
- `sucursal` (str): Branch name
- `fecha` (date/datetime): Date of the record
- `ingreso_efectivo` (float): Cash income
- `ingreso_credito` (float): Credit card income
- `ingreso_debito` (float): Debit card income
- `ingreso_total` (float): Total income
- Additional payment method columns as available

### Sales Details DataFrame

The sales details DataFrame (after cleaning) contains:
- `sucursal` (str): Branch name
- `operating_date` (date): Date of operation
- `order_id` (str): Ticket/order identifier
- `group` (str): Product group/category
- `subtotal_item` (float): Item subtotal
- `total_item` (float): Item total
- Additional columns for item details, modifiers, etc.

For more details on data formats and structures, see the [Concepts Guide](docs/user-guide/concepts.md).

## Security and Best Practices

### Secrets Management

**Never commit secrets or sensitive data to version control.**

- Environment variables (`WS_BASE`, `WS_USER`, `WS_PASS`) should be stored in `secrets.env` or `.env` files (both are in `.gitignore`)
- Real data files (`.xlsx`, `.csv`) should not be committed (see `.gitignore`)
- The package is configured to ignore common sensitive file patterns

### Data Privacy

- The package processes financial data. Ensure you comply with relevant data protection regulations
- Use secure storage for your data directory
- Consider encrypting data at rest for production deployments

## Logging

The package uses Python's standard `logging` module with named loggers for easy configuration.

### Logger Names

- `pos_core.etl`: ETL pipeline operations
- `pos_core.forecasting`: Forecasting operations
- `pos_core.qa`: Quality assurance operations

### Configuring Logging

To enable debug logging, configure the logging module:

```python
import logging

# Set level for specific module
logging.getLogger("pos_core.etl").setLevel(logging.DEBUG)

# Or configure all pos_core loggers
logging.getLogger("pos_core").setLevel(logging.DEBUG)

# Configure format and handler
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
```

### Logging Levels

- **INFO**: General progress information (default)
- **DEBUG**: Detailed debugging information (useful for troubleshooting extraction issues)
- **WARNING**: Non-critical issues (e.g., missing data for a branch)
- **ERROR**: Errors that prevent operation completion

## Performance

### Tested Volumes

The package has been tested on:
- **3+ years** of daily data
- **7+ branches** (sucursales)
- **Multiple payment methods** per branch
- **Incremental processing** of large date ranges

### Expected Performance

- **ETL**: Processing 6 months of data for 7 branches typically takes 5-15 minutes (depending on network speed for downloads)
- **Forecasting**: Generating 7-day forecasts for 7 branches and 4 metrics typically takes 1-3 minutes
- **QA**: Running full QA checks on 3 years of data typically takes 10-30 seconds

### Optimization Tips

- Use `chunk_size_days` to balance HTTP request size vs. number of requests
- Process only needed branches using the `branches` parameter
- Reuse aggregated CSV files instead of re-running ETL
- For very large datasets, consider processing in date range chunks

## Troubleshooting

### Common Issues

1. **Missing sucursales.json**: Ensure the file exists at the expected location or specify the path explicitly in `PaymentsETLConfig.from_root()` or `SalesETLConfig.from_root()`.

2. **Authentication Errors**: Verify that `WS_BASE`, `WS_USER`, and `WS_PASS` environment variables are set correctly.

3. **Insufficient Data for Forecasting**: ARIMA models require at least 30 days of historical data. Ensure your payments DataFrame has sufficient history.

4. **ARIMA Convergence Warnings**: Common when data is short or noisy; forecasts still work, but you can restrict metrics/branches or reduce `horizon_days` to reduce warnings.

5. **Missing Date Ranges**: The ETL pipeline uses metadata to track completed stages and automatically skips work that's already been done. To force re-run, use `refresh=True` in query functions or `force=True` in stage functions. Alternatively, delete the relevant files in `a_raw/` or metadata files in `_meta/` subdirectories.

6. **Branch Code Windows**: If a branch code changed over time, ensure your `sucursales.json` includes validity windows (`valid_from`, `valid_to`).

7. **Debugging Extraction Issues**: Enable DEBUG logging for `pos_core.etl` to see detailed HTTP request/response information.

## Development

### Code Quality Checks

Before committing code, ensure it passes linting and formatting checks. This project uses `ruff` for both linting and formatting.

**If pre-commit hooks block your commit or push**, or if you want to run checks manually, run:

```bash
# Fix linting issues automatically
python3 -m ruff check --fix src/ tests/

# Format code
python3 -m ruff format src/ tests/

# Verify everything is fixed (run again after fixing)
python3 -m ruff check --fix src/ tests/
python3 -m ruff format src/ tests/
```

These commands will:
- Automatically fix most linting issues (line length, unused imports, etc.)
- Format code according to the project's style guide
- Ensure your code is ready to commit

**Note**: The project also uses pre-commit hooks (configured in `.pre-commit-config.yaml`) that automatically run these checks on commit. If you encounter issues during commit, run the commands above to fix them.

### Type Checking

The project uses `mypy` for static type checking:

```bash
python3 -m mypy src/pos_core
```

## Testing

Run the smoke tests to verify installation:

```bash
python -m pytest tests/
```

Tests are located in:
- `tests/test_etl_smoke.py`: ETL API imports and configuration
- `tests/test_forecasting_smoke.py`: Forecasting API with synthetic data
- `tests/test_qa_smoke.py`: QA API with minimal test data

For development, install with dev dependencies:

```bash
pip install -e .[dev]
```

This includes pytest, mypy, ruff, and black for code quality checks.

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Contributing

This package is designed for production use. When contributing:
- Follow the existing code structure and conventions
- Add type hints to all functions
- Include comprehensive docstrings
- Update tests for new features
- Maintain backward compatibility with existing APIs

## Support

- **Documentation**: Full documentation at [https://toxicfyre.github.io/pos-pipeline-core-etl/](https://toxicfyre.github.io/pos-pipeline-core-etl/)
- **Issues**: Report issues on [GitHub](https://github.com/ToxicFyre/pos-pipeline-core-etl/issues)
- **Source Code**: View source code documentation in [`src/pos_core/`](src/pos_core/)

