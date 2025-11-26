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

The package follows an ETL naming convention. Create the following directory structure:

```
data/
├── a_raw/          # Raw data files downloaded from POS API
│   ├── payments/
│   │   └── batch/
│   └── sales/
│       └── batch/
├── b_clean/        # Cleaned and normalized data files
│   ├── payments/
│   │   └── batch/
│   └── sales/
│       └── batch/
└── c_processed/    # Aggregated and processed datasets
    ├── payments/
    └── sales/
```

## Usage Examples

### Example 1: Recommended – Sales Detail ETL (Query API)

**Recommended approach**: Using the high-level query API for sales detail ETL. This example shows how to get sales data aggregated by ticket and by product group for a specific week.

```python
from pathlib import Path
from pos_core.etl import SalesETLConfig, get_sales

# Set up configuration
data_root = Path("data")
sucursales_json = Path("utils/sucursales.json")
config = SalesETLConfig.from_root(data_root, sucursales_json)

# Define the week (Monday to Sunday)
week_start = "2025-01-06"  # Monday
week_end = "2025-01-12"    # Sunday

# Get sales data aggregated by ticket (runs ETL stages only if needed)
df_ticket = get_sales(
    start_date=week_start,
    end_date=week_end,
    config=config,
    level="ticket",
    refresh=True,  # Force re-run all stages
)

print(f"Aggregated by ticket: {len(df_ticket)} tickets")
print(df_ticket.head())

# Get sales data aggregated by group (reuses existing data if available)
df_group = get_sales(
    start_date=week_start,
    end_date=week_end,
    config=config,
    level="group",
    refresh=False,  # Use existing data if available
)

print(f"Aggregated by group: pivot table with {len(df_group)} groups")
print(df_group.head())
```

The final output (`sales_by_group_*.csv`) will be a pivot table with:
- **Rows**: Product groups (e.g., "CAFE Y BEBIDAS CALIENTES", "COMIDAS", "PIZZA", etc.)
- **Columns**: Sucursales (branches)
- **Values**: Total sales amounts for each group-branch combination

**Note**: For fine-grained control, you can also use stage functions (`download_sales`, `clean_sales`, `aggregate_sales`) or low-level functions in `pos_core.etl.a_extract`, `pos_core.etl.b_transform`, and `pos_core.etl.c_load`.

### Example 2: Recommended – Main Payments ETL Workflow

**Recommended approach**: Using the query API to get payments data. The query functions automatically handle running ETL stages only when needed.

```python
from pathlib import Path
from datetime import date, timedelta

from pos_core.etl import PaymentsETLConfig, get_payments

# Calculate date range (3 years ago to today)
end_date = date.today()
start_date = end_date - timedelta(days=3 * 365)

# Set up configuration
data_root = Path("data")
sucursales_json = Path("utils/sucursales.json")

config = PaymentsETLConfig.from_root(data_root, sucursales_json)

# Get payments data (automatically runs ETL stages only if needed)
# This will:
# 1. Check metadata and download missing payment reports from POS API
# 2. Clean raw Excel files into normalized CSVs (if needed)
# 3. Aggregate cleaned data into daily dataset (if needed)
payments_df = get_payments(
    start_date=start_date.strftime("%Y-%m-%d"),
    end_date=end_date.strftime("%Y-%m-%d"),
    config=config,
    refresh=False,  # Use existing data if available
)

# The resulting DataFrame has one row per sucursal per day
print(f"Total rows: {len(payments_df)}")
print(f"Date range: {payments_df['fecha'].min()} to {payments_df['fecha'].max()}")
print(f"Branches: {payments_df['sucursal'].nunique()}")
print(f"\nColumns: {list(payments_df.columns)}")
print(f"\nFirst few rows:")
print(payments_df.head())

# Save to CSV for future use
output_path = data_root / "c_processed" / "payments" / "aggregated_payments_daily.csv"
output_path.parent.mkdir(parents=True, exist_ok=True)
payments_df.to_csv(output_path, index=False)
print(f"\nSaved to: {output_path}")

# Example: Filter for a specific branch
banana_payments = payments_df[payments_df['sucursal'] == 'Banana']
print(f"\nBanana payments: {len(banana_payments)} days")

# Example: Get summary statistics
summary = payments_df.groupby('sucursal').agg({
    'ingreso_total': ['sum', 'mean', 'min', 'max'],
    'fecha': ['min', 'max', 'count']
})
print(f"\nSummary by branch:")
print(summary)
```

The resulting DataFrame contains columns such as:
- `sucursal`: Branch name
- `fecha`: Date (YYYY-MM-DD)
- `ingreso_efectivo`: Cash income
- `ingreso_credito`: Credit card income
- `ingreso_debito`: Debit card income
- `ingreso_total`: Total income
- Additional payment method columns (AMEX, UberEats, Rappi, etc.)

### Example 3: Recommended – Forecasting Workflow

**Recommended approach**: Using the query API to get forecasts. This automatically handles getting historical data and running the forecast.

```python
from pathlib import Path
from pos_core.etl import PaymentsETLConfig, get_payments_forecast

# Set up configuration
data_root = Path("data")
sucursales_json = Path("utils/sucursales.json")
config = PaymentsETLConfig.from_root(data_root, sucursales_json)

# Get payments forecast (automatically gets historical data and runs forecast)
forecast_df = get_payments_forecast(
    as_of="2025-11-24",  # Forecast as of this date
    horizon_weeks=13,  # Forecast 13 weeks ahead
    config=config,
    refresh=False,  # Use existing data if available
)

print("Forecast DataFrame:")
print(forecast_df.head(20))
```

**Alternative**: If you need the full `ForecastResult` with deposit schedule and metadata, use `run_payments_forecast()` directly:

```python
from pathlib import Path
import pandas as pd
from pos_core.etl import PaymentsETLConfig, get_payments
from pos_core.forecasting import ForecastConfig, run_payments_forecast

# Get historical data
config = PaymentsETLConfig.from_root(Path("data"), Path("utils/sucursales.json"))
payments_df = get_payments("2022-01-01", "2025-11-24", config)

# Ensure fecha is datetime
payments_df['fecha'] = pd.to_datetime(payments_df['fecha'])

# Configure forecast
forecast_config = ForecastConfig(
    horizon_days=7,  # Forecast next 7 days
    metrics=[
        "ingreso_efectivo",
        "ingreso_credito",
        "ingreso_debito",
        "ingreso_total"
    ],
    branches=None  # Forecast for all branches (or specify: ["Kavia", "QIN"])
)

# Run forecast
result = run_payments_forecast(payments_df, config=forecast_config)

# Access forecast results
print("Forecast DataFrame:")
print(result.forecast.head(20))

# The forecast DataFrame has columns:
# - sucursal: Branch name
# - fecha: Forecast date
# - metric: Metric name (ingreso_efectivo, ingreso_credito, etc.)
# - valor: Forecasted value

# Access deposit schedule (cash flow view)
print("\nDeposit Schedule:")
print(result.deposit_schedule)

# The deposit schedule has columns:
# - fecha: Deposit date
# - efectivo: Total cash deposits
# - credito: Total credit card deposits
# - debito: Total debit card deposits
# - total: Total deposits

# Access metadata
print(f"\nForecast Metadata:")
print(f"Branches: {result.metadata['branches']}")
print(f"Metrics: {result.metadata['metrics']}")
print(f"Horizon: {result.metadata['horizon_days']} days")
print(f"Last historical date: {result.metadata['last_historical_date']}")
print(f"Successful forecasts: {result.metadata['successful_forecasts']}")
print(f"Failed forecasts: {result.metadata['failed_forecasts']}")

# Example: Get forecast for a specific branch and metric
banana_cash = result.forecast[
    (result.forecast['sucursal'] == 'Banana') &
    (result.forecast['metric'] == 'ingreso_efectivo')
]
print(f"\nBanana cash forecast (next 7 days):")
print(banana_cash[['fecha', 'valor']])

# Example: Pivot forecast for easier viewing
forecast_pivot = result.forecast.pivot_table(
    index=['sucursal', 'fecha'],
    columns='metric',
    values='valor'
)
print(f"\nForecast Pivot (first 10 rows):")
print(forecast_pivot.head(10))

# Save results
forecast_output = data_root / "c_processed" / "forecasts" / "next_7_days_forecast.csv"
forecast_output.parent.mkdir(parents=True, exist_ok=True)
result.forecast.to_csv(forecast_output, index=False)
print(f"\nSaved forecast to: {forecast_output}")

deposit_output = data_root / "c_processed" / "forecasts" / "next_7_days_deposits.csv"
result.deposit_schedule.to_csv(deposit_output, index=False)
print(f"Saved deposit schedule to: {deposit_output}")
```

## API Reference

### ETL Module (`pos_core.etl`)

#### Configuration
- **`PaymentsETLConfig`**, **`PaymentsPaths`**: Configuration for payments ETL pipeline
- **`SalesETLConfig`**, **`SalesPaths`**: Configuration for sales ETL pipeline

#### Query Functions (Recommended)
- **`get_payments()`**: Get payments data, running ETL stages only if needed
- **`get_sales()`**: Get sales data at specified level (ticket/group/day), running ETL stages only if needed
- **`get_payments_forecast()`**: Get payments forecast, automatically handling historical data retrieval

#### Stage Functions (Fine-Grained Control)
- **Payments**: `download_payments()`, `clean_payments()`, `aggregate_payments()`
- **Sales**: `download_sales()`, `clean_sales()`, `aggregate_sales()`

#### Orchestration Functions
- **`build_payments_dataset()`**: Complete payments ETL orchestration (uses stage functions internally)

See [`src/pos_core/etl/api.py`](src/pos_core/etl/api.py) and [`src/pos_core/etl/queries.py`](src/pos_core/etl/queries.py) when viewing this repo for detailed API documentation.

### Forecasting Module (`pos_core.forecasting`)

- **`ForecastConfig`**: Configuration for forecasting (horizon_days, metrics, branches)
- **`ForecastResult`**: Result dataclass containing forecast DataFrame, deposit schedule, and metadata
- **`run_payments_forecast()`**: Main forecasting function

See [`src/pos_core/forecasting/api.py`](src/pos_core/forecasting/api.py) when viewing this repo for detailed API documentation.

### QA Module (`pos_core.qa`)

- **`PaymentsQAResult`**: Result dataclass with QA summary and detailed findings
- **`run_payments_qa()`**: Main QA function for data validation

See [`src/pos_core/qa/api.py`](src/pos_core/qa/api.py) when viewing this repo for detailed API documentation.

### Exceptions Module (`pos_core.exceptions`)

- **`PosAPIError`**: Base exception for all POS Core ETL errors
- **`ConfigError`**: Raised for configuration errors
- **`DataQualityError`**: Raised for data quality validation failures

See [`src/pos_core/exceptions.py`](src/pos_core/exceptions.py) when viewing this repo for detailed exception documentation.

## API Stability

**Public APIs**: Functions and dataclasses documented in the "API Reference" section above are considered stable and will follow semantic versioning. These include:

- All exports from `pos_core.etl`, `pos_core.forecasting`, and `pos_core.qa` modules (as defined in their `__all__` attributes)
- Exception classes in `pos_core.exceptions`
- The `__version__` export from `pos_core`

**Internal APIs**: Other modules, functions, and classes not explicitly exported are considered internal and may change between minor versions without notice. This includes:

- Functions in `pos_core.etl.a_extract`, `pos_core.etl.b_transform`, `pos_core.etl.c_load`
- Internal helper functions and utilities
- Any module or function not listed in a package's `__all__` attribute

When in doubt, only use the documented public APIs. If you need access to internal functionality, please open an issue to discuss making it part of the public API.

## Configuration

### Payments ETL Configuration

```python
from pathlib import Path
from pos_core.etl import PaymentsETLConfig

# Default configuration using standard directory structure
config = PaymentsETLConfig.from_root(
    data_root=Path("data"),
    sucursales_file=Path("utils/sucursales.json"),
    chunk_size_days=180  # Maximum days per HTTP request
)

# Alternative: using from_data_root (alias)
config = PaymentsETLConfig.from_data_root(
    data_root=Path("data"),
    sucursales_json=Path("utils/sucursales.json"),
    chunk_size_days=180
)

# Custom configuration
from pos_core.etl import PaymentsPaths

custom_paths = PaymentsPaths(
    raw_payments=Path("custom/raw"),
    clean_payments=Path("custom/clean"),
    proc_payments=Path("custom/processed"),
    sucursales_json=Path("custom/sucursales.json")
)

config = PaymentsETLConfig(
    paths=custom_paths,
    chunk_size_days=90,
    excluded_branches=["CEDIS"]  # Branches to exclude from processing
)
```

### Sales ETL Configuration

```python
from pathlib import Path
from pos_core.etl import SalesETLConfig

# Default configuration using standard directory structure
config = SalesETLConfig.from_root(
    data_root=Path("data"),
    sucursales_file=Path("utils/sucursales.json")
)
```

### Forecasting Configuration

```python
from pos_core.forecasting import ForecastConfig

# Default configuration (7 days, all metrics, all branches)
config = ForecastConfig()

# Custom configuration
config = ForecastConfig(
    horizon_days=14,  # Forecast next 14 days
    metrics=["ingreso_efectivo", "ingreso_total"],  # Only cash and total
    branches=["Banana", "Queen"]  # Only specific branches
)
```

## Data Formats

### Date Formats

All dates should be in `YYYY-MM-DD` format (e.g., `"2025-01-15"`).

### Payments DataFrame

The aggregated payments DataFrame contains:
- `sucursal` (str): Branch name
- `fecha` (date/datetime): Date of the record. Note: `fecha` is parsed as datetime when you pass the DataFrame into `run_payments_forecast()` (or do `pd.to_datetime()` yourself, as shown in Example 3).
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

For issues, questions, or contributions, please refer to the source code documentation when viewing this repo:
- [`src/pos_core/etl/api.py`](src/pos_core/etl/api.py)
- [`src/pos_core/forecasting/api.py`](src/pos_core/forecasting/api.py)
- [`src/pos_core/qa/api.py`](src/pos_core/qa/api.py)

