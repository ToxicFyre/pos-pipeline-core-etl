# Configuration

This guide covers all configuration options for POS Core ETL, including branch configuration, environment variables, and data paths.

## Table of Contents

- [Branch Configuration](#branch-configuration-sucursalesjson)
- [Environment Variables](#environment-variables)
- [Data Paths Configuration](#data-paths-configuration)
- [Forecasting Configuration](#forecasting-configuration)

## Branch Configuration (sucursales.json)

The `sucursales.json` file maps branch names to codes and tracks validity windows for branches that change codes over time.

### File Location

**Default location**: `utils/sucursales.json` (relative to your project root)

You can specify a custom location when creating `DataPaths`:

```python
from pathlib import Path
from pos_core import DataPaths

paths = DataPaths.from_root(
    Path("data"),
    Path("custom/path/sucursales.json")  # Custom location
)
```

### File Structure

**Example structure**:

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

### Fields

- **`code`** (string, required): Branch code used by the POS system (e.g., "8888")
- **`valid_from`** (string, required): Date when this code became active (YYYY-MM-DD format)
- **`valid_to`** (string | null): Date when this code became inactive (null if still active)

### Branch Code Windows

Branches may change their codes over time. The package handles this through validity windows:

```json
{
  "MyBranch": {
    "code": "5678",
    "valid_from": "2024-06-01",
    "valid_to": null
  },
  "MyBranch_OLD": {
    "code": "1234",
    "valid_from": "2020-01-01",
    "valid_to": "2024-05-31"
  }
}
```

The `BranchRegistry` automatically resolves the correct code for a given date:

```python
from pos_core.branches import BranchRegistry

registry = BranchRegistry(paths)

# Get code for a specific date
code = registry.get_code_for_date("MyBranch", "2023-01-15")  # Returns "1234"
code = registry.get_code_for_date("MyBranch", "2024-07-01")  # Returns "5678"
```

## Environment Variables

### Required for Extraction

**Required for downloading raw data** from the POS API:

- **`WS_BASE`** (required): Base URL of your POS instance
- **`WS_USER`** (required): Username for authentication
- **`WS_PASS`** (required): Password for authentication

**Example (bash)**:
```bash
export WS_BASE="https://your-pos-instance.com"
export WS_USER="your_username"
export WS_PASS="your_password"
```

**Example (PowerShell)**:
```powershell
$env:WS_BASE = "https://your-pos-instance.com"
$env:WS_USER = "your_username"
$env:WS_PASS = "your_password"
```

**Example (Python)**:
```python
import os
os.environ["WS_BASE"] = "https://your-pos-instance.com"
os.environ["WS_USER"] = "your_username"
os.environ["WS_PASS"] = "your_password"
```

### When Environment Variables Are Not Needed

If you only work with already-downloaded files in `a_raw/`, these environment variables are **not needed**. The package will skip extraction and work directly with existing raw data files.

### Security Best Practices

**Never commit secrets or sensitive data to version control.**

- Store environment variables in `.env` files (add to `.gitignore`)
- Use environment variable management tools in production
- Never hardcode credentials in your code

## Data Paths Configuration

### Default Configuration

The simplest way to configure paths is using `DataPaths.from_root()`:

```python
from pathlib import Path
from pos_core import DataPaths

paths = DataPaths.from_root(
    data_root=Path("data"),
    sucursales_file=Path("utils/sucursales.json")
)
```

This creates a standard directory structure:

```
data/
├── a_raw/          # Bronze: Raw Wansoft exports
│   ├── payments/
│   └── sales/
├── b_clean/        # Silver: Core facts
│   ├── payments/
│   └── sales/
└── c_processed/    # Gold: Marts
    ├── payments/
    └── sales/
```

### Custom Paths

For advanced use cases, you can create custom paths:

```python
from pathlib import Path
from pos_core import DataPaths

# Create custom DataPaths
paths = DataPaths(
    raw_payments=Path("custom/raw/payments"),
    raw_sales=Path("custom/raw/sales"),
    clean_payments=Path("custom/clean/payments"),
    clean_sales=Path("custom/clean/sales"),
    mart_payments=Path("custom/marts/payments"),
    mart_sales=Path("custom/marts/sales"),
    sucursales_json=Path("custom/sucursales.json")
)
```

### DataPaths Attributes

The `DataPaths` object provides access to all path configurations:

- `raw_payments` (Path): Directory for raw payment Excel files
- `raw_sales` (Path): Directory for raw sales Excel files
- `clean_payments` (Path): Directory for cleaned payment CSV files
- `clean_sales` (Path): Directory for cleaned sales CSV files
- `mart_payments` (Path): Directory for payment marts
- `mart_sales` (Path): Directory for sales marts
- `sucursales_json` (Path): Path to `sucursales.json` file

### Ensuring Directories Exist

The `DataPaths` object can automatically create directories:

```python
paths.ensure_dirs()  # Creates all directories if they don't exist
```

This is automatically called by `fetch()` functions, but you can call it manually if needed.

## Directory Structure

The package follows a bronze/silver/gold data layer convention:

```
data/
├── a_raw/          # Bronze: Raw Wansoft exports (Excel files)
│   ├── payments/
│   │   └── batch/  # Date-partitioned raw files
│   └── sales/
│       └── batch/  # Date-partitioned raw files
├── b_clean/        # Silver: Core facts at atomic grain (CSV files)
│   ├── payments/
│   │   └── batch/  # Date-partitioned core fact files
│   └── sales/
│       └── batch/  # Date-partitioned core fact files
└── c_processed/    # Gold: Marts (aggregated tables)
    ├── payments/
    │   └── _meta/  # Metadata for marts
    └── sales/
        └── _meta/  # Metadata for marts
```

### Metadata Files

Metadata files are stored in `_meta/` subdirectories to track ETL stage completion:

- `a_raw/payments/batch/_meta/2025-01-01_2025-01-31.json`
- `b_clean/payments/batch/_meta/2025-01-01_2025-01-31.json`
- `c_processed/payments/_meta/2025-01-01_2025-01-31.json`

These files enable idempotent operations - the package automatically skips work that's already been done.

## Forecasting Configuration

### ForecastConfig

Configure forecasting behavior:

```python
from pos_core.forecasting import ForecastConfig

# Default configuration (7 days, all metrics, all branches)
config = ForecastConfig()

# Custom configuration
config = ForecastConfig(
    horizon_days=91,  # 13 weeks
    metrics=["ingreso_efectivo", "ingreso_total"],  # Specific metrics
    branches=["Banana", "Queen"]  # Specific branches
)
```

### Parameters

- **`horizon_days`** (int, default: 7): Number of days ahead to forecast
- **`metrics`** (list[str], default: all available): List of metrics to forecast
  - Common metrics: `"ingreso_efectivo"`, `"ingreso_credito"`, `"ingreso_debito"`, `"ingreso_total"`
- **`branches`** (list[str] | None, default: None): List of branch names to forecast
  - If None, forecasts all branches found in the data

### Example

```python
from pos_core.forecasting import ForecastConfig, run_payments_forecast
from pos_core.payments import marts as payments_marts

# Get historical data
payments_df = payments_marts.fetch_daily(paths, "2022-01-01", "2025-01-31")

# Configure forecast
config = ForecastConfig(
    horizon_days=91,  # 13 weeks
    metrics=["ingreso_efectivo", "ingreso_total"]
)

# Run forecast
result = run_payments_forecast(payments_df, config)
```

## QA Configuration

### QA Levels

The QA module supports different levels of checks:

```python
from pos_core.qa import run_payments_qa

# Level 0: Schema validation (always run)
result = run_payments_qa(df, level=0)

# Level 3: Missing and duplicate days
result = run_payments_qa(df, level=3)

# Level 4: Statistical anomalies (z-score) - default
result = run_payments_qa(df, level=4)
```

**QA Levels:**
- **Level 0**: Schema validation (always run)
- **Level 3**: Missing days and duplicate detection
- **Level 4**: Statistical anomalies (z-score analysis)

## Next Steps

- **[Quickstart](quickstart.md)** - Get started with a working example
- **[Concepts](concepts.md)** - Understand data layers, grains, and API design
- **[Examples](examples.md)** - Complete runnable example scripts
- **[API Reference](../api-reference/etl.md)** - Detailed function documentation
