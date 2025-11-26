# Configuration

## Branch Configuration (sucursales.json)

The `sucursales.json` file maps branch names to codes and tracks validity windows.

**Default location**: `utils/sucursales.json` (relative to your data root's parent directory)

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
  }
}
```

### Fields

- `code`: Branch code used by the POS system
- `valid_from`: Date when this code became active (YYYY-MM-DD format)
- `valid_to`: Date when this code became inactive (null if still active)

## Environment Variables

Required for online extraction (downloading data from POS API):

- `WS_BASE` (required): Base URL of your POS instance
- `WS_USER` (optional): Username for authentication
- `WS_PASS` (optional): Password for authentication

**Note**: These are only needed if you're downloading data from the POS API. If you only work with already-downloaded files, you can ignore these.

## Directory Structure

The package follows an ETL naming convention:

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

## ETL Configuration

### Payments ETL Configuration

```python
from pathlib import Path
from pos_core.etl import PaymentsETLConfig

# Default configuration using from_root (recommended)
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

# Default configuration
config = SalesETLConfig.from_root(
    data_root=Path("data"),
    sucursales_file=Path("utils/sucursales.json")
)

# Custom configuration
from pos_core.etl import SalesPaths

custom_paths = SalesPaths(
    raw_sales=Path("custom/raw"),
    clean_sales=Path("custom/clean"),
    proc_sales=Path("custom/processed"),
    sucursales_json=Path("custom/sucursales.json")
)

config = SalesETLConfig(
    paths=custom_paths,
    chunk_days=90  # Only if chunking is needed
)
```

## Forecasting Configuration

```python
from pos_core.forecasting import ForecastConfig

# Default (7 days, all metrics, all branches)
config = ForecastConfig()

# Custom configuration
config = ForecastConfig(
    horizon_days=14,
    metrics=["ingreso_efectivo", "ingreso_total"],
    branches=["Banana", "Queen"]
)
```
