# ETL API Reference

## `PaymentsETLConfig`

Configuration dataclass for payments ETL pipeline.

### Attributes

- `paths` (PaymentsPaths): All filesystem paths used by the pipeline
- `chunk_size_days` (int): Maximum number of days per HTTP request chunk (default: 180)
- `excluded_branches` (List[str]): List of branch names to exclude from processing (default: ["CEDIS"])

### Methods

#### `from_data_root()`

Build a default config given a data_root.

```python
config = PaymentsETLConfig.from_data_root(
    data_root=Path("data"),
    sucursales_json=Path("utils/sucursales.json"),
    chunk_size_days=180
)
```

## `PaymentsPaths`

Path configuration for ETL stages.

### Attributes

- `raw_payments` (Path): Directory for raw payment Excel files
- `clean_payments` (Path): Directory for cleaned payment CSV files
- `proc_payments` (Path): Directory for processed/aggregated payment data
- `sucursales_json` (Path): Path to sucursales.json configuration file

## `build_payments_dataset()`

Main orchestration function for payments ETL.

### Signature

```python
def build_payments_dataset(
    start_date: str,
    end_date: str,
    config: PaymentsETLConfig,
    branches: Optional[List[str]] = None,
    steps: Optional[List[str]] = None,
) -> pd.DataFrame
```

### Parameters

- `start_date` (str): Start date in YYYY-MM-DD format (inclusive)
- `end_date` (str): End date in YYYY-MM-DD format (inclusive)
- `config` (PaymentsETLConfig): Configuration instance
- `branches` (Optional[List[str]]): List of branch names to process. If None, processes all branches.
- `steps` (Optional[List[str]]): List of steps to execute. Valid steps: "extract", "transform", "aggregate". If None, executes all steps.

### Returns

DataFrame containing the aggregated payments data (one row per sucursal + fecha).

### Raises

- `ConfigError`: If invalid step names are provided
- `FileNotFoundError`: If the aggregated file is expected but missing

### Example

```python
from pathlib import Path
from pos_core.etl import PaymentsETLConfig, build_payments_dataset

config = PaymentsETLConfig.from_data_root(Path("data"))
df = build_payments_dataset("2023-01-01", "2023-12-31", config)
```

