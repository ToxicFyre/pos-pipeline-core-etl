# ETL API Reference

## Configuration

### `PaymentsETLConfig`

Configuration dataclass for payments ETL pipeline.

#### Attributes

- `paths` (PaymentsPaths): All filesystem paths used by the pipeline
- `chunk_size_days` (int): Maximum number of days per HTTP request chunk (default: 180)
- `excluded_branches` (List[str]): List of branch names to exclude from processing (default: ["CEDIS"])

#### Methods

##### `from_data_root()`

Build a default config given a data_root.

```python
config = PaymentsETLConfig.from_data_root(
    data_root=Path("data"),
    sucursales_json=Path("utils/sucursales.json"),
    chunk_size_days=180
)
```

##### `from_root()`

Alias for `from_data_root()` for consistency with `SalesETLConfig`.

```python
config = PaymentsETLConfig.from_root(
    data_root=Path("data"),
    sucursales_file=Path("utils/sucursales.json"),
    chunk_size_days=180
)
```

### `PaymentsPaths`

Path configuration for ETL stages.

#### Attributes

- `raw_payments` (Path): Directory for raw payment Excel files
- `clean_payments` (Path): Directory for cleaned payment CSV files
- `proc_payments` (Path): Directory for processed/aggregated payment data
- `sucursales_json` (Path): Path to sucursales.json configuration file

### `SalesETLConfig`

Configuration dataclass for sales ETL pipeline.

#### Attributes

- `paths` (SalesPaths): All filesystem paths used by the pipeline
- `chunk_days` (int): Maximum number of days per HTTP request chunk (default: 180)

#### Methods

##### `from_root()`

Build a default config given a data_root and sucursales file.

```python
config = SalesETLConfig.from_root(
    data_root=Path("data"),
    sucursales_file=Path("utils/sucursales.json")
)
```

### `SalesPaths`

Path configuration for sales ETL stages.

#### Attributes

- `raw_sales` (Path): Directory for raw sales Excel files
- `clean_sales` (Path): Directory for cleaned sales CSV files
- `proc_sales` (Path): Directory for processed/aggregated sales data
- `sucursales_json` (Path): Path to sucursales.json configuration file

## Query Functions

Query functions are the recommended way to get data. They automatically run ETL stages only when needed based on metadata.

### `get_payments()`

Get payments data, running stages only if needed.

#### Signature

```python
def get_payments(
    start_date: str,
    end_date: str,
    config: PaymentsETLConfig,
    branches: Optional[List[str]] = None,
    refresh: bool = False,
) -> pd.DataFrame
```

#### Parameters

- `start_date` (str): Start date in YYYY-MM-DD format (inclusive)
- `end_date` (str): End date in YYYY-MM-DD format (inclusive)
- `config` (PaymentsETLConfig): Configuration instance
- `branches` (Optional[List[str]]): List of branch names to process. If None, processes all branches.
- `refresh` (bool): If True, force re-run all stages. If False, check metadata and skip completed stages.

#### Returns

DataFrame containing aggregated payments data (one row per sucursal + fecha).

#### Example

```python
from pos_core.etl import PaymentsETLConfig, get_payments

config = PaymentsETLConfig.from_root(Path("data"), Path("utils/sucursales.json"))
df = get_payments("2025-01-01", "2025-01-31", config, refresh=False)
```

### `get_sales()`

Get sales data at the specified level, running stages only if needed.

#### Signature

```python
def get_sales(
    start_date: str,
    end_date: str,
    config: SalesETLConfig,
    branches: Optional[List[str]] = None,
    level: str = "ticket",  # "ticket" | "group" | "day"
    refresh: bool = False,
) -> pd.DataFrame
```

#### Parameters

- `start_date` (str): Start date in YYYY-MM-DD format (inclusive)
- `end_date` (str): End date in YYYY-MM-DD format (inclusive)
- `config` (SalesETLConfig): Configuration instance
- `branches` (Optional[List[str]]): List of branch names to process. If None, processes all branches.
- `level` (str): Aggregation level: "ticket", "group", or "day" (default: "ticket")
- `refresh` (bool): If True, force re-run all stages. If False, check metadata and skip completed stages.

#### Returns

DataFrame containing aggregated sales data at the specified level.

#### Example

```python
from pos_core.etl import SalesETLConfig, get_sales

config = SalesETLConfig.from_root(Path("data"), Path("utils/sucursales.json"))
df_ticket = get_sales("2025-01-01", "2025-01-31", config, level="ticket")
df_group = get_sales("2025-01-01", "2025-01-31", config, level="group")
```

### `get_payments_forecast()`

Get payments forecast for the specified horizon.

#### Signature

```python
def get_payments_forecast(
    as_of: str,  # Date string
    horizon_weeks: int,
    config: PaymentsETLConfig,
    refresh: bool = False,
) -> pd.DataFrame
```

#### Parameters

- `as_of` (str): Date string in YYYY-MM-DD format (forecast as of this date)
- `horizon_weeks` (int): Number of weeks to forecast ahead
- `config` (PaymentsETLConfig): Configuration instance
- `refresh` (bool): If True, force re-run ETL stages before forecasting. If False, use existing data if available.

#### Returns

DataFrame containing forecast results with columns: `sucursal`, `fecha`, `metric`, `valor`.

#### Example

```python
from pos_core.etl import PaymentsETLConfig, get_payments_forecast

config = PaymentsETLConfig.from_root(Path("data"), Path("utils/sucursales.json"))
forecast = get_payments_forecast("2025-11-24", horizon_weeks=13, config=config)
```

## Stage Functions

Stage functions provide fine-grained control over individual ETL stages. They are used internally by query functions and can be called directly for advanced use cases.

### Payments Stage Functions

#### `download_payments()`

Download raw payments Excel for the given range.

```python
def download_payments(
    start_date: str,
    end_date: str,
    config: PaymentsETLConfig,
    branches: Optional[List[str]] = None,
    force: bool = False,
) -> None
```

#### `clean_payments()`

Transform raw payments files into clean CSV/Parquet.

```python
def clean_payments(
    start_date: str,
    end_date: str,
    config: PaymentsETLConfig,
    branches: Optional[List[str]] = None,
    force: bool = False,
) -> None
```

#### `aggregate_payments()`

Aggregate clean payments into the canonical dataset and return it.

```python
def aggregate_payments(
    start_date: str,
    end_date: str,
    config: PaymentsETLConfig,
    branches: Optional[List[str]] = None,
    force: bool = False,
) -> pd.DataFrame
```

### Sales Stage Functions

#### `download_sales()`

Download raw sales Excel for the given range.

```python
def download_sales(
    start_date: str,
    end_date: str,
    config: SalesETLConfig,
    branches: Optional[List[str]] = None,
    force: bool = False,
) -> None
```

#### `clean_sales()`

Transform raw sales files into clean CSV.

```python
def clean_sales(
    start_date: str,
    end_date: str,
    config: SalesETLConfig,
    branches: Optional[List[str]] = None,
    force: bool = False,
) -> None
```

#### `aggregate_sales()`

Aggregate clean sales at the specified level.

```python
def aggregate_sales(
    start_date: str,
    end_date: str,
    config: SalesETLConfig,
    level: str = "ticket",  # "ticket" | "group" | "day"
    branches: Optional[List[str]] = None,
    force: bool = False,
) -> pd.DataFrame
```

## Orchestration Functions

### `build_payments_dataset()`

Main orchestration function for payments ETL. This is a thin wrapper around the stage functions.

#### Signature

```python
def build_payments_dataset(
    start_date: str,
    end_date: str,
    config: PaymentsETLConfig,
    branches: Optional[List[str]] = None,
    steps: Optional[List[str]] = None,
) -> pd.DataFrame
```

#### Parameters

- `start_date` (str): Start date in YYYY-MM-DD format (inclusive)
- `end_date` (str): End date in YYYY-MM-DD format (inclusive)
- `config` (PaymentsETLConfig): Configuration instance
- `branches` (Optional[List[str]]): List of branch names to process. If None, processes all branches.
- `steps` (Optional[List[str]]): List of steps to execute. Valid steps: "extract", "transform", "aggregate". If None, executes all steps.

#### Returns

DataFrame containing the aggregated payments data (one row per sucursal + fecha).

#### Raises

- `ConfigError`: If invalid step names are provided
- `FileNotFoundError`: If the aggregated file is expected but missing

#### Example

```python
from pathlib import Path
from pos_core.etl import PaymentsETLConfig, build_payments_dataset

config = PaymentsETLConfig.from_data_root(Path("data"))
df = build_payments_dataset("2023-01-01", "2023-12-31", config)
```

**Note**: For most use cases, `get_payments()` is recommended as it provides automatic idempotence through metadata checks.
