# ETL API Reference

This page documents the ETL API for POS Core ETL. The API is organized by domain (payments, sales) and layer (raw, core, marts).

## Configuration

### `DataPaths`

Configuration class for data directory paths.

#### Methods

##### `from_root()`

Create a DataPaths instance from root directories.

```python
from pathlib import Path
from pos_core import DataPaths

paths = DataPaths.from_root(
    data_root=Path("data"),
    sucursales_file=Path("utils/sucursales.json")
)
```

**Parameters:**
- `data_root` (Path): Root directory for data (contains `a_raw/`, `b_clean/`, `c_processed/`)
- `sucursales_file` (Path): Path to `sucursales.json` configuration file

**Returns:** DataPaths instance

#### Attributes

- `raw_payments` (Path): Directory for raw payment Excel files (`a_raw/payments/`)
- `raw_sales` (Path): Directory for raw sales Excel files (`a_raw/sales/`)
- `raw_order_times` (Path): Directory for raw order times Excel files (`a_raw/order_times/`)
- `clean_payments` (Path): Directory for cleaned payment CSV files (`b_clean/payments/`)
- `clean_sales` (Path): Directory for cleaned sales CSV files (`b_clean/sales/`)
- `clean_order_times` (Path): Directory for cleaned order times CSV files (`b_clean/order_times/`)
- `mart_payments` (Path): Directory for payment marts (`c_processed/payments/`)
- `mart_sales` (Path): Directory for sales marts (`c_processed/sales/`)
- `mart_order_times` (Path): Directory for order times marts (`c_processed/order_times/`)
- `sucursales_json` (Path): Path to `sucursales.json` file

## Payments API

### Core Fact (Silver Layer)

#### `payments.core.fetch()`

Ensure `fact_payments_ticket` exists for the given range, then return it.

**Signature:**

```python
from pos_core.payments import core

df = core.fetch(
    paths: DataPaths,
    start_date: str,
    end_date: str,
    branches: list[str] | None = None,
    *,
    mode: str = "missing",
) -> pd.DataFrame
```

**Parameters:**
- `paths` (DataPaths): DataPaths configuration
- `start_date` (str): Start date in YYYY-MM-DD format (inclusive)
- `end_date` (str): End date in YYYY-MM-DD format (inclusive)
- `branches` (list[str] | None): Optional list of branch names to filter
- `mode` (str): Processing mode - `"missing"` (default) or `"force"`

**Returns:** DataFrame with `fact_payments_ticket` structure (ticket × payment method grain)

**Raises:**
- `ValueError`: If mode is not `"missing"` or `"force"`

**Example:**

```python
from pos_core import DataPaths
from pos_core.payments import core
from pathlib import Path

paths = DataPaths.from_root(Path("data"), Path("utils/sucursales.json"))
df = core.fetch(paths, "2025-01-01", "2025-01-31")
```

#### `payments.core.load()`

Load `fact_payments_ticket` from disk without running ETL.

**Signature:**

```python
df = core.load(
    paths: DataPaths,
    start_date: str,
    end_date: str,
    branches: list[str] | None = None,
) -> pd.DataFrame
```

**Parameters:** Same as `fetch()`, except no `mode` parameter

**Returns:** DataFrame with `fact_payments_ticket` structure

**Raises:**
- `FileNotFoundError`: If the data doesn't exist

**Example:**

```python
# Read existing data only (faster, but requires data to exist)
df = core.load(paths, "2025-01-01", "2025-01-31")
```

### Daily Mart (Gold Layer)

#### `payments.marts.fetch_daily()`

Ensure the daily payments mart exists for the range, then return it.

**Signature:**

```python
from pos_core.payments import marts

df = marts.fetch_daily(
    paths: DataPaths,
    start_date: str,
    end_date: str,
    branches: list[str] | None = None,
    *,
    mode: str = "missing",
) -> pd.DataFrame
```

**Parameters:**
- `paths` (DataPaths): DataPaths configuration
- `start_date` (str): Start date in YYYY-MM-DD format (inclusive)
- `end_date` (str): End date in YYYY-MM-DD format (inclusive)
- `branches` (list[str] | None): Optional list of branch names to filter
- `mode` (str): Processing mode - `"missing"` (default) or `"force"`

**Returns:** DataFrame with `mart_payments_daily` structure (sucursal × date grain)

**Example:**

```python
from pos_core.payments import marts

# Most common use case: get daily aggregations
df = marts.fetch_daily(paths, "2025-01-01", "2025-01-31")

# Force refresh
df = marts.fetch_daily(paths, "2025-01-01", "2025-01-31", mode="force")

# Filter by branches
df = marts.fetch_daily(paths, "2025-01-01", "2025-01-31", branches=["Banana", "Queen"])
```

#### `payments.marts.load_daily()`

Load daily payments mart from disk without running ETL.

**Signature:**

```python
df = marts.load_daily(
    paths: DataPaths,
    start_date: str,
    end_date: str,
    branches: list[str] | None = None,
) -> pd.DataFrame
```

**Parameters:** Same as `fetch_daily()`, except no `mode` parameter

**Returns:** DataFrame with `mart_payments_daily` structure

**Raises:**
- `FileNotFoundError`: If the mart doesn't exist

### Raw Data (Bronze Layer)

#### `payments.raw.fetch()`

Download raw payment Excel files from the POS system.

**Signature:**

```python
from pos_core.payments import raw

raw.fetch(
    paths: DataPaths,
    start_date: str,
    end_date: str,
    branches: list[str] | None = None,
    *,
    mode: str = "missing",
) -> None
```

**Parameters:**
- `paths` (DataPaths): DataPaths configuration
- `start_date` (str): Start date in YYYY-MM-DD format (inclusive)
- `end_date` (str): End date in YYYY-MM-DD format (inclusive)
- `branches` (list[str] | None): Optional list of branch names to filter
- `mode` (str): Processing mode - `"missing"` (default) or `"force"`

**Note:** Requires `WS_BASE`, `WS_USER`, and `WS_PASS` environment variables to be set.

#### `payments.raw.load()`

Load raw payment Excel files from disk.

**Signature:**

```python
df = raw.load(
    paths: DataPaths,
    start_date: str,
    end_date: str,
    branches: list[str] | None = None,
) -> pd.DataFrame
```

## Sales API

### Core Fact (Silver Layer)

#### `sales.core.fetch()`

Ensure `fact_sales_item_line` exists for the given range, then return it.

**Signature:**

```python
from pos_core.sales import core

df = core.fetch(
    paths: DataPaths,
    start_date: str,
    end_date: str,
    branches: list[str] | None = None,
    *,
    mode: str = "missing",
) -> pd.DataFrame
```

**Parameters:**
- `paths` (DataPaths): DataPaths configuration
- `start_date` (str): Start date in YYYY-MM-DD format (inclusive)
- `end_date` (str): End date in YYYY-MM-DD format (inclusive)
- `branches` (list[str] | None): Optional list of branch names to filter
- `mode` (str): Processing mode - `"missing"` (default) or `"force"`

**Returns:** DataFrame with `fact_sales_item_line` structure (item/modifier line grain)

**Example:**

```python
from pos_core.sales import core

df = core.fetch(paths, "2025-01-01", "2025-01-31")
```

#### `sales.core.load()`

Load `fact_sales_item_line` from disk without running ETL.

**Signature:**

```python
df = core.load(
    paths: DataPaths,
    start_date: str,
    end_date: str,
    branches: list[str] | None = None,
) -> pd.DataFrame
```

### Ticket Mart (Gold Layer)

#### `sales.marts.fetch_ticket()`

Ensure the ticket-level sales mart exists for the range, then return it.

**Signature:**

```python
from pos_core.sales import marts

df = marts.fetch_ticket(
    paths: DataPaths,
    start_date: str,
    end_date: str,
    branches: list[str] | None = None,
    *,
    mode: str = "missing",
) -> pd.DataFrame
```

**Parameters:** Same as `sales.core.fetch()`

**Returns:** DataFrame with `mart_sales_by_ticket` structure (one row per ticket)

**Example:**

```python
df = marts.fetch_ticket(paths, "2025-01-01", "2025-01-31")
```

#### `sales.marts.load_ticket()`

Load ticket-level sales mart from disk without running ETL.

**Signature:**

```python
df = marts.load_ticket(
    paths: DataPaths,
    start_date: str,
    end_date: str,
    branches: list[str] | None = None,
) -> pd.DataFrame
```

### Group Mart (Gold Layer)

#### `sales.marts.fetch_group()`

Ensure the group-level sales mart exists for the range, then return it.

**Signature:**

```python
df = marts.fetch_group(
    paths: DataPaths,
    start_date: str,
    end_date: str,
    branches: list[str] | None = None,
    *,
    mode: str = "missing",
) -> pd.DataFrame
```

**Parameters:** Same as `sales.core.fetch()`

**Returns:** DataFrame with `mart_sales_by_group` structure (category pivot table)

**Example:**

```python
df = marts.fetch_group(paths, "2025-01-01", "2025-01-31")
```

#### `sales.marts.load_group()`

Load group-level sales mart from disk without running ETL.

**Signature:**

```python
df = marts.load_group(
    paths: DataPaths,
    start_date: str,
    end_date: str,
    branches: list[str] | None = None,
) -> pd.DataFrame
```

### Raw Data (Bronze Layer)

#### `sales.raw.fetch()`

Download raw sales Excel files from the POS system.

**Signature:**

```python
from pos_core.sales import raw

raw.fetch(
    paths: DataPaths,
    start_date: str,
    end_date: str,
    branches: list[str] | None = None,
    *,
    mode: str = "missing",
) -> None
```

**Note:** Requires `WS_BASE`, `WS_USER`, and `WS_PASS` environment variables to be set.

#### `sales.raw.load()`

Load raw sales Excel files from disk.

**Signature:**

```python
df = raw.load(
    paths: DataPaths,
    start_date: str,
    end_date: str,
    branches: list[str] | None = None,
) -> pd.DataFrame
```

## Order Times API

### Raw Data (Bronze Layer)

#### `order_times.raw.fetch()`

Download raw order times Excel files from the POS system.

**Signature:**

```python
from pos_core.order_times import raw

raw.fetch(
    paths: DataPaths,
    start_date: str,
    end_date: str,
    branches: list[str] | None = None,
    *,
    mode: str = "missing",
) -> None
```

**Parameters:**
- `paths` (DataPaths): DataPaths configuration
- `start_date` (str): Start date in YYYY-MM-DD format (inclusive)
- `end_date` (str): End date in YYYY-MM-DD format (inclusive)
- `branches` (list[str] | None): Optional list of branch names to filter
- `mode` (str): Processing mode - `"missing"` (default) or `"force"`

**Note:** Requires `WS_BASE`, `WS_USER`, and `WS_PASS` environment variables to be set.

**Example:**

```python
from pos_core.order_times import raw

# Download order times for a date range
raw.fetch(paths, "2025-01-01", "2025-01-31")

# Download for specific branches
raw.fetch(paths, "2025-01-01", "2025-01-31", branches=["Punto Valle"])

# Force re-download
raw.fetch(paths, "2025-01-01", "2025-01-31", mode="force")
```

#### `order_times.raw.load()`

Verify that raw order times data exists for the given range.

**Signature:**

```python
raw.load(
    paths: DataPaths,
    start_date: str,
    end_date: str,
) -> None
```

**Parameters:**
- `paths` (DataPaths): DataPaths configuration
- `start_date` (str): Start date in YYYY-MM-DD format (inclusive)
- `end_date` (str): End date in YYYY-MM-DD format (inclusive)

**Raises:**
- `FileNotFoundError`: If required raw order times files are missing

**Example:**

```python
# Verify data exists (raises error if missing)
raw.load(paths, "2025-01-01", "2025-01-31")
```

## Processing Modes

### `mode="missing"` (Default)

- Only runs ETL stages for date ranges that don't have completed outputs
- Checks metadata to determine if stages need to run
- Skips work that's already been done
- **Recommended for most use cases**

### `mode="force"`

- Forces re-run of all ETL stages for the given date range
- Ignores existing metadata and outputs
- Useful when:
  - You've fixed a bug in transformation logic
  - You want to refresh data from source
  - You're debugging ETL issues

## Function Behavior

### `fetch()` Functions

- **May run ETL**: Checks if data exists, runs ETL if needed (based on mode)
- **Idempotent**: Safe to call multiple times
- **Returns DataFrame**: Always returns the requested data

### `load()` Functions

- **Never runs ETL**: Only reads existing data from disk
- **Faster**: No ETL overhead
- **Raises error**: If data doesn't exist
- **Use when**: You're certain the data already exists

## Data Structures

### `fact_payments_ticket` (Core Fact)

Grain: ticket × payment method

Key columns:
- `sucursal`: Branch name
- `operating_date`: Date of operation
- `order_index`: Ticket/order identifier
- `payment_method`: Payment method (e.g., "Efectivo", "Tarjeta Crédito")

### `mart_payments_daily` (Daily Mart)

Grain: sucursal × date

Key columns:
- `sucursal`: Branch name
- `fecha`: Date
- `ingreso_efectivo`: Cash income
- `ingreso_credito`: Credit card income
- `ingreso_debito`: Debit card income
- `num_tickets`: Number of tickets
- Additional payment method columns

### `fact_sales_item_line` (Core Fact)

Grain: item/modifier line

Key columns:
- `sucursal`: Branch name
- `operating_date`: Date of operation
- `order_id`: Ticket/order identifier
- `item_key`: Item identifier
- `group`: Product group/category
- `subtotal_item`: Item subtotal
- `total_item`: Item total

### `mart_sales_by_ticket` (Ticket Mart)

Grain: one row per ticket

Key columns:
- `sucursal`: Branch name
- `operating_date`: Date of operation
- `order_id`: Ticket/order identifier
- `total`: Ticket total
- Additional aggregated columns

### `mart_sales_by_group` (Group Mart)

Grain: category pivot table

Structure: Groups as columns, sucursales/dates as rows

## Next Steps

- **[Forecasting API](forecasting.md)** - Generate time series forecasts
- **[QA API](qa.md)** - Data quality assurance
- **[Concepts](../user-guide/concepts.md)** - Understand data layers and grains
