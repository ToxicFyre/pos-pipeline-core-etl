# Examples

This package includes three runnable example scripts in the `examples/` directory.

## Example 1: Sales Detail ETL

**File**: `examples/sales_week_by_group.py`

Demonstrates low-level APIs for sales detail ETL:

- Downloads sales detail reports for a specific week
- Cleans Excel files to CSV
- Aggregates by ticket and by product group
- Creates a pivot table (groups × sucursales)

**Usage**:

```bash
python examples/sales_week_by_group.py
```

**Note**: Modify `week_start` and `week_end` variables in the script.

## Example 2: Payments ETL

**File**: `examples/payments_full_etl.py`

Demonstrates the high-level payments ETL API:

- Downloads payment reports (if needed)
- Cleans and aggregates payments data
- Creates daily aggregated dataset
- Uses `build_payments_dataset()` - the primary public API

**Usage**:

```bash
python examples/payments_full_etl.py
```

**Note**: Modify date range in the script if needed.

## Example 3: Forecasting

**File**: `examples/payments_forecast.py`

Demonstrates the forecasting API:

- Loads aggregated payments data
- Generates 7-day forecasts for all branches and metrics
- Creates deposit schedule for cash flow planning
- Uses `run_payments_forecast()` - the forecasting API

**Usage**:

```bash
python examples/payments_forecast.py
```

**Note**: Requires running `payments_full_etl.py` first (or having the aggregated CSV file).

## Prerequisites

Before running any example:

1. Install the package: `pip install -e .`
2. Create `utils/sucursales.json` (see [Configuration](configuration.md))
3. Set `WS_BASE` environment variable (for online extraction)
4. Create data directory structure (or modify paths in scripts)

See `examples/README.md` for more details.

