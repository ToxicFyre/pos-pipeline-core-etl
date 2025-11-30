# Examples

This package includes runnable example scripts in the `examples/` directory. All examples are self-contained and can be run directly.

## Prerequisites

Before running any example:

1. **Install the package**: `pip install -e .` (or `pip install pos-core-etl` for production)
2. **Create `utils/sucursales.json`**: Branch configuration file (see [Configuration](configuration.md))
3. **Set environment variables** (required for online extraction):
   - `WS_BASE`: Base URL of your POS instance
   - `WS_USER`: Username for authentication
   - `WS_PASS`: Password for authentication

   **Example:**
   ```bash
   export WS_BASE="https://your-pos-instance.com"
   export WS_USER="your_username"
   export WS_PASS="your_password"
   ```
4. **Create data directory structure** (or modify paths in scripts):
   ```
   data/
   ├── a_raw/
   ├── b_clean/
   └── c_processed/
   ```

## Example Scripts

### Example 1: Sales Detail ETL (Query API)

**File**: `examples/sales_week_by_group.py`

Demonstrates the high-level query API for sales detail ETL:

- Uses `get_sales()` to get sales data at different aggregation levels
- Automatically handles ETL stages (download, clean, aggregate) only when needed
- Shows both ticket-level and group-level aggregation
- Creates a pivot table (groups × sucursales)

**Usage**:

```bash
python examples/sales_week_by_group.py
```

**Note**: Modify `week_start` and `week_end` variables in the script.

**Key Features**:
- Uses `refresh=True` for the first call to ensure fresh data
- Uses `refresh=False` for subsequent calls to reuse existing data
- Demonstrates the `level` parameter ("ticket" vs "group")

### Example 2: Payments ETL (Query API)

**File**: `examples/payments_full_etl.py`

Demonstrates the high-level payments ETL API:

- Uses `get_payments()` to get payments data
- Automatically handles downloading, cleaning, and aggregating
- Uses metadata to skip work that's already been done
- Creates daily aggregated dataset

**Usage**:

```bash
python examples/payments_full_etl.py
```

**Note**: Modify date range in the script if needed.

**Alternative**: You can also use `build_payments_dataset()` for complete orchestration, or use stage functions (`download_payments`, `clean_payments`, `aggregate_payments`) for fine-grained control.

### Example 3: Forecasting (Query API)

**File**: `examples/payments_forecast.py`

Demonstrates the forecasting query API:

- Uses `get_payments_forecast()` to get forecasts
- Automatically gets historical data and runs the forecast
- Returns forecast DataFrame directly
- Shows how to work with forecast results

**Usage**:

```bash
python examples/payments_forecast.py
```

**Note**: The query API automatically handles getting historical data. For full control including deposit schedule, metadata, and debug information, use `run_payments_forecast()` directly with `debug=True`:

```python
from pos_core.forecasting import ForecastConfig, run_payments_forecast

config = ForecastConfig(horizon_days=7)
result = run_payments_forecast(payments_df, config=config, debug=True)

# Access debug information
if result.debug:
    for model_name, branches in result.debug.items():
        print(f"Model: {model_name}")
        for branch, metrics in branches.items():
            for metric, debug_info in metrics.items():
                print(f"  {branch}/{metric}: {debug_info.model_name}")
```

## Advanced: Stage Functions

For fine-grained control, you can use stage functions directly:

```python
from pathlib import Path
from pos_core.etl import PaymentsETLConfig, download_payments, clean_payments, aggregate_payments

config = PaymentsETLConfig.from_root(Path("data"), Path("utils/sucursales.json"))

# Run stages individually
download_payments("2025-01-01", "2025-01-31", config)
clean_payments("2025-01-01", "2025-01-31", config)
df = aggregate_payments("2025-01-01", "2025-01-31", config)
```

This gives you control over when each stage runs, but query functions (`get_payments`, `get_sales`) are recommended for most use cases as they handle idempotence automatically.

## Next Steps

- **Configure**: Learn about [Configuration](configuration.md) options
- **Understand Concepts**: Read about [Concepts](concepts.md) and design decisions
- **Explore API**: Check the [API Reference](../api-reference/etl.md) for detailed documentation

## More Information

For detailed information about each example script, see the [examples README](https://github.com/ToxicFyre/pos-pipeline-core-etl/blob/main/examples/README.md) in the repository.
