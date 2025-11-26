# Examples

This directory contains runnable example scripts demonstrating how to use the POS Core ETL package.

## Prerequisites

Before running any example, ensure you have:

1. **Installed the package**: `pip install -e .` (or `pip install pos-core-etl` for production)
2. **Created `utils/sucursales.json`**: Branch configuration file (see main README for format)
3. **Set environment variables** (for online extraction):
   - `WS_BASE`: Base URL of your POS instance
   - `WS_USER` (optional): Username for authentication
   - `WS_PASS` (optional): Password for authentication
4. **Created data directory structure** (or modify paths in scripts):
   ```
   data/
   ├── a_raw/
   ├── b_clean/
   └── c_processed/
   ```

## Example Scripts

### 1. `sales_week_by_group.py`
**Advanced example** using low-level APIs for sales detail ETL.

- Downloads sales detail reports for a specific week
- Cleans Excel files to CSV
- Aggregates by ticket and by product group
- Creates a pivot table (groups × sucursales)

**Usage:**
```bash
python examples/sales_week_by_group.py
```

**Note:** Modify `week_start` and `week_end` variables in the script.

### 2. `payments_full_etl.py`
**Recommended example** using the high-level payments ETL API.

- Downloads payment reports (if needed)
- Cleans and aggregates payments data
- Creates daily aggregated dataset
- Uses `build_payments_dataset()` - the primary public API

**Usage:**
```bash
python examples/payments_full_etl.py
```

**Note:** Modify date range in the script if needed.

### 3. `payments_forecast.py`
**Recommended example** for generating forecasts.

- Loads aggregated payments data
- Generates 7-day forecasts for all branches and metrics
- Creates deposit schedule for cash flow planning
- Uses `run_payments_forecast()` - the forecasting API

**Usage:**
```bash
python examples/payments_forecast.py
```

**Note:** Requires running `payments_full_etl.py` first (or having the aggregated CSV file).

## Running Examples

All examples are self-contained and can be run directly. They include comments indicating what needs to be modified (paths, dates, etc.).

For more details, see the main [README.md](../README.md) in the project root.

