# Concepts

This page explains key concepts and design decisions in POS Core ETL.

## Branch Code Windows

Branches (sucursales) may change their codes over time. The package handles this through "validity windows" in `sucursales.json`.

Each branch entry can specify:
- `valid_from`: When this code became active
- `valid_to`: When this code became inactive (null if still active)

During extraction, the package automatically selects the correct code for each date range based on these windows.

**Example**: If a branch changed codes on 2024-06-01, you'd have two entries:

```json
{
  "MyBranch": [
    {
      "code": "1234",
      "valid_from": "2024-01-01",
      "valid_to": "2024-05-31"
    },
    {
      "code": "5678",
      "valid_from": "2024-06-01",
      "valid_to": null
    }
  ]
}
```

## ETL Directory Convention

The package uses a three-stage directory structure:

- **`a_raw/`**: Raw data files downloaded from POS API (Excel files)
- **`b_clean/`**: Cleaned and normalized data (CSV files)
- **`c_processed/`**: Aggregated and processed datasets (CSV files)

This convention makes it easy to:
- Identify which stage each file belongs to
- Re-run specific stages without re-processing everything
- Debug issues at each stage

## Payments vs Sales

The package provides two levels of APIs:

### Payments (High-Level API)

The **payments** API is the primary public interface:

- `build_payments_dataset()`: Complete ETL orchestration
- `run_payments_forecast()`: Forecasting
- `run_payments_qa()`: Quality assurance

This is the recommended way to work with payment data.

### Sales (Low-Level Utilities)

The **sales** API provides lower-level utilities:

- Direct access to extraction functions
- Manual cleaning and transformation
- Fine-grained control over the pipeline

Use this when you need more control or are working with sales detail data (which doesn't have a high-level API yet).

## POS System Requirements

This package is designed for POS systems that:

1. **Expose HTTP exports** for:
   - Payment reports
   - Sales detail reports
   - Transfer reports

2. **Use Excel format** for exported reports

3. **Support authentication** via username/password (optional)

The package is currently optimized for Wansoft-style POS systems, but the architecture allows for future extension to other POS backends.

## Incremental Processing

The ETL pipeline is designed for incremental processing:

- **Smart date range chunking**: Automatically splits large date ranges into manageable chunks
- **Existing data discovery**: Skips downloading files that already exist
- **Resumable**: Can be interrupted and resumed without losing progress

This makes it practical to process years of historical data.

## Forecasting Model

The forecasting module uses **ARIMA (AutoRegressive Integrated Moving Average)** models:

- **Log transformation**: Applied to handle non-negative values
- **Automatic hyperparameter selection**: Searches for optimal ARIMA parameters
- **Per-branch, per-metric**: Separate models for each combination

The models require at least 30 days of historical data to generate reliable forecasts.

