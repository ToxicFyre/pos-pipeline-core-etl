# QA API Reference

## `PaymentsQAResult`

Result dataclass with QA summary and detailed findings.

### Attributes

- `summary` (dict): Dictionary with summary statistics and counts
- `missing_days` (pd.DataFrame | None): DataFrame with missing days per sucursal, or None if none found
- `duplicate_days` (pd.DataFrame | None): DataFrame with duplicate (sucursal, fecha) rows, or None if none found
- `zscore_anomalies` (pd.DataFrame | None): DataFrame with z-score anomalies, or None if none found
- `zero_method_flags` (pd.DataFrame | None): DataFrame with rows where tickets > 0 but payment methods are zero, or None if none found

### Summary Keys

- `total_rows`: Total number of rows in the dataset
- `total_sucursales`: Number of unique branches
- `min_fecha`: Minimum date in the dataset
- `max_fecha`: Maximum date in the dataset
- `has_missing_days`: Boolean indicating if missing days were found
- `has_duplicates`: Boolean indicating if duplicates were found
- `has_zscore_anomalies`: Boolean indicating if z-score anomalies were found
- `has_zero_method_flags`: Boolean indicating if zero method flags were found
- `missing_days_count`: Number of missing days
- `duplicate_days_count`: Number of duplicate days
- `zscore_anomalies_count`: Number of z-score anomalies
- `zero_method_flags_count`: Number of zero method flags
- `schema_errors`: List of schema validation errors

## `run_payments_qa()`

Main QA function for data validation.

### Signature

```python
def run_payments_qa(
    payments_df: pd.DataFrame,
    level: int = 4,
) -> PaymentsQAResult
```

### Parameters

- `payments_df` (pd.DataFrame): Aggregated payments data, typically the output of the ETL step. Expected columns include at least:
  - `sucursal` (branch name)
  - `fecha` (date or datetime)
  - payment method columns (ingreso_efectivo, ingreso_credito, etc.)
  - `num_tickets` (ticket count)
- `level` (int): QA level to run (default: 4). Controls which checks are executed:
  - Level 0: Schema validation (always run)
  - Level 3: Missing and duplicate days
  - Level 4: Statistical anomalies (z-score)

### Returns

PaymentsQAResult containing:
- `summary`: dictionary with counts and flags
- `missing_days`: DataFrame with missing days per sucursal, or None
- `duplicate_days`: DataFrame with duplicate rows, or None
- `zscore_anomalies`: DataFrame with z-score anomalies, or None
- `zero_method_flags`: DataFrame with zero method flags, or None

### Raises

- `DataQualityError`: If required columns are missing.

### Example

```python
from pos_core.qa import run_payments_qa

qa_result = run_payments_qa(payments_df, level=4)

print(f"Missing days: {qa_result.summary['missing_days_count']}")
print(f"Anomalies: {qa_result.summary['zscore_anomalies_count']}")

if qa_result.missing_days is not None:
    print(qa_result.missing_days)
```

