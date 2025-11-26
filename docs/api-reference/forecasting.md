# Forecasting API Reference

## `ForecastConfig`

Configuration for payments forecasting.

### Attributes

- `horizon_days` (int): Number of days ahead to forecast (default: 7)
- `metrics` (List[str]): List of metrics to forecast (default: cash, credit, debit, total)
- `branches` (Optional[List[str]]): List of branch names to forecast. If None, infers from payments_df.

### Example

```python
from pos_core.forecasting import ForecastConfig

config = ForecastConfig(
    horizon_days=14,
    metrics=["ingreso_efectivo", "ingreso_total"],
    branches=["Banana", "Queen"]
)
```

## `ForecastResult`

Result dataclass containing forecast DataFrame, deposit schedule, and metadata.

### Attributes

- `forecast` (pd.DataFrame): DataFrame with columns: sucursal, fecha, metric, valor
- `deposit_schedule` (pd.DataFrame): DataFrame with cash-flow deposit schedule
- `metadata` (Dict[str, object]): Dictionary with additional metadata

### Metadata Keys

- `branches`: List of branches forecasted
- `metrics`: List of metrics forecasted
- `horizon_days`: Number of days forecasted
- `last_historical_date`: Last date in historical data
- `successful_forecasts`: Number of successful forecasts
- `failed_forecasts`: Number of failed forecasts

## `run_payments_forecast()`

Main forecasting function.

### Signature

```python
def run_payments_forecast(
    payments_df: pd.DataFrame,
    config: Optional[ForecastConfig] = None,
) -> ForecastResult
```

### Parameters

- `payments_df` (pd.DataFrame): Aggregated payments data, typically the output of the ETL step. Expected columns include at least:
  - `sucursal` (branch name)
  - `fecha` (date or datetime)
  - the metrics in config.metrics (e.g. ingreso_efectivo, ingreso_credito, ...)
- `config` (Optional[ForecastConfig]): ForecastConfig for horizon, metrics, and branches. If None, uses defaults.

### Returns

ForecastResult containing:
- `forecast`: per-branch, per-metric predictions for the next horizon_days
- `deposit_schedule`: computed cash-flow deposit schedule
- `metadata`: additional information about the forecast

### Raises

- `DataQualityError`: If required columns are missing, or no forecasts are generated.

### Example

```python
from pos_core.forecasting import ForecastConfig, run_payments_forecast

config = ForecastConfig(horizon_days=7)
result = run_payments_forecast(payments_df, config=config)

print(result.forecast.head())
print(result.deposit_schedule)
```

