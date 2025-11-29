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
- `debug` (Optional[Dict[str, Dict[str, Dict[str, ModelDebugInfo]]]]): Optional nested dictionary of debug information. Only populated when `run_payments_forecast()` is called with `debug=True`. Structure: `debug[model_name][branch][metric] = ModelDebugInfo`

### Metadata Keys

- `branches`: List of branches forecasted
- `metrics`: List of metrics forecasted
- `horizon_days`: Number of days forecasted
- `last_historical_date`: Last date in historical data
- `successful_forecasts`: Number of successful forecasts
- `failed_forecasts`: Number of failed forecasts

### Debug Information

When `debug=True` is passed to `run_payments_forecast()`, the result includes model-specific debug information in `result.debug`. This allows you to inspect how each model generated its forecasts.

**Debug Structure:**
```python
result.debug[model_name][branch][metric] = ModelDebugInfo
```

**Example:**
```python
result = run_payments_forecast(payments_df, config=config, debug=True)

# Access debug info for naive model, Kavia branch, efectivo metric
naive_debug = result.debug["naive_last_week"]["Kavia"]["ingreso_efectivo"]
print(naive_debug.model_name)  # "naive_last_week"
print(naive_debug.data["source_dates"])  # Mapping of forecast dates to source dates
```

Each `ModelDebugInfo` contains:
- `model_name` (str): Identifier for the model (e.g., "naive_last_week", "arima")
- `version` (Optional[str]): Optional version string
- `data` (Dict[str, Any]): Model-specific debug payload

The `data` dictionary structure varies by model:
- **NaiveLastWeekModel**: Contains `source_dates` mapping (forecast_date -> source_date)
- **ARIMA models**: Contains model parameters, AIC/BIC values, residuals, etc.

## `run_payments_forecast()`

Main forecasting function.

### Signature

```python
def run_payments_forecast(
    payments_df: pd.DataFrame,
    config: Optional[ForecastConfig] = None,
    debug: bool = False,
) -> ForecastResult
```

### Parameters

- `payments_df` (pd.DataFrame): Aggregated payments data, typically the output of the ETL step. Expected columns include at least:
  - `sucursal` (branch name)
  - `fecha` (date or datetime)
  - the metrics in config.metrics (e.g. ingreso_efectivo, ingreso_credito, ...)
- `config` (Optional[ForecastConfig]): ForecastConfig for horizon, metrics, and branches. If None, uses defaults.
- `debug` (bool): If True, collects debug information from models and includes it in `result.debug`. Default is False to keep the API simple for normal use.

### Returns

ForecastResult containing:
- `forecast`: per-branch, per-metric predictions for the next horizon_days
- `deposit_schedule`: computed cash-flow deposit schedule
- `metadata`: additional information about the forecast
- `debug`: model debug information (only if `debug=True`)

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

### Example with Debug Information

```python
from pos_core.forecasting import ForecastConfig, run_payments_forecast

config = ForecastConfig(horizon_days=7)
result = run_payments_forecast(payments_df, config=config, debug=True)

# Access debug info for a specific model/branch/metric
if result.debug:
    naive_debug = result.debug["naive_last_week"]["Kavia"]["ingreso_efectivo"]
    source_dates = naive_debug.data["source_dates"]
    print(f"Forecast dates mapped to source dates: {source_dates}")
```

## `ModelDebugInfo`

Generic container for model-specific debug information.

### Attributes

- `model_name` (str): Short identifier for the model (e.g., "naive_last_week", "arima")
- `version` (Optional[str]): Optional version string if model behavior changes over time
- `data` (Dict[str, Any]): Model-specific payload (dict of JSON-like values)

### Model-Specific Data Schemas

Each model populates `data` with its own schema:

**NaiveLastWeekModel:**
- `horizon_steps` (int): Number of forecast steps
- `source_dates` (Dict[pd.Timestamp, pd.Timestamp]): Mapping of forecast dates to source historical dates

**ARIMA Models** (future):
- `order` (tuple): ARIMA order parameters
- `aic` (float): Akaike Information Criterion
- `bic` (float): Bayesian Information Criterion
- `residuals_tail` (dict): Tail of residuals for inspection

### Accessing Debug Info

Debug information can be accessed at two levels:

1. **Model-level**: Direct access to `model.debug_` attribute
   ```python
   from pos_core.forecasting.models.naive import NaiveLastWeekModel
   
   model = NaiveLastWeekModel()
   model_state = model.train(series)
   forecast = model.forecast(model_state, steps=7)
   
   # Access debug info directly from model
   debug = model.debug_
   print(debug.model_name)  # "naive_last_week"
   print(debug.data["source_dates"])
   ```

2. **Pipeline-level**: Through `ForecastResult.debug` (when `debug=True`)
   ```python
   result = run_payments_forecast(payments_df, config=config, debug=True)
   debug = result.debug["naive_last_week"]["Kavia"]["ingreso_efectivo"]
   ```

