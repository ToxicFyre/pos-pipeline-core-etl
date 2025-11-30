"""Forecasting models module.

Forecasting Model Debug Checklist
==================================

When adding a new forecasting model, follow this checklist to ensure
debug information is properly exposed:

1. Add debug attribute to __init__:
   ```python
   def __init__(self, ...) -> None:
       # ... other initialization ...
       self.debug_: ModelDebugInfo | None = None
   ```

2. Populate debug info in forecast() method:
   ```python
   def forecast(self, model: Any, steps: int, **kwargs: Any) -> pd.Series:
       # ... compute forecast ...
       forecast_series = pd.Series(...)

       # Populate generic debug channel with model-specific payload
       self.debug_ = ModelDebugInfo(
           model_name="your_model_name",  # e.g., "arima", "xgboost"
           version="v1",  # optional version string
           data={
               # Model-specific debug fields:
               # Examples:
               # "order": self.order,
               # "aic": result.aic,
               # "bic": result.bic,
               # "residuals_tail": result.resid.tail(20).to_dict(),
           },
       )

       return forecast_series
   ```

3. Important constraints:
   - Never change the return type of forecast(); it always returns pd.Series
   - The debug_ attribute should be set after computing the forecast
   - Use model_name consistently (same string across all instances)
   - Keep data dict JSON-serializable if possible (use ISO strings for dates)

4. If run_payments_forecast(debug=True) uses this model:
   - The model's .debug_ will automatically be collected into ForecastResult.debug
   - Debug info is stored in nested structure: debug[model_name][branch][metric] = ModelDebugInfo
   - This allows tracking debug info per model, branch, and metric combination
   - Multiple models can coexist: debug["naive_last_week"] and debug["arima"] both exist
   - Each branch/metric combination gets its own debug info instance

Example implementations:
- NaiveLastWeekModel: see models/naive.py
- Future ARIMA/XGBoost models: follow the same pattern
"""

from pos_core.forecasting.models.arima import LogARIMAModel
from pos_core.forecasting.models.base import ForecastModel
from pos_core.forecasting.models.naive import NaiveLastWeekModel

__all__ = ["ForecastModel", "LogARIMAModel", "NaiveLastWeekModel"]
