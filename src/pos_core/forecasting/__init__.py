"""Payments forecasting module.

This module provides time series forecasting for payment data.

Example:
    >>> from pos_core import DataPaths
    >>> from pos_core.payments import marts as payments_marts
    >>> from pos_core.forecasting import run_payments_forecast, ForecastConfig
    >>>
    >>> paths = DataPaths.from_root("data", "utils/sucursales.json")
    >>>
    >>> # Get historical payment data (daily mart)
    >>> payments_df = payments_marts.fetch_daily(paths, "2022-01-01", "2025-01-31")
    >>>
    >>> # Run forecast
    >>> config = ForecastConfig(horizon_days=91)  # 13 weeks
    >>> result = run_payments_forecast(payments_df, config)
    >>>
    >>> # Access results
    >>> print(result.forecast.head())  # Per-branch/metric forecasts
    >>> print(result.deposit_schedule.head())  # Cash-flow deposits

"""

from pos_core.forecasting.api import ForecastConfig, ForecastResult, run_payments_forecast

__all__ = ["ForecastConfig", "ForecastResult", "run_payments_forecast"]
