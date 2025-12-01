"""Log ARIMA model implementation for time series forecasting.

This module implements a log-transformed ARIMA model using SARIMAX from statsmodels.
The log transformation helps handle non-negative values, multiplicative seasonality,
and heteroscedasticity common in payment/sales data.
"""

from __future__ import annotations

import warnings
from datetime import timedelta
from itertools import product
from typing import Any

import numpy as np
import pandas as pd
from statsmodels.tools.sm_exceptions import ConvergenceWarning, HessianInversionWarning
from statsmodels.tsa.statespace.sarimax import SARIMAX

from pos_core.forecasting.config import FORECAST_DAYS, SEASONAL_PERIOD
from pos_core.forecasting.models.base import ForecastModel

# Suppress frivolous warnings from statsmodels ARIMA fitting
# These warnings are common during grid search when trying many parameter combinations
warnings.filterwarnings("ignore", category=ConvergenceWarning)
warnings.filterwarnings("ignore", category=HessianInversionWarning)
warnings.filterwarnings("ignore", message=".*invertible.*", category=RuntimeWarning)
warnings.filterwarnings("ignore", message=".*non-stationary.*", category=RuntimeWarning)


class LogARIMAModel(ForecastModel):
    """Log-transformed ARIMA model for time series forecasting.

    Uses log1p transformation to handle zero values correctly, then applies
    SARIMAX with automatic hyperparameter selection via grid search.
    """

    def __init__(
        self,
        seasonal_period: int = SEASONAL_PERIOD,
        p_range: tuple[int, ...] = (0, 1, 2),
        d_range: tuple[int, ...] = (0, 1),
        q_range: tuple[int, ...] = (0, 1, 2),
        p_seasonal_range: tuple[int, ...] = (0, 1),
        d_seasonal_range: tuple[int, ...] = (0, 1),
        q_seasonal_range: tuple[int, ...] = (0, 1),
    ):
        """Initialize LogARIMAModel with hyperparameter search ranges.

        Args:
            seasonal_period: Seasonal period (default: 7 for weekly seasonality)
            p_range: AR order parameter range (default: (0, 1, 2))
            d_range: Differencing order parameter range (default: (0, 1))
            q_range: MA order parameter range (default: (0, 1, 2))
            p_seasonal_range: Seasonal AR order parameter range (default: (0, 1))
            d_seasonal_range: Seasonal differencing order parameter range (default: (0, 1))
            q_seasonal_range: Seasonal MA order parameter range (default: (0, 1))

        """
        self.seasonal_period = seasonal_period
        self.p_range = p_range
        self.d_range = d_range
        self.q_range = q_range
        self.p_seasonal_range = p_seasonal_range
        self.d_seasonal_range = d_seasonal_range
        self.q_seasonal_range = q_seasonal_range

    def train(self, series: pd.Series, **_kwargs: Any) -> Any:
        """Train log ARIMA model on a time series.

        Args:
            series: Time series with DateTimeIndex (raw values, not log-transformed)
            **_kwargs: Additional parameters (unused, for interface compatibility)

        Returns:
            Fitted SARIMAX model object

        Raises:
            ValueError: If insufficient data or no valid model found

        """
        # Convert to log space for ARIMA modeling
        # Using log transformation helps handle:
        # 1. Non-negative values (payments can't be negative)
        # 2. Multiplicative seasonality (common in sales data)
        # 3. Heteroscedasticity (variance increases with level)
        # Note: series should already have missing days filled with 0.0
        series_float = series.astype(float)

        # Ensure no NaN/inf values (fill with 0.0 if any remain)
        series_float = series_float.replace([np.inf, -np.inf], np.nan).fillna(0.0)

        # Use log1p (log(1+x)) instead of log(x) to handle zeros correctly
        # log1p(0) = 0, whereas log(0) = -inf
        # This is important because branches may have zero-sale days (closed, holidays)
        series_log = np.log1p(series_float)

        # After log transform, zeros become zeros, so we keep all values
        # Only handle any remaining NaN/inf values (shouldn't happen, but safety)
        series_log = series_log.replace([np.inf, -np.inf], np.nan)
        if series_log.isna().any():
            # If there are still NaN values, fill with a small negative value
            # (log1p of a very small number) to avoid issues
            series_log = series_log.fillna(np.log1p(1e-10))

        if len(series_log) < 30:
            raise ValueError(f"Insufficient data: only {len(series_log)} observations")

        best_aic = np.inf
        best_model = None

        # Grid search over ARIMA hyperparameters
        # We try all combinations and select the model with lowest AIC
        # (Akaike Information Criterion)
        # AIC balances model fit with complexity (lower is better)
        # Many combinations will fail (e.g., non-stationary, non-invertible),
        # which is expected
        for p, d, q in product(self.p_range, self.d_range, self.q_range):
            for p_seas, d_seas, q_seas in product(
                self.p_seasonal_range, self.d_seasonal_range, self.q_seasonal_range
            ):
                order = (p, d, q)
                seasonal_order = (p_seas, d_seas, q_seas, self.seasonal_period)

                try:
                    model = SARIMAX(
                        series_log,
                        order=order,
                        seasonal_order=seasonal_order,
                        enforce_stationarity=False,
                        enforce_invertibility=False,
                    )
                    res = model.fit(disp=False)
                    if res.aic < best_aic:
                        best_aic = res.aic
                        best_model = res
                except Exception:
                    # Many combinations will fail; that's fine
                    continue

        if best_model is None:
            raise ValueError("No valid model found during hyperparameter search")

        return best_model

    def forecast(self, model: Any, steps: int, **kwargs: Any) -> pd.Series:
        """Generate forecast from a trained ARIMA model.

        Args:
            model: Trained SARIMAX model object (from train() method)
            steps: Number of periods to forecast ahead
            **kwargs: Additional parameters. Can include 'last_date' (pd.Timestamp)
                     to specify the last date of the training series for index creation.

        Returns:
            Forecast series with DateTimeIndex, back-transformed to original scale

        """
        # Generate forecast in log space
        forecast_log = model.get_forecast(steps=steps)
        forecast_series_log = forecast_log.predicted_mean

        # Back-transform from log space to original scale using expm1 (exp(x) - 1)
        # This is the inverse of log1p: expm1(log1p(x)) = x
        forecast_series = np.expm1(forecast_series_log)

        # Ensure forecasts are non-negative (shouldn't happen after expm1, but safety check)
        # Negative forecasts would be nonsensical for payment amounts
        forecast_series = forecast_series.clip(lower=0.0)

        # Create future dates for forecast index
        # Try to get last date from kwargs, then from model, then use current date
        last_date = kwargs.get("last_date")
        if last_date is None:
            # Try to get from model's endog index
            if hasattr(model, "model") and hasattr(model.model, "endog"):
                last_date = model.model.endog.index[-1]
            else:
                # Fallback to current date (shouldn't happen in normal usage)
                last_date = pd.Timestamp.now()

        future_dates = pd.date_range(start=last_date + timedelta(days=1), periods=steps, freq="D")
        forecast_series.index = future_dates

        return forecast_series


# Backward compatibility: function interface
def train_log_arima(
    series: pd.Series,
    steps: int = FORECAST_DAYS,
    seasonal_period: int = SEASONAL_PERIOD,
    p_range: tuple[int, ...] = (0, 1, 2),
    d_range: tuple[int, ...] = (0, 1),
    q_range: tuple[int, ...] = (0, 1, 2),
    p_seasonal_range: tuple[int, ...] = (0, 1),
    d_seasonal_range: tuple[int, ...] = (0, 1),
    q_seasonal_range: tuple[int, ...] = (0, 1),
) -> tuple[Any, pd.Series]:
    """Train log ARIMA model and generate forecast (backward compatibility function).

    This function provides backward compatibility with the old interface.
    New code should use LogARIMAModel class directly.

    Args:
        series: Time series with DateTimeIndex (raw values, not log-transformed)
        steps: Number of days to forecast
        seasonal_period: Seasonal period (default: 7 for weekly seasonality)
        p_range: AR order parameter range (default: (0, 1, 2))
        d_range: Differencing order parameter range (default: (0, 1))
        q_range: MA order parameter range (default: (0, 1, 2))
        p_seasonal_range: Seasonal AR order parameter range (default: (0, 1))
        d_seasonal_range: Seasonal differencing order parameter range (default: (0, 1))
        q_seasonal_range: Seasonal MA order parameter range (default: (0, 1))

    Returns:
        Tuple of (fitted_model, forecast_series) where forecast is
        back-transformed to original scale

    """
    model = LogARIMAModel(
        seasonal_period=seasonal_period,
        p_range=p_range,
        d_range=d_range,
        q_range=q_range,
        p_seasonal_range=p_seasonal_range,
        d_seasonal_range=d_seasonal_range,
        q_seasonal_range=q_seasonal_range,
    )
    trained_model = model.train(series)
    last_date = series.index[-1]
    forecast = model.forecast(trained_model, steps=steps, last_date=last_date)
    return trained_model, forecast
