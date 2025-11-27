"""Naive last week forecasting model implementation.

This module implements a simple naive forecasting model that predicts future values
by finding equivalent historical weekdays from previous weeks, skipping holidays.
This provides a baseline forecast alternative to ARIMA.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Dict, Optional, Set

import pandas as pd

from pos_core.forecasting.deposit_schedule import is_holiday_or_adjacent
from pos_core.forecasting.models.base import ForecastModel


class NaiveLastWeekModel(ForecastModel):
    """Naive forecasting model that uses last week's equivalent weekday.

    This model predicts future values by looking back to find the same weekday
    from previous weeks, while avoiding holidays and their adjacent days.
    It provides a simple baseline forecast without statistical modeling.
    """

    def __init__(self, max_lookback_weeks: int = 8):
        """Initialize NaiveLastWeekModel.

        Args:
            max_lookback_weeks: Maximum number of weeks to look back when searching
                               for equivalent historical weekday (default: 8)
        """
        self.max_lookback_weeks = max_lookback_weeks

    def train(self, series: pd.Series, **kwargs: Any) -> Dict[str, Any]:
        """Store historical series and extract holidays for forecasting.

        Args:
            series: Time series with DateTimeIndex (raw values, not transformed)
            **kwargs: Additional parameters. Can include 'holidays' (set of date objects)
                     to specify which dates are holidays.

        Returns:
            Dictionary containing the series and holidays for use in forecast()

        Raises:
            ValueError: If insufficient data (< 7 days)
        """
        if len(series) < 7:
            raise ValueError(f"Insufficient data: only {len(series)} observations (need at least 7)")

        # Extract holidays from kwargs, or create empty set
        holidays = kwargs.get("holidays", set())
        if not isinstance(holidays, set):
            holidays = set(holidays) if holidays else set()

        # Store the series and holidays
        return {
            "series": series.copy(),
            "holidays": holidays,
        }

    def forecast(self, model: Any, steps: int, **kwargs: Any) -> pd.Series:
        """Generate forecast using equivalent historical weekdays.

        For each forecast date, finds the same weekday from previous weeks,
        skipping holidays and their adjacent days.

        Args:
            model: Dictionary with 'series' and 'holidays' (from train() method)
            steps: Number of periods to forecast ahead
            **kwargs: Additional parameters. Can include 'last_date' (pd.Timestamp)
                     to specify the last date of the training series.

        Returns:
            Forecast series with DateTimeIndex
        """
        series = model["series"]
        holidays = model["holidays"]

        # Get last date from kwargs or from series
        last_date = kwargs.get("last_date")
        if last_date is None:
            last_date = series.index[-1]

        # Convert to date if it's a Timestamp
        if isinstance(last_date, pd.Timestamp):
            last_date_obj = last_date.date()
        else:
            last_date_obj = last_date

        # Generate forecast dates
        future_dates = pd.date_range(start=last_date + timedelta(days=1), periods=steps, freq="D")

        # Generate forecasts for each future date
        forecast_values = []
        for future_date in future_dates:
            future_date_obj = future_date.date()
            
            # Find equivalent historical weekday
            historical_value = self._find_equivalent_historical_weekday(
                target_date=future_date_obj,
                series=series,
                holidays=holidays,
            )
            
            forecast_values.append(historical_value)

        # Create forecast series
        forecast_series = pd.Series(forecast_values, index=future_dates)
        return forecast_series

    def _find_equivalent_historical_weekday(
        self,
        target_date: date,
        series: pd.Series,
        holidays: Set[date],
    ) -> float:
        """Find equivalent historical weekday value, skipping holidays.

        Looks back week-by-week for the same weekday, avoiding holidays
        and their adjacent days.

        Args:
            target_date: The future date to forecast
            series: Historical time series
            holidays: Set of holiday dates

        Returns:
            Historical value from equivalent weekday, or 0.0 if not found
        """
        target_weekday = target_date.weekday()  # 0=Monday, 6=Sunday

        # Look back week by week for the same weekday
        for weeks_back in range(1, self.max_lookback_weeks + 1):
            candidate_date = target_date - timedelta(weeks=weeks_back)

            # Check if this candidate date has the same weekday
            if candidate_date.weekday() != target_weekday:
                continue

            # Skip if candidate is a holiday or adjacent to a holiday
            if is_holiday_or_adjacent(candidate_date, holidays):
                continue

            # Try to get value from series
            # Convert candidate_date to pandas Timestamp for indexing
            candidate_timestamp = pd.Timestamp(candidate_date)

            if candidate_timestamp in series.index:
                value = series.loc[candidate_timestamp]
                # Return the value if it's valid (not NaN)
                if pd.notna(value):
                    return float(value)

        # If no equivalent weekday found, return 0.0 (fallback)
        return 0.0
