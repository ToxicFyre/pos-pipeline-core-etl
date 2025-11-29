"""Naive Last Week forecasting model.

This model forecasts by finding equivalent historical weekdays from past weeks,
skipping holidays and holiday-adjacent dates, and using those historical values directly.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Optional, Set

import pandas as pd

from pos_core.forecasting.deposit_schedule import is_holiday_or_adjacent
from pos_core.forecasting.models.base import ForecastModel


def find_equivalent_historical_weekday(
    target_date: date,
    last_data_date: date,
    holidays: Set[date],
    max_weeks_back: int = 52,
) -> Optional[date]:
    """Find equivalent historical weekday from past weeks, skipping holidays.

    This function looks for the same weekday in previous weeks that:
    1. Is on or before last_data_date (we have data for it)
    2. Is NOT a holiday or adjacent to a holiday

    Args:
        target_date: The date we want to forecast for
        last_data_date: The last date for which we have historical data
        holidays: Set of holiday dates to skip
        max_weeks_back: Maximum number of weeks to search back (default: 52)

    Returns:
        The equivalent historical date, or None if not found within max_weeks_back

    Example:
        If target_date is Nov 24 (Sunday), last_data_date is Nov 23, and Nov 17 is a holiday:
        - Nov 17 (Sunday) is skipped (holiday or adjacent)
        - Nov 10 (Sunday) is returned
    """
    candidate = target_date - timedelta(days=7)

    for _ in range(max_weeks_back):
        # Check if candidate is within our historical data range
        if candidate <= last_data_date:
            # Check if candidate is NOT a holiday or adjacent to holiday
            if not is_holiday_or_adjacent(candidate, holidays):
                return candidate

        # Move back another week
        candidate = candidate - timedelta(days=7)

    return None


class NaiveLastWeekModel(ForecastModel):
    """Naive forecasting model that uses equivalent historical weekdays.

    This model forecasts by:
    1. For each forecast date, finding the equivalent weekday from past weeks
    2. Skipping holidays and dates adjacent to holidays
    3. Returning the historical value from that equivalent date

    This is a simple baseline model that captures weekly patterns without
    any statistical modeling.
    """

    def train(self, series: pd.Series, holidays: Optional[Set[date]] = None, **kwargs: Any) -> dict:
        """Store historical series and holidays for use in forecast.

        Args:
            series: Time series with DateTimeIndex (raw values)
            holidays: Optional set of holiday dates. If None, uses empty set.
            **kwargs: Additional parameters (ignored)

        Returns:
            Dictionary containing the series and holidays for use in forecast()
        """
        return {
            "series": series,
            "holidays": holidays or set(),
        }

    def forecast(self, model: dict, steps: int, **kwargs: Any) -> pd.Series:
        """Generate forecast using equivalent historical weekdays.

        For each forecast date:
        1. Find equivalent historical weekday (going back week by week)
        2. Skip holidays and holiday-adjacent dates
        3. Return the historical value from that date

        Args:
            model: Dictionary from train() containing series and holidays
            steps: Number of periods to forecast ahead
            **kwargs: Additional parameters. Can include 'last_date' (pd.Timestamp)
                     to specify the last date of the training series for index creation.

        Returns:
            Forecast series with DateTimeIndex
        """
        series = model["series"]
        holidays = model["holidays"]

        # Get last date from kwargs or from series index
        last_date = kwargs.get("last_date")
        if last_date is None:
            last_date = series.index[-1]

        # Convert to date if it's a Timestamp
        if isinstance(last_date, pd.Timestamp):
            last_data_date = last_date.date()
        else:
            last_data_date = last_date

        # Create series lookup by date for fast access
        series_by_date = {}
        for idx, value in series.items():
            if isinstance(idx, pd.Timestamp):
                series_by_date[idx.date()] = value
            else:
                series_by_date[idx] = value

        # Generate forecast dates
        forecast_dates = pd.date_range(
            start=last_date + timedelta(days=1), periods=steps, freq="D"
        )

        # Forecast each date
        forecast_values = []
        for forecast_date in forecast_dates:
            target_date = forecast_date.date()

            # Find equivalent historical weekday
            equivalent_date = find_equivalent_historical_weekday(
                target_date=target_date,
                last_data_date=last_data_date,
                holidays=holidays,
            )

            if equivalent_date is not None and equivalent_date in series_by_date:
                value = series_by_date[equivalent_date]
            else:
                # Fallback: return 0.0 if no equivalent found
                value = 0.0

            forecast_values.append(value)

        return pd.Series(forecast_values, index=forecast_dates)

