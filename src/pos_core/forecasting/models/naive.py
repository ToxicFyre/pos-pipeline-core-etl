"""Naive Last Week forecast model implementation.

This module implements a simple baseline forecasting model that uses historical
values from the same weekday in previous weeks, avoiding holidays and days
adjacent to holidays.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import pandas as pd

from pos_core.forecasting.deposit_schedule import is_holiday_or_adjacent
from pos_core.forecasting.models.base import ForecastModel


def is_weekday(d: date) -> bool:
    """Check if a date is a weekday (Monday-Friday).

    Args:
        d: Date to check

    Returns:
        True if date is Monday-Friday (weekday < 5)
    """
    return d.weekday() < 5


def find_equivalent_historical_weekday(
    target_date: date,
    series: pd.Series,
    holidays: set[date],
    max_weeks_back: int = 10,
) -> date | None:
    """Find the most recent historical date with the same weekday, avoiding holidays.

    Args:
        target_date: Target date to find equivalent for
        series: Historical time series with DateTimeIndex
        holidays: Set of holiday dates to avoid
        max_weeks_back: Maximum weeks to look back (default: 10)

    Returns:
        Equivalent historical date, or None if not found
    """
    target_weekday = target_date.weekday()

    # Determine index type from first element (if available)
    index_is_timestamp = False
    if len(series.index) > 0:
        index_is_timestamp = isinstance(series.index[0], pd.Timestamp)

    # Look back through weeks
    for week_offset in range(1, max_weeks_back + 1):
        candidate_date = target_date - timedelta(days=week_offset * 7)

        # Check if candidate has same weekday
        if candidate_date.weekday() != target_weekday:
            continue

        # Check if candidate is not holiday or adjacent to holiday
        if is_holiday_or_adjacent(candidate_date, holidays):
            continue

        # Check if candidate exists in series
        # Convert candidate_date to match the index type
        if index_is_timestamp:
            candidate_idx = pd.Timestamp(candidate_date)
        else:
            candidate_idx = candidate_date

        if candidate_idx not in series.index:
            continue

        # All checks passed
        return candidate_date

    # No suitable date found
    return None


class NaiveLastWeekModel(ForecastModel):
    """Naive Last Week forecasting model.

    For each forecast date, finds the most recent historical date with the same
    weekday that is not a holiday or adjacent to a holiday, and uses that value
    as the forecast.
    """

    def __init__(self, max_weeks_back: int = 10):
        """Initialize NaiveLastWeekModel.

        Args:
            max_weeks_back: Maximum number of weeks to look back when finding
                equivalent historical weekdays (default: 10)
        """
        self.max_weeks_back = max_weeks_back
        self.training_series: pd.Series | None = None
        self.holidays: set[date] = set()

    def train(self, series: pd.Series, **kwargs: Any) -> Any:
        """Train the naive model on a time series.

        Args:
            series: Time series with DateTimeIndex (raw values)
            **kwargs: Additional parameters
                - holidays (set[date], optional): Set of holiday dates to avoid

        Returns:
            The model instance itself (for consistency with ForecastModel interface)
        """
        # Store a copy of the training series
        self.training_series = series.copy()

        # Extract holidays from kwargs if provided
        if "holidays" in kwargs:
            self.holidays = kwargs["holidays"]
        else:
            self.holidays = set()

        return self

    def forecast(self, model: Any, steps: int, **kwargs: Any) -> pd.Series:
        """Generate forecast from the trained model.

        Args:
            model: Trained model (should be self)
            steps: Number of periods to forecast ahead
            **kwargs: Additional parameters
                - last_date (pd.Timestamp, optional): Last date of training series
                    for index creation

        Returns:
            Forecast series with DateTimeIndex containing forecast values

        Raises:
            ValueError: If model has not been trained
        """
        # Validate that model has been trained
        if self.training_series is None:
            raise ValueError("Model has not been trained. Call train() first.")

        # Get last date from kwargs or training series
        last_date = kwargs.get("last_date")
        if last_date is None:
            last_date = self.training_series.index[-1]

        # Convert to Timestamp if needed
        if isinstance(last_date, date):
            last_date = pd.Timestamp(last_date)
        elif not isinstance(last_date, pd.Timestamp):
            last_date = pd.to_datetime(last_date)

        # Generate forecast for each step
        forecast_values = []
        forecast_dates = []

        for step in range(1, steps + 1):
            forecast_date = last_date + timedelta(days=step)
            # Convert to date object for weekday matching
            if isinstance(forecast_date, pd.Timestamp):
                forecast_date_date = forecast_date.date()
            elif isinstance(forecast_date, date):
                forecast_date_date = forecast_date
            else:
                # Try to convert
                forecast_date_date = pd.to_datetime(forecast_date).date()

            # Find equivalent historical weekday
            equivalent_date = find_equivalent_historical_weekday(
                target_date=forecast_date_date,
                series=self.training_series,
                holidays=self.holidays,
                max_weeks_back=self.max_weeks_back,
            )

            # Get value from historical date (or 0.0 if not found)
            if equivalent_date is not None:
                # Find the value in the training series
                # Convert equivalent_date to match the index type
                if len(self.training_series.index) > 0:
                    index_is_timestamp = isinstance(self.training_series.index[0], pd.Timestamp)
                    if index_is_timestamp:
                        equivalent_idx = pd.Timestamp(equivalent_date)
                    else:
                        equivalent_idx = equivalent_date

                    if equivalent_idx in self.training_series.index:
                        value = self.training_series.loc[equivalent_idx]
                    else:
                        value = 0.0
                else:
                    value = 0.0
            else:
                value = 0.0

            forecast_values.append(float(value))
            forecast_dates.append(forecast_date)

        # Create forecast series with DateTimeIndex
        forecast_series = pd.Series(forecast_values, index=forecast_dates)
        return forecast_series
