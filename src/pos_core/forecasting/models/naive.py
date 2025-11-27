"""Naive Last Week forecast model implementation.

This module implements a simple "naive last week" forecasting model that
uses historical values from the same weekday, avoiding holidays and days
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
        True if date is a weekday (Monday=0 to Friday=4)
    """
    return d.weekday() < 5


def find_equivalent_historical_weekday(
    target_date: date,
    series: pd.Series,
    holidays: set[date],
    max_weeks_back: int = 10,
) -> date | None:
    """Find the most recent historical date with the same weekday, avoiding holidays.

    For each forecast date, finds the "equivalent historical weekday":
    - Same weekday
    - Not a holiday
    - Not adjacent to a holiday (using is_holiday_or_adjacent)
    - Looks back up to N weeks (default: 10)

    Args:
        target_date: Target date to find equivalent for
        series: Historical time series with DateTimeIndex
        holidays: Set of holiday dates to avoid
        max_weeks_back: Maximum number of weeks to look back (default: 10)

    Returns:
        Equivalent historical date, or None if not found
    """
    target_weekday = target_date.weekday()

    # Convert series index to dates for comparison
    # Handle both DatetimeIndex (Timestamp) and date index
    if len(series) == 0:
        return None

    first_index = series.index[0]
    if isinstance(first_index, pd.Timestamp):
        series_dates = {idx.date() for idx in series.index}
    else:
        series_dates = set(series.index)

    # Look back up to max_weeks_back weeks
    for week_offset in range(1, max_weeks_back + 1):
        candidate_date = target_date - timedelta(weeks=week_offset)

        # Check if candidate has the same weekday
        if candidate_date.weekday() != target_weekday:
            continue

        # Check if candidate is not a holiday or adjacent to a holiday
        if is_holiday_or_adjacent(candidate_date, holidays):
            continue

        # Check if candidate exists in the series
        if candidate_date in series_dates:
            return candidate_date

    # If no suitable date found, return None
    return None


class NaiveLastWeekModel(ForecastModel):
    """Naive Last Week forecast model.

    This model uses historical values from the same weekday in previous weeks,
    avoiding holidays and days adjacent to holidays. For each forecast date,
    it finds the most recent historical date with the same weekday that is
    not a holiday and not adjacent to a holiday.
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
        """Train the naive model by storing the training series and holidays.

        Args:
            series: Time series with DateTimeIndex (raw values)
            **kwargs: Additional parameters. Can include 'holidays' (set[date])
                     to specify holiday dates to avoid.

        Returns:
            The model instance itself (for consistency with ForecastModel interface)
        """
        # Store the training series
        self.training_series = series.copy()

        # Extract and store holidays from kwargs
        if "holidays" in kwargs:
            self.holidays = kwargs["holidays"]
        else:
            self.holidays = set()

        # Return self for consistency with ForecastModel interface
        return self

    def forecast(self, model: Any, steps: int, **kwargs: Any) -> pd.Series:
        """Generate forecast using naive last week approach.

        For each forecast date, finds the equivalent historical weekday and
        returns the value from that historical date.

        Args:
            model: Trained model (should be self, for consistency with interface)
            steps: Number of periods to forecast ahead
            **kwargs: Additional parameters. Can include 'last_date' (pd.Timestamp)
                     to specify the last date of the training series for index creation.

        Returns:
            Forecast series with DateTimeIndex

        Raises:
            ValueError: If model has not been trained or insufficient data
        """
        if self.training_series is None or len(self.training_series) == 0:
            raise ValueError("Model has not been trained. Call train() first.")

        # Get last date from kwargs or from training series
        last_date = kwargs.get("last_date")
        if last_date is None:
            last_date = self.training_series.index[-1]

        # Convert to date if it's a Timestamp
        if isinstance(last_date, pd.Timestamp):
            last_date = last_date.date()

        # Generate forecast dates
        forecast_dates = []
        forecast_values = []

        current_date = last_date
        for _ in range(steps):
            current_date = current_date + timedelta(days=1)
            forecast_dates.append(current_date)

            # Find equivalent historical weekday
            equivalent_date = find_equivalent_historical_weekday(
                current_date,
                self.training_series,
                self.holidays,
                max_weeks_back=self.max_weeks_back,
            )

            if equivalent_date is not None:
                # Return value from the historical date
                # Convert equivalent_date to Timestamp if needed for indexing
                if isinstance(self.training_series.index[0], pd.Timestamp):
                    equivalent_timestamp = pd.Timestamp(equivalent_date)
                    if equivalent_timestamp in self.training_series.index:
                        forecast_values.append(float(self.training_series[equivalent_timestamp]))
                    else:
                        # Fallback: use 0.0 if date not found
                        forecast_values.append(0.0)
                else:
                    # Index is already dates
                    if equivalent_date in self.training_series.index:
                        forecast_values.append(float(self.training_series[equivalent_date]))
                    else:
                        forecast_values.append(0.0)
            else:
                # No equivalent date found, use 0.0 as fallback
                forecast_values.append(0.0)

        # Create forecast series with DateTimeIndex
        forecast_index = pd.to_datetime(forecast_dates)
        forecast_series = pd.Series(forecast_values, index=forecast_index)

        return forecast_series
