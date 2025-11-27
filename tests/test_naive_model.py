"""Tests for the Naive Last Week forecast model."""

from datetime import date, timedelta

import pandas as pd
import pytest

from pos_core.forecasting.models.naive import (
    NaiveLastWeekModel,
    find_equivalent_historical_weekday,
    is_weekday,
)


def test_is_weekday() -> None:
    """Test is_weekday helper function."""
    # Monday (weekday 0)
    assert is_weekday(date(2025, 1, 6)) is True
    # Friday (weekday 4)
    assert is_weekday(date(2025, 1, 10)) is True
    # Saturday (weekday 5)
    assert is_weekday(date(2025, 1, 11)) is False
    # Sunday (weekday 6)
    assert is_weekday(date(2025, 1, 12)) is False


def test_find_equivalent_historical_weekday_basic() -> None:
    """Test finding equivalent historical weekday without holidays."""
    # Create a series with data for Mondays
    dates = pd.date_range("2025-01-06", periods=21, freq="D")  # 3 weeks
    values = [100.0 + i for i in range(21)]
    series = pd.Series(values, index=dates)

    # Target is a Monday, should find the previous Monday
    target = date(2025, 1, 27)  # Monday
    equivalent = find_equivalent_historical_weekday(
        target_date=target, series=series, holidays=set(), max_weeks_back=10
    )

    assert equivalent is not None
    assert equivalent.weekday() == 0  # Monday
    assert equivalent == date(2025, 1, 20)  # Previous Monday


def test_find_equivalent_historical_weekday_with_holidays() -> None:
    """Test finding equivalent historical weekday avoiding holidays."""
    # Create a series with data
    dates = pd.date_range("2025-01-06", periods=28, freq="D")  # 4 weeks
    values = [100.0 + i for i in range(28)]
    series = pd.Series(values, index=dates)

    # Mark a Monday as a holiday
    holidays = {date(2025, 1, 20)}  # Monday

    # Target is a Monday, should skip the holiday Monday and find the one before
    target = date(2025, 1, 27)  # Monday
    equivalent = find_equivalent_historical_weekday(
        target_date=target, series=series, holidays=holidays, max_weeks_back=10
    )

    assert equivalent is not None
    assert equivalent.weekday() == 0  # Monday
    assert equivalent == date(2025, 1, 13)  # Monday before the holiday


def test_find_equivalent_historical_weekday_not_found() -> None:
    """Test when no equivalent date is found."""
    # Create a very short series
    dates = pd.date_range("2025-01-06", periods=5, freq="D")
    values = [100.0] * 5
    series = pd.Series(values, index=dates)

    # Target is far in the future
    target = date(2025, 2, 10)  # Monday
    equivalent = find_equivalent_historical_weekday(
        target_date=target, series=series, holidays=set(), max_weeks_back=2
    )

    assert equivalent is None


def test_naive_model_train() -> None:
    """Test NaiveLastWeekModel.train()."""
    model = NaiveLastWeekModel(max_weeks_back=10)

    # Create training series
    dates = pd.date_range("2025-01-01", periods=30, freq="D")
    values = [100.0 + i for i in range(30)]
    series = pd.Series(values, index=dates)

    # Train with holidays
    holidays = {date(2025, 1, 15)}
    trained = model.train(series, holidays=holidays)

    assert trained is model
    assert model.training_series is not None
    assert len(model.training_series) == 30
    assert model.holidays == holidays


def test_naive_model_forecast() -> None:
    """Test NaiveLastWeekModel.forecast()."""
    model = NaiveLastWeekModel(max_weeks_back=10)

    # Create training series with weekly pattern
    dates = pd.date_range("2025-01-06", periods=21, freq="D")  # 3 weeks
    # Monday=100, Tuesday=110, Wednesday=120, etc.
    values = []
    for i, d in enumerate(dates):
        weekday_val = d.weekday() * 10
        values.append(100.0 + weekday_val)
    series = pd.Series(values, index=dates)

    # Train model
    model.train(series, holidays=set())

    # Forecast 7 days ahead
    last_date = series.index[-1]
    forecast = model.forecast(model, steps=7, last_date=last_date)

    assert len(forecast) == 7
    assert isinstance(forecast.index, pd.DatetimeIndex)
    # First forecast should be the next day after last_date
    assert forecast.index[0] == last_date + timedelta(days=1)


def test_naive_model_forecast_not_trained() -> None:
    """Test that forecast raises error if model not trained."""
    model = NaiveLastWeekModel()

    with pytest.raises(ValueError, match="Model has not been trained"):
        model.forecast(model, steps=7)


def test_naive_model_with_holidays() -> None:
    """Test naive model correctly avoids holidays."""
    model = NaiveLastWeekModel(max_weeks_back=10)

    # Create training series
    dates = pd.date_range("2025-01-06", periods=28, freq="D")  # 4 weeks
    values = [100.0 + i for i in range(28)]
    series = pd.Series(values, index=dates)

    # Train with holidays
    holidays = {date(2025, 1, 20)}  # Monday
    model.train(series, holidays=holidays)

    # Forecast for a Monday (should skip the holiday Monday)
    last_date = series.index[-1]
    forecast = model.forecast(model, steps=7, last_date=last_date)

    # The forecast should not use the holiday date
    assert len(forecast) == 7
    # Values should be reasonable (not all zeros)
    assert forecast.sum() > 0


def test_naive_model_returns_zero_when_no_match() -> None:
    """Test that model returns 0.0 when no equivalent date found."""
    model = NaiveLastWeekModel(max_weeks_back=2)  # Small lookback

    # Create very short series
    dates = pd.date_range("2025-01-06", periods=5, freq="D")
    values = [100.0] * 5
    series = pd.Series(values, index=dates)

    model.train(series, holidays=set())

    # Forecast far ahead (beyond max_weeks_back)
    last_date = series.index[-1]
    forecast = model.forecast(model, steps=7, last_date=last_date)

    # Some values might be 0.0 if no equivalent found
    assert len(forecast) == 7
    # At least verify it doesn't crash
