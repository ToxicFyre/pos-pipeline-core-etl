"""Tests for the naive forecasting model.

This test verifies that the NaiveLastWeekModel can be used with the forecasting API
and produces reasonable forecasts.
"""

import pandas as pd
from datetime import date

from pos_core.forecasting import ForecastConfig, ForecastResult, run_payments_forecast


def test_naive_model_smoke() -> None:
    """Test that naive model can run with minimal synthetic data."""
    # Build a dummy payments_df with 40 days of data for one branch
    num_days = 40
    data = {
        "sucursal": ["Kavia"] * num_days,
        "fecha": pd.date_range("2025-01-01", periods=num_days, freq="D"),
        "ingreso_efectivo": range(100, 100 + num_days),
        "ingreso_credito": range(200, 200 + num_days),
        "ingreso_debito": range(150, 150 + num_days),
        "ingreso_total": range(450, 450 + num_days),
        "is_national_holiday": [False] * num_days,  # No holidays for simple test
    }
    df = pd.DataFrame(data)

    # Create config with naive model
    config = ForecastConfig(horizon_days=3, branches=["Kavia"], model_type="naive")

    # Run forecast
    result = run_payments_forecast(df, config=config)

    # Verify result structure
    assert isinstance(result, ForecastResult)
    assert not result.forecast.empty, "Forecast DataFrame should not be empty"
    assert not result.deposit_schedule.empty, "Deposit schedule DataFrame should not be empty"

    # Verify forecast columns
    assert "sucursal" in result.forecast.columns
    assert "fecha" in result.forecast.columns
    assert "metric" in result.forecast.columns
    assert "valor" in result.forecast.columns

    # Verify we got forecasts for all metrics
    forecasted_metrics = set(result.forecast["metric"].unique())
    expected_metrics = {
        "ingreso_efectivo",
        "ingreso_credito",
        "ingreso_debito",
        "ingreso_total",
    }
    assert forecasted_metrics == expected_metrics

    # Verify metadata
    assert "branches" in result.metadata
    assert "metrics" in result.metadata
    assert "horizon_days" in result.metadata
    assert result.metadata["horizon_days"] == 3
    assert result.metadata["successful_forecasts"] > 0


def test_naive_model_with_holidays() -> None:
    """Test that naive model correctly handles holidays."""
    # Build a dummy payments_df with holidays
    num_days = 50
    dates = pd.date_range("2025-01-01", periods=num_days, freq="D")
    
    # Mark Jan 1 as a holiday
    is_holiday = [d.day == 1 and d.month == 1 for d in dates]
    
    data = {
        "sucursal": ["Kavia"] * num_days,
        "fecha": dates,
        "ingreso_efectivo": range(100, 100 + num_days),
        "ingreso_credito": range(200, 200 + num_days),
        "ingreso_debito": range(150, 150 + num_days),
        "ingreso_total": range(450, 450 + num_days),
        "is_national_holiday": is_holiday,
    }
    df = pd.DataFrame(data)

    # Create config with naive model
    config = ForecastConfig(horizon_days=7, branches=["Kavia"], model_type="naive")

    # Run forecast
    result = run_payments_forecast(df, config=config)

    # Verify result structure
    assert isinstance(result, ForecastResult)
    assert not result.forecast.empty, "Forecast DataFrame should not be empty"
    
    # Verify we got forecasts
    assert result.metadata["successful_forecasts"] > 0


def test_arima_model_still_works() -> None:
    """Test that ARIMA model still works with the new model_type parameter."""
    # Build a dummy payments_df
    num_days = 40
    data = {
        "sucursal": ["Kavia"] * num_days,
        "fecha": pd.date_range("2025-01-01", periods=num_days, freq="D"),
        "ingreso_efectivo": range(100, 100 + num_days),
        "ingreso_credito": range(200, 200 + num_days),
        "ingreso_debito": range(150, 150 + num_days),
        "ingreso_total": range(450, 450 + num_days),
    }
    df = pd.DataFrame(data)

    # Create config with explicit "arima" model (should be default)
    config = ForecastConfig(horizon_days=3, branches=["Kavia"], model_type="arima")

    # Run forecast
    result = run_payments_forecast(df, config=config)

    # Verify result structure
    assert isinstance(result, ForecastResult)
    assert not result.forecast.empty


def test_default_model_is_arima() -> None:
    """Test that default model is still ARIMA for backward compatibility."""
    num_days = 40
    data = {
        "sucursal": ["Kavia"] * num_days,
        "fecha": pd.date_range("2025-01-01", periods=num_days, freq="D"),
        "ingreso_efectivo": range(100, 100 + num_days),
        "ingreso_credito": range(200, 200 + num_days),
        "ingreso_debito": range(150, 150 + num_days),
        "ingreso_total": range(450, 450 + num_days),
    }
    df = pd.DataFrame(data)

    # Create config without specifying model_type (should default to "arima")
    config = ForecastConfig(horizon_days=3, branches=["Kavia"])

    # Run forecast
    result = run_payments_forecast(df, config=config)

    # Verify result structure (should work as before)
    assert isinstance(result, ForecastResult)
    assert not result.forecast.empty


def test_invalid_model_type_raises_error() -> None:
    """Test that invalid model_type raises ValueError."""
    num_days = 40
    data = {
        "sucursal": ["Kavia"] * num_days,
        "fecha": pd.date_range("2025-01-01", periods=num_days, freq="D"),
        "ingreso_efectivo": range(100, 100 + num_days),
        "ingreso_credito": range(200, 200 + num_days),
        "ingreso_debito": range(150, 150 + num_days),
        "ingreso_total": range(450, 450 + num_days),
    }
    df = pd.DataFrame(data)

    # Create config with invalid model_type
    config = ForecastConfig(horizon_days=3, branches=["Kavia"], model_type="invalid")

    # Run forecast and expect ValueError
    try:
        result = run_payments_forecast(df, config=config)
        assert False, "Expected ValueError for invalid model_type"
    except ValueError as e:
        assert "Unknown model_type" in str(e)
        assert "invalid" in str(e)
