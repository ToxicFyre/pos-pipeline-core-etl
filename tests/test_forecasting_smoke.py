"""Smoke test for forecasting API imports and basic functionality.

This test verifies that the forecasting API can be imported and basic forecasting
can be run without runtime errors.
"""

# at the top of tests/test_forecasting_smoke.py
import warnings

import pandas as pd
from statsmodels.tools.sm_exceptions import ConvergenceWarning

from pos_core.forecasting import ForecastConfig, ForecastResult, run_payments_forecast

warnings.filterwarnings("ignore", category=ConvergenceWarning)


def test_forecasting_smoke() -> None:
    """Test that forecasting API can run with minimal synthetic data."""
    # Build a dummy payments_df with 40 days of data for one branch
    # (ARIMA models require at least 30 days)
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

    # Create config with small horizon
    config = ForecastConfig(horizon_days=3, branches=["Kavia"])

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

    # Verify deposit schedule columns
    assert "fecha" in result.deposit_schedule.columns
    assert "efectivo" in result.deposit_schedule.columns
    assert "credito" in result.deposit_schedule.columns
    assert "debito" in result.deposit_schedule.columns

    # Verify metadata
    assert "branches" in result.metadata
    assert "metrics" in result.metadata
    assert "horizon_days" in result.metadata
    assert result.metadata["horizon_days"] == 3


def test_imports_work() -> None:
    """Test that forecasting API can be imported without errors."""
    assert ForecastConfig is not None
    assert ForecastResult is not None
    assert run_payments_forecast is not None
    assert callable(run_payments_forecast)
