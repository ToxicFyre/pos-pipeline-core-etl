"""Smoke test for forecasting API imports and basic functionality.

This test verifies that the forecasting API can be imported and basic forecasting
can be run without runtime errors.

The module also includes a live test that uses real credentials to download
a small amount of data and validate the forecasting pipeline end-to-end.
"""

# at the top of tests/test_forecasting_smoke.py
import os
import warnings
from datetime import date, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
import pytest
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


@pytest.mark.live
def test_naive_forecasting_with_live_data() -> None:
    """Live test: Use real credentials to download data and validate naive forecasting.

    This test uses environment variables to authenticate with the POS API,
    downloads a small amount of real payment data (45 days), and validates
    that the naive forecasting model produces reasonable predictions.

    Prerequisites:
        - WS_BASE: POS API base URL (required)
        - WS_USER: POS username (required)
        - WS_PASS: POS password (required)

    The test will be skipped if credentials are not available.
    """
    # Check for required credentials
    ws_base = os.environ.get("WS_BASE")
    ws_user = os.environ.get("WS_USER")
    ws_pass = os.environ.get("WS_PASS")

    if not all([ws_base, ws_user, ws_pass]):
        pytest.skip(
            "Live test skipped: WS_BASE, WS_USER, and WS_PASS environment variables required"
        )

    # Strip quotes from environment variables if present
    ws_base = ws_base.strip('"').strip("'") if ws_base else None
    ws_user = ws_user.strip('"').strip("'") if ws_user else None
    ws_pass = ws_pass.strip('"').strip("'") if ws_pass else None

    # Set cleaned values back for use by the ETL functions
    os.environ["WS_BASE"] = ws_base
    os.environ["WS_USER"] = ws_user
    os.environ["WS_PASS"] = ws_pass

    # Import ETL functions
    from pos_core.etl import PaymentsETLConfig, get_payments
    from pos_core.forecasting.models.naive import NaiveLastWeekModel

    # Use a temporary directory for test data
    with TemporaryDirectory() as tmpdir:
        data_root = Path(tmpdir) / "data"
        data_root.mkdir()

        # Create a minimal sucursales.json with a single test branch
        # Using CrediClub as a known branch from the codebase
        utils_dir = data_root.parent / "utils"
        utils_dir.mkdir()
        sucursales_json = utils_dir / "sucursales.json"
        sucursales_json.write_text('{"Kavia": {"code": "8777", "valid_from": "2024-02-21"}}')

        # Configure ETL
        config = PaymentsETLConfig.from_root(data_root, sucursales_json)

        # Download 45 days of data (enough for naive model to work)
        end_date = date.today() - timedelta(days=1)  # Yesterday
        start_date = end_date - timedelta(days=44)  # 45 days total

        print(f"\n[Live Test] Downloading payments data from {start_date} to {end_date}")

        # Get payments data (this will download, clean, and aggregate)
        try:
            payments_df = get_payments(
                start_date=start_date.strftime("%Y-%m-%d"),
                end_date=end_date.strftime("%Y-%m-%d"),
                config=config,
                branches=["Kavia"],
                refresh=True,  # Force download
            )
        except Exception as e:
            import traceback
            error_details = traceback.format_exc()
            print(f"\n[Live Test] Error downloading data: {e}")
            print(f"[Live Test] Full traceback:\n{error_details}")
            pytest.skip(f"Failed to download live data: {e}")

        # Validate the downloaded data
        assert not payments_df.empty, "Downloaded payments data should not be empty"
        assert "sucursal" in payments_df.columns
        assert "fecha" in payments_df.columns
        assert "ingreso_efectivo" in payments_df.columns
        assert "ingreso_credito" in payments_df.columns
        assert "ingreso_debito" in payments_df.columns

        print(f"[Live Test] Downloaded {len(payments_df)} rows of payment data")

        # Ensure fecha is datetime
        if "fecha" in payments_df.columns:
            payments_df["fecha"] = pd.to_datetime(payments_df["fecha"])

        # Add ingreso_total if missing (sum of payment types)
        if "ingreso_total" not in payments_df.columns:
            payments_df["ingreso_total"] = (
                payments_df["ingreso_efectivo"]
                + payments_df["ingreso_credito"]
                + payments_df["ingreso_debito"]
            )
            print(f"[Live Test] Added ingreso_total column (sum of payment types)")

        # Filter to Kavia branch only
        kavia_df = payments_df[payments_df["sucursal"] == "Kavia"].copy()
        assert not kavia_df.empty, "Should have data for Kavia branch"

        print(f"[Live Test] Kavia branch has {len(kavia_df)} days of data")

        # Test naive forecasting model directly
        # Create a time series for ingreso_efectivo
        kavia_df = kavia_df.sort_values("fecha")
        kavia_df.set_index("fecha", inplace=True)
        efectivo_series = kavia_df["ingreso_efectivo"]

        # Train naive model
        naive_model = NaiveLastWeekModel()
        model_state = naive_model.train(efectivo_series)

        # Forecast next 7 days
        forecast_series = naive_model.forecast(model_state, steps=7)

        # Validate forecast
        assert len(forecast_series) == 7, "Should forecast 7 days"
        assert all(forecast_series >= 0), "Forecasted values should be non-negative"
        assert not forecast_series.isna().any(), "Forecast should not contain NaN values"

        print(f"[Live Test] Generated 7-day forecast:")
        print(forecast_series)

        # Test the full forecasting pipeline
        forecast_config = ForecastConfig(horizon_days=7, branches=["Kavia"])
        result = run_payments_forecast(payments_df, config=forecast_config)

        # Validate result structure
        assert isinstance(result, ForecastResult)
        assert not result.forecast.empty, "Forecast DataFrame should not be empty"
        assert not result.deposit_schedule.empty, "Deposit schedule should not be empty"

        # Verify forecast columns
        assert "sucursal" in result.forecast.columns
        assert "fecha" in result.forecast.columns
        assert "metric" in result.forecast.columns
        assert "valor" in result.forecast.columns

        # Verify we have forecasts for all payment types
        metrics_in_forecast = set(result.forecast["metric"].unique())
        expected_metrics = {"ingreso_efectivo", "ingreso_credito", "ingreso_debito"}
        assert expected_metrics.issubset(
            metrics_in_forecast
        ), f"Missing metrics: {expected_metrics - metrics_in_forecast}"

        # Verify forecast values are reasonable (non-negative)
        assert (result.forecast["valor"] >= 0).all(), "All forecast values should be non-negative"

        # Verify metadata
        assert "branches" in result.metadata
        assert "metrics" in result.metadata
        assert "horizon_days" in result.metadata
        assert result.metadata["horizon_days"] == 7

        print(f"[Live Test] ✓ Successfully validated naive forecasting with live data")
        print(f"[Live Test] Forecast shape: {result.forecast.shape}")
        print(f"[Live Test] Deposit schedule shape: {result.deposit_schedule.shape}")
