"""Smoke test for forecasting API imports and basic functionality.

This test verifies that the forecasting API can be imported and basic forecasting
can be run without runtime errors.

The module also includes a live test that uses real credentials to download
a small amount of data and validate the forecasting pipeline end-to-end.
"""

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


def test_run_payments_forecast_exposes_debug_info() -> None:
    """Test that run_payments_forecast exposes debug info when debug=True."""
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

    # Use NaiveLastWeekModel so we can test debug info
    from pos_core.forecasting.models.naive import NaiveLastWeekModel

    config = ForecastConfig(horizon_days=3, branches=["Kavia"], model=NaiveLastWeekModel())

    # Run forecast with debug=True
    result = run_payments_forecast(df, config=config, debug=True)

    # Verify debug info is populated with nested structure
    assert result.debug is not None, "Debug info should be populated when debug=True"
    assert "naive_last_week" in result.debug, "Naive model debug info should be present"
    assert "Kavia" in result.debug["naive_last_week"], "Debug info should be tracked per branch"

    # Check debug info for at least one metric (ingreso_efectivo)
    assert "ingreso_efectivo" in result.debug["naive_last_week"]["Kavia"], (
        "Debug info should be tracked per metric"
    )

    naive_debug = result.debug["naive_last_week"]["Kavia"]["ingreso_efectivo"]
    assert naive_debug.model_name == "naive_last_week"
    assert "source_dates" in naive_debug.data, (
        "Naive model should expose 'source_dates' in debug.data"
    )

    # Verify source_dates mapping structure
    source_dates = naive_debug.data["source_dates"]
    assert isinstance(source_dates, dict)
    assert len(source_dates) == 3, "Should have 3 forecast dates (horizon_days=3)"

    # Verify debug info exists for all metrics that were forecasted
    expected_metrics = {"ingreso_efectivo", "ingreso_credito", "ingreso_debito", "ingreso_total"}
    actual_metrics = set(result.debug["naive_last_week"]["Kavia"].keys())
    missing = expected_metrics - actual_metrics
    assert expected_metrics.issubset(actual_metrics), (
        f"Debug info should exist for all forecasted metrics. Missing: {missing}"
    )


def test_run_payments_forecast_no_debug_by_default() -> None:
    """Test that debug info is None by default (debug=False)."""
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

    config = ForecastConfig(horizon_days=3, branches=["Kavia"])

    # Run forecast without debug flag (defaults to False)
    result = run_payments_forecast(df, config=config)

    # Verify debug info is None
    assert result.debug is None, "Debug info should be None when debug=False (default)"


def test_debug_info_tracks_multiple_branches_and_metrics() -> None:
    """Test that debug info is properly tracked per branch and metric combination."""
    num_days = 40
    kavia_dates = pd.date_range("2025-01-01", periods=num_days, freq="D")
    crediclub_dates = pd.date_range("2025-01-06", periods=num_days, freq="D")

    data = {
        "sucursal": (["Kavia"] * num_days) + (["CrediClub"] * num_days),
        "fecha": list(kavia_dates) + list(crediclub_dates),
        "ingreso_efectivo": list(range(100, 100 + num_days)) * 2,
        "ingreso_credito": list(range(200, 200 + num_days)) * 2,
        "ingreso_debito": list(range(150, 150 + num_days)) * 2,
        "ingreso_total": list(range(450, 450 + num_days)) * 2,
    }
    df = pd.DataFrame(data)

    from pos_core.forecasting.models.naive import NaiveLastWeekModel

    config = ForecastConfig(
        horizon_days=3, branches=["Kavia", "CrediClub"], model=NaiveLastWeekModel()
    )

    # Run forecast with debug=True
    result = run_payments_forecast(df, config=config, debug=True)

    # Verify nested structure exists for both branches
    assert result.debug is not None
    assert "naive_last_week" in result.debug
    assert "Kavia" in result.debug["naive_last_week"]
    assert "CrediClub" in result.debug["naive_last_week"]

    # Verify debug info exists for both branches and multiple metrics
    kavia_metrics = set(result.debug["naive_last_week"]["Kavia"].keys())
    crediclub_metrics = set(result.debug["naive_last_week"]["CrediClub"].keys())

    assert "ingreso_efectivo" in kavia_metrics
    assert "ingreso_efectivo" in crediclub_metrics

    # Verify each branch/metric combination has its own debug info
    kavia_efectivo_debug = result.debug["naive_last_week"]["Kavia"]["ingreso_efectivo"]
    crediclub_efectivo_debug = result.debug["naive_last_week"]["CrediClub"]["ingreso_efectivo"]

    assert kavia_efectivo_debug.model_name == "naive_last_week"
    assert crediclub_efectivo_debug.model_name == "naive_last_week"
    # They should have different source_dates mappings
    kavia_sources = kavia_efectivo_debug.data["source_dates"]
    crediclub_sources = crediclub_efectivo_debug.data["source_dates"]
    assert kavia_sources != crediclub_sources


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
    assert ws_base is not None
    assert ws_user is not None
    assert ws_pass is not None
    ws_base = ws_base.strip('"').strip("'")
    ws_user = ws_user.strip('"').strip("'")
    ws_pass = ws_pass.strip('"').strip("'")

    # Set cleaned values back for use by the ETL functions
    os.environ["WS_BASE"] = ws_base
    os.environ["WS_USER"] = ws_user
    os.environ["WS_PASS"] = ws_pass

    # Import new ETL API
    from pos_core import DataPaths
    from pos_core.forecasting.models.naive import NaiveLastWeekModel
    from pos_core.payments import marts as payments_marts

    # Use a temporary directory for test data
    with TemporaryDirectory() as tmpdir:
        data_root = Path(tmpdir) / "data"
        data_root.mkdir()

        # Create a minimal sucursales.json with a single test branch
        sucursales_json = data_root / "sucursales.json"
        sucursales_json.write_text('{"Kavia": {"code": "8777", "valid_from": "2024-02-21"}}')

        # Configure ETL with new API
        paths = DataPaths.from_root(data_root, sucursales_json)

        # Download 45 days of data (enough for naive model to work)
        end_date = date.today() - timedelta(days=1)  # Yesterday
        start_date = end_date - timedelta(days=44)  # 45 days total

        print(f"\n[Live Test] Downloading payments data from {start_date} to {end_date}")

        # Get payments data (this will download, clean, and aggregate)
        try:
            payments_df = payments_marts.fetch_daily(
                paths=paths,
                start_date=start_date.strftime("%Y-%m-%d"),
                end_date=end_date.strftime("%Y-%m-%d"),
                branches=["Kavia"],
                mode="force",
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
            print("[Live Test] Added ingreso_total column (sum of payment types)")

        # Filter to Kavia branch only
        kavia_df = payments_df[payments_df["sucursal"] == "Kavia"].copy()
        assert not kavia_df.empty, "Should have data for Kavia branch"

        print(f"[Live Test] Kavia branch has {len(kavia_df)} days of data")

        # Test naive forecasting model directly
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

        print("[Live Test] Generated 7-day forecast:")
        print(forecast_series)

        # --- Debug inspection for naive model ---
        assert naive_model.debug_ is not None, "Model should populate debug_ after forecast"

        debug = naive_model.debug_
        assert debug.model_name == "naive_last_week"

        assert "source_dates" in debug.data, "Naive model must expose 'source_dates' in debug.data"
        mapping = debug.data["source_dates"]

        assert isinstance(mapping, dict)
        assert len(mapping) == len(forecast_series)

        for target_date, source_date in mapping.items():
            assert target_date in forecast_series.index
            assert source_date in efectivo_series.index
            assert forecast_series.loc[target_date] == efectivo_series.loc[source_date], (
                f"Forecast for {target_date} should copy value from {source_date}"
            )

        # Test the full forecasting pipeline
        forecast_config = ForecastConfig(horizon_days=7, branches=["Kavia"])
        result = run_payments_forecast(payments_df.reset_index(), config=forecast_config)

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
        assert expected_metrics.issubset(metrics_in_forecast), (
            f"Missing metrics: {expected_metrics - metrics_in_forecast}"
        )

        # Verify forecast values are reasonable (non-negative)
        assert (result.forecast["valor"] >= 0).all(), "All forecast values should be non-negative"

        # Verify metadata
        assert "branches" in result.metadata
        assert "metrics" in result.metadata
        assert "horizon_days" in result.metadata
        assert result.metadata["horizon_days"] == 7

        print("[Live Test] ✓ Successfully validated naive forecasting with live data")
        print(f"[Live Test] Forecast shape: {result.forecast.shape}")
        print(f"[Live Test] Deposit schedule shape: {result.deposit_schedule.shape}")
