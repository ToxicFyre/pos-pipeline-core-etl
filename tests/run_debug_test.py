#!/usr/bin/env python3
"""Run the naive forecasting test and show debug output."""

import os
import sys
from datetime import date, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from pos_core import DataPaths
from pos_core.forecasting.models.naive import NaiveLastWeekModel
from pos_core.payments import marts as payments_marts

# Check for required credentials
ws_base = os.environ.get("WS_BASE")
ws_user = os.environ.get("WS_USER")
ws_pass = os.environ.get("WS_PASS")

if not all([ws_base, ws_user, ws_pass]):
    print("ERROR: WS_BASE, WS_USER, and WS_PASS environment variables required")
    print("Skipping test - credentials not available")
    sys.exit(0)

# Strip quotes from environment variables if present
assert ws_base is not None
assert ws_user is not None
assert ws_pass is not None
ws_base = ws_base.strip('"').strip("'")
ws_user = ws_user.strip('"').strip("'")
ws_pass = ws_pass.strip('"').strip("'")

# Set cleaned values back
os.environ["WS_BASE"] = ws_base
os.environ["WS_USER"] = ws_user
os.environ["WS_PASS"] = ws_pass

# Use a temporary directory for test data
with TemporaryDirectory() as tmpdir:
    data_root = Path(tmpdir) / "data"
    data_root.mkdir()

    # Create a minimal sucursales.json
    utils_dir = data_root.parent / "utils"
    utils_dir.mkdir()
    sucursales_json = utils_dir / "sucursales.json"
    sucursales_json.write_text('{"Kavia": {"code": "8777", "valid_from": "2024-02-21"}}')

    # Configure ETL with new API
    paths = DataPaths.from_root(data_root, sucursales_json)

    # Download 45 days of data
    end_date = date.today() - timedelta(days=1)  # Yesterday
    start_date = end_date - timedelta(days=44)  # 45 days total

    print(f"\n[Live Test] Downloading payments data from {start_date} to {end_date}")

    # Get payments data using new API
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

        print(f"\n[Live Test] Error downloading data: {e}")
        print(f"[Live Test] Full traceback:\n{traceback.format_exc()}")
        sys.exit(1)

    print(f"[Live Test] Downloaded {len(payments_df)} rows of payment data")

    # Ensure fecha is datetime
    if "fecha" in payments_df.columns:
        payments_df["fecha"] = pd.to_datetime(payments_df["fecha"])

    # Add ingreso_total if missing
    if "ingreso_total" not in payments_df.columns:
        payments_df["ingreso_total"] = (
            payments_df["ingreso_efectivo"]
            + payments_df["ingreso_credito"]
            + payments_df["ingreso_debito"]
        )

    # Filter to Kavia branch only
    kavia_df = payments_df[payments_df["sucursal"] == "Kavia"].copy()
    print(f"[Live Test] Kavia branch has {len(kavia_df)} days of data")

    # Test naive forecasting model directly
    kavia_df = kavia_df.sort_values("fecha")
    kavia_df.set_index("fecha", inplace=True)
    efectivo_series = kavia_df["ingreso_efectivo"]

    print(
        f"\n[Live Test] Historical data range: "
        f"{efectivo_series.index.min()} to {efectivo_series.index.max()}"
    )
    print(f"[Live Test] Historical data points: {len(efectivo_series)}")

    # Train naive model
    naive_model = NaiveLastWeekModel()
    model_state = naive_model.train(efectivo_series)

    # Forecast next 7 days
    forecast_series = naive_model.forecast(model_state, steps=7)

    print("\n[Live Test] Generated 7-day forecast:")
    print(forecast_series)

    # Show debug information
    print("\n" + "=" * 80)
    print("DEBUG INFORMATION")
    print("=" * 80)

    assert naive_model.debug_ is not None, "Model should populate debug_ after forecast"

    debug = naive_model.debug_
    print(f"\nModel Name: {debug.model_name}")
    print(f"Version: {debug.version}")
    print(f"\nDebug Data Keys: {list(debug.data.keys())}")

    if "source_dates" in debug.data:
        source_dates = debug.data["source_dates"]
        print("\nSource Dates Mapping (forecast_date -> source_date):")
        print("-" * 80)
        for target_date, source_date in sorted(source_dates.items()):
            target_value = forecast_series.loc[target_date]
            source_value = efectivo_series.loc[source_date]
            print(
                f"  {target_date.date()} -> {source_date.date()}  "
                f"(value: {target_value:.2f} from {source_value:.2f})"
            )

        print(f"\nTotal mappings: {len(source_dates)}")
        print(f"Forecast dates: {len(forecast_series)}")

    if "horizon_steps" in debug.data:
        print(f"\nHorizon Steps: {debug.data['horizon_steps']}")

    print("\n" + "=" * 80)
    print("VALIDATION")
    print("=" * 80)

    # Validate the mapping
    mapping = debug.data["source_dates"]
    for target_date, source_date in mapping.items():
        assert target_date in forecast_series.index
        assert source_date in efectivo_series.index
        assert forecast_series.loc[target_date] == efectivo_series.loc[source_date], (
            f"Forecast for {target_date} should copy value from {source_date}"
        )

    print("\n✓ All validations passed!")
    print("✓ Each forecast date correctly maps to its source date")
    print("✓ Values match exactly between forecast and source")
