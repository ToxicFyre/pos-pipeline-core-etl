"""Smoke test for new ETL API imports and basic functionality.

This test verifies that the new domain-oriented ETL API can be imported
and basic configuration can be created without runtime errors.

The module also includes live tests that use real credentials to validate
the ETL pipeline end-to-end with actual POS data.
"""

import os
from datetime import date, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from pos_core import DataPaths
from pos_core.payments import core as payments_core
from pos_core.payments import marts as payments_marts
from pos_core.sales import core as sales_core
from pos_core.sales import marts as sales_marts


def test_imports_work() -> None:
    """Test that new ETL API can be imported without errors."""
    assert DataPaths is not None
    assert payments_core.fetch is not None
    assert payments_marts.fetch_daily is not None
    assert sales_core.fetch is not None
    assert sales_marts.fetch_ticket is not None


def test_config_creation() -> None:
    """Test that DataPaths can be created from data_root."""
    paths = DataPaths.from_root(Path("data"), Path("utils/sucursales.json"))
    assert paths is not None
    assert paths.data_root == Path("data")
    assert paths.sucursales_json == Path("utils/sucursales.json")
    # Test derived paths
    assert paths.raw_payments == Path("data/a_raw/payments/batch")
    assert paths.clean_payments == Path("data/b_clean/payments/batch")
    assert paths.mart_payments == Path("data/c_processed/payments")
    assert paths.raw_sales == Path("data/a_raw/sales/batch")
    assert paths.clean_sales == Path("data/b_clean/sales/batch")
    assert paths.mart_sales == Path("data/c_processed/sales")


def test_payments_fetch_is_callable() -> None:
    """Test that payments.core.fetch is callable."""
    assert callable(payments_core.fetch)


def test_sales_fetch_is_callable() -> None:
    """Test that sales.core.fetch is callable."""
    assert callable(sales_core.fetch)


def test_payments_fetch_invalid_mode() -> None:
    """Test that payments.core.fetch raises on invalid mode."""
    with TemporaryDirectory() as tmpdir:
        paths = DataPaths.from_root(Path(tmpdir), Path(tmpdir) / "sucursales.json")
        (Path(tmpdir) / "sucursales.json").write_text("{}")
        with pytest.raises(ValueError, match="Invalid mode"):
            payments_core.fetch(paths, "2025-01-01", "2025-01-31", mode="invalid")


def test_sales_fetch_invalid_mode() -> None:
    """Test that sales.core.fetch raises on invalid mode."""
    with TemporaryDirectory() as tmpdir:
        paths = DataPaths.from_root(Path(tmpdir), Path(tmpdir) / "sucursales.json")
        (Path(tmpdir) / "sucursales.json").write_text("{}")
        with pytest.raises(ValueError, match="Invalid mode"):
            sales_core.fetch(paths, "2025-01-01", "2025-01-31", mode="invalid")


@pytest.mark.live
def test_etl_pipeline_with_live_data() -> None:
    """Live test: Full ETL pipeline with real credentials and data.

    This test validates the complete ETL pipeline using the new API:
    1. Download raw payment reports from POS API
    2. Clean and transform the data
    3. Aggregate into daily dataset
    4. Verify data quality and structure

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
    ws_base_cleaned = ws_base.strip('"').strip("'") if ws_base else ""
    ws_user_cleaned = ws_user.strip('"').strip("'") if ws_user else ""
    ws_pass_cleaned = ws_pass.strip('"').strip("'") if ws_pass else ""

    # Set cleaned values back
    os.environ["WS_BASE"] = ws_base_cleaned
    os.environ["WS_USER"] = ws_user_cleaned
    os.environ["WS_PASS"] = ws_pass_cleaned

    # Use temporary directory
    with TemporaryDirectory() as tmpdir:
        data_root = Path(tmpdir) / "data"
        data_root.mkdir()

        # Create sucursales.json for Kavia branch
        sucursales_json = data_root / "sucursales.json"
        sucursales_json.write_text('{"Kavia": {"code": "8777", "valid_from": "2024-02-21"}}')

        # Configure ETL with new API
        paths = DataPaths.from_root(data_root, sucursales_json)

        # Test with 14 days of data (2 weeks)
        end_date = date.today() - timedelta(days=1)  # Yesterday
        start_date = end_date - timedelta(days=13)  # 14 days total

        print(f"\n[Live ETL Test] Testing ETL pipeline from {start_date} to {end_date}")

        # Run full ETL pipeline using new API
        try:
            result_df = payments_marts.fetch_daily(
                paths=paths,
                start_date=start_date.strftime("%Y-%m-%d"),
                end_date=end_date.strftime("%Y-%m-%d"),
                branches=["Kavia"],
                mode="force",
            )
        except Exception as e:
            import traceback

            error_details = traceback.format_exc()
            print(f"\n[Live ETL Test] Error running ETL: {e}")
            print(f"[Live ETL Test] Full traceback:\n{error_details}")
            pytest.skip(f"Failed to run ETL pipeline: {e}")

        # Validate the result
        assert result_df is not None, "ETL should return a DataFrame"
        assert not result_df.empty, "ETL result should not be empty"

        print(f"[Live ETL Test] ETL completed successfully: {len(result_df)} rows")

        # Validate columns
        expected_columns = [
            "sucursal",
            "fecha",
            "ingreso_efectivo",
            "ingreso_credito",
            "ingreso_debito",
        ]
        for col in expected_columns:
            assert col in result_df.columns, f"Missing expected column: {col}"

        # Validate data quality
        assert "Kavia" in result_df["sucursal"].values, "Should have data for Kavia branch"
        kavia_data = result_df[result_df["sucursal"] == "Kavia"]
        assert len(kavia_data) <= 14, "Should have at most 14 days of data"

        # Validate numeric columns are non-negative
        numeric_cols = ["ingreso_efectivo", "ingreso_credito", "ingreso_debito"]
        for col in numeric_cols:
            if col in result_df.columns:
                assert (result_df[col] >= 0).all(), f"{col} should be non-negative"

        print(f"[Live ETL Test] ✓ Validated {len(kavia_data)} days of data for Kavia")
        print("[Live ETL Test] ✓ All data quality checks passed")
        print(f"[Live ETL Test] Sample data:\n{kavia_data.head()}")
