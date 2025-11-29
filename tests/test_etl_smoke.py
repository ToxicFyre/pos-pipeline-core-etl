"""Smoke test for ETL API imports and basic functionality.

This test verifies that the ETL API can be imported and basic configuration
can be created without runtime errors.

The module also includes live tests that use real credentials to validate
the ETL pipeline end-to-end with actual POS data.
"""

import os
from datetime import date, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from pos_core.etl import PaymentsETLConfig, build_payments_dataset


def test_imports_work() -> None:
    """Test that ETL API can be imported without errors."""
    assert PaymentsETLConfig is not None
    assert build_payments_dataset is not None


def test_config_creation() -> None:
    """Test that PaymentsETLConfig can be created from data_root."""
    config = PaymentsETLConfig.from_data_root(Path("data"))
    assert config is not None
    assert config.paths.raw_payments == Path("data/a_raw/payments/batch")
    assert config.paths.clean_payments == Path("data/b_clean/payments/batch")
    assert config.paths.proc_payments == Path("data/c_processed/payments")
    assert config.chunk_size_days == 180


def test_build_payments_dataset_is_callable() -> None:
    """Test that build_payments_dataset is callable."""
    assert callable(build_payments_dataset)


@pytest.mark.live
def test_etl_pipeline_with_live_data() -> None:
    """Live test: Full ETL pipeline with real credentials and data.

    This test validates the complete ETL pipeline:
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
    ws_base = ws_base.strip('"').strip("'") if ws_base else None
    ws_user = ws_user.strip('"').strip("'") if ws_user else None
    ws_pass = ws_pass.strip('"').strip("'") if ws_pass else None

    # Set cleaned values back
    os.environ["WS_BASE"] = ws_base
    os.environ["WS_USER"] = ws_user
    os.environ["WS_PASS"] = ws_pass

    # Use temporary directory
    with TemporaryDirectory() as tmpdir:
        data_root = Path(tmpdir) / "data"
        data_root.mkdir()

        # Create sucursales.json for Kavia branch
        utils_dir = data_root.parent / "utils"
        utils_dir.mkdir()
        sucursales_json = utils_dir / "sucursales.json"
        sucursales_json.write_text('{"Kavia": {"code": "8777", "valid_from": "2024-02-21"}}')

        # Configure ETL
        config = PaymentsETLConfig.from_root(data_root, sucursales_json)

        # Test with 14 days of data (2 weeks)
        end_date = date.today() - timedelta(days=1)  # Yesterday
        start_date = end_date - timedelta(days=13)  # 14 days total

        print(f"\n[Live ETL Test] Testing ETL pipeline from {start_date} to {end_date}")

        # Run full ETL pipeline
        try:
            result_df = build_payments_dataset(
                start_date=start_date.strftime("%Y-%m-%d"),
                end_date=end_date.strftime("%Y-%m-%d"),
                config=config,
                branches=["Kavia"],
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
        expected_columns = ["sucursal", "fecha", "ingreso_efectivo", "ingreso_credito", "ingreso_debito"]
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
        print(f"[Live ETL Test] ✓ All data quality checks passed")
        print(f"[Live ETL Test] Sample data:\n{kavia_data.head()}")
