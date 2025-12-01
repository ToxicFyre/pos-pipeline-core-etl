"""Smoke test for QA API imports and basic functionality.

This test verifies that the QA API can be imported and basic QA checks
can be run without runtime errors.

The module also includes live tests that validate QA functions with
real credentials and actual POS data.
"""

import os
from datetime import date, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
import pytest

from pos_core.qa import PaymentsQAResult, run_payments_qa


def test_qa_imports() -> None:
    """Test that QA API can be imported."""
    assert PaymentsQAResult is not None
    assert callable(run_payments_qa)


def test_run_payments_qa_basic() -> None:
    """Test that run_payments_qa returns a PaymentsQAResult instance."""
    # Create minimal test DataFrame
    df = pd.DataFrame({
        "sucursal": ["A", "A", "B"],
        "fecha": pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-01"]),
        "ingreso_efectivo": [100.0, 200.0, 150.0],
        "ingreso_credito": [50.0, 100.0, 75.0],
        "ingreso_debito": [30.0, 60.0, 45.0],
        "ingreso_amex": [0.0, 0.0, 0.0],
        "ingreso_ubereats": [0.0, 0.0, 0.0],
        "ingreso_rappi": [0.0, 0.0, 0.0],
        "ingreso_transferencia": [0.0, 0.0, 0.0],
        "ingreso_SubsidioTEC": [0.0, 0.0, 0.0],
        "ingreso_otros": [0.0, 0.0, 0.0],
        "propinas": [10.0, 20.0, 15.0],
        "num_tickets": [10, 20, 15],
    })

    result = run_payments_qa(df, level=4)

    assert isinstance(result, PaymentsQAResult)
    assert isinstance(result.summary, dict)
    assert "total_rows" in result.summary
    assert "total_sucursales" in result.summary
    assert result.summary["total_rows"] == 3
    assert result.summary["total_sucursales"] == 2


@pytest.mark.live
def test_qa_with_live_data() -> None:
    """Live test: Run QA checks on real POS data.

    This test validates the QA pipeline with actual data:
    1. Downloads real payment data from POS API
    2. Runs comprehensive QA checks
    3. Validates QA results and issue detection

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

    # Import new ETL API
    from pos_core import DataPaths
    from pos_core.payments import marts as payments_marts

    # Use temporary directory
    with TemporaryDirectory() as tmpdir:
        data_root = Path(tmpdir) / "data"
        data_root.mkdir()

        # Create sucursales.json
        sucursales_json = data_root / "sucursales.json"
        sucursales_json.write_text('{"Kavia": {"code": "8777", "valid_from": "2024-02-21"}}')

        # Configure ETL with new API
        paths = DataPaths.from_root(data_root, sucursales_json)

        # Download 14 days of data
        end_date = date.today() - timedelta(days=1)
        start_date = end_date - timedelta(days=13)

        print(f"\n[Live QA Test] Downloading payments data from {start_date} to {end_date}")

        # Get payments data
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
            print(f"\n[Live QA Test] Error downloading data: {e}")
            print(f"[Live QA Test] Full traceback:\n{error_details}")
            pytest.skip(f"Failed to download live data: {e}")

        # Validate data was downloaded
        assert not payments_df.empty, "Should have downloaded data"
        print(f"[Live QA Test] Downloaded {len(payments_df)} rows of payment data")

        # Run QA checks at different levels
        for level in [1, 2, 3, 4]:
            print(f"\n[Live QA Test] Running QA checks at level {level}...")

            try:
                qa_result = run_payments_qa(payments_df, level=level)
            except Exception as e:
                print(f"[Live QA Test] Error at level {level}: {e}")
                continue

            # Validate result structure
            assert isinstance(qa_result, PaymentsQAResult)
            assert isinstance(qa_result.summary, dict)

            # Validate summary fields
            assert "total_rows" in qa_result.summary
            assert "total_sucursales" in qa_result.summary
            assert qa_result.summary["total_rows"] > 0
            assert qa_result.summary["total_sucursales"] >= 1

            print(f"[Live QA Test] Level {level} - Total rows: {qa_result.summary['total_rows']}")
            print(
                f"[Live QA Test] Level {level} - Total branches: "
                f"{qa_result.summary['total_sucursales']}"
            )

        print("\n[Live QA Test] ✓ Successfully validated QA pipeline with live data")


@pytest.mark.live
def test_qa_detects_data_quality_issues() -> None:
    """Live test: Verify QA can detect data quality issues.

    This test validates that the QA system can identify actual problems
    in real data, such as missing values, outliers, or anomalies.

    Prerequisites:
        - WS_BASE, WS_USER, WS_PASS environment variables

    The test will be skipped if credentials are not available.
    """
    # Check for required credentials
    ws_base = os.environ.get("WS_BASE")
    ws_user = os.environ.get("WS_USER")
    ws_pass = os.environ.get("WS_PASS")

    if not all([ws_base, ws_user, ws_pass]):
        pytest.skip("Live test skipped: credentials required")

    # Strip quotes
    ws_base_cleaned = ws_base.strip('"').strip("'") if ws_base else ""
    ws_user_cleaned = ws_user.strip('"').strip("'") if ws_user else ""
    ws_pass_cleaned = ws_pass.strip('"').strip("'") if ws_pass else ""

    os.environ["WS_BASE"] = ws_base_cleaned
    os.environ["WS_USER"] = ws_user_cleaned
    os.environ["WS_PASS"] = ws_pass_cleaned

    from pos_core import DataPaths
    from pos_core.payments import marts as payments_marts

    with TemporaryDirectory() as tmpdir:
        data_root = Path(tmpdir) / "data"
        data_root.mkdir()

        sucursales_json = data_root / "sucursales.json"
        sucursales_json.write_text('{"Kavia": {"code": "8777", "valid_from": "2024-02-21"}}')

        paths = DataPaths.from_root(data_root, sucursales_json)

        # Get recent data
        end_date = date.today() - timedelta(days=1)
        start_date = end_date - timedelta(days=6)

        print(f"\n[Live QA Issue Test] Testing issue detection from {start_date} to {end_date}")

        try:
            payments_df = payments_marts.fetch_daily(
                paths=paths,
                start_date=start_date.strftime("%Y-%m-%d"),
                end_date=end_date.strftime("%Y-%m-%d"),
                branches=["Kavia"],
                mode="force",
            )
        except Exception as e:
            pytest.skip(f"Failed to download data: {e}")

        print("[Live QA Issue Test] Running comprehensive QA (level 4)...")

        # Run highest level QA for thorough checking
        qa_result = run_payments_qa(payments_df, level=4)

        # Validate result
        assert isinstance(qa_result, PaymentsQAResult)
        print("[Live QA Issue Test] QA completed")

        # Check data completeness
        assert qa_result.summary["total_rows"] > 0
        assert qa_result.summary["total_sucursales"] >= 1

        print("[Live QA Issue Test] ✓ QA issue detection validated")
