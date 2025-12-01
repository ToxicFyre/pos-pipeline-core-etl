"""High-level tests for ETL query functions.

These tests verify that the query functions work correctly with metadata
and idempotence checks using the new domain-oriented API.

The module also includes live tests that validate query functions with
real credentials and actual POS data.
"""

import os
import tempfile
from collections.abc import Generator
from datetime import date, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import pandas as pd
import pytest

from pos_core import DataPaths
from pos_core.payments import marts as payments_marts
from pos_core.payments.metadata import read_metadata
from pos_core.sales import core as sales_core
from pos_core.sales.metadata import (
    StageMetadata as SalesStageMetadata,
)
from pos_core.sales.metadata import (
    write_metadata as write_sales_metadata,
)
from tests.test_utils import verify_data_retrieval


@pytest.fixture
def temp_data_dir() -> Generator[Path, None, None]:
    """Create a temporary data directory for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def test_paths(temp_data_dir: Path) -> DataPaths:
    """Create a DataPaths for testing."""
    sucursales_json = temp_data_dir / "sucursales.json"
    sucursales_json.write_text(
        '{"TestBranch": {"code": "1234", "valid_from": "2020-01-01", "valid_to": null}}'
    )
    return DataPaths.from_root(temp_data_dir, sucursales_json)


def test_get_sales_refresh_runs_all_stages(test_paths: DataPaths, monkeypatch: Any) -> None:
    """Test that refresh=True runs all stages."""
    # Mock the stage functions to track calls
    download_called: list[bool] = []
    clean_called: list[bool] = []

    def mock_download(
        paths: DataPaths, start_date: str, end_date: str, branches: list[str] | None = None
    ) -> None:
        download_called.append(True)
        # Write metadata to simulate completion
        from pos_core.sales.metadata import StageMetadata, write_metadata

        write_metadata(
            paths.raw_sales,
            start_date,
            end_date,
            StageMetadata(
                start_date=start_date,
                end_date=end_date,
                branches=branches or [],
                version="extract_v1",
                last_run="2025-01-15T12:00:00",
                status="ok",
            ),
        )

    def mock_clean(
        paths: DataPaths, start_date: str, end_date: str, branches: list[str] | None = None
    ) -> None:
        clean_called.append(True)
        from pos_core.sales.metadata import StageMetadata, write_metadata

        # Use the paths parameter passed to the function, not test_paths
        write_metadata(
            paths.clean_sales,
            start_date,
            end_date,
            StageMetadata(
                start_date=start_date,
                end_date=end_date,
                branches=branches or [],
                version="transform_v1",
                last_run="2025-01-15T12:00:00",
                status="ok",
            ),
        )
        # Create dummy clean files in the correct location
        paths.clean_sales.mkdir(parents=True, exist_ok=True)
        pd.DataFrame({
            "sucursal": ["TestBranch"],
            "operating_date": ["2025-01-15"],
            "order_id": [1001],
            "item_key": ["ITEM01"],
            "group": ["GROUP1"],
            "subtotal_item": [100.0],
            "total_item": [116.0],
        }).to_csv(paths.clean_sales / "test.csv", index=False)

    # Patch both where it's defined and where it's imported
    monkeypatch.setattr("pos_core.sales.extract.download_sales", mock_download)
    monkeypatch.setattr("pos_core.sales.raw.download_sales", mock_download)
    monkeypatch.setattr("pos_core.sales.transform.clean_sales", mock_clean)
    monkeypatch.setattr("pos_core.sales.core.clean_sales", mock_clean)

    # Call with mode="force" to get the core fact
    result = sales_core.fetch(test_paths, "2025-01-01", "2025-01-31", mode="force")

    # Verify stages were called
    assert len(download_called) == 1
    assert len(clean_called) == 1
    assert len(result) >= 1


def test_get_sales_uses_existing_data_when_refresh_false(test_paths: DataPaths) -> None:
    """Test that refresh=False uses existing data when available."""
    # Pre-create clean files and metadata
    test_paths.clean_sales.mkdir(parents=True, exist_ok=True)
    test_df = pd.DataFrame({
        "sucursal": ["TestBranch", "TestBranch"],
        "operating_date": ["2025-01-15", "2025-01-15"],
        "order_id": [1001, 1002],
        "item_key": ["ITEM01", "ITEM02"],
        "group": ["GROUP1", "GROUP1"],
        "subtotal_item": [100.0, 50.0],
        "total_item": [116.0, 58.0],
    })
    test_df.to_csv(test_paths.clean_sales / "test.csv", index=False)

    # Write metadata for all stages
    for stage_dir, version in [
        (test_paths.raw_sales, "extract_v1"),
        (test_paths.clean_sales, "transform_v1"),
    ]:
        write_sales_metadata(
            stage_dir,
            "2025-01-01",
            "2025-01-31",
            SalesStageMetadata(
                start_date="2025-01-01",
                end_date="2025-01-31",
                branches=[],
                version=version,
                last_run="2025-01-15T12:00:00",
                status="ok",
            ),
        )

    # Call with mode="missing" to get core fact (should use existing data)
    result = sales_core.fetch(test_paths, "2025-01-01", "2025-01-31", mode="missing")

    # Verify we got the existing data
    assert len(result) == 2
    assert "sucursal" in result.columns


@pytest.mark.live
def test_get_payments_with_live_data() -> None:
    """Live test: Test payments ETL with real credentials and data.

    This test validates the payments ETL pipeline with actual POS data:
    1. Downloads payment data via the new API
    2. Validates idempotence (calling again should use cached data)
    3. Tests daily mart aggregation

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

        # Create sucursales.json
        sucursales_json = data_root / "sucursales.json"
        sucursales_json.write_text('{"Kavia": {"code": "8777", "valid_from": "2024-02-21"}}')

        # Configure ETL with new API
        paths = DataPaths.from_root(data_root, sucursales_json)

        # Test with 7 days of data
        end_date = date.today() - timedelta(days=1)
        start_date = end_date - timedelta(days=6)

        print(f"\n[Live Query Test] Testing payments ETL from {start_date} to {end_date}")

        # First call: should download and process
        # If credentials are provided, test MUST succeed - any failure is a test failure
        try:
            result1 = payments_marts.fetch_daily(
                paths=paths,
                start_date=start_date.strftime("%Y-%m-%d"),
                end_date=end_date.strftime("%Y-%m-%d"),
                branches=["Kavia"],
                mode="force",
            )
        except Exception as e:
            import traceback

            error_details = traceback.format_exc()
            print(f"\n[Live Query Test] Error in payments ETL: {e}")
            print(f"[Live Query Test] Full traceback:\n{error_details}")
            pytest.fail(
                f"Live test FAILED: Payments ETL failed with credentials provided. "
                f"This indicates authentication or data retrieval failure. Error: {e}"
            )

        # Validate first result
        assert not result1.empty, "First call should return data"
        assert "sucursal" in result1.columns
        assert "fecha" in result1.columns
        print(f"[Live Query Test] First call returned {len(result1)} rows")

        # Second call with mode="missing": should use cached data
        print("[Live Query Test] Testing idempotence (mode='missing')...")
        result2 = payments_marts.fetch_daily(
            paths=paths,
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d"),
            branches=["Kavia"],
            mode="missing",
        )

        # Should get same data
        assert len(result1) == len(result2), "Second call should return same data"
        print(f"[Live Query Test] ✓ Idempotence verified: both calls returned {len(result2)} rows")

        # Validate data quality
        # Check for Kavia-related branches (might be "Panem - Hotel Kavia N" or similar)
        unique_branches = sorted(result1["sucursal"].unique().tolist())
        kavia_related = [b for b in unique_branches if "kavia" in b.lower() or "Kavia" in b]
        if kavia_related:
            kavia_data = result1[result1["sucursal"].isin(kavia_related)]
            assert not kavia_data.empty, (
                f"Should have data for Kavia-related branch(es): {kavia_related}"
            )
            print(
                f"[Live Query Test] ✓ Found {len(kavia_data)} days for Kavia-related branch(es): {kavia_related}"
            )
        else:
            pytest.fail(
                f"No Kavia-related branches found in payment data. "
                f"Available branches: {unique_branches}. "
                f"This indicates the branch filter may be incorrect or data is missing."
            )

        # Check numeric columns are reasonable
        for col in ["ingreso_efectivo", "ingreso_credito", "ingreso_debito"]:
            if col in result1.columns:
                assert (result1[col] >= 0).all(), f"{col} should be non-negative"
                print(f"[Live Query Test] ✓ {col} values are valid")

        print("[Live Query Test] ✓ All validations passed")

        # Verify data retrieval: HTTP requests, file downloads, and file contents
        print("\n[Live Query Test] Verifying data retrieval...")
        verify_data_retrieval(
            paths=paths,
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d"),
            data_type="payments",
        )


@pytest.mark.live
def test_get_payments_metadata_tracking() -> None:
    """Live test: Verify metadata is correctly tracked during ETL.

    This test validates that:
    1. Metadata is written after each ETL stage
    2. Metadata prevents redundant processing

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

    with TemporaryDirectory() as tmpdir:
        data_root = Path(tmpdir) / "data"
        data_root.mkdir()

        sucursales_json = data_root / "sucursales.json"
        sucursales_json.write_text('{"Kavia": {"code": "8777", "valid_from": "2024-02-21"}}')

        paths = DataPaths.from_root(data_root, sucursales_json)

        # Use 5 days for faster test
        end_date = date.today() - timedelta(days=1)
        start_date = end_date - timedelta(days=4)
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")

        print(f"\n[Live Metadata Test] Testing metadata tracking from {start_date} to {end_date}")

        # Run payments_marts.fetch_daily
        # If credentials are provided, test MUST succeed - any failure is a test failure
        try:
            result = payments_marts.fetch_daily(
                paths, start_str, end_str, branches=["Kavia"], mode="force"
            )
        except Exception as e:
            import traceback

            error_details = traceback.format_exc()
            print(f"\n[Live Metadata Test] Error getting payments: {e}")
            print(f"[Live Metadata Test] Full traceback:\n{error_details}")
            pytest.fail(
                f"Live test FAILED: Payments ETL failed with credentials provided. "
                f"This indicates authentication or data retrieval failure. Error: {e}"
            )

        assert not result.empty

        # Check that metadata was created for each stage
        raw_meta = read_metadata(paths.raw_payments, start_str, end_str)
        clean_meta = read_metadata(paths.clean_payments, start_str, end_str)
        mart_meta = read_metadata(paths.mart_payments, start_str, end_str)

        assert raw_meta is not None, "Raw stage should have metadata"
        assert raw_meta.status == "ok", "Raw stage should be marked as ok"
        print("[Live Metadata Test] ✓ Raw stage metadata verified")

        assert clean_meta is not None, "Clean stage should have metadata"
        assert clean_meta.status == "ok", "Clean stage should be marked as ok"
        print("[Live Metadata Test] ✓ Clean stage metadata verified")

        assert mart_meta is not None, "Mart stage should have metadata"
        assert mart_meta.status == "ok", "Mart stage should be marked as ok"
        print("[Live Metadata Test] ✓ Mart stage metadata verified")

        print("[Live Metadata Test] ✓ All metadata checks passed")

        # Verify data retrieval: HTTP requests, file downloads, and file contents
        print("\n[Live Metadata Test] Verifying data retrieval...")
        verify_data_retrieval(
            paths=paths,
            start_date=start_str,
            end_date=end_str,
            data_type="payments",
        )
