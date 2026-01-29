"""Smoke tests for transfers ETL API.

This module includes both unit tests and live tests for the transfers pipeline.
Live tests require WS_BASE, WS_USER, and WS_PASS environment variables.
"""

import os
from datetime import date, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from pos_core import DataPaths
from pos_core.transfers import core as transfers_core
from pos_core.transfers import marts as transfers_marts
from pos_core.transfers import raw as transfers_raw


def test_transfers_imports_work() -> None:
    """Test that transfers API can be imported without errors."""
    assert DataPaths is not None
    assert transfers_raw.fetch is not None
    assert transfers_core.fetch is not None
    assert transfers_marts.fetch_pivot is not None


def test_transfers_fetch_is_callable() -> None:
    """Test that transfers.core.fetch is callable."""
    assert callable(transfers_core.fetch)
    assert callable(transfers_marts.fetch_pivot)


def test_transfers_fetch_invalid_mode() -> None:
    """Test that transfers.core.fetch raises on invalid mode."""
    with TemporaryDirectory() as tmpdir:
        paths = DataPaths.from_root(Path(tmpdir), Path(tmpdir) / "sucursales.json")
        (Path(tmpdir) / "sucursales.json").write_text("{}")
        with pytest.raises(ValueError, match="Invalid mode"):
            transfers_core.fetch(paths, "2025-01-01", "2025-01-31", mode="invalid")


def test_datapaths_has_transfer_properties() -> None:
    """Test that DataPaths has transfer-related properties."""
    paths = DataPaths.from_root(Path("data"), Path("utils/sucursales.json"))
    assert paths.raw_transfers == Path("data/a_raw/transfers/batch")
    assert paths.clean_transfers == Path("data/b_clean/transfers/batch")
    assert paths.mart_transfers == Path("data/c_processed/transfers")


@pytest.mark.live
def test_transfers_pipeline_with_live_data() -> None:
    """Live test: Full transfers ETL pipeline with real credentials.

    This test validates the complete transfers ETL pipeline:
    1. Download raw transfer reports from POS API (Inventory > Transfers > Issued)
    2. Clean and transform the data
    3. Aggregate into pivot mart
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

    # Use temporary directory (ignore cleanup errors on Windows due to file locking)
    with TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        data_root = Path(tmpdir) / "data"
        data_root.mkdir()

        # Create sucursales.json - use CEDIS for transfers
        # Transfers are typically issued FROM CEDIS to other branches
        # IMPORTANT: valid_from must be BEFORE start_date for the branch to be processed!
        sucursales_json = data_root / "sucursales.json"
        sucursales_json.write_text(
            '{"CEDIS": {"code": "5392", "valid_from": "2020-01-01", "valid_to": null}}'
        )

        # Configure ETL with new API
        paths = DataPaths.from_root(data_root, sucursales_json)

        # Test with 7 days of data
        end_date = date.today() - timedelta(days=1)  # Yesterday
        start_date = end_date - timedelta(days=6)  # 7 days total

        print(f"\n[Live Transfers Test] Testing pipeline from {start_date} to {end_date}")

        # Step 1: Test raw extraction
        try:
            print("[Live Transfers Test] Step 1: Downloading raw transfers...")
            transfers_raw.fetch(
                paths=paths,
                start_date=start_date.strftime("%Y-%m-%d"),
                end_date=end_date.strftime("%Y-%m-%d"),
                mode="force",
            )
            print("[Live Transfers Test] Raw download completed.")
        except Exception as e:
            import traceback

            error_details = traceback.format_exc()
            print(f"[Live Transfers Test] Error downloading: {e}")
            print(f"[Live Transfers Test] Traceback:\n{error_details}")
            pytest.fail(f"Live test FAILED: Raw download failed. Error: {e}")

        # Verify raw files exist
        raw_files = list(paths.raw_transfers.rglob("*.xlsx"))
        print(f"[Live Transfers Test] Found {len(raw_files)} raw Excel files")
        if not raw_files:
            pytest.fail("Live test FAILED: No raw Excel files downloaded")

        # Step 2: Test core fact (cleaning)
        try:
            print("[Live Transfers Test] Step 2: Cleaning transfers...")
            core_df = transfers_core.fetch(
                paths=paths,
                start_date=start_date.strftime("%Y-%m-%d"),
                end_date=end_date.strftime("%Y-%m-%d"),
                mode="force",
            )
            print(f"[Live Transfers Test] Core fact has {len(core_df)} rows")
            print(f"[Live Transfers Test] Core fact columns: {list(core_df.columns)}")
        except Exception as e:
            import traceback

            error_details = traceback.format_exc()
            print(f"[Live Transfers Test] Error cleaning: {e}")
            print(f"[Live Transfers Test] Traceback:\n{error_details}")
            pytest.fail(f"Live test FAILED: Cleaning failed. Error: {e}")

        # Verify core fact structure
        assert not core_df.empty, "Core fact DataFrame is empty"
        required_columns = ["Almacén origen", "Sucursal destino", "Departamento", "Costo"]
        for col in required_columns:
            assert col in core_df.columns, f"Missing required column: {col}"

        # Print sample of destination branches for debugging
        if "Sucursal destino" in core_df.columns:
            unique_destinations = core_df["Sucursal destino"].unique()
            print(f"[Live Transfers Test] Unique destination branches: {unique_destinations[:10]}")

        # Print sample of origin warehouses for debugging
        if "Almacén origen" in core_df.columns:
            unique_origins = core_df["Almacén origen"].unique()
            print(f"[Live Transfers Test] Unique origin warehouses: {unique_origins[:10]}")

        # Print sample of departments for debugging
        if "Departamento" in core_df.columns:
            unique_depts = core_df["Departamento"].unique()
            print(f"[Live Transfers Test] Unique departments: {unique_depts[:10]}")

        # Step 3: Test pivot mart (aggregation)
        try:
            print("[Live Transfers Test] Step 3: Building pivot mart...")
            pivot_df = transfers_marts.fetch_pivot(
                paths=paths,
                start_date=start_date.strftime("%Y-%m-%d"),
                end_date=end_date.strftime("%Y-%m-%d"),
                mode="force",
                include_cedis=True,  # Include CEDIS to see all data
            )
            print(f"[Live Transfers Test] Pivot mart shape: {pivot_df.shape}")
            print(f"[Live Transfers Test] Pivot mart:\n{pivot_df}")
        except Exception as e:
            import traceback

            error_details = traceback.format_exc()
            print(f"[Live Transfers Test] Error building pivot: {e}")
            print(f"[Live Transfers Test] Traceback:\n{error_details}")
            pytest.fail(f"Live test FAILED: Pivot aggregation failed. Error: {e}")

        # Verify pivot mart file was saved (with date-stamped filename)
        mart_file = paths.mart_transfers / f"mart_transfers_pivot_{start_date}_{end_date}.csv"
        assert mart_file.exists(), f"Mart file not found: {mart_file}"

        print("[Live Transfers Test] All steps completed successfully!")


@pytest.mark.live
def test_transfers_debug_branch_mapping() -> None:
    """Live test: Debug branch name mapping in transfers.

    This test helps identify which branch names are being returned
    from the POS system so the SUC_MAP can be updated if needed.
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

    os.environ["WS_BASE"] = ws_base_cleaned
    os.environ["WS_USER"] = ws_user_cleaned
    os.environ["WS_PASS"] = ws_pass_cleaned

    with TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        data_root = Path(tmpdir) / "data"
        data_root.mkdir()

        sucursales_json = data_root / "sucursales.json"
        sucursales_json.write_text(
            '{"CEDIS": {"code": "5392", "valid_from": "2020-01-01", "valid_to": null}}'
        )

        paths = DataPaths.from_root(data_root, sucursales_json)

        # Just get 3 days for quick debugging
        end_date = date.today() - timedelta(days=1)
        start_date = end_date - timedelta(days=2)

        print(f"\n[Debug Branch Mapping] Date range: {start_date} to {end_date}")

        # Get core fact
        try:
            core_df = transfers_core.fetch(
                paths=paths,
                start_date=start_date.strftime("%Y-%m-%d"),
                end_date=end_date.strftime("%Y-%m-%d"),
                mode="force",
            )
        except Exception as e:
            pytest.fail(f"Failed to fetch core fact: {e}")

        if core_df.empty:
            pytest.skip("No transfer data found for the date range")

        # Analyze the data
        print("\n=== BRANCH MAPPING DEBUG ===")

        # Current SUC_MAP from transfers.py
        from pos_core.etl.marts.transfers import SUC_MAP

        print(f"\nCurrent SUC_MAP keys: {list(SUC_MAP.keys())}")

        # Actual destination branches in data
        if "Sucursal destino" in core_df.columns:
            actual_destinations = core_df["Sucursal destino"].str.strip().str.upper().unique()
            print(f"\nActual destination branches in data ({len(actual_destinations)}):")
            for dest in sorted(actual_destinations):
                mapped_to = SUC_MAP.get(dest, "NOT MAPPED")
                print(f"  '{dest}' -> {mapped_to}")

        # Actual origin warehouses
        if "Almacén origen" in core_df.columns:
            actual_origins = core_df["Almacén origen"].str.strip().str.upper().unique()
            print(f"\nActual origin warehouses in data ({len(actual_origins)}):")
            for orig in sorted(actual_origins):
                print(f"  '{orig}'")

        # Actual departments
        if "Departamento" in core_df.columns:
            actual_depts = core_df["Departamento"].str.strip().str.upper().unique()
            print(f"\nActual departments in data ({len(actual_depts)}):")
            for dept in sorted(actual_depts):
                print(f"  '{dept}'")

        # Summary
        print("\n=== MAPPING SUMMARY ===")
        if "Sucursal destino" in core_df.columns:
            mapped_count = 0
            unmapped_count = 0
            for dest in core_df["Sucursal destino"].str.strip().str.upper():
                if dest in SUC_MAP:
                    mapped_count += 1
                else:
                    unmapped_count += 1
            total = mapped_count + unmapped_count
            print(f"Mapped rows: {mapped_count} / {total} ({100 * mapped_count / total:.1f}%)")
            print(
                f"Unmapped rows: {unmapped_count} / {total} ({100 * unmapped_count / total:.1f}%)"
            )

        print("\n=== END DEBUG ===")
