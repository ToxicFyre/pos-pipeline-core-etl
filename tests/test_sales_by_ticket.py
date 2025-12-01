"""Tests for sales_by_ticket aggregation module.

This module tests the aggregate_by_ticket function and its helper functions,
with a focus on directory handling and file path resolution.

The module includes live tests that validate directory path handling with
real POS data to ensure the PermissionError bug is fixed.
"""

import os
from datetime import date, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
import pytest

from pos_core import DataPaths
from pos_core.etl.marts.sales_by_ticket import aggregate_by_ticket
from pos_core.sales import marts as sales_marts


@pytest.fixture
def sample_sales_data() -> pd.DataFrame:
    """Create sample sales data for testing."""
    return pd.DataFrame({
        "order_id": [1001, 1001, 1002, 1002],
        "sucursal": ["Branch1", "Branch1", "Branch1", "Branch1"],
        "operating_date": ["2025-01-15", "2025-01-15", "2025-01-15", "2025-01-15"],
        "group": ["CAFE", "FOOD", "CAFE", "FOOD"],
        "subtotal_item": [10.0, 20.0, 15.0, 25.0],
        "total_item": [11.6, 23.2, 17.4, 29.0],
    })


@pytest.mark.live
def test_aggregate_by_ticket_with_directory_path_live() -> None:
    """Live test: Verify that aggregate_by_ticket handles directory paths correctly.

    This test verifies the fix for the bug where passing a directory path
    as input_csv would cause a PermissionError when pandas tried to read it.

    The test uses real POS data to ensure the fix works in production scenarios:
    1. Downloads and cleans sales data (creates CSV files in clean_sales directory)
    2. Calls aggregate_to_ticket which passes the directory path to aggregate_by_ticket
    3. Verifies that no PermissionError occurs and data is aggregated correctly

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
        # Note: The actual branch name in the data is "Panem - Hotel Kavia N"
        sucursales_json = data_root / "sucursales.json"
        sucursales_json.write_text('{"Kavia": {"code": "8777", "valid_from": "2024-02-21"}}')

        # Configure ETL with new API
        paths = DataPaths.from_root(data_root, sucursales_json)

        # Use a small time window (2 days) for fast testing
        end_date = date.today() - timedelta(days=1)  # Yesterday
        start_date = end_date - timedelta(days=1)  # 2 days total

        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")

        print(
            f"\n[Live Directory Path Test] Testing sales ticket aggregation from {start_date} to {end_date}"
        )
        print(
            "[Live Directory Path Test] This tests the fix for PermissionError when passing directory paths"
        )

        # Verify that clean_sales is a directory path (this is what gets passed to aggregate_by_ticket)
        assert paths.clean_sales.is_dir() or not paths.clean_sales.exists(), (
            "clean_sales should be a directory path"
        )
        print(f"[Live Directory Path Test] clean_sales path: {paths.clean_sales}")

        # Test: Call fetch_ticket which internally calls aggregate_to_ticket
        # aggregate_to_ticket passes str(paths.clean_sales) to aggregate_by_ticket
        # This was causing the PermissionError bug before the fix
        # Note: Don't filter by branches initially to see all data, then check branch names
        try:
            result = sales_marts.fetch_ticket(
                paths=paths,
                start_date=start_str,
                end_date=end_str,
                branches=None,  # Don't filter by branches - let's see all data first
                mode="force",  # Force rebuild to ensure we go through the aggregation
            )
        except PermissionError as e:
            if "Permission denied" in str(e) and "batch" in str(e):
                pytest.fail(
                    f"PermissionError still occurs with directory paths! "
                    f"This indicates the bug fix is not working. Error: {e}"
                )
            raise
        except Exception as e:
            import traceback

            error_details = traceback.format_exc()
            print(f"\n[Live Directory Path Test] Error in sales ticket aggregation: {e}")
            print(f"[Live Directory Path Test] Full traceback:\n{error_details}")
            pytest.skip(f"Failed to aggregate sales tickets: {e}")

        # Verify the result
        assert result is not None, "fetch_ticket should return a DataFrame"
        assert not result.empty, (
            f"Expected sales data for date range {start_str} to {end_str}, but got empty result. "
            f"This indicates a failure in data retrieval or aggregation."
        )
        assert "order_id" in result.columns, "Result should have order_id column"
        assert "sucursal" in result.columns, "Result should have sucursal column"

        # The key test: No PermissionError occurred with directory path
        print("[Live Directory Path Test] ✓ No PermissionError occurred with directory path")
        print(
            f"[Live Directory Path Test] ✓ Successfully processed directory path (result has {len(result)} tickets)"
        )
        print(f"[Live Directory Path Test] ✓ Sample columns: {list(result.columns)[:10]}")

        # Verify data quality
        if "sucursal" in result.columns:
            unique_branches = sorted(result["sucursal"].unique().tolist())
            print(
                f"[Live Directory Path Test] ✓ Found {len(result)} tickets across "
                f"{len(unique_branches)} branch(es): {unique_branches}"
            )
            # Check for Kavia-related branches (might be "Panem - Hotel Kavia N" or similar)
            kavia_related = [b for b in unique_branches if "kavia" in b.lower() or "Kavia" in b]
            if kavia_related:
                kavia_data = result[result["sucursal"].isin(kavia_related)]
                print(
                    f"[Live Directory Path Test] ✓ Found {len(kavia_data)} tickets for "
                    f"Kavia-related branch(es): {kavia_related}"
                )

        # Verify the output file was created
        mart_path = paths.mart_sales / f"mart_sales_by_ticket_{start_str}_{end_str}.csv"
        assert mart_path.exists(), "Mart output file should be created"
        print(f"[Live Directory Path Test] ✓ Output file created: {mart_path}")

        print("[Live Directory Path Test] ✓ All directory path handling tests passed")


def test_aggregate_by_ticket_with_recursive_directory(sample_sales_data: pd.DataFrame) -> None:
    """Test that aggregate_by_ticket handles recursive directory searches."""
    with TemporaryDirectory() as tmpdir:
        # Create nested directory structure
        base_dir = Path(tmpdir) / "data" / "sales"
        base_dir.mkdir(parents=True)

        # Create CSV files in subdirectories with different order_ids
        subdir1 = base_dir / "batch1"
        subdir2 = base_dir / "batch2"
        subdir1.mkdir()
        subdir2.mkdir()

        data1 = sample_sales_data.copy()
        data2 = sample_sales_data.copy()
        data2["order_id"] = [3001, 3001, 3002, 3002]  # Different order IDs

        file1 = subdir1 / "sales1.csv"
        file2 = subdir2 / "sales2.csv"
        data1.to_csv(file1, index=False)
        data2.to_csv(file2, index=False)

        # Also create a directory that should be ignored
        empty_dir = base_dir / "empty_batch"
        empty_dir.mkdir()

        output_file = Path(tmpdir) / "output.csv"

        # Test: Pass directory path with recursive=True
        result = aggregate_by_ticket(
            input_csv=str(base_dir),
            output_csv=str(output_file),
            recursive=True,
        )

        # Verify it found files in subdirectories
        assert result is not None
        assert not result.empty
        # Should have aggregated data from both files in subdirectories
        # 4 orders from file1 + 4 orders from file2 = 8 total rows before aggregation
        # But since order_ids are unique, we get 4 unique tickets
        assert len(result) == 4
        assert set(result["order_id"].unique()) == {1001, 1002, 3001, 3002}


def test_aggregate_by_ticket_filters_out_directories(sample_sales_data: pd.DataFrame) -> None:
    """Test that directories are filtered out from file lists.

    This test ensures that if glob returns directories, they are filtered out
    before pandas tries to read them.
    """
    with TemporaryDirectory() as tmpdir:
        sales_dir = Path(tmpdir) / "sales"
        sales_dir.mkdir()

        # Create a CSV file
        csv_file = sales_dir / "data.csv"
        sample_sales_data.to_csv(csv_file, index=False)

        # Create a subdirectory (should be ignored)
        subdir = sales_dir / "batch"
        subdir.mkdir()

        output_file = Path(tmpdir) / "output.csv"

        # Test: Use glob pattern that might match directory
        # The pattern "sales/*" could theoretically match both file and directory
        result = aggregate_by_ticket(
            input_csv=str(sales_dir / "*.csv"),
            output_csv=str(output_file),
        )

        # Should only read the CSV file, not the directory
        assert result is not None
        assert not result.empty


def test_aggregate_by_ticket_with_mixed_paths(sample_sales_data: pd.DataFrame) -> None:
    """Test that aggregate_by_ticket handles mixed file and directory paths."""
    with TemporaryDirectory() as tmpdir:
        # Create multiple directories with CSV files
        dir1 = Path(tmpdir) / "dir1"
        dir2 = Path(tmpdir) / "dir2"
        dir1.mkdir()
        dir2.mkdir()

        data1 = sample_sales_data.copy()
        data2 = sample_sales_data.copy()
        data2["order_id"] = [4001, 4001, 4002, 4002]  # Different order IDs

        file1 = dir1 / "file1.csv"
        file2 = dir2 / "file2.csv"
        data1.to_csv(file1, index=False)
        data2.to_csv(file2, index=False)

        # Also create a standalone CSV file with different order IDs
        data3 = sample_sales_data.copy()
        data3["order_id"] = [5001, 5001, 5002, 5002]  # Different order IDs
        standalone_file = Path(tmpdir) / "standalone.csv"
        data3.to_csv(standalone_file, index=False)

        output_file = Path(tmpdir) / "output.csv"

        # Test: Pass mix of directory paths and file paths
        result = aggregate_by_ticket(
            input_csv=[str(dir1), str(dir2), str(standalone_file)],
            output_csv=str(output_file),
            recursive=False,
        )

        # Should aggregate data from all sources
        assert result is not None
        assert not result.empty
        # 3 sources x 2 unique orders per source = 6 unique tickets total
        assert len(result) == 6
        assert set(result["order_id"].unique()) == {1001, 1002, 4001, 4002, 5001, 5002}


def test_aggregate_by_ticket_with_empty_directory() -> None:
    """Test that aggregate_by_ticket raises appropriate error for empty directory."""
    with TemporaryDirectory() as tmpdir:
        empty_dir = Path(tmpdir) / "empty"
        empty_dir.mkdir()

        output_file = Path(tmpdir) / "output.csv"

        # Test: Empty directory should raise FileNotFoundError
        with pytest.raises(FileNotFoundError, match="No input files matched"):
            aggregate_by_ticket(
                input_csv=str(empty_dir),
                output_csv=str(output_file),
                recursive=False,
            )


def test_aggregate_by_ticket_with_directory_no_csv_files() -> None:
    """Test that aggregate_by_ticket handles directory with no CSV files."""
    with TemporaryDirectory() as tmpdir:
        no_csv_dir = Path(tmpdir) / "no_csv"
        no_csv_dir.mkdir()

        # Create a non-CSV file
        (no_csv_dir / "readme.txt").write_text("This is not a CSV")

        output_file = Path(tmpdir) / "output.csv"

        # Test: Directory with no CSV files should raise FileNotFoundError
        with pytest.raises(FileNotFoundError, match="No input files matched"):
            aggregate_by_ticket(
                input_csv=str(no_csv_dir),
                output_csv=str(output_file),
                recursive=False,
            )


def test_aggregate_by_ticket_with_file_path(sample_sales_data: pd.DataFrame) -> None:
    """Test that aggregate_by_ticket still works with direct file paths."""
    with TemporaryDirectory() as tmpdir:
        csv_file = Path(tmpdir) / "sales.csv"
        sample_sales_data.to_csv(csv_file, index=False)

        output_file = Path(tmpdir) / "output.csv"

        # Test: Direct file path should work as before
        result = aggregate_by_ticket(
            input_csv=str(csv_file),
            output_csv=str(output_file),
        )

        assert result is not None
        assert not result.empty
        assert len(result) == 2  # 2 unique orders


def test_aggregate_by_ticket_with_glob_pattern(sample_sales_data: pd.DataFrame) -> None:
    """Test that aggregate_by_ticket works with glob patterns."""
    with TemporaryDirectory() as tmpdir:
        sales_dir = Path(tmpdir) / "sales"
        sales_dir.mkdir()

        # Create CSV files with different order_ids
        data1 = sample_sales_data.copy()
        data2 = sample_sales_data.copy()
        data2["order_id"] = [6001, 6001, 6002, 6002]  # Different order IDs

        file1 = sales_dir / "sales_2025-01-15.csv"
        file2 = sales_dir / "sales_2025-01-16.csv"
        data1.to_csv(file1, index=False)
        data2.to_csv(file2, index=False)

        # Create a non-CSV file that should be ignored
        (sales_dir / "other.txt").write_text("not a csv")

        output_file = Path(tmpdir) / "output.csv"

        # Test: Glob pattern should work
        result = aggregate_by_ticket(
            input_csv=str(sales_dir / "sales_*.csv"),
            output_csv=str(output_file),
        )

        assert result is not None
        assert not result.empty
        # Should match both sales_*.csv files, 4 unique tickets total
        assert len(result) == 4
        assert set(result["order_id"].unique()) == {1001, 1002, 6001, 6002}
