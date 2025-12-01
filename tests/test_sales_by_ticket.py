"""Tests for sales_by_ticket aggregation module.

This module tests the aggregate_by_ticket function and its helper functions,
with a focus on directory handling and file path resolution.
"""

from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
import pytest

from pos_core.etl.marts.sales_by_ticket import aggregate_by_ticket


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


def test_aggregate_by_ticket_with_directory_path(sample_sales_data: pd.DataFrame) -> None:
    """Test that aggregate_by_ticket handles directory paths correctly.

    This test verifies the fix for the bug where passing a directory path
    as input_csv would cause a PermissionError when pandas tried to read it.
    """
    with TemporaryDirectory() as tmpdir:
        # Create a directory structure with CSV files
        sales_dir = Path(tmpdir) / "sales"
        sales_dir.mkdir()

        # Create CSV files with different order_ids to avoid aggregation
        data1 = sample_sales_data.copy()
        data2 = sample_sales_data.copy()
        data2["order_id"] = [2001, 2001, 2002, 2002]  # Different order IDs

        file1 = sales_dir / "sales_2025-01-15.csv"
        file2 = sales_dir / "sales_2025-01-16.csv"
        data1.to_csv(file1, index=False)
        data2.to_csv(file2, index=False)

        # Create output directory
        output_dir = Path(tmpdir) / "output"
        output_dir.mkdir()
        output_file = output_dir / "aggregated.csv"

        # Test: Pass directory path as input_csv (this was causing the bug)
        result = aggregate_by_ticket(
            input_csv=str(sales_dir),
            output_csv=str(output_file),
            recursive=False,
        )

        # Verify it worked - should have aggregated data from both files
        assert result is not None
        assert not result.empty
        assert "order_id" in result.columns
        # Should have 4 unique orders total (1001, 1002 from file1, 2001, 2002 from file2)
        assert len(result) == 4
        assert set(result["order_id"].unique()) == {1001, 1002, 2001, 2002}


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
