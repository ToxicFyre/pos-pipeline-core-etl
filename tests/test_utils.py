"""Shared test utilities for live tests.

This module provides common verification functions used across multiple test files.
"""

from pathlib import Path

import pandas as pd
import pytest

from pos_core.config import DataPaths


def verify_data_retrieval(
    paths: DataPaths,
    start_date: str,
    end_date: str,
    data_type: str = "payments",
) -> None:
    """Verify that data was actually retrieved from the API and saved to disk.

    This function validates:
    1. HTTP requests were made (files exist in raw directory)
    2. Files were downloaded to disk (raw files exist)
    3. Files contain data (files are not empty)

    Args:
        paths: DataPaths configuration
        start_date: Start date string (YYYY-MM-DD)
        end_date: End date string (YYYY-MM-DD)
        data_type: Type of data to verify ("payments" or "sales")

    Raises:
        AssertionError: If any verification fails
    """
    if data_type == "payments":
        raw_dir = paths.raw_payments
        clean_dir = paths.clean_payments
        mart_dir = paths.mart_payments
        file_extensions = {".xlsx", ".xls"}  # Raw files are Excel
    elif data_type == "sales":
        raw_dir = paths.raw_sales
        clean_dir = paths.clean_sales
        mart_dir = paths.mart_sales
        file_extensions = {".xlsx", ".xls"}  # Raw files are Excel
    else:
        raise ValueError(f"Unknown data_type: {data_type}")

    # 1. Verify HTTP requests were made - check raw files exist
    raw_files = []
    if raw_dir.exists():
        for ext in file_extensions:
            raw_files.extend(list(raw_dir.rglob(f"*{ext}")))
    
    assert len(raw_files) > 0, (
        f"Live test FAILED: No raw {data_type} files found in {raw_dir}. "
        f"This indicates HTTP requests were not made or files were not downloaded."
    )
    print(f"[Data Retrieval Verification] ✓ Found {len(raw_files)} raw {data_type} file(s)")

    # 2. Verify files were downloaded to disk - check file sizes
    total_size = 0
    for raw_file in raw_files:
        assert raw_file.exists(), f"Raw file should exist: {raw_file}"
        file_size = raw_file.stat().st_size
        assert file_size > 0, (
            f"Live test FAILED: Raw file is empty: {raw_file}. "
            f"This indicates the download failed or returned no data."
        )
        total_size += file_size
    
    print(f"[Data Retrieval Verification] ✓ Raw files total size: {total_size:,} bytes")

    # 3. Verify files contain data - check clean files exist and have data
    clean_files = []
    if clean_dir.exists():
        clean_files = list(clean_dir.rglob("*.csv"))
    
    assert len(clean_files) > 0, (
        f"Live test FAILED: No clean {data_type} CSV files found in {clean_dir}. "
        f"This indicates the cleaning stage failed or produced no output."
    )
    print(f"[Data Retrieval Verification] ✓ Found {len(clean_files)} clean {data_type} file(s)")

    # Verify clean files contain data
    total_rows = 0
    for clean_file in clean_files:
        assert clean_file.exists(), f"Clean file should exist: {clean_file}"
        file_size = clean_file.stat().st_size
        assert file_size > 0, (
            f"Live test FAILED: Clean file is empty: {clean_file}. "
            f"This indicates the cleaning stage produced no data."
        )
        
        # Read CSV and verify it has rows
        try:
            df = pd.read_csv(clean_file, nrows=1)  # Just check header + 1 row
            assert len(df.columns) > 0, f"Clean file has no columns: {clean_file}"
            # Count total rows (read full file for accurate count)
            df_full = pd.read_csv(clean_file)
            total_rows += len(df_full)
        except Exception as e:
            pytest.fail(
                f"Live test FAILED: Could not read clean file {clean_file}: {e}. "
                f"This indicates the file is corrupted or invalid."
            )
    
    assert total_rows > 0, (
        f"Live test FAILED: Clean files contain no data rows. "
        f"This indicates the cleaning stage produced empty output."
    )
    print(f"[Data Retrieval Verification] ✓ Clean files contain {total_rows:,} total rows")

    # Verify mart files exist (if applicable)
    mart_files = []
    if mart_dir.exists():
        mart_files = list(mart_dir.rglob("*.csv"))
    
    if mart_files:
        print(f"[Data Retrieval Verification] ✓ Found {len(mart_files)} mart {data_type} file(s)")
        for mart_file in mart_files:
            assert mart_file.exists(), f"Mart file should exist: {mart_file}"
            file_size = mart_file.stat().st_size
            assert file_size > 0, (
                f"Live test FAILED: Mart file is empty: {mart_file}. "
                f"This indicates the aggregation stage produced no output."
            )

    print("[Data Retrieval Verification] ✓ All data retrieval verifications passed")
