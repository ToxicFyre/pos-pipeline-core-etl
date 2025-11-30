"""Bronze layer: Raw sales data extraction.

This module provides fetch/load functions for the bronze layer (raw Wansoft exports).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pos_core.config import DataPaths

from pos_core.sales.extract import download_sales
from pos_core.sales.metadata import should_run_stage

logger = logging.getLogger(__name__)


def fetch(
    paths: DataPaths,
    start_date: str,
    end_date: str,
    branches: list[str] | None = None,
    *,
    mode: str = "missing",
) -> None:
    """Ensure raw sales data exists for the given range.

    Downloads raw sales Excel files from Wansoft API if needed, depending on mode:
    - mode="missing" (default): only download missing partitions
    - mode="force": re-download all partitions in the range

    Args:
        paths: DataPaths configuration.
        start_date: Start date in YYYY-MM-DD format (inclusive).
        end_date: End date in YYYY-MM-DD format (inclusive).
        branches: Optional list of branch names to filter.
        mode: Processing mode - "missing" (default) or "force".

    Raises:
        ValueError: If mode is not "missing" or "force".
    """
    if mode not in ("missing", "force"):
        raise ValueError(f"Invalid mode '{mode}'. Must be 'missing' or 'force'.")

    paths.ensure_dirs()

    if mode == "force":
        logger.info("Mode=force: downloading all sales for %s to %s", start_date, end_date)
        download_sales(paths, start_date, end_date, branches)
        return

    # mode="missing": check if extraction is needed
    if should_run_stage(paths.raw_sales, start_date, end_date, "extract_v1"):
        logger.info("Downloading missing sales for %s to %s", start_date, end_date)
        download_sales(paths, start_date, end_date, branches)
    else:
        logger.debug("Raw sales already exist for %s to %s", start_date, end_date)


def load(
    paths: DataPaths,
    start_date: str,
    end_date: str,
    branches: list[str] | None = None,
) -> None:
    """Verify that raw sales data exists for the given range.

    This function does NOT download data. It only checks if the required
    partitions exist and raises an error if they are missing.

    Args:
        paths: DataPaths configuration.
        start_date: Start date in YYYY-MM-DD format (inclusive).
        end_date: End date in YYYY-MM-DD format (inclusive).
        branches: Optional list of branch names (for validation).

    Raises:
        FileNotFoundError: If required raw sales files are missing.
    """
    from pos_core.sales.metadata import read_metadata

    meta = read_metadata(paths.raw_sales, start_date, end_date)
    if meta is None or meta.status != "ok":
        raise FileNotFoundError(
            f"Raw sales data not found for range {start_date} to {end_date}. "
            f"Use sales.raw.fetch() to download data."
        )
