"""Gold layer: Sales marts (aggregated tables).

This module provides fetch/load functions for gold-layer sales marts,
including ticket-level and group-level aggregations for analysis.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from pos_core.config import DataPaths

from pos_core.sales.aggregate import aggregate_to_group, aggregate_to_ticket
from pos_core.sales.core import fetch as fetch_core
from pos_core.sales.metadata import read_metadata

logger = logging.getLogger(__name__)


def fetch_ticket(
    paths: DataPaths,
    start_date: str,
    end_date: str,
    branches: list[str] | None = None,
    *,
    mode: str = "missing",
) -> pd.DataFrame:
    """Ensure the ticket-level sales mart exists for the range, then return it.

    This function:
    1. Ensures core fact exists (builds/refreshes if needed based on mode)
    2. Builds/refreshes ticket mart if needed based on mode
    3. Returns the ticket mart DataFrame

    Args:
        paths: DataPaths configuration.
        start_date: Start date in YYYY-MM-DD format (inclusive).
        end_date: End date in YYYY-MM-DD format (inclusive).
        branches: Optional list of branch names to filter.
        mode: Processing mode - "missing" (default) or "force".

    Returns:
        DataFrame with mart_sales_by_ticket structure (one row per ticket).

    Raises:
        ValueError: If mode is not "missing" or "force".

    """
    if mode not in ("missing", "force"):
        raise ValueError(f"Invalid mode '{mode}'. Must be 'missing' or 'force'.")

    paths.ensure_dirs()

    # Ensure core fact exists
    fetch_core(paths, start_date, end_date, branches, mode=mode)

    # Check if mart exists and needs rebuilding
    mart_path = paths.mart_sales / f"mart_sales_by_ticket_{start_date}_{end_date}.csv"
    meta = read_metadata(paths.mart_sales, start_date, end_date)

    if mode == "force" or not (mart_path.exists() and meta and meta.status == "ok"):
        logger.info("Building mart_sales_by_ticket for %s to %s", start_date, end_date)
        return aggregate_to_ticket(paths, start_date, end_date, branches)
    else:
        logger.debug("Loading existing mart_sales_by_ticket")
        df = pd.read_csv(mart_path)
        if branches and "sucursal" in df.columns:
            df = df[df["sucursal"].isin(branches)]
        return df


def load_ticket(
    paths: DataPaths,
    start_date: str,
    end_date: str,
    branches: list[str] | None = None,
) -> pd.DataFrame:
    """Load the ticket-level sales mart from disk without running ETL.

    Raises a clear error if the mart file is missing.

    Args:
        paths: DataPaths configuration.
        start_date: Start date in YYYY-MM-DD format (inclusive).
        end_date: End date in YYYY-MM-DD format (inclusive).
        branches: Optional list of branch names to filter.

    Returns:
        DataFrame with mart_sales_by_ticket structure.

    Raises:
        FileNotFoundError: If the ticket mart file is missing.

    """
    mart_path = paths.mart_sales / f"mart_sales_by_ticket_{start_date}_{end_date}.csv"
    meta = read_metadata(paths.mart_sales, start_date, end_date)

    if not mart_path.exists() or meta is None or meta.status != "ok":
        raise FileNotFoundError(
            f"Ticket sales mart not found for range {start_date} to {end_date}. "
            f"Use sales.marts.fetch_ticket() to build the mart."
        )

    df = pd.read_csv(mart_path)

    # Filter by branches
    if branches and "sucursal" in df.columns:
        df = df[df["sucursal"].isin(branches)]

    return df


def fetch_group(
    paths: DataPaths,
    start_date: str,
    end_date: str,
    branches: list[str] | None = None,
    *,
    mode: str = "missing",
) -> pd.DataFrame:
    """Ensure the group-level sales mart exists for the range, then return it.

    This function:
    1. Ensures ticket mart exists (builds/refreshes if needed based on mode)
    2. Builds/refreshes group mart if needed based on mode
    3. Returns the group mart DataFrame

    Args:
        paths: DataPaths configuration.
        start_date: Start date in YYYY-MM-DD format (inclusive).
        end_date: End date in YYYY-MM-DD format (inclusive).
        branches: Optional list of branch names to filter.
        mode: Processing mode - "missing" (default) or "force".

    Returns:
        DataFrame with mart_sales_by_group structure (category pivot).

    Raises:
        ValueError: If mode is not "missing" or "force".

    """
    if mode not in ("missing", "force"):
        raise ValueError(f"Invalid mode '{mode}'. Must be 'missing' or 'force'.")

    paths.ensure_dirs()

    # Ensure ticket mart exists (which ensures core fact exists)
    fetch_ticket(paths, start_date, end_date, branches, mode=mode)

    # Check if group mart exists and needs rebuilding
    mart_path = paths.mart_sales / f"mart_sales_by_group_{start_date}_{end_date}.csv"

    if mode == "force" or not mart_path.exists():
        logger.info("Building mart_sales_by_group for %s to %s", start_date, end_date)
        df = aggregate_to_group(paths, start_date, end_date, branches)
    else:
        logger.debug("Loading existing mart_sales_by_group")
        df = pd.read_csv(mart_path, index_col=0)

    # Filter by branches: select only columns that match the requested branches
    # The group mart is a pivot table where branches are columns
    if branches:
        # Use case-insensitive partial matching to find columns
        # (branch names in columns might have variations like "Panem - Hotel Kavia N")
        matching_cols = []
        branches_lower = [b.lower() for b in branches]
        for col in df.columns:
            col_lower = str(col).lower()
            if any(branch_lower in col_lower for branch_lower in branches_lower):
                matching_cols.append(col)

        if matching_cols:
            df = df[matching_cols]
        else:
            logger.warning(
                f"No matching branch columns found for {branches}. "
                f"Available columns: {list(df.columns)}"
            )

    return df


def load_group(
    paths: DataPaths,
    start_date: str,
    end_date: str,
    branches: list[str] | None = None,
) -> pd.DataFrame:
    """Load the group-level sales mart from disk without running ETL.

    Raises a clear error if the mart file is missing.

    Args:
        paths: DataPaths configuration.
        start_date: Start date in YYYY-MM-DD format (inclusive).
        end_date: End date in YYYY-MM-DD format (inclusive).
        branches: Optional list of branch names to filter. Since the group mart is a pivot
            table with branches as columns, this selects only the matching branch columns.

    Returns:
        DataFrame with mart_sales_by_group structure (category pivot table).

    Raises:
        FileNotFoundError: If the group mart file is missing.

    """
    mart_path = paths.mart_sales / f"mart_sales_by_group_{start_date}_{end_date}.csv"

    if not mart_path.exists():
        raise FileNotFoundError(
            f"Group sales mart not found for range {start_date} to {end_date}. "
            f"Use sales.marts.fetch_group() to build the mart."
        )

    df = pd.read_csv(mart_path, index_col=0)

    # Filter by branches: select only columns that match the requested branches
    # The group mart is a pivot table where branches are columns
    if branches:
        # Use case-insensitive partial matching to find columns
        # (branch names in columns might have variations like "Panem - Hotel Kavia N")
        matching_cols = []
        branches_lower = [b.lower() for b in branches]
        for col in df.columns:
            col_lower = str(col).lower()
            if any(branch_lower in col_lower for branch_lower in branches_lower):
                matching_cols.append(col)

        if matching_cols:
            df = df[matching_cols]
        else:
            logger.warning(
                f"No matching branch columns found for {branches}. "
                f"Available columns: {list(df.columns)}"
            )

    return df
