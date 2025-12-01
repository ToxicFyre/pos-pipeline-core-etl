"""Public API for sales data.

This module provides the main entry point for loading sales data.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from pos_core.config import DataPaths

logger = logging.getLogger(__name__)


def get_sales(
    paths: DataPaths,
    start_date: str,
    end_date: str,
    grain: str = "item",
    branches: list[str] | None = None,
    refresh: bool = False,
) -> pd.DataFrame:
    """Load sales data at the specified grain.

    This function orchestrates the ETL pipeline to deliver sales data:
    1. Downloads raw data from Wansoft API (if needed)
    2. Cleans into fact_sales_item_line (if needed)
    3. Aggregates to requested grain (if needed)

    Args:
        paths: DataPaths configuration with data directories.
        start_date: Start date in YYYY-MM-DD format (inclusive).
        end_date: End date in YYYY-MM-DD format (inclusive).
        grain: Data grain to return:
            - "item": Core fact (fact_sales_item_line). One row per item/modifier line.
              This is the default and most granular grain.
            - "ticket": Ticket mart (mart_sales_by_ticket). One row per ticket.
            - "group": Group mart (mart_sales_by_group). Category pivot table.
        branches: Optional list of branch names to filter. If None, returns all branches.
        refresh: If True, force re-run all ETL stages. Default False uses cached data.

    Returns:
        DataFrame at the requested grain.

    Raises:
        ValueError: If grain is not "item", "ticket", or "group".

    Examples:
        >>> from pos_core import DataPaths
        >>> paths = DataPaths.from_root("data", "utils/sucursales.json")
        >>> df = get_sales(paths, "2025-01-01", "2025-01-31")  # item-line fact
        >>> df = get_sales(paths, "2025-01-01", "2025-01-31", grain="ticket")
        >>> df = get_sales(paths, "2025-01-01", "2025-01-31", grain="group")

    """
    if grain not in ("item", "ticket", "group"):
        raise ValueError(f"Invalid grain '{grain}'. Must be 'item', 'ticket', or 'group'.")

    # Validate dates
    try:
        pd.to_datetime(start_date)
        pd.to_datetime(end_date)
    except ValueError as e:
        raise ValueError(f"Invalid date format: {e}") from e

    # Import internal modules here to avoid circular imports
    from pos_core.sales.extract import download_sales
    from pos_core.sales.transform import clean_sales

    # Ensure directories exist
    paths.ensure_dirs()

    if refresh:
        logger.info("Refresh=True: running all ETL stages for %s to %s", start_date, end_date)
        download_sales(paths, start_date, end_date, branches)
        clean_sales(paths, start_date, end_date, branches)
        return _get_grain(paths, start_date, end_date, grain, branches, force=True)

    # Check what exists and run only needed stages
    from pos_core.sales.metadata import should_run_stage

    # Check raw stage
    if should_run_stage(paths.raw_sales, start_date, end_date, "extract_v1"):
        logger.info("Downloading sales for %s to %s", start_date, end_date)
        download_sales(paths, start_date, end_date, branches)

    # Check clean stage
    if should_run_stage(paths.clean_sales, start_date, end_date, "transform_v1"):
        logger.info("Cleaning sales for %s to %s", start_date, end_date)
        clean_sales(paths, start_date, end_date, branches)

    return _get_grain(paths, start_date, end_date, grain, branches, force=False)


def _get_grain(
    paths: DataPaths,
    start_date: str,
    end_date: str,
    grain: str,
    branches: list[str] | None,
    force: bool,
) -> pd.DataFrame:
    """Return data at the specified grain, building if necessary."""
    if grain == "item":
        return _load_fact(paths, start_date, end_date, branches)
    elif grain == "ticket":
        return _get_ticket_mart(paths, start_date, end_date, branches, force)
    else:  # group
        return _get_group_mart(paths, start_date, end_date, branches, force)


def _load_fact(
    paths: DataPaths,
    start_date: str,
    end_date: str,
    branches: list[str] | None,
) -> pd.DataFrame:
    """Load fact_sales_item_line from clean CSVs."""
    import glob

    csv_pattern = str(paths.clean_sales / "*.csv")
    csv_files = glob.glob(csv_pattern)

    if not csv_files:
        raise FileNotFoundError(f"No cleaned sales CSVs found in {paths.clean_sales}")

    dfs = [pd.read_csv(f, encoding="utf-8") for f in csv_files]
    df = pd.concat(dfs, ignore_index=True)

    # Filter by date range
    if "operating_date" in df.columns:
        df["operating_date"] = pd.to_datetime(df["operating_date"]).dt.date
        start = pd.to_datetime(start_date).date()
        end = pd.to_datetime(end_date).date()
        df = df[(df["operating_date"] >= start) & (df["operating_date"] <= end)]

    # Filter by branches
    if branches and "sucursal" in df.columns:
        df = df[df["sucursal"].isin(branches)]

    return df


def _get_ticket_mart(
    paths: DataPaths,
    start_date: str,
    end_date: str,
    branches: list[str] | None,
    force: bool,
) -> pd.DataFrame:
    """Get or build mart_sales_by_ticket."""
    from pos_core.sales.aggregate import aggregate_to_ticket
    from pos_core.sales.metadata import read_metadata

    mart_path = paths.mart_sales / f"mart_sales_by_ticket_{start_date}_{end_date}.csv"
    meta = read_metadata(paths.mart_sales, start_date, end_date)

    if not force and mart_path.exists() and meta and meta.status == "ok":
        logger.info("Loading existing ticket mart: %s", mart_path)
        df = pd.read_csv(mart_path)
        if branches and "sucursal" in df.columns:
            df = df[df["sucursal"].isin(branches)]
        return df

    logger.info("Building mart_sales_by_ticket for %s to %s", start_date, end_date)
    return aggregate_to_ticket(paths, start_date, end_date, branches)


def _get_group_mart(
    paths: DataPaths,
    start_date: str,
    end_date: str,
    branches: list[str] | None,
    force: bool,
) -> pd.DataFrame:
    """Get or build mart_sales_by_group."""
    from pos_core.sales.aggregate import aggregate_to_group
    from pos_core.sales.metadata import read_metadata

    mart_path = paths.mart_sales / f"mart_sales_by_group_{start_date}_{end_date}.csv"
    meta = read_metadata(paths.mart_sales, start_date, end_date)

    if not force and mart_path.exists() and meta and meta.status == "ok":
        logger.info("Loading existing group mart: %s", mart_path)
        return pd.read_csv(mart_path)

    logger.info("Building mart_sales_by_group for %s to %s", start_date, end_date)
    return aggregate_to_group(paths, start_date, end_date, branches)
