"""Public API for payments data.

This module provides the main entry point for loading payment data.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from pos_core.config import DataPaths

logger = logging.getLogger(__name__)


def get_payments(
    paths: DataPaths,
    start_date: str,
    end_date: str,
    grain: str = "daily",
    branches: list[str] | None = None,
    refresh: bool = False,
) -> pd.DataFrame:
    """Load payments data at the specified grain.

    This function orchestrates the ETL pipeline to deliver payment data:
    1. Downloads raw data from Wansoft API (if needed)
    2. Cleans into fact_payments_ticket (if needed)
    3. Aggregates to requested grain (if needed)

    Args:
        paths: DataPaths configuration with data directories.
        start_date: Start date in YYYY-MM-DD format (inclusive).
        end_date: End date in YYYY-MM-DD format (inclusive).
        grain: Data grain to return:
            - "ticket": Core fact (fact_payments_ticket). One row per ticket x payment method.
            - "daily": Daily mart (mart_payments_daily). One row per sucursal x date.
              This is the default and most common use case.
        branches: Optional list of branch names to filter. If None, returns all branches.
        refresh: If True, force re-run all ETL stages. Default False uses cached data.

    Returns:
        DataFrame at the requested grain.

    Raises:
        ValueError: If grain is not "ticket" or "daily".

    Examples:
        >>> from pos_core import DataPaths
        >>> paths = DataPaths.from_root("data", "utils/sucursales.json")
        >>> df = get_payments(paths, "2025-01-01", "2025-01-31")  # daily mart
        >>> df = get_payments(paths, "2025-01-01", "2025-01-31", grain="ticket")  # core fact

    """
    if grain not in ("ticket", "daily"):
        raise ValueError(f"Invalid grain '{grain}'. Must be 'ticket' or 'daily'.")

    # Validate dates
    try:
        pd.to_datetime(start_date)
        pd.to_datetime(end_date)
    except ValueError as e:
        raise ValueError(f"Invalid date format: {e}") from e

    # Import internal modules here to avoid circular imports
    from pos_core.payments.extract import download_payments
    from pos_core.payments.transform import clean_payments

    # Ensure directories exist
    paths.ensure_dirs()

    if refresh:
        logger.info("Refresh=True: running all ETL stages for %s to %s", start_date, end_date)
        download_payments(paths, start_date, end_date, branches)
        clean_payments(paths, start_date, end_date, branches)
        if grain == "daily":
            from pos_core.payments.aggregate import aggregate_to_daily

            return aggregate_to_daily(paths, start_date, end_date, branches)
        else:
            return _load_fact(paths, start_date, end_date, branches)

    # Check what exists and run only needed stages
    from pos_core.payments.metadata import (
        read_metadata,
        should_run_stage,
    )

    # Check raw stage
    if should_run_stage(paths.raw_payments, start_date, end_date, "extract_v1"):
        logger.info("Downloading payments for %s to %s", start_date, end_date)
        download_payments(paths, start_date, end_date, branches)

    # Check clean stage
    if should_run_stage(paths.clean_payments, start_date, end_date, "transform_v1"):
        logger.info("Cleaning payments for %s to %s", start_date, end_date)
        clean_payments(paths, start_date, end_date, branches)

    # Return requested grain
    if grain == "ticket":
        return _load_fact(paths, start_date, end_date, branches)
    else:
        # Check if mart exists
        mart_path = paths.mart_payments / "mart_payments_daily.csv"
        meta = read_metadata(paths.mart_payments, start_date, end_date)
        if mart_path.exists() and meta and meta.status == "ok":
            logger.info("Loading existing mart: %s", mart_path)
            df = pd.read_csv(mart_path)
            if branches:
                df = df[df["sucursal"].isin(branches)]
            return df

        # Build the mart
        from pos_core.payments.aggregate import aggregate_to_daily

        logger.info("Building mart_payments_daily for %s to %s", start_date, end_date)
        return aggregate_to_daily(paths, start_date, end_date, branches)


def _load_fact(
    paths: DataPaths,
    start_date: str,
    end_date: str,
    branches: list[str] | None,
) -> pd.DataFrame:
    """Load fact_payments_ticket from clean CSVs."""
    import glob

    csv_pattern = str(paths.clean_payments / "*.csv")
    csv_files = glob.glob(csv_pattern)

    if not csv_files:
        raise FileNotFoundError(f"No cleaned payment CSVs found in {paths.clean_payments}")

    dfs = [pd.read_csv(f, encoding="utf-8-sig") for f in csv_files]
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
