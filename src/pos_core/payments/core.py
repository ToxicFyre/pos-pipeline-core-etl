"""Silver layer: Core payment fact table (fact_payments_ticket).

This module provides fetch/load functions for the silver layer core fact table
at ticket × payment method grain.
"""

from __future__ import annotations

import glob
import logging
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from pos_core.config import DataPaths

from pos_core.payments.metadata import read_metadata, should_run_stage
from pos_core.payments.raw import fetch as fetch_raw
from pos_core.payments.transform import clean_payments

logger = logging.getLogger(__name__)


def fetch(
    paths: DataPaths,
    start_date: str,
    end_date: str,
    branches: list[str] | None = None,
    *,
    mode: str = "missing",
) -> pd.DataFrame:
    """Ensure fact_payments_ticket exists for the given range, then return it.

    Runs extraction + transformation as needed (depending on mode), then returns
    the core fact DataFrame at ticket × payment method grain.

    Args:
        paths: DataPaths configuration.
        start_date: Start date in YYYY-MM-DD format (inclusive).
        end_date: End date in YYYY-MM-DD format (inclusive).
        branches: Optional list of branch names to filter.
        mode: Processing mode - "missing" (default) or "force".

    Returns:
        DataFrame with fact_payments_ticket structure (ticket × payment method grain).

    Raises:
        ValueError: If mode is not "missing" or "force".
    """
    if mode not in ("missing", "force"):
        raise ValueError(f"Invalid mode '{mode}'. Must be 'missing' or 'force'.")

    paths.ensure_dirs()

    # Ensure raw data exists
    if mode == "force":
        fetch_raw(paths, start_date, end_date, branches, mode="force")
    else:
        fetch_raw(paths, start_date, end_date, branches, mode="missing")

    # Ensure clean data exists
    if mode == "force" or should_run_stage(
        paths.clean_payments, start_date, end_date, "transform_v1"
    ):
        logger.info("Cleaning payments for %s to %s", start_date, end_date)
        clean_payments(paths, start_date, end_date, branches)
    else:
        logger.debug("Clean payments already exist for %s to %s", start_date, end_date)

    # Load and return the core fact
    return _load_fact(paths, start_date, end_date, branches)


def load(
    paths: DataPaths,
    start_date: str,
    end_date: str,
    branches: list[str] | None = None,
) -> pd.DataFrame:
    """Load fact_payments_ticket from disk without running ETL.

    Raises a clear error if required partitions are missing.

    Args:
        paths: DataPaths configuration.
        start_date: Start date in YYYY-MM-DD format (inclusive).
        end_date: End date in YYYY-MM-DD format (inclusive).
        branches: Optional list of branch names to filter.

    Returns:
        DataFrame with fact_payments_ticket structure.

    Raises:
        FileNotFoundError: If required clean payment CSVs are missing.
    """
    # Check if clean data exists
    meta = read_metadata(paths.clean_payments, start_date, end_date)
    if meta is None or meta.status != "ok":
        raise FileNotFoundError(
            f"Clean payment data not found for range {start_date} to {end_date}. "
            f"Use payments.core.fetch() to build the core fact."
        )

    return _load_fact(paths, start_date, end_date, branches)


def _load_fact(
    paths: DataPaths,
    start_date: str,
    end_date: str,
    branches: list[str] | None,
) -> pd.DataFrame:
    """Load fact_payments_ticket from clean CSVs."""
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
