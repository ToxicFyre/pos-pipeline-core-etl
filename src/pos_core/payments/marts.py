"""Gold layer: Payment marts (aggregated tables).

This module provides fetch/load functions for gold-layer payment marts,
including daily aggregations for forecasting and analysis.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from pos_core.config import DataPaths

from pos_core.payments.aggregate import aggregate_to_daily
from pos_core.payments.core import fetch as fetch_core
from pos_core.payments.metadata import read_metadata

logger = logging.getLogger(__name__)


def fetch_daily(
    paths: DataPaths,
    start_date: str,
    end_date: str,
    branches: list[str] | None = None,
    *,
    mode: str = "missing",
) -> pd.DataFrame:
    """Ensure the daily payments mart exists for the range, then return it.

    This function:
    1. Ensures core fact exists (builds/refreshes if needed based on mode)
    2. Builds/refreshes daily mart if needed based on mode
    3. Returns the daily mart DataFrame

    Args:
        paths: DataPaths configuration.
        start_date: Start date in YYYY-MM-DD format (inclusive).
        end_date: End date in YYYY-MM-DD format (inclusive).
        branches: Optional list of branch names to filter.
        mode: Processing mode - "missing" (default) or "force".

    Returns:
        DataFrame with mart_payments_daily structure (sucursal x date grain).

    Raises:
        ValueError: If mode is not "missing" or "force".

    """
    if mode not in ("missing", "force"):
        raise ValueError(f"Invalid mode '{mode}'. Must be 'missing' or 'force'.")

    paths.ensure_dirs()

    # Ensure core fact exists
    fetch_core(paths, start_date, end_date, branches, mode=mode)

    # Check if mart exists and needs rebuilding
    mart_path = paths.mart_payments / "mart_payments_daily.csv"
    meta = read_metadata(paths.mart_payments, start_date, end_date)

    if mode == "force" or not (mart_path.exists() and meta and meta.status == "ok"):
        logger.info("Building mart_payments_daily for %s to %s", start_date, end_date)
        return aggregate_to_daily(paths, start_date, end_date, branches)
    else:
        logger.debug("Loading existing mart_payments_daily")
        df = pd.read_csv(mart_path)

        # Filter by date range
        if "fecha" in df.columns:
            df["fecha"] = pd.to_datetime(df["fecha"]).dt.date
            start = pd.to_datetime(start_date).date()
            end = pd.to_datetime(end_date).date()
            df = df[(df["fecha"] >= start) & (df["fecha"] <= end)]

        # Filter by branches if specified
        if branches and "sucursal" in df.columns:
            df = df[df["sucursal"].isin(branches)]
        return df


def load_daily(
    paths: DataPaths,
    start_date: str,
    end_date: str,
    branches: list[str] | None = None,
) -> pd.DataFrame:
    """Load the daily payments mart from disk without running ETL.

    Raises a clear error if the mart file is missing.

    Args:
        paths: DataPaths configuration.
        start_date: Start date in YYYY-MM-DD format (inclusive).
        end_date: End date in YYYY-MM-DD format (inclusive).
        branches: Optional list of branch names to filter.

    Returns:
        DataFrame with mart_payments_daily structure.

    Raises:
        FileNotFoundError: If the daily mart file is missing.

    """
    mart_path = paths.mart_payments / "mart_payments_daily.csv"
    meta = read_metadata(paths.mart_payments, start_date, end_date)

    if not mart_path.exists() or meta is None or meta.status != "ok":
        raise FileNotFoundError(
            f"Daily payments mart not found for range {start_date} to {end_date}. "
            f"Use payments.marts.fetch_daily() to build the mart."
        )

    df = pd.read_csv(mart_path)

    # Filter by date range
    if "fecha" in df.columns:
        df["fecha"] = pd.to_datetime(df["fecha"]).dt.date
        start = pd.to_datetime(start_date).date()
        end = pd.to_datetime(end_date).date()
        df = df[(df["fecha"] >= start) & (df["fecha"] <= end)]

    # Filter by branches
    if branches and "sucursal" in df.columns:
        df = df[df["sucursal"].isin(branches)]

    return df
