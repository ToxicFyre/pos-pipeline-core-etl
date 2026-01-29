"""Gold layer: Transfer marts (aggregated tables).

This module provides fetch/load functions for gold-layer transfer marts,
including the pivot table showing transfer costs by branch and category.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from pos_core.config import DataPaths

from pos_core.transfers.aggregate import aggregate_to_pivot
from pos_core.transfers.core import fetch as fetch_core
from pos_core.transfers.metadata import read_metadata

logger = logging.getLogger(__name__)


def fetch_pivot(
    paths: DataPaths,
    start_date: str,
    end_date: str,
    branches: list[str] | None = None,
    *,
    mode: str = "missing",
    include_cedis: bool = False,
) -> pd.DataFrame:
    """Ensure the transfer pivot mart exists for the range, then return it.

    This function:
    1. Ensures core fact exists (builds/refreshes if needed based on mode)
    2. Builds/refreshes pivot mart if needed based on mode
    3. Returns the pivot mart DataFrame

    Args:
        paths: DataPaths configuration.
        start_date: Start date in YYYY-MM-DD format (inclusive).
        end_date: End date in YYYY-MM-DD format (inclusive).
        branches: Optional list of branch names to filter.
        mode: Processing mode - "missing" (default) or "force".
        include_cedis: If True, include rows where destination is CEDIS.

    Returns:
        DataFrame with mart_transfers_pivot structure (branch x category pivot).

    Raises:
        ValueError: If mode is not "missing" or "force".

    """
    if mode not in ("missing", "force"):
        raise ValueError(f"Invalid mode '{mode}'. Must be 'missing' or 'force'.")

    paths.ensure_dirs()

    # Ensure core fact exists
    fetch_core(paths, start_date, end_date, branches, mode=mode)

    # Check if mart exists and needs rebuilding
    mart_path = paths.mart_transfers / "mart_transfers_pivot.csv"
    meta = read_metadata(paths.mart_transfers, start_date, end_date)

    if mode == "force" or not (mart_path.exists() and meta and meta.status == "ok"):
        logger.info("Building mart_transfers_pivot for %s to %s", start_date, end_date)
        return aggregate_to_pivot(
            paths, start_date, end_date, branches, include_cedis=include_cedis
        )
    else:
        logger.debug("Loading existing mart_transfers_pivot")
        df = pd.read_csv(mart_path, index_col=0)
        return df


def load_pivot(
    paths: DataPaths,
    start_date: str,
    end_date: str,
    branches: list[str] | None = None,  # noqa: ARG001
) -> pd.DataFrame:
    """Load the transfer pivot mart from disk without running ETL.

    Raises a clear error if the mart file is missing.

    Args:
        paths: DataPaths configuration.
        start_date: Start date in YYYY-MM-DD format (inclusive).
        end_date: End date in YYYY-MM-DD format (inclusive).
        branches: Optional list of branch names to filter (unused for pivot).

    Returns:
        DataFrame with mart_transfers_pivot structure.

    Raises:
        FileNotFoundError: If the pivot mart file is missing.

    """
    mart_path = paths.mart_transfers / "mart_transfers_pivot.csv"
    meta = read_metadata(paths.mart_transfers, start_date, end_date)

    if not mart_path.exists() or meta is None or meta.status != "ok":
        raise FileNotFoundError(
            f"Transfer pivot mart not found for range {start_date} to {end_date}. "
            f"Use transfers.marts.fetch_pivot() to build the mart."
        )

    df = pd.read_csv(mart_path, index_col=0)
    return df
