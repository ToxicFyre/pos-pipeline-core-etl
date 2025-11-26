"""Data preparation utilities for time series forecasting.

This module provides functions for transforming raw payment data into
time series suitable for forecasting models.
"""

from __future__ import annotations

import pandas as pd


def build_daily_series(df: pd.DataFrame, branch: str, metric: str) -> pd.Series:
    """Build daily time series for a specific branch and metric.

    Missing days (weekends, holidays when branch is closed) are treated as zero-sale days.

    Args:
        df: DataFrame with columns 'sucursal', 'fecha', and metric columns
        branch: Branch name (sucursal)
        metric: Metric column name

    Returns:
        Daily Series indexed by date, with missing days filled with 0.0
    """
    branch_data = df.loc[df["sucursal"] == branch, ["fecha", metric]].copy()
    branch_data = branch_data.set_index("fecha")

    # Create full date range from first to last date
    if len(branch_data) == 0:
        return pd.Series(dtype=float)

    date_range = pd.date_range(start=branch_data.index.min(), end=branch_data.index.max(), freq="D")
    branch_data = branch_data.reindex(date_range, fill_value=0.0)
    branch_data = branch_data.sort_index()

    # Fill any remaining NaN values with 0.0 (shouldn't happen, but safety check)
    branch_data[metric] = branch_data[metric].fillna(0.0)

    return branch_data[metric]


def calculate_ingreso_total(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate ingreso_total as sum of all ingreso columns.

    Args:
        df: DataFrame with payment columns

    Returns:
        DataFrame with added 'ingreso_total' column

    Raises:
        ValueError: If no 'ingreso_' columns are found in the data
    """
    df = df.copy()

    # Find all ingreso columns (excluding propinas, num_tickets, etc.)
    ingreso_cols = [col for col in df.columns if col.startswith("ingreso_")]

    if not ingreso_cols:
        raise ValueError("No 'ingreso_' columns found in data")

    # Sum all ingreso columns (excluding ingreso_total if it already exists)
    ingreso_cols_to_sum = [col for col in ingreso_cols if col != "ingreso_total"]
    df["ingreso_total"] = df[ingreso_cols_to_sum].sum(axis=1)

    return df
