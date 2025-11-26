"""Public API for payments QA pipeline.

This module provides a clean, in-memory API for running QA checks on aggregated
payments DataFrames without reading or writing files.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

import pandas as pd

from pos_core.exceptions import DataQualityError
from pos_core.qa.qa_payments import (
    MONEY_COLUMNS,
    REQUIRED_COLUMNS,
    TICKET_COLUMN,
    detect_duplicate_days,
    detect_missing_days,
    detect_zero_method_flags,
    detect_zscore_anomalies,
    prepare_payments_df,
)

logger = logging.getLogger(__name__)


@dataclass
class PaymentsQAResult:
    """Result of the payments QA pipeline.

    Attributes:
        summary: Dictionary with summary statistics and counts.
        missing_days: DataFrame with missing days per sucursal, or None if none found.
        duplicate_days: DataFrame with duplicate (sucursal, fecha) rows, or None if none found.
        zscore_anomalies: DataFrame with z-score anomalies, or None if none found.
        zero_method_flags: DataFrame with rows where tickets > 0 but payment methods
            are zero, or None if none found.
    """

    summary: dict
    missing_days: pd.DataFrame | None
    duplicate_days: pd.DataFrame | None
    zscore_anomalies: pd.DataFrame | None
    zero_method_flags: pd.DataFrame | None


def run_payments_qa(
    payments_df: pd.DataFrame,
    level: int = 4,
) -> PaymentsQAResult:
    """Run the existing QA checks (from qa_payments.py) in memory,
    without reading or writing any files.

    This function:
    - does NOT read or write any files,
    - does NOT parse CLI arguments or read environment variables,
    - does NOT print (logging only),
    - MAY log progress via the logging module.

    Args:
        payments_df: Aggregated payments data, typically the output of
            the ETL step (e.g. aggregated_payments_daily).
            Expected columns include at least:
            - 'sucursal' (branch name)
            - 'fecha' (date or datetime)
            - payment method columns (ingreso_efectivo, ingreso_credito, etc.)
            - 'num_tickets' (ticket count)
        level: QA level to run (default: 4). Controls which checks are executed:
            - Level 0: Schema validation (always run)
            - Level 3: Missing and duplicate days
            - Level 4: Statistical anomalies (z-score)

    Returns:
        PaymentsQAResult containing:
        - summary: dictionary with counts and flags
        - missing_days: DataFrame with missing days per sucursal, or None
        - duplicate_days: DataFrame with duplicate rows, or None
        - zscore_anomalies: DataFrame with z-score anomalies, or None
        - zero_method_flags: DataFrame with zero method flags, or None

    Raises:
        DataQualityError: If required columns are missing.
    """
    # Prepare DataFrame (ensure fecha is datetime, compute helper columns)
    df = prepare_payments_df(payments_df.copy())

    # Validate required columns
    missing_cols = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing_cols:
        raise DataQualityError(
            f"Missing required columns in payments_df: {missing_cols}. Required: {REQUIRED_COLUMNS}"
        )

    logger.info(f"Running QA checks at level {level} for {len(df)} rows")

    # Initialize results
    missing_days_df: Optional[pd.DataFrame] = None
    duplicate_days_df: Optional[pd.DataFrame] = None
    zscore_anomalies_df: Optional[pd.DataFrame] = None
    zero_method_flags_df: Optional[pd.DataFrame] = None

    # Level 0: Schema validation (always run)
    # Check for nulls in critical columns
    null_errors = []
    for col in ["sucursal", "fecha"]:
        null_count = df[col].isna().sum()
        if null_count > 0:
            null_errors.append(f"{col}: {null_count} nulls")

    # Check for negative values
    negative_errors = []
    for col in MONEY_COLUMNS + [TICKET_COLUMN]:
        neg_count = (df[col] < -1e-6).sum()
        if neg_count > 0:
            negative_errors.append(f"{col}: {neg_count} negative values")

    # Level 3: Missing and duplicate days
    if level >= 3:
        logger.debug("Running Level 3 checks: missing and duplicate days")
        missing_days_df = detect_missing_days(df)
        duplicate_days_df = detect_duplicate_days(df)

    # Level 4: Statistical anomalies (z-score)
    if level >= 4:
        logger.debug("Running Level 4 checks: z-score anomalies")
        zscore_anomalies_df = detect_zscore_anomalies(df)

    # Zero method flags (always run if level >= 3)
    if level >= 3:
        logger.debug("Running zero method flags check")
        zero_method_flags_df = detect_zero_method_flags(df)

    # Build summary
    summary = {
        "total_rows": len(df),
        "total_sucursales": df["sucursal"].nunique(),
        "min_fecha": df["fecha"].min().isoformat() if not df.empty else None,
        "max_fecha": df["fecha"].max().isoformat() if not df.empty else None,
        "has_missing_days": missing_days_df is not None and not missing_days_df.empty,
        "has_duplicates": duplicate_days_df is not None and not duplicate_days_df.empty,
        "has_zscore_anomalies": zscore_anomalies_df is not None and not zscore_anomalies_df.empty,
        "has_zero_method_flags": zero_method_flags_df is not None
        and not zero_method_flags_df.empty,
        "missing_days_count": len(missing_days_df) if missing_days_df is not None else 0,
        "duplicate_days_count": len(duplicate_days_df) if duplicate_days_df is not None else 0,
        "zscore_anomalies_count": len(zscore_anomalies_df)
        if zscore_anomalies_df is not None
        else 0,
        "zero_method_flags_count": len(zero_method_flags_df)
        if zero_method_flags_df is not None
        else 0,
        "schema_errors": null_errors + negative_errors,
    }

    logger.info(
        f"QA complete: {summary['missing_days_count']} missing days, "
        f"{summary['duplicate_days_count']} duplicates, "
        f"{summary['zscore_anomalies_count']} z-score anomalies, "
        f"{summary['zero_method_flags_count']} zero method flags"
    )

    return PaymentsQAResult(
        summary=summary,
        missing_days=missing_days_df,
        duplicate_days=duplicate_days_df,
        zscore_anomalies=zscore_anomalies_df,
        zero_method_flags=zero_method_flags_df,
    )
