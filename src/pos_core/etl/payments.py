"""Payments ETL stage functions.

This module provides stage-level functions for the payments ETL pipeline:
- download_payments: Download raw payments Excel files
- clean_payments: Transform raw Excel files into clean CSVs
- aggregate_payments: Aggregate clean CSVs into daily dataset
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import List, Optional

import pandas as pd

from pos_core.etl.api import PaymentsETLConfig
from pos_core.etl.metadata import (
    StageMetadata,
    should_skip_stage,
    write_metadata,
)

logger = logging.getLogger(__name__)

# Cleaner version constant
PAYMENTS_CLEANER_VERSION = "payments_cleaner_v1"


def download_payments(
    start_date: str,
    end_date: str,
    config: PaymentsETLConfig,
    branches: Optional[List[str]] = None,
    force: bool = False,
) -> None:
    """Download raw payments Excel for the given range.

    Downloads payment reports from the POS HTTP API for the specified date range
    and branches. Skips already-downloaded files unless force=True.

    Args:
        start_date: Start date in YYYY-MM-DD format (inclusive).
        end_date: End date in YYYY-MM-DD format (inclusive).
        config: PaymentsETLConfig instance with paths and settings.
        branches: Optional list of branch names to process. If None, processes all branches.
        force: If True, re-download even if files already exist (default: False).

    Examples:
        >>> from pos_core.etl.api import PaymentsETLConfig
        >>> config = PaymentsETLConfig.from_root("data", "utils/sucursales.json")
        >>> download_payments("2025-01-01", "2025-01-31", config)
    """
    from pos_core.etl.a_extract.HTTP_extraction import download_payments_reports

    # Check metadata for idempotence
    if should_skip_stage(
        config.paths.raw_payments,
        start_date,
        end_date,
        cleaner_version="download_v1",  # Download doesn't use cleaner, but we track version
        force=force,
    ):
        logger.info("Skipping download (already completed): %s to %s", start_date, end_date)
        return

    # Ensure output directory exists
    config.paths.raw_payments.mkdir(parents=True, exist_ok=True)

    logger.info("Downloading payments reports for %s to %s", start_date, end_date)
    try:
        download_payments_reports(
            start_date=start_date,
            end_date=end_date,
            output_dir=config.paths.raw_payments,
            sucursales_json=config.paths.sucursales_json,
            branches=branches,
            chunk_size_days=config.chunk_size_days,
        )

        # Write metadata
        metadata = StageMetadata(
            start_date=start_date,
            end_date=end_date,
            branches=branches or [],
            cleaner_version="download_v1",
            last_run=datetime.now().isoformat(),
            status="ok",
        )
        write_metadata(config.paths.raw_payments, start_date, end_date, metadata)
    except Exception as e:
        logger.error("Error downloading payments: %s", e)
        # Write failed metadata
        metadata = StageMetadata(
            start_date=start_date,
            end_date=end_date,
            branches=branches or [],
            cleaner_version="download_v1",
            last_run=datetime.now().isoformat(),
            status="failed",
        )
        write_metadata(config.paths.raw_payments, start_date, end_date, metadata)
        raise


def clean_payments(
    start_date: str,
    end_date: str,
    config: PaymentsETLConfig,
    branches: Optional[List[str]] = None,
    force: bool = False,
) -> None:
    """Transform raw payments files into clean CSV/Parquet.

    Processes all raw payment Excel files in the input directory and writes
    normalized CSV files to the output directory.

    Args:
        start_date: Start date in YYYY-MM-DD format (inclusive).
        end_date: End date in YYYY-MM-DD format (inclusive).
        config: PaymentsETLConfig instance with paths and settings.
        branches: Optional list of branch names to process. If None, processes all branches.
        force: If True, re-clean even if files already exist (default: False).

    Examples:
        >>> from pos_core.etl.api import PaymentsETLConfig
        >>> config = PaymentsETLConfig.from_root("data", "utils/sucursales.json")
        >>> clean_payments("2025-01-01", "2025-01-31", config)
    """
    from pos_core.etl.b_transform.pos_excel_payments_cleaner import clean_payments_directory

    # Check metadata for idempotence
    if should_skip_stage(
        config.paths.clean_payments,
        start_date,
        end_date,
        cleaner_version=PAYMENTS_CLEANER_VERSION,
        force=force,
    ):
        logger.info("Skipping clean (already completed): %s to %s", start_date, end_date)
        return

    # Ensure output directory exists
    config.paths.clean_payments.mkdir(parents=True, exist_ok=True)

    logger.info("Cleaning payments files for %s to %s", start_date, end_date)
    try:
        clean_payments_directory(
            input_dir=config.paths.raw_payments,
            output_dir=config.paths.clean_payments,
            recursive=True,
        )

        # Write metadata
        metadata = StageMetadata(
            start_date=start_date,
            end_date=end_date,
            branches=branches or [],
            cleaner_version=PAYMENTS_CLEANER_VERSION,
            last_run=datetime.now().isoformat(),
            status="ok",
        )
        write_metadata(config.paths.clean_payments, start_date, end_date, metadata)
    except Exception as e:
        logger.error("Error cleaning payments: %s", e)
        # Write failed metadata
        metadata = StageMetadata(
            start_date=start_date,
            end_date=end_date,
            branches=branches or [],
            cleaner_version=PAYMENTS_CLEANER_VERSION,
            last_run=datetime.now().isoformat(),
            status="failed",
        )
        write_metadata(config.paths.clean_payments, start_date, end_date, metadata)
        raise


def aggregate_payments(
    start_date: str,
    end_date: str,
    config: PaymentsETLConfig,
    branches: Optional[List[str]] = None,
    force: bool = False,
) -> pd.DataFrame:
    """Aggregate clean payments into the canonical dataset and return it.

    Aggregates cleaned payment CSV files into a daily-level dataset with one row
    per sucursal per day. Returns the aggregated DataFrame.

    Args:
        start_date: Start date in YYYY-MM-DD format (inclusive).
        end_date: End date in YYYY-MM-DD format (inclusive).
        config: PaymentsETLConfig instance with paths and settings.
        branches: Optional list of branch names to process. If None, processes all branches.
        force: If True, re-aggregate even if file already exists (default: False).

    Returns:
        DataFrame containing aggregated payments data (one row per sucursal + fecha).

    Examples:
        >>> from pos_core.etl.api import PaymentsETLConfig
        >>> config = PaymentsETLConfig.from_root("data", "utils/sucursales.json")
        >>> df = aggregate_payments("2025-01-01", "2025-01-31", config)
        >>> len(df)
        31
    """
    from pos_core.etl.c_load.aggregate_payments_by_day import aggregate_payments_daily

    # Check metadata for idempotence
    if should_skip_stage(
        config.paths.proc_payments,
        start_date,
        end_date,
        cleaner_version="aggregate_v1",
        force=force,
    ):
        logger.info("Skipping aggregate (already completed): %s to %s", start_date, end_date)
        aggregated_path = config.paths.proc_payments / "aggregated_payments_daily.csv"
        if aggregated_path.exists():
            return pd.read_csv(aggregated_path)

    # Ensure output directory exists
    config.paths.proc_payments.mkdir(parents=True, exist_ok=True)

    aggregated_path = config.paths.proc_payments / "aggregated_payments_daily.csv"

    logger.info("Aggregating payments data for %s to %s", start_date, end_date)
    try:
        result_df = aggregate_payments_daily(
            clean_dir=config.paths.clean_payments,
            output_path=aggregated_path,
        )

        # Write metadata
        metadata = StageMetadata(
            start_date=start_date,
            end_date=end_date,
            branches=branches or [],
            cleaner_version="aggregate_v1",
            last_run=datetime.now().isoformat(),
            status="ok",
        )
        write_metadata(config.paths.proc_payments, start_date, end_date, metadata)

        logger.info("Aggregated payments: %d rows", len(result_df))
        return result_df
    except Exception as e:
        logger.error("Error aggregating payments: %s", e)
        # Write failed metadata
        metadata = StageMetadata(
            start_date=start_date,
            end_date=end_date,
            branches=branches or [],
            cleaner_version="aggregate_v1",
            last_run=datetime.now().isoformat(),
            status="failed",
        )
        write_metadata(config.paths.proc_payments, start_date, end_date, metadata)
        raise
