"""Gold layer: Aggregate payments into mart_payments_daily.

This module aggregates the core fact (fact_payments_ticket) into
daily branch-level summaries for forecasting and analysis.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from pos_core.config import DataPaths

from pos_core.payments.metadata import StageMetadata, write_metadata

logger = logging.getLogger(__name__)


def aggregate_to_daily(
    paths: DataPaths,
    start_date: str,
    end_date: str,
    branches: list[str] | None = None,
) -> pd.DataFrame:
    """Aggregate fact_payments_ticket into mart_payments_daily.

    Creates a daily-level summary with:
    - One row per sucursal × date
    - Columns: ingreso_efectivo, ingreso_credito, ingreso_debito, etc.
    - propinas, num_tickets, tickets_with_eliminations

    Args:
        paths: DataPaths configuration.
        start_date: Start date in YYYY-MM-DD format.
        end_date: End date in YYYY-MM-DD format.
        branches: Optional list of branches to include.

    Returns:
        DataFrame with mart_payments_daily structure.
    """
    # Import the actual aggregation logic
    from pos_core.etl.marts.payments_daily import aggregate_payments_daily

    paths.ensure_dirs()

    output_path = paths.mart_payments / "mart_payments_daily.csv"

    logger.info("Aggregating payments to daily mart for %s to %s", start_date, end_date)

    try:
        result_df = aggregate_payments_daily(
            clean_dir=paths.clean_payments,
            output_path=output_path,
        )

        # Filter by branches if specified
        if branches and "sucursal" in result_df.columns:
            result_df = result_df[result_df["sucursal"].isin(branches)]

        # Write success metadata
        metadata = StageMetadata(
            start_date=start_date,
            end_date=end_date,
            branches=branches or [],
            version="aggregate_daily_v1",
            last_run=datetime.now().isoformat(),
            status="ok",
        )
        write_metadata(paths.mart_payments, start_date, end_date, metadata)

        return result_df

    except Exception as e:
        logger.error("Error aggregating payments: %s", e)
        metadata = StageMetadata(
            start_date=start_date,
            end_date=end_date,
            branches=branches or [],
            version="aggregate_daily_v1",
            last_run=datetime.now().isoformat(),
            status="failed",
        )
        write_metadata(paths.mart_payments, start_date, end_date, metadata)
        raise
