"""Gold layer: Aggregate sales into various marts.

This module aggregates the core fact (fact_sales_item_line) into
ticket-level and group-level summaries for analysis.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from pos_core.config import DataPaths

from pos_core.sales.metadata import StageMetadata, write_metadata

logger = logging.getLogger(__name__)


def aggregate_to_ticket(
    paths: DataPaths,
    start_date: str,
    end_date: str,
    branches: list[str] | None = None,
) -> pd.DataFrame:
    """Aggregate fact_sales_item_line into mart_sales_by_ticket.

    Creates a ticket-level summary with:
    - One row per ticket (sucursal + order_id)
    - Group subtotals and totals

    Args:
        paths: DataPaths configuration.
        start_date: Start date in YYYY-MM-DD format.
        end_date: End date in YYYY-MM-DD format.
        branches: Optional list of branches to include.

    Returns:
        DataFrame with mart_sales_by_ticket structure.

    """
    # Import the actual aggregation logic
    from pos_core.etl.marts.sales_by_ticket import aggregate_by_ticket

    paths.ensure_dirs()

    output_path = str(paths.mart_sales / f"mart_sales_by_ticket_{start_date}_{end_date}.csv")

    logger.info("Aggregating sales to ticket mart for %s to %s", start_date, end_date)

    try:
        result_df = aggregate_by_ticket(
            input_csv=str(paths.clean_sales),
            output_csv=output_path,
            recursive=True,
        )

        # Filter by date range if operating_date column exists
        if "operating_date" in result_df.columns:
            # Convert operating_date to date if it's not already
            if result_df["operating_date"].dtype == "object" or hasattr(result_df["operating_date"].dtype, "tz"):
                result_df["operating_date"] = pd.to_datetime(result_df["operating_date"]).dt.date
            else:
                # Already a date type, but ensure it's date not datetime
                result_df["operating_date"] = pd.to_datetime(result_df["operating_date"]).dt.date

            start = pd.to_datetime(start_date).date()
            end = pd.to_datetime(end_date).date()

            result_df = result_df[
                (result_df["operating_date"] >= start) & (result_df["operating_date"] <= end)
            ]

        # Filter by branches if specified
        if branches and "sucursal" in result_df.columns:
            result_df = result_df[result_df["sucursal"].isin(branches)]

        # Write success metadata
        metadata = StageMetadata(
            start_date=start_date,
            end_date=end_date,
            branches=branches or [],
            version="aggregate_ticket_v1",
            last_run=datetime.now().isoformat(),
            status="ok",
        )
        write_metadata(paths.mart_sales, start_date, end_date, metadata)

        return result_df

    except Exception as e:
        logger.error("Error aggregating sales to ticket: %s", e)
        metadata = StageMetadata(
            start_date=start_date,
            end_date=end_date,
            branches=branches or [],
            version="aggregate_ticket_v1",
            last_run=datetime.now().isoformat(),
            status="failed",
        )
        write_metadata(paths.mart_sales, start_date, end_date, metadata)
        raise


def aggregate_to_group(
    paths: DataPaths,
    start_date: str,
    end_date: str,
    branches: list[str] | None = None,
) -> pd.DataFrame:
    """Aggregate sales into mart_sales_by_group (category pivot).

    Creates a pivot table with:
    - Rows: Product groups (categories)
    - Columns: Sucursales
    - Values: Total sales amounts

    Args:
        paths: DataPaths configuration.
        start_date: Start date in YYYY-MM-DD format.
        end_date: End date in YYYY-MM-DD format.
        branches: Optional list of branches to include (affects columns).

    Returns:
        DataFrame with mart_sales_by_group structure.

    """
    # Import the actual aggregation logic
    from pos_core.etl.marts.sales_by_group import build_category_pivot

    paths.ensure_dirs()

    ticket_path = paths.mart_sales / f"mart_sales_by_ticket_{start_date}_{end_date}.csv"
    output_path = paths.mart_sales / f"mart_sales_by_group_{start_date}_{end_date}.csv"

    logger.info("Aggregating sales to group mart for %s to %s", start_date, end_date)

    try:
        # Build from ticket-level mart first (if it doesn't exist)
        if not ticket_path.exists():
            aggregate_to_ticket(paths, start_date, end_date, branches)

        # Build the category pivot using the ticket mart as input
        result_df = build_category_pivot(
            input_csv=str(ticket_path),
            output_csv=str(output_path),
        )

        return result_df

    except Exception as e:
        logger.error("Error aggregating sales to group: %s", e)
        raise
