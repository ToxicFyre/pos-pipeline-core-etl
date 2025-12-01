"""Silver layer: Clean payment Excel files into fact_payments_ticket.

This module transforms raw Excel files into the core fact table
(fact_payments_ticket) with one row per ticket x payment method.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pos_core.config import DataPaths

from pos_core.payments.metadata import StageMetadata, write_metadata

logger = logging.getLogger(__name__)


def clean_payments(
    paths: DataPaths,
    start_date: str,
    end_date: str,
    branches: list[str] | None = None,
) -> None:
    """Clean raw payment Excel files into fact_payments_ticket CSVs.

    The output is the core fact at ticket x payment method grain:
    - Key: (sucursal, operating_date, order_index, payment_method)
    - One row per payment line on a ticket

    Args:
        paths: DataPaths configuration.
        start_date: Start date in YYYY-MM-DD format.
        end_date: End date in YYYY-MM-DD format.
        branches: Optional list of branches (for metadata tracking).

    """
    # Import the actual cleaning logic
    from pos_core.etl.staging.payments_cleaner import clean_payments_directory

    paths.ensure_dirs()

    logger.info("Cleaning payments for %s to %s", start_date, end_date)

    try:
        clean_payments_directory(
            input_dir=paths.raw_payments,
            output_dir=paths.clean_payments,
            recursive=True,
        )

        # Write success metadata
        metadata = StageMetadata(
            start_date=start_date,
            end_date=end_date,
            branches=branches or [],
            version="transform_v1",
            last_run=datetime.now().isoformat(),
            status="ok",
        )
        write_metadata(paths.clean_payments, start_date, end_date, metadata)

    except Exception as e:
        logger.error("Error cleaning payments: %s", e)
        metadata = StageMetadata(
            start_date=start_date,
            end_date=end_date,
            branches=branches or [],
            version="transform_v1",
            last_run=datetime.now().isoformat(),
            status="failed",
        )
        write_metadata(paths.clean_payments, start_date, end_date, metadata)
        raise
