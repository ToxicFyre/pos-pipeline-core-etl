"""Bronze layer: Download payment reports from Wansoft API.

This module handles downloading raw payment Excel files from the POS system.
The files are saved to the bronze layer (data/a_raw/payments/).
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pos_core.config import DataPaths

from pos_core.payments.metadata import StageMetadata, write_metadata

logger = logging.getLogger(__name__)


def download_payments(
    paths: DataPaths,
    start_date: str,
    end_date: str,
    branches: list[str] | None = None,
) -> None:
    """Download raw payment Excel files from Wansoft API.

    Args:
        paths: DataPaths configuration.
        start_date: Start date in YYYY-MM-DD format.
        end_date: End date in YYYY-MM-DD format.
        branches: Optional list of branches to download. If None, downloads all.
    """
    # Import the actual extraction logic
    from pos_core.etl.raw.extraction import download_payments_reports

    paths.ensure_dirs()

    logger.info("Downloading payments for %s to %s", start_date, end_date)

    try:
        download_payments_reports(
            start_date=start_date,
            end_date=end_date,
            output_dir=paths.raw_payments,
            sucursales_json=paths.sucursales_json,
            branches=branches,
            chunk_size_days=180,
        )

        # Write success metadata
        metadata = StageMetadata(
            start_date=start_date,
            end_date=end_date,
            branches=branches or [],
            version="extract_v1",
            last_run=datetime.now().isoformat(),
            status="ok",
        )
        write_metadata(paths.raw_payments, start_date, end_date, metadata)

    except Exception as e:
        logger.error("Error downloading payments: %s", e)
        metadata = StageMetadata(
            start_date=start_date,
            end_date=end_date,
            branches=branches or [],
            version="extract_v1",
            last_run=datetime.now().isoformat(),
            status="failed",
        )
        write_metadata(paths.raw_payments, start_date, end_date, metadata)
        raise
