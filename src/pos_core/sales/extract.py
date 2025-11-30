"""Bronze layer: Download sales reports from Wansoft API.

This module handles downloading raw sales Excel files from the POS system.
The files are saved to the bronze layer (data/a_raw/sales/).
"""

from __future__ import annotations

import logging
import os
from datetime import date, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pos_core.config import DataPaths

from pos_core.sales.metadata import StageMetadata, write_metadata

logger = logging.getLogger(__name__)


def download_sales(
    paths: DataPaths,
    start_date: str,
    end_date: str,
    branches: list[str] | None = None,
) -> None:
    """Download raw sales Excel files from Wansoft API.

    Args:
        paths: DataPaths configuration.
        start_date: Start date in YYYY-MM-DD format.
        end_date: End date in YYYY-MM-DD format.
        branches: Optional list of branches to download. If None, downloads all.
    """
    # Import the actual extraction logic
    from pos_core.etl.branch_config import load_branch_segments_from_json
    from pos_core.etl.raw.extraction import (
        build_out_name,
        export_sales_report,
        login_if_needed,
        make_session,
    )

    paths.ensure_dirs()

    logger.info("Downloading sales for %s to %s", start_date, end_date)

    # Get base URL from environment
    base_url = os.environ.get("WS_BASE")
    if not base_url:
        raise ValueError("WS_BASE environment variable must be set for downloading data.")

    downloaded_branches: list[str] = []
    try:
        # Create session and authenticate
        session = make_session()
        login_if_needed(session, base_url=base_url, user=None, pwd=None)

        # Load branch configuration
        branch_segments = load_branch_segments_from_json(paths.sucursales_json)
        start_dt = date.fromisoformat(start_date)
        end_dt = date.fromisoformat(end_date)

        # Download reports for each branch
        for branch_name, segments in branch_segments.items():
            if branches is not None and branch_name not in branches:
                continue

            for segment in segments:
                code = segment.code
                if segment.valid_from and segment.valid_from > end_dt:
                    continue
                if segment.valid_to and segment.valid_to < start_dt:
                    continue

                try:
                    suggested, blob = export_sales_report(
                        s=session,
                        base_url=base_url,
                        report="Detail",
                        subsidiary_id=code,
                        start=start_dt,
                        end=end_dt,
                    )

                    out_name = build_out_name("Detail", branch_name, start_dt, end_dt, suggested)
                    out_path = paths.raw_sales / out_name
                    out_path.write_bytes(blob)
                    logger.info("Downloaded: %s", out_path)
                    if branch_name not in downloaded_branches:
                        downloaded_branches.append(branch_name)
                except Exception as e:
                    logger.warning("Error downloading %s (%s): %s", branch_name, code, e)

        # Write success metadata
        metadata = StageMetadata(
            start_date=start_date,
            end_date=end_date,
            branches=downloaded_branches,
            version="extract_v1",
            last_run=datetime.now().isoformat(),
            status="ok",
        )
        write_metadata(paths.raw_sales, start_date, end_date, metadata)

    except Exception as e:
        logger.error("Error downloading sales: %s", e)
        metadata = StageMetadata(
            start_date=start_date,
            end_date=end_date,
            branches=downloaded_branches,
            version="extract_v1",
            last_run=datetime.now().isoformat(),
            status="failed",
        )
        write_metadata(paths.raw_sales, start_date, end_date, metadata)
        raise
