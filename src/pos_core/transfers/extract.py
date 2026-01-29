"""Bronze layer: Download transfer reports from Wansoft API.

This module handles downloading raw transfer Excel files from the POS system.
The files are saved to the bronze layer (data/a_raw/transfers/).
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pos_core.config import DataPaths

from pos_core.transfers.metadata import StageMetadata, write_metadata

logger = logging.getLogger(__name__)


def download_transfers(
    paths: DataPaths,
    start_date: str,
    end_date: str,
    branches: list[str] | None = None,
) -> None:
    """Download raw transfer Excel files from Wansoft API.

    Downloads Inventory > Transfers > Issued reports for the specified date range
    and branches. Files are saved to paths.raw_transfers.

    Args:
        paths: DataPaths configuration.
        start_date: Start date in YYYY-MM-DD format.
        end_date: End date in YYYY-MM-DD format.
        branches: Optional list of branches to download. If None, downloads all.

    Raises:
        ValueError: If required environment variables are not set.

    """
    from pos_core.etl.branch_config import load_branch_segments_from_json
    from pos_core.etl.raw.extraction import (
        export_transfers_issued,
        login_if_needed,
        make_session,
    )
    from pos_core.etl.utils import parse_date

    paths.ensure_dirs()

    logger.info("Downloading transfers for %s to %s", start_date, end_date)

    # Get base URL from environment
    base_url = os.environ.get("WS_BASE")
    if not base_url:
        raise ValueError("WS_BASE environment variable must be set")
    base_url = base_url.rstrip("/")

    # Parse dates
    global_start = parse_date(start_date)
    global_end = parse_date(end_date)

    try:
        # Load branch segments
        branch_segments = load_branch_segments_from_json(paths.sucursales_json)

        # Filter branches if specified
        if branches is not None:
            branch_segments = {
                name: windows for name, windows in branch_segments.items() if name in branches
            }

        # Create session and authenticate
        s = make_session()
        login_if_needed(s, base_url, None, None)

        # Download for each branch
        for branch_name, windows in branch_segments.items():
            logger.info("Processing branch: %s", branch_name)
            for seg in windows:
                # Calculate intersection of code window with requested date range
                seg_start = max(global_start, seg.valid_from)
                seg_end = min(global_end, seg.valid_to or global_end)
                if seg_start > seg_end:
                    continue

                code = seg.code
                branch_dir = paths.raw_transfers / branch_name / code
                branch_dir.mkdir(parents=True, exist_ok=True)

                logger.info(
                    "  Downloading transfers for code=%s, %s to %s",
                    code,
                    seg_start,
                    seg_end,
                )

                # Export the report
                _suggested, blob = export_transfers_issued(
                    s=s,
                    base_url=base_url,
                    subsidiary_id=code,
                    start=seg_start,
                    end=seg_end,
                )

                # Save file with standardized name
                out_name = f"TransfersIssued_{branch_name}_{seg_start}_{seg_end}.xlsx"
                out_path = branch_dir / out_name
                out_path.write_bytes(blob)
                logger.debug("Saved %s (%d bytes)", out_path, len(blob))

        # Write success metadata
        metadata = StageMetadata(
            start_date=start_date,
            end_date=end_date,
            branches=branches or [],
            version="extract_v1",
            last_run=datetime.now().isoformat(),
            status="ok",
        )
        write_metadata(paths.raw_transfers, start_date, end_date, metadata)

    except Exception as e:
        logger.error("Error downloading transfers: %s", e)
        metadata = StageMetadata(
            start_date=start_date,
            end_date=end_date,
            branches=branches or [],
            version="extract_v1",
            last_run=datetime.now().isoformat(),
            status="failed",
        )
        write_metadata(paths.raw_transfers, start_date, end_date, metadata)
        raise
