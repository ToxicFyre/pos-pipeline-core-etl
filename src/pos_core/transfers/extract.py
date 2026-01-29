"""Bronze layer: Download transfer reports from Wansoft API.

This module handles downloading raw transfer Excel files from the POS system.
The files are saved to the bronze layer (data/a_raw/transfers/).

Note: Unlike payments/sales which download per-branch, transfers are downloaded
from CEDIS (the central warehouse) and contain all destination branches in a
single report.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pos_core.config import DataPaths

from pos_core.transfers.metadata import StageMetadata, write_metadata

logger = logging.getLogger(__name__)

# Default CEDIS code - this is the central warehouse that issues transfers
DEFAULT_CEDIS_CODE = "5392"


def _load_cedis_code(sucursales_path: Path) -> str:
    """Load CEDIS code from sucursales.json, or use default.

    Transfers are always downloaded from CEDIS (the central warehouse).
    This function tries to find a CEDIS entry in sucursales.json,
    falling back to the default code if not found.

    Args:
        sucursales_path: Path to sucursales.json file.

    Returns:
        CEDIS subsidiary code.

    """
    try:
        data = json.loads(sucursales_path.read_text(encoding="utf-8"))
        # Look for CEDIS entry (case-insensitive)
        for key, rec in data.items():
            if key.upper() == "CEDIS" or key.upper().startswith("CEDIS"):
                return str(rec["code"]) if isinstance(rec, dict) else str(rec)
    except Exception as e:
        logger.warning("Could not load CEDIS code from %s: %s", sucursales_path, e)

    logger.info("Using default CEDIS code: %s", DEFAULT_CEDIS_CODE)
    return DEFAULT_CEDIS_CODE


def download_transfers(
    paths: DataPaths,
    start_date: str,
    end_date: str,
    branches: list[str] | None = None,  # noqa: ARG001
) -> None:
    """Download raw transfer Excel files from Wansoft API.

    Downloads Inventory > Transfers > Issued report from CEDIS (central warehouse)
    for the specified date range. The report contains transfers to all branches.

    Unlike payments/sales which are downloaded per-branch, transfers are always
    downloaded from CEDIS as a single report containing all destination branches.

    Args:
        paths: DataPaths configuration.
        start_date: Start date in YYYY-MM-DD format.
        end_date: End date in YYYY-MM-DD format.
        branches: Unused for transfers (kept for API consistency).
            Transfers always download from CEDIS.

    Raises:
        ValueError: If required environment variables are not set.

    """
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

    # Get CEDIS code - transfers are always from CEDIS
    cedis_code = _load_cedis_code(paths.sucursales_json)

    try:
        # Create session and authenticate
        s = make_session()
        login_if_needed(s, base_url, None, None)

        # Create output directory
        output_dir = paths.raw_transfers / "CEDIS" / cedis_code
        output_dir.mkdir(parents=True, exist_ok=True)

        logger.info("Downloading transfers from CEDIS (code=%s)", cedis_code)

        # Export the report
        _suggested, blob = export_transfers_issued(
            s=s,
            base_url=base_url,
            subsidiary_id=cedis_code,
            start=global_start,
            end=global_end,
        )

        # Save file with standardized name
        out_name = f"TransfersIssued_CEDIS_{global_start}_{global_end}.xlsx"
        out_path = output_dir / out_name
        out_path.write_bytes(blob)
        logger.info("Saved %s (%d bytes)", out_path, len(blob))

        # Write success metadata
        metadata = StageMetadata(
            start_date=start_date,
            end_date=end_date,
            branches=["CEDIS"],
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
            branches=["CEDIS"],
            version="extract_v1",
            last_run=datetime.now().isoformat(),
            status="failed",
        )
        write_metadata(paths.raw_transfers, start_date, end_date, metadata)
        raise
