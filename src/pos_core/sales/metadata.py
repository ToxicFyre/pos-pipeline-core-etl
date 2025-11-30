"""Metadata tracking for sales ETL stages.

This module handles idempotence by tracking which ETL stages have been
completed for which date ranges.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class StageMetadata:
    """Metadata for a completed ETL stage.

    Attributes:
        start_date: Start date of the processed range.
        end_date: End date of the processed range.
        branches: List of branches processed.
        version: Version string for the stage logic.
        last_run: ISO timestamp of when stage was run.
        status: "ok", "failed", or "partial".
    """

    start_date: str
    end_date: str
    branches: list[str]
    version: str
    last_run: str
    status: str


def _meta_path(stage_dir: Path, start_date: str, end_date: str) -> Path:
    """Get path to metadata file for a date range."""
    meta_dir = stage_dir / "_meta"
    meta_dir.mkdir(parents=True, exist_ok=True)
    return meta_dir / f"{start_date}_{end_date}.json"


def write_metadata(
    stage_dir: Path,
    start_date: str,
    end_date: str,
    metadata: StageMetadata,
) -> None:
    """Write metadata file for a stage completion."""
    path = _meta_path(stage_dir, start_date, end_date)
    path.write_text(json.dumps(asdict(metadata), indent=2))
    logger.debug("Wrote metadata: %s", path)


def read_metadata(
    stage_dir: Path,
    start_date: str,
    end_date: str,
) -> Optional[StageMetadata]:
    """Read metadata file for a date range, if it exists."""
    path = _meta_path(stage_dir, start_date, end_date)
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text())
        return StageMetadata(**data)
    except Exception as e:
        logger.warning("Error reading metadata %s: %s", path, e)
        return None


def should_run_stage(
    stage_dir: Path,
    start_date: str,
    end_date: str,
    version: str,
) -> bool:
    """Check if a stage needs to run based on metadata.

    Returns True if:
    - No metadata exists for this date range
    - Metadata status is not "ok"
    - Metadata version doesn't match current version
    """
    meta = read_metadata(stage_dir, start_date, end_date)
    if meta is None:
        return True
    if meta.status != "ok":
        return True
    if meta.version != version:
        return True
    return False
