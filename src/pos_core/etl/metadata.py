"""Metadata handling for ETL stages.

This module provides utilities for storing and reading stage metadata to enable
idempotent ETL operations. Metadata is stored as JSON files in _meta/ subdirectories.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass
class StageMetadata:
    """Metadata for an ETL stage run.

    Attributes:
        start_date: Start date in YYYY-MM-DD format.
        end_date: End date in YYYY-MM-DD format.
        branches: List of branch names processed.
        cleaner_version: Version identifier for the cleaner (e.g., "sales_cleaner_v1").
        last_run: ISO timestamp of when the stage was run.
        status: Status of the stage run: "ok", "failed", or "partial".

    """

    start_date: str
    end_date: str
    branches: list[str]
    cleaner_version: str
    last_run: str  # ISO timestamp
    status: str  # "ok" | "failed" | "partial"

    def to_dict(self) -> dict:
        """Convert metadata to dictionary for JSON serialization."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> StageMetadata:
        """Create metadata from dictionary."""
        return cls(**data)


def metadata_path(stage_dir: Path, start_date: str, end_date: str) -> Path:
    """Compute the metadata file path for a date range.

    Args:
        stage_dir: Directory for the stage (e.g., a_raw/sales).
        start_date: Start date in YYYY-MM-DD format.
        end_date: End date in YYYY-MM-DD format.

    Returns:
        Path to the metadata JSON file.

    """
    meta_dir = stage_dir / "_meta"
    filename = f"{start_date}_{end_date}.json"
    return meta_dir / filename


def write_metadata(
    stage_dir: Path,
    start_date: str,
    end_date: str,
    metadata: StageMetadata,
) -> None:
    """Write metadata JSON to _meta/ subdirectory.

    Args:
        stage_dir: Directory for the stage (e.g., a_raw/sales).
        start_date: Start date in YYYY-MM-DD format.
        end_date: End date in YYYY-MM-DD format.
        metadata: Metadata to write.

    Examples:
        >>> from pathlib import Path
        >>> meta = StageMetadata(
        ...     start_date="2025-01-01",
        ...     end_date="2025-01-31",
        ...     branches=["Kavia"],
        ...     cleaner_version="v1",
        ...     last_run="2025-01-15T12:00:00",
        ...     status="ok"
        ... )
        >>> write_metadata(Path("data/a_raw/sales"), "2025-01-01", "2025-01-31", meta)

    """
    meta_path = metadata_path(stage_dir, start_date, end_date)
    meta_path.parent.mkdir(parents=True, exist_ok=True)

    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(metadata.to_dict(), f, indent=2, ensure_ascii=False)


def read_metadata(
    stage_dir: Path,
    start_date: str,
    end_date: str,
) -> StageMetadata | None:
    """Read metadata JSON if it exists.

    Args:
        stage_dir: Directory for the stage (e.g., a_raw/sales).
        start_date: Start date in YYYY-MM-DD format.
        end_date: End date in YYYY-MM-DD format.

    Returns:
        StageMetadata if file exists, None otherwise.

    Examples:
        >>> meta = read_metadata(Path("data/a_raw/sales"), "2025-01-01", "2025-01-31")
        >>> if meta and meta.status == "ok":
        ...     print("Stage already completed")

    """
    meta_path = metadata_path(stage_dir, start_date, end_date)

    if not meta_path.exists():
        return None

    try:
        with open(meta_path, encoding="utf-8") as f:
            data = json.load(f)
        return StageMetadata.from_dict(data)
    except (json.JSONDecodeError, KeyError, TypeError):
        # If metadata file is corrupted, treat as missing
        return None


def should_skip_stage(
    stage_dir: Path,
    start_date: str,
    end_date: str,
    cleaner_version: str,
    force: bool = False,
) -> bool:
    """Check if a stage should be skipped based on metadata.

    Args:
        stage_dir: Directory for the stage (e.g., a_raw/sales).
        start_date: Start date in YYYY-MM-DD format.
        end_date: End date in YYYY-MM-DD format.
        cleaner_version: Current cleaner version to check against.
        force: If True, never skip (default: False).

    Returns:
        True if stage should be skipped, False otherwise.

    """
    if force:
        return False

    metadata = read_metadata(stage_dir, start_date, end_date)
    if metadata is None:
        return False

    # Check if status is ok and cleaner version matches
    return bool(metadata.status == "ok" and metadata.cleaner_version == cleaner_version)
