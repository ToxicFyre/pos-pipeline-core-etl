"""Silver layer: Clean transfer Excel files into fact_transfers_line.

This module transforms raw Excel files into the core fact table
(fact_transfers_line) with one row per transfer line item.
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pos_core.config import DataPaths

from pos_core.transfers.metadata import StageMetadata, write_metadata

logger = logging.getLogger(__name__)


def clean_transfers_directory(
    input_dir: Path,
    output_dir: Path,
    recursive: bool = True,
) -> list[Path]:
    """Clean all transfer Excel files in a directory.

    Processes all Excel files in the input directory and writes cleaned
    CSV files to the output directory, preserving the directory structure.

    Args:
        input_dir: Directory containing raw Excel files.
        output_dir: Directory to write cleaned CSV files.
        recursive: If True, search subdirectories recursively.

    Returns:
        List of paths to the cleaned CSV files.

    """
    from pos_core.etl.staging.transfer_cleaner import clean_to_minimal_csv

    pattern = "**/*.xlsx" if recursive else "*.xlsx"
    excel_files = list(input_dir.glob(pattern))

    if not excel_files:
        logger.warning("No Excel files found in %s", input_dir)
        return []

    cleaned_files: list[Path] = []

    for excel_path in excel_files:
        # Skip temp files
        if excel_path.name.startswith("~"):
            continue

        # Compute relative path to preserve directory structure
        try:
            rel_path = excel_path.relative_to(input_dir)
        except ValueError:
            rel_path = Path(excel_path.name)

        # Output path with .csv extension
        output_path = output_dir / rel_path.with_suffix(".csv")
        output_path.parent.mkdir(parents=True, exist_ok=True)

        logger.debug("Cleaning %s -> %s", excel_path, output_path)

        try:
            clean_to_minimal_csv(excel_path, output_path)
            cleaned_files.append(output_path)
        except Exception as e:
            logger.error("Failed to clean %s: %s", excel_path, e)
            # Continue processing other files

    logger.info("Cleaned %d transfer files", len(cleaned_files))
    return cleaned_files


def clean_transfers(
    paths: DataPaths,
    start_date: str,
    end_date: str,
    branches: list[str] | None = None,
) -> None:
    """Clean raw transfer Excel files into fact_transfers_line CSVs.

    The output is the core fact at transfer line grain:
    - Key: (orden, almacen_origen, sucursal_destino, producto)
    - One row per transfer line item

    Args:
        paths: DataPaths configuration.
        start_date: Start date in YYYY-MM-DD format.
        end_date: End date in YYYY-MM-DD format.
        branches: Optional list of branches (for metadata tracking).

    """
    paths.ensure_dirs()

    logger.info("Cleaning transfers for %s to %s", start_date, end_date)

    try:
        clean_transfers_directory(
            input_dir=paths.raw_transfers,
            output_dir=paths.clean_transfers,
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
        write_metadata(paths.clean_transfers, start_date, end_date, metadata)

    except Exception as e:
        logger.error("Error cleaning transfers: %s", e)
        metadata = StageMetadata(
            start_date=start_date,
            end_date=end_date,
            branches=branches or [],
            version="transform_v1",
            last_run=datetime.now().isoformat(),
            status="failed",
        )
        write_metadata(paths.clean_transfers, start_date, end_date, metadata)
        raise
