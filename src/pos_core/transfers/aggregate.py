"""Gold layer: Aggregate transfers into mart_transfers_pivot.

This module aggregates the core fact (fact_transfers_line) into
a pivot table showing transfer costs by branch and product category.
"""

from __future__ import annotations

import glob
import logging
from datetime import datetime
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from pos_core.config import DataPaths

from pos_core.transfers.metadata import StageMetadata, write_metadata

logger = logging.getLogger(__name__)


def aggregate_to_pivot(
    paths: DataPaths,
    start_date: str,
    end_date: str,
    branches: list[str] | None = None,
    include_cedis: bool = False,
) -> pd.DataFrame:
    """Aggregate fact_transfers_line into mart_transfers_pivot.

    Creates a "Gasto de Insumos" pivot table with:
    - Rows: Categories (No-Procesados, Cafe, Comida Salada, etc.)
    - Columns: Branches (Kavia, PV, Qin, Zambrano, Carreta, Nativa, Crediclub)

    Args:
        paths: DataPaths configuration.
        start_date: Start date in YYYY-MM-DD format.
        end_date: End date in YYYY-MM-DD format.
        branches: Optional list of branches to include.
        include_cedis: If True, include rows where destination is CEDIS.

    Returns:
        DataFrame with mart_transfers_pivot structure.

    """
    from pos_core.etl.marts.transfers import build_table

    paths.ensure_dirs()

    logger.info("Aggregating transfers to pivot mart for %s to %s", start_date, end_date)

    try:
        # First, we need to combine all cleaned CSVs into a single file
        # The build_table function expects a single CSV path
        csv_pattern = str(paths.clean_transfers / "**/*.csv")
        csv_files = glob.glob(csv_pattern, recursive=True)

        if not csv_files:
            logger.warning("No cleaned transfer CSVs found in %s", paths.clean_transfers)
            # Return empty DataFrame with Gasto de Insumos structure
            from pos_core.etl.marts.transfers import BRANCH_COL_ORDER, CATEGORY_ROW_ORDER

            empty_df = pd.DataFrame(
                index=[*CATEGORY_ROW_ORDER, "TOTAL"],
                columns=[*BRANCH_COL_ORDER, "TOTAL"],
                dtype=float,
            )
            empty_df = empty_df.fillna(0.0)
            return empty_df

        # Concatenate all cleaned CSVs
        dfs = []
        for csv_file in csv_files:
            try:
                df = pd.read_csv(csv_file, encoding="utf-8-sig")
                dfs.append(df)
            except Exception as e:
                logger.warning("Failed to read %s: %s", csv_file, e)

        if not dfs:
            raise ValueError("No valid CSV files could be read")

        combined_df = pd.concat(dfs, ignore_index=True)

        # Save combined CSV temporarily for build_table
        combined_path = paths.mart_transfers / "_temp_combined.csv"
        paths.mart_transfers.mkdir(parents=True, exist_ok=True)
        combined_df.to_csv(combined_path, index=False, encoding="utf-8-sig")

        try:
            # Build the pivot table
            result_df, unmapped = build_table(str(combined_path), include_cedis=include_cedis)

            if len(unmapped) > 0:
                lost = pd.to_numeric(unmapped["Costo"], errors="coerce").fillna(0).sum()
                logger.warning("%d unmapped rows (total $%.2f)", len(unmapped), lost)

            # Save the mart with date-stamped filename
            output_path = paths.mart_transfers / f"mart_transfers_pivot_{start_date}_{end_date}.csv"
            result_df.to_csv(output_path, index=True, encoding="utf-8-sig")
            logger.info("Saved mart to %s", output_path)

        finally:
            # Clean up temp file
            if combined_path.exists():
                combined_path.unlink()

        # Write success metadata
        metadata = StageMetadata(
            start_date=start_date,
            end_date=end_date,
            branches=branches or [],
            version="aggregate_pivot_v1",
            last_run=datetime.now().isoformat(),
            status="ok",
        )
        write_metadata(paths.mart_transfers, start_date, end_date, metadata)

        return result_df

    except Exception as e:
        logger.error("Error aggregating transfers: %s", e)
        metadata = StageMetadata(
            start_date=start_date,
            end_date=end_date,
            branches=branches or [],
            version="aggregate_pivot_v1",
            last_run=datetime.now().isoformat(),
            status="failed",
        )
        write_metadata(paths.mart_transfers, start_date, end_date, metadata)
        raise
