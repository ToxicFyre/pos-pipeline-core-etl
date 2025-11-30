"""Silver layer: Clean sales Excel files into fact_sales_item_line.

This module transforms raw Excel files into the core fact table
(fact_sales_item_line) with one row per item/modifier line on a ticket.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pos_core.config import DataPaths

from pos_core.sales.metadata import StageMetadata, write_metadata

logger = logging.getLogger(__name__)


def clean_sales(
    paths: DataPaths,
    start_date: str,
    end_date: str,
    branches: list[str] | None = None,
) -> None:
    """Clean raw sales Excel files into fact_sales_item_line CSVs.

    The output is the core fact at item/modifier line grain:
    - Key: (sucursal, operating_date, order_id, item_key, [modifier_cols])
    - One row per item or modifier line on a ticket
    - Multiple rows can share the same order_id (ticket)

    Args:
        paths: DataPaths configuration.
        start_date: Start date in YYYY-MM-DD format.
        end_date: End date in YYYY-MM-DD format.
        branches: Optional list of branches (for metadata tracking).
    """
    # Import the actual cleaning logic
    from pos_core.etl.staging.sales_cleaner import (
        output_name_for,
        transform_detalle_ventas,
    )

    paths.ensure_dirs()

    logger.info("Cleaning sales for %s to %s", start_date, end_date)

    try:
        # Process each Excel file in the raw directory
        for xlsx_file in paths.raw_sales.glob("*.xlsx"):
            try:
                df = transform_detalle_ventas(xlsx_file)
                out_name_path = output_name_for(xlsx_file, df)
                out_path = paths.clean_sales / str(out_name_path)
                out_path.parent.mkdir(parents=True, exist_ok=True)
                df.to_csv(str(out_path), index=False, encoding="utf-8")
                logger.info("Cleaned: %s (%d rows)", out_path, len(df))
            except Exception as e:
                logger.warning("Error cleaning %s: %s", xlsx_file, e)

        # Write success metadata
        metadata = StageMetadata(
            start_date=start_date,
            end_date=end_date,
            branches=branches or [],
            version="transform_v1",
            last_run=datetime.now().isoformat(),
            status="ok",
        )
        write_metadata(paths.clean_sales, start_date, end_date, metadata)

    except Exception as e:
        logger.error("Error cleaning sales: %s", e)
        metadata = StageMetadata(
            start_date=start_date,
            end_date=end_date,
            branches=branches or [],
            version="transform_v1",
            last_run=datetime.now().isoformat(),
            status="failed",
        )
        write_metadata(paths.clean_sales, start_date, end_date, metadata)
        raise
