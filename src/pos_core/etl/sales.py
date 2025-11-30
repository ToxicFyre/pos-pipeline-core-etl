"""Sales ETL stage functions - Orchestration layer.

This module orchestrates the sales ETL pipeline across data layers:
- download_sales: Raw (Bronze) layer - Download from Wansoft HTTP API
- clean_sales: Staging (Silver) layer - Transform raw Excel files into clean CSVs
- aggregate_sales: Marts (Gold) layer - Aggregate at specified level (ticket/group/day)

Grain and Layers
----------------
- **Core Fact (Silver+)**: The staging output IS the core sales fact
  (``fact_sales_item_line``) at item/modifier-line grain.
- **Marts (Gold)**: All aggregations in ``aggregate_sales()`` produce mart-level
  tables (ticket, group, day aggregations).

Data directory mapping:
    data/a_raw/      → Raw (Bronze) - Direct Wansoft exports
    data/b_clean/    → Staging (Silver) - Cleaned data = core fact (item-line grain)
    data/c_processed → Marts (Gold) - All aggregated tables (ticket, group, day)
"""

from __future__ import annotations

import logging
import os
from datetime import date, datetime
from typing import List, Optional

import pandas as pd

from pos_core.etl.metadata import (
    StageMetadata,
    should_skip_stage,
    write_metadata,
)
from pos_core.etl.sales_config import SalesETLConfig

logger = logging.getLogger(__name__)

# Cleaner version constant
SALES_CLEANER_VERSION = "sales_cleaner_v1"


def download_sales(
    start_date: str,
    end_date: str,
    config: SalesETLConfig,
    branches: Optional[List[str]] = None,
    force: bool = False,
) -> None:
    """Download raw sales Excel for the given range.

    Downloads sales detail reports from the POS HTTP API for the specified date range
    and branches. Skips already-downloaded files unless force=True.

    Args:
        start_date: Start date in YYYY-MM-DD format (inclusive).
        end_date: End date in YYYY-MM-DD format (inclusive).
        config: SalesETLConfig instance with paths and settings.
        branches: Optional list of branch names to process. If None, processes all branches.
        force: If True, re-download even if files already exist (default: False).

    Examples:
        >>> from pos_core.etl.sales_config import SalesETLConfig
        >>> config = SalesETLConfig.from_root("data", "utils/sucursales.json")
        >>> download_sales("2025-01-01", "2025-01-31", config)
    """
    # Raw (Bronze) layer: Extract from Wansoft HTTP API
    from pos_core.etl.branch_config import load_branch_segments_from_json
    from pos_core.etl.raw.extraction import (
        build_out_name,
        export_sales_report,
        login_if_needed,
        make_session,
    )

    # Check metadata for idempotence
    if should_skip_stage(
        config.paths.raw_sales,
        start_date,
        end_date,
        cleaner_version="download_v1",
        force=force,
    ):
        logger.info("Skipping download (already completed): %s to %s", start_date, end_date)
        return

    # Ensure output directory exists
    config.paths.raw_sales.mkdir(parents=True, exist_ok=True)

    # Get base URL from environment
    base_url = os.environ.get("WS_BASE")
    if not base_url:
        raise ValueError(
            "WS_BASE environment variable must be set. "
            "Set it in your environment or modify this script to set base_url directly."
        )

    # Create session and authenticate
    session = make_session()
    login_if_needed(session, base_url=base_url, user=None, pwd=None)

    # Load branch configuration
    branch_segments = load_branch_segments_from_json(config.paths.sucursales_json)
    start_dt = date.fromisoformat(start_date)
    end_dt = date.fromisoformat(end_date)

    logger.info("Downloading sales reports for %s to %s", start_date, end_date)

    downloaded_branches = []
    try:
        # Download reports for each branch
        for branch_name, segments in branch_segments.items():
            # Filter by branches if specified
            if branches is not None and branch_name not in branches:
                continue

            for segment in segments:
                code = segment.code
                # Check if this code was valid during the date range
                if segment.valid_from and segment.valid_from > end_dt:
                    continue
                if segment.valid_to and segment.valid_to < start_dt:
                    continue

                try:
                    # Export the report
                    suggested, blob = export_sales_report(
                        s=session,
                        base_url=base_url,
                        report="Detail",
                        subsidiary_id=code,
                        start=start_dt,
                        end=end_dt,
                    )

                    # Save file
                    out_name = build_out_name("Detail", branch_name, start_dt, end_dt, suggested)
                    out_path = config.paths.raw_sales / out_name
                    out_path.write_bytes(blob)
                    logger.info("Downloaded: %s", out_path)
                    if branch_name not in downloaded_branches:
                        downloaded_branches.append(branch_name)
                except Exception as e:
                    logger.warning("Error downloading %s (%s): %s", branch_name, code, e)

        # Write metadata
        metadata = StageMetadata(
            start_date=start_date,
            end_date=end_date,
            branches=downloaded_branches,
            cleaner_version="download_v1",
            last_run=datetime.now().isoformat(),
            status="ok",
        )
        write_metadata(config.paths.raw_sales, start_date, end_date, metadata)
    except Exception as e:
        logger.error("Error downloading sales: %s", e)
        # Write failed metadata
        metadata = StageMetadata(
            start_date=start_date,
            end_date=end_date,
            branches=downloaded_branches,
            cleaner_version="download_v1",
            last_run=datetime.now().isoformat(),
            status="failed",
        )
        write_metadata(config.paths.raw_sales, start_date, end_date, metadata)
        raise


def clean_sales(
    start_date: str,
    end_date: str,
    config: SalesETLConfig,
    branches: Optional[List[str]] = None,
    force: bool = False,
) -> None:
    """Transform raw sales files into clean CSV.

    Processes all raw sales Excel files in the input directory and writes
    normalized CSV files to the output directory.

    Args:
        start_date: Start date in YYYY-MM-DD format (inclusive).
        end_date: End date in YYYY-MM-DD format (inclusive).
        config: SalesETLConfig instance with paths and settings.
        branches: Optional list of branch names to process. If None, processes all branches.
        force: If True, re-clean even if files already exist (default: False).

    Examples:
        >>> from pos_core.etl.sales_config import SalesETLConfig
        >>> config = SalesETLConfig.from_root("data", "utils/sucursales.json")
        >>> clean_sales("2025-01-01", "2025-01-31", config)
    """
    # Staging (Silver) layer: Clean and normalize raw Excel files
    from pos_core.etl.staging.sales_cleaner import (
        output_name_for,
        transform_detalle_ventas,
    )

    # Check metadata for idempotence
    if should_skip_stage(
        config.paths.clean_sales,
        start_date,
        end_date,
        cleaner_version=SALES_CLEANER_VERSION,
        force=force,
    ):
        logger.info("Skipping clean (already completed): %s to %s", start_date, end_date)
        return

    # Ensure output directory exists
    config.paths.clean_sales.mkdir(parents=True, exist_ok=True)

    logger.info("Cleaning sales files for %s to %s", start_date, end_date)

    try:
        for xlsx_file in config.paths.raw_sales.glob("*.xlsx"):
            try:
                df = transform_detalle_ventas(xlsx_file)
                out_name_path = output_name_for(xlsx_file, df)
                out_path = config.paths.clean_sales / str(out_name_path)
                out_path.parent.mkdir(parents=True, exist_ok=True)
                df.to_csv(str(out_path), index=False, encoding="utf-8")
                logger.info("Cleaned: %s (%d rows)", out_path, len(df))
            except Exception as e:
                logger.warning("Error cleaning %s: %s", xlsx_file, e)

        # Write metadata
        metadata = StageMetadata(
            start_date=start_date,
            end_date=end_date,
            branches=branches or [],
            cleaner_version=SALES_CLEANER_VERSION,
            last_run=datetime.now().isoformat(),
            status="ok",
        )
        write_metadata(config.paths.clean_sales, start_date, end_date, metadata)
    except Exception as e:
        logger.error("Error cleaning sales: %s", e)
        # Write failed metadata
        metadata = StageMetadata(
            start_date=start_date,
            end_date=end_date,
            branches=branches or [],
            cleaner_version=SALES_CLEANER_VERSION,
            last_run=datetime.now().isoformat(),
            status="failed",
        )
        write_metadata(config.paths.clean_sales, start_date, end_date, metadata)
        raise


def aggregate_sales(
    start_date: str,
    end_date: str,
    config: SalesETLConfig,
    level: str = "ticket",  # "ticket" | "group" | "day"
    branches: Optional[List[str]] = None,
    force: bool = False,
) -> pd.DataFrame:
    """Aggregate clean sales at the specified level.

    Aggregates cleaned sales CSV files at the specified level:
    - "ticket": One row per ticket/order
    - "group": Pivot table with groups as rows, sucursales as columns
    - "day": One row per sucursal per day (if implemented)

    Args:
        start_date: Start date in YYYY-MM-DD format (inclusive).
        end_date: End date in YYYY-MM-DD format (inclusive).
        config: SalesETLConfig instance with paths and settings.
        level: Aggregation level: "ticket", "group", or "day" (default: "ticket").
        branches: Optional list of branch names to process. If None, processes all branches.
        force: If True, re-aggregate even if file already exists (default: False).

    Returns:
        DataFrame containing aggregated sales data at the specified level.

    Raises:
        ValueError: If level is not one of "ticket", "group", or "day".

    Examples:
        >>> from pos_core.etl.sales_config import SalesETLConfig
        >>> config = SalesETLConfig.from_root("data", "utils/sucursales.json")
        >>> df = aggregate_sales("2025-01-01", "2025-01-31", config, level="ticket")
        >>> len(df)
        100
    """
    # Marts (Gold) layer: Ticket-level aggregation (from item-line core facts)
    # Marts (Gold) layer: Category pivot tables (from ticket-level data)
    from pos_core.etl.marts.sales_by_group import build_category_pivot
    from pos_core.etl.marts.sales_by_ticket import aggregate_by_ticket

    # Check metadata for idempotence (level-specific)
    meta_key = f"aggregate_{level}_v1"
    if should_skip_stage(
        config.paths.proc_sales,
        start_date,
        end_date,
        cleaner_version=meta_key,
        force=force,
    ):
        logger.info(
            "Skipping aggregate (already completed): %s to %s at level %s",
            start_date,
            end_date,
            level,
        )
        # Try to load existing file
        if level == "ticket":
            ticket_csv = config.paths.proc_sales / f"sales_by_ticket_{start_date}_{end_date}.csv"
            if ticket_csv.exists():
                return pd.read_csv(ticket_csv)
        elif level == "group":
            group_csv = config.paths.proc_sales / f"sales_by_group_{start_date}_{end_date}.csv"
            if group_csv.exists():
                return pd.read_csv(group_csv)

    # Ensure output directory exists
    config.paths.proc_sales.mkdir(parents=True, exist_ok=True)

    logger.info("Aggregating sales data for %s to %s at level %s", start_date, end_date, level)

    try:
        if level == "ticket":
            # Aggregate by ticket
            ticket_csv = config.paths.proc_sales / f"sales_by_ticket_{start_date}_{end_date}.csv"
            result_df = aggregate_by_ticket(
                input_csv=str(config.paths.clean_sales / "*.csv"),
                output_csv=str(ticket_csv),
                recursive=True,
            )
            logger.info("Aggregated by ticket: %d tickets", len(result_df))

            # Write metadata
            metadata = StageMetadata(
                start_date=start_date,
                end_date=end_date,
                branches=branches or [],
                cleaner_version=meta_key,
                last_run=datetime.now().isoformat(),
                status="ok",
            )
            write_metadata(config.paths.proc_sales, start_date, end_date, metadata)
            return result_df

        elif level == "group":
            # First aggregate by ticket, then build category pivot
            ticket_csv = config.paths.proc_sales / f"sales_by_ticket_{start_date}_{end_date}.csv"
            if not ticket_csv.exists() or force:
                # Need to aggregate by ticket first
                aggregate_by_ticket(
                    input_csv=str(config.paths.clean_sales / "*.csv"),
                    output_csv=str(ticket_csv),
                    recursive=True,
                )

            # Build category pivot
            group_csv = config.paths.proc_sales / f"sales_by_group_{start_date}_{end_date}.csv"
            result_df = build_category_pivot(input_csv=str(ticket_csv), output_csv=str(group_csv))
            logger.info("Aggregated by group: pivot table with %d groups", len(result_df))

            # Write metadata
            metadata = StageMetadata(
                start_date=start_date,
                end_date=end_date,
                branches=branches or [],
                cleaner_version=meta_key,
                last_run=datetime.now().isoformat(),
                status="ok",
            )
            write_metadata(config.paths.proc_sales, start_date, end_date, metadata)
            return result_df

        elif level == "day":
            # TODO: Implement day-level aggregation if needed
            raise NotImplementedError("Day-level aggregation not yet implemented")

        else:
            raise ValueError(f"Invalid level: {level}. Must be one of 'ticket', 'group', 'day'")
    except Exception as e:
        logger.error("Error aggregating sales: %s", e)
        # Write failed metadata
        metadata = StageMetadata(
            start_date=start_date,
            end_date=end_date,
            branches=branches or [],
            cleaner_version=meta_key,
            last_run=datetime.now().isoformat(),
            status="failed",
        )
        write_metadata(config.paths.proc_sales, start_date, end_date, metadata)
        raise
