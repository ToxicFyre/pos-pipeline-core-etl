r"""CLI entry point for payments ETL pipeline.

This module provides a command-line interface for running the payments ETL
pipeline. The actual ETL logic is implemented in pos_core.etl.api.

Examples:
    Command-line usage:
        # Download and process payments from 2022-11-01 to today
        python -m pos_core.etl.build_payments_dataset

        # Process a specific date range
        python -m pos_core.etl.build_payments_dataset \\
            --start 2023-01-01 --end 2023-12-31

        # Use custom data root
        python -m pos_core.etl.build_payments_dataset \\
            --data-root /path/to/data --start 2023-01-01 --end 2023-12-31

"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

from pos_core.etl.api import PaymentsETLConfig, build_payments_dataset
from pos_core.etl.utils import parse_date


def main() -> None:
    """Execute the build_payments_dataset command-line tool.

    Parses command-line arguments and executes the payments ETL pipeline
    using the new API.

    Command-line arguments:
        --start: Global start date (YYYY-MM-DD, default: 2022-11-01)
        --end: Global end date (YYYY-MM-DD, default: today)
        --data-root: Root directory for ETL data (default: "data")
        --max-days-per-chunk: Maximum days per HTTP request (default: 180)
        --verbose: Enable verbose logging (DEBUG level)

    Raises:
        SystemExit: If start date is after end date or other errors occur.

    Examples:
        $ python -m pos_core.etl.build_payments_dataset
        $ python -m pos_core.etl.build_payments_dataset --start 2023-01-01
        $ python -m pos_core.etl.build_payments_dataset --data-root /custom/path

    """
    parser = argparse.ArgumentParser(
        description=(
            "Backfill payments dataset from POS (all branches, skipping already-downloaded ranges)."
        )
    )
    parser.add_argument(
        "--start",
        type=str,
        default="2022-11-01",
        help="Global start date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--end",
        type=str,
        default=date.today().isoformat(),
        help="Global end date (YYYY-MM-DD, default: today).",
    )
    parser.add_argument(
        "--data-root",
        type=str,
        default="data",
        help="Root directory for ETL data (default: ./data)",
    )
    parser.add_argument(
        "--max-days-per-chunk",
        type=int,
        default=180,
        help=("Maximum number of days per HTTP request chunk (inclusive)."),
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging (DEBUG level).",
    )

    args = parser.parse_args()

    # Set up logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(levelname)s: %(message)s",
    )

    # Parse dates
    try:
        start_date = parse_date(args.start)
        end_date = parse_date(args.end)
    except ValueError as e:
        raise SystemExit(f"ERROR: Invalid date format. {e}") from e

    if start_date > end_date:
        raise SystemExit("ERROR: start date is after end date.")

    # Build config
    data_root = Path(args.data_root)
    config = PaymentsETLConfig.from_data_root(
        data_root=data_root,
        chunk_size_days=args.max_days_per_chunk,
    )

    print(f"Data root: {data_root}")
    print(f"Start date: {args.start}")
    print(f"End date: {args.end}")
    print(f"Chunk size: {args.max_days_per_chunk} days")
    print()

    # Run ETL
    try:
        df = build_payments_dataset(
            start_date=args.start,
            end_date=args.end,
            config=config,
        )
        print(f"\nDONE. Aggregated payments dataset: {len(df)} rows")
        print(f"Output: {config.paths.proc_payments / 'aggregated_payments_daily.csv'}")
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
