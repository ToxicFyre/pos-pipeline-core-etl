"""Public API for payments ETL pipeline.

This module provides a clean, configurable API for running the payments ETL
pipeline with explicit data_root paths instead of hardcoded directories.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from pos_core.exceptions import ConfigError

logger = logging.getLogger(__name__)


@dataclass
class PaymentsPaths:
    """All filesystem paths used by the payments ETL.

    Attributes:
        raw_payments: Directory for raw payment Excel files (e.g., data/a_raw/payments/batch).
        clean_payments: Directory for cleaned payment CSV files (e.g., data/b_clean/payments/batch).
        proc_payments: Directory for processed/aggregated payment data
            (e.g., data/c_processed/payments).
        sucursales_json: Path to sucursales.json configuration file.

    """

    raw_payments: Path
    clean_payments: Path
    proc_payments: Path
    sucursales_json: Path


@dataclass
class PaymentsETLConfig:
    """Configuration for the payments ETL pipeline.

    Attributes:
        paths: All filesystem paths used by the pipeline.
        chunk_size_days: Maximum number of days per HTTP request chunk (default: 180).
        excluded_branches: List of branch names to exclude from processing (default: ["CEDIS"]).

    """

    paths: PaymentsPaths
    chunk_size_days: int = 180
    excluded_branches: list[str] = field(default_factory=lambda: ["CEDIS"])

    @classmethod
    def from_data_root(
        cls,
        data_root: Path | str,
        sucursales_json: Path | str | None = None,
        chunk_size_days: int = 180,
    ) -> PaymentsETLConfig:
        """Build a default config given a data_root, using the existing directory convention.

        Uses the existing directory structure:
          data_root/a_raw/payments/batch
          data_root/b_clean/payments/batch
          data_root/c_processed/payments

        Args:
            data_root: Root directory for ETL data (will be converted to Path if string).
            sucursales_json: Optional path to sucursales.json. If None, defaults to
                data_root.parent / "utils" / "sucursales.json".
            chunk_size_days: Maximum days per HTTP request chunk (default: 180).

        Returns:
            PaymentsETLConfig instance with default paths.

        Examples:
            >>> from pathlib import Path
            >>> config = PaymentsETLConfig.from_data_root(Path("data"))
            >>> config.paths.raw_payments
            PosixPath('data/a_raw/payments/batch')

        """
        # Convert string to Path if needed
        if isinstance(data_root, str):
            data_root = Path(data_root)

        if sucursales_json is None:
            # Keep current convention: ../utils/sucursales.json
            sucursales_json = data_root.parent / "utils" / "sucursales.json"
        elif isinstance(sucursales_json, str):
            sucursales_json = Path(sucursales_json)

        paths = PaymentsPaths(
            raw_payments=data_root / "a_raw" / "payments" / "batch",
            clean_payments=data_root / "b_clean" / "payments" / "batch",
            proc_payments=data_root / "c_processed" / "payments",
            sucursales_json=sucursales_json,
        )

        return cls(paths=paths, chunk_size_days=chunk_size_days)

    @classmethod
    def from_root(
        cls,
        data_root: Path | str,
        sucursales_file: Path | str,
        chunk_size_days: int = 180,
    ) -> PaymentsETLConfig:
        """Build a default config given a data_root and sucursales file.

        Alias for `from_data_root` for consistency with SalesETLConfig.

        Args:
            data_root: Root directory for ETL data (will be converted to Path if string).
            sucursales_file: Path to sucursales.json configuration file
                (will be converted to Path if string).
            chunk_size_days: Maximum days per HTTP request chunk (default: 180).

        Returns:
            PaymentsETLConfig instance with default paths.

        """
        return cls.from_data_root(
            data_root=data_root,
            sucursales_json=sucursales_file,
            chunk_size_days=chunk_size_days,
        )


def ensure_dirs(config: PaymentsETLConfig) -> None:
    """Create all directories needed by the payments ETL.

    Args:
        config: PaymentsETLConfig instance containing paths to create.

    """
    for p in (
        config.paths.raw_payments,
        config.paths.clean_payments,
        config.paths.proc_payments,
    ):
        p.mkdir(parents=True, exist_ok=True)
        logger.debug("Ensured directory exists: %s", p)


def build_payments_dataset(
    start_date: str,
    end_date: str,
    config: PaymentsETLConfig,
    branches: list[str] | None = None,
    steps: list[str] | None = None,
) -> pd.DataFrame:
    """High-level entry point for the payments ETL.

    Orchestrates the complete payments ETL pipeline:
    1. Downloads missing payment reports from POS HTTP API
    2. Cleans the raw Excel files into normalized CSVs
    3. Aggregates cleaned data into a daily dataset

    Uses the existing ETL logic but:
    - Does NOT assume any hard-coded 'data/' directory
    - Uses config.paths.* for all filesystem I/O
    - Calls ensure_dirs(config) before writing anything
    - Returns the final aggregated payments DataFrame

    This function is a thin wrapper around the stage functions:
    - download_payments
    - clean_payments
    - aggregate_payments

    Args:
        start_date: Start date in YYYY-MM-DD format (inclusive).
        end_date: End date in YYYY-MM-DD format (inclusive).
        config: PaymentsETLConfig instance with all paths and settings.
        branches: Optional list of branch names to process. If None, processes all branches.
        steps: Optional list of steps to execute. Valid steps: "extract", "transform", "aggregate".
            If None, executes all steps in order.

    Returns:
        DataFrame containing the aggregated payments data (one row per sucursal + fecha).

    Raises:
        FileNotFoundError: If the aggregated file is expected but missing.
        ConfigError: If invalid step names are provided.

    Examples:
        >>> from pathlib import Path
        >>> from pos_core.etl.api import PaymentsETLConfig, build_payments_dataset
        >>> config = PaymentsETLConfig.from_data_root(Path("data"))
        >>> df = build_payments_dataset("2023-01-01", "2023-12-31", config)
        >>> df.head()

    """
    # Import here to avoid circular imports
    from pos_core.etl.payments import (
        aggregate_payments,
        clean_payments,
        download_payments,
    )

    ensure_dirs(config)

    # Normalize steps if provided, otherwise run all
    all_steps = ["extract", "transform", "aggregate"]
    if steps is None:
        steps = all_steps
    else:
        # preserve order but filter to known
        steps = [s for s in all_steps if s in steps]
        if not steps:
            raise ConfigError(f"No valid steps provided. Valid steps: {all_steps}")

    logger.info("Running payments ETL for %s to %s", start_date, end_date)
    logger.info("Steps to execute: %s", steps)

    if "extract" in steps:
        download_payments(start_date, end_date, config, branches)

    if "transform" in steps:
        clean_payments(start_date, end_date, config, branches)

    result_df = None
    if "aggregate" in steps:
        result_df = aggregate_payments(start_date, end_date, config, branches)
    else:
        # If we didn't aggregate now, but the file exists, load it
        aggregated_path = config.paths.proc_payments / "aggregated_payments_daily.csv"
        if aggregated_path.exists():
            logger.info("Loading existing aggregated file: %s", aggregated_path)
            result_df = pd.read_csv(aggregated_path)

    if result_df is None:
        aggregated_path = config.paths.proc_payments / "aggregated_payments_daily.csv"
        raise FileNotFoundError(
            f"Could not find aggregated payments file at {aggregated_path}. "
            "Run the 'aggregate' step to generate it."
        )

    logger.info("ETL complete. Returned %d rows.", len(result_df))
    return result_df
