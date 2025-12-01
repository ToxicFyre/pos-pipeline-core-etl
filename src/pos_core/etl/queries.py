"""Query functions for ETL data.

This module provides high-level query functions that return DataFrames and
automatically run ETL stages only when needed based on metadata.
"""

from __future__ import annotations

import logging

import pandas as pd

from pos_core.etl.api import PaymentsETLConfig
from pos_core.etl.metadata import read_metadata
from pos_core.etl.payments import (
    aggregate_payments,
    clean_payments,
    download_payments,
)
from pos_core.etl.sales import (
    aggregate_sales,
    clean_sales,
    download_sales,
)
from pos_core.etl.sales_config import SalesETLConfig

logger = logging.getLogger(__name__)


def get_sales(
    start_date: str,
    end_date: str,
    config: SalesETLConfig,
    branches: list[str] | None = None,
    level: str = "ticket",  # "ticket" | "group" | "day"
    refresh: bool = False,
) -> pd.DataFrame:
    """Get sales data at the specified level, running stages only if needed.

    This function intelligently runs only the ETL stages that are needed:
    - If refresh=True: runs all stages (download, clean, aggregate)
    - If refresh=False: checks metadata and only runs missing/outdated stages
    - Returns a DataFrame with the aggregated sales data

    Args:
        start_date: Start date in YYYY-MM-DD format (inclusive).
        end_date: End date in YYYY-MM-DD format (inclusive).
        config: SalesETLConfig instance with paths and settings.
        branches: Optional list of branch names to process. If None, processes all branches.
        level: Aggregation level: "ticket", "group", or "day" (default: "ticket").
        refresh: If True, force re-run all stages (default: False).

    Returns:
        DataFrame containing aggregated sales data at the specified level.

    Examples:
        >>> from pos_core.etl.sales_config import SalesETLConfig
        >>> from pos_core.etl.queries import get_sales
        >>> config = SalesETLConfig.from_root("data", "utils/sucursales.json")
        >>> df = get_sales("2025-01-01", "2025-01-31", config, level="ticket")
        >>> len(df)
        100

    """
    # Normalize dates (basic validation)
    try:
        _ = pd.to_datetime(start_date)
        _ = pd.to_datetime(end_date)
    except ValueError as e:
        raise ValueError(f"Invalid date format: {e}") from e

    if refresh:
        # Force re-run all stages
        logger.info("Refresh=True: running all stages for %s to %s", start_date, end_date)
        download_sales(start_date, end_date, config, branches, force=True)
        clean_sales(start_date, end_date, config, branches, force=True)
        return aggregate_sales(
            start_date, end_date, config, level=level, branches=branches, force=True
        )

    # Check what stages need to run
    needs_download = True
    needs_clean = True
    needs_aggregate = True

    # Check raw stage metadata
    raw_meta = read_metadata(config.paths.raw_sales, start_date, end_date)
    if raw_meta and raw_meta.status == "ok":
        needs_download = False

    # Check clean stage metadata
    clean_meta = read_metadata(config.paths.clean_sales, start_date, end_date)
    if (
        clean_meta
        and clean_meta.status == "ok"
        and clean_meta.cleaner_version == "sales_cleaner_v1"
    ):
        needs_clean = False

    # Check aggregate stage metadata (level-specific)
    meta_key = f"aggregate_{level}_v1"
    agg_meta = read_metadata(config.paths.proc_sales, start_date, end_date)
    if agg_meta and agg_meta.status == "ok" and agg_meta.cleaner_version == meta_key:
        needs_aggregate = False

    # Run missing stages
    if needs_download:
        logger.info("Missing raw data: downloading for %s to %s", start_date, end_date)
        download_sales(start_date, end_date, config, branches, force=False)

    if needs_clean:
        logger.info("Missing clean data: cleaning for %s to %s", start_date, end_date)
        clean_sales(start_date, end_date, config, branches, force=False)

    if needs_aggregate:
        logger.info(
            "Missing aggregated data: aggregating for %s to %s at level %s",
            start_date,
            end_date,
            level,
        )
        return aggregate_sales(
            start_date, end_date, config, level=level, branches=branches, force=False
        )

    # All stages are up-to-date, load existing file
    logger.info("All stages up-to-date: loading existing data for %s to %s", start_date, end_date)
    if level == "ticket":
        ticket_csv = config.paths.proc_sales / f"sales_by_ticket_{start_date}_{end_date}.csv"
        if ticket_csv.exists():
            return pd.read_csv(ticket_csv)
    elif level == "group":
        group_csv = config.paths.proc_sales / f"sales_by_group_{start_date}_{end_date}.csv"
        if group_csv.exists():
            return pd.read_csv(group_csv)
    elif level == "day":
        raise NotImplementedError("Day-level aggregation not yet implemented")

    # File doesn't exist even though metadata says it should
    logger.warning("Metadata says data exists but file not found, re-aggregating")
    return aggregate_sales(start_date, end_date, config, level=level, branches=branches, force=True)


def get_payments(
    start_date: str,
    end_date: str,
    config: PaymentsETLConfig,
    branches: list[str] | None = None,
    refresh: bool = False,
) -> pd.DataFrame:
    """Get payments data, running stages only if needed.

    This function intelligently runs only the ETL stages that are needed:
    - If refresh=True: runs all stages (download, clean, aggregate)
    - If refresh=False: checks metadata and only runs missing/outdated stages
    - Returns a DataFrame with the aggregated payments data

    Args:
        start_date: Start date in YYYY-MM-DD format (inclusive).
        end_date: End date in YYYY-MM-DD format (inclusive).
        config: PaymentsETLConfig instance with paths and settings.
        branches: Optional list of branch names to process. If None, processes all branches.
        refresh: If True, force re-run all stages (default: False).

    Returns:
        DataFrame containing aggregated payments data (one row per sucursal + fecha).

    Examples:
        >>> from pos_core.etl.api import PaymentsETLConfig
        >>> from pos_core.etl.queries import get_payments
        >>> config = PaymentsETLConfig.from_root("data", "utils/sucursales.json")
        >>> df = get_payments("2025-01-01", "2025-01-31", config)
        >>> len(df)
        31

    """
    # Normalize dates (basic validation)
    try:
        _ = pd.to_datetime(start_date)
        _ = pd.to_datetime(end_date)
    except ValueError as e:
        raise ValueError(f"Invalid date format: {e}") from e

    if refresh:
        # Force re-run all stages
        logger.info("Refresh=True: running all stages for %s to %s", start_date, end_date)
        download_payments(start_date, end_date, config, branches, force=True)
        clean_payments(start_date, end_date, config, branches, force=True)
        return aggregate_payments(start_date, end_date, config, branches=branches, force=True)

    # Check what stages need to run
    needs_download = True
    needs_clean = True
    needs_aggregate = True

    # Check raw stage metadata
    raw_meta = read_metadata(config.paths.raw_payments, start_date, end_date)
    if raw_meta and raw_meta.status == "ok":
        needs_download = False

    # Check clean stage metadata
    clean_meta = read_metadata(config.paths.clean_payments, start_date, end_date)
    if (
        clean_meta
        and clean_meta.status == "ok"
        and clean_meta.cleaner_version == "payments_cleaner_v1"
    ):
        needs_clean = False

    # Check aggregate stage metadata
    agg_meta = read_metadata(config.paths.proc_payments, start_date, end_date)
    if agg_meta and agg_meta.status == "ok" and agg_meta.cleaner_version == "aggregate_v1":
        needs_aggregate = False

    # Run missing stages
    if needs_download:
        logger.info("Missing raw data: downloading for %s to %s", start_date, end_date)
        download_payments(start_date, end_date, config, branches, force=False)

    if needs_clean:
        logger.info("Missing clean data: cleaning for %s to %s", start_date, end_date)
        clean_payments(start_date, end_date, config, branches, force=False)

    if needs_aggregate:
        logger.info("Missing aggregated data: aggregating for %s to %s", start_date, end_date)
        return aggregate_payments(start_date, end_date, config, branches=branches, force=False)

    # All stages are up-to-date, load existing file
    logger.info("All stages up-to-date: loading existing data for %s to %s", start_date, end_date)
    aggregated_path = config.paths.proc_payments / "aggregated_payments_daily.csv"
    if aggregated_path.exists():
        return pd.read_csv(aggregated_path)

    # File doesn't exist even though metadata says it should
    logger.warning("Metadata says data exists but file not found, re-aggregating")
    return aggregate_payments(start_date, end_date, config, branches=branches, force=True)


def get_payments_forecast(
    as_of: str,  # Date string
    horizon_weeks: int,
    config: PaymentsETLConfig,
    refresh: bool = False,
) -> pd.DataFrame:
    """Get payments forecast for the specified horizon.

    This function:
    1. Gets historical payments data (using get_payments)
    2. Runs the forecasting pipeline
    3. Returns forecast DataFrame

    Args:
        as_of: Date string in YYYY-MM-DD format (forecast as of this date).
        horizon_weeks: Number of weeks to forecast ahead.
        config: PaymentsETLConfig instance with paths and settings.
        refresh: If True, force re-run ETL stages before forecasting (default: False).

    Returns:
        DataFrame containing forecast results.

    Examples:
        >>> from pos_core.etl.api import PaymentsETLConfig
        >>> from pos_core.etl.queries import get_payments_forecast
        >>> config = PaymentsETLConfig.from_root("data", "utils/sucursales.json")
        >>> forecast = get_payments_forecast("2025-11-24", 13, config)
        >>> len(forecast)
        91

    """
    from datetime import timedelta

    from pos_core.forecasting import ForecastConfig, run_payments_forecast

    # Calculate date range for historical data (use last 3 years or similar)
    as_of_date = pd.to_datetime(as_of)
    start_date = (as_of_date - timedelta(days=3 * 365)).strftime("%Y-%m-%d")
    end_date = as_of_date.strftime("%Y-%m-%d")

    # Get historical payments data
    logger.info("Getting historical payments data for forecast (as_of=%s)", as_of)
    payments_df = get_payments(start_date, end_date, config, refresh=refresh)

    # Ensure fecha column is datetime
    if "fecha" in payments_df.columns:
        payments_df["fecha"] = pd.to_datetime(payments_df["fecha"])

    # Configure forecast
    horizon_days = horizon_weeks * 7
    forecast_config = ForecastConfig(horizon_days=horizon_days)

    # Run forecast
    logger.info("Running payments forecast (horizon=%d days)", horizon_days)
    result = run_payments_forecast(payments_df, config=forecast_config)

    # Return forecast DataFrame
    return result.forecast
