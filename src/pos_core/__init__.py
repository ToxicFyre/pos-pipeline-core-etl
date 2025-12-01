"""POS Core ETL - Point of Sale data processing, forecasting, and QA.

This package provides a clean, domain-oriented API for working with
POS data across multiple layers:

- **Bronze (raw)**: Direct Wansoft exports
- **Silver (core facts)**: Cleaned, atomic-grain data
- **Gold (marts)**: Aggregated tables for analysis

Module Structure:
    pos_core.payments: Payment data ETL (payments.core, payments.marts, payments.raw)
    pos_core.sales: Sales data ETL (sales.core, sales.marts, sales.raw)
    pos_core.forecasting: Time series forecasting
    pos_core.qa: Data quality assurance
    pos_core.branches: Branch registry for code resolution
    pos_core.paths: DataPaths configuration

Quick Start:
    >>> from pos_core import DataPaths
    >>> from pos_core.payments import marts as payments_marts
    >>> from pos_core.sales import core as sales_core
    >>> from pos_core.forecasting import ForecastConfig, run_payments_forecast
    >>>
    >>> paths = DataPaths.from_root("data", "utils/sucursales.json")
    >>>
    >>> # Payments: daily mart
    >>> payments_daily = payments_marts.fetch_daily(paths, "2025-01-01", "2025-01-31")
    >>>
    >>> # Sales: core item-line fact
    >>> sales_items = sales_core.fetch(paths, "2025-01-01", "2025-01-31")
    >>>
    >>> # Forecasting on payments daily mart
    >>> config = ForecastConfig(horizon_days=91)
    >>> result = run_payments_forecast(payments_daily, config)
    >>> print(result.forecast.head())

Grain Reference:
    Payments:
        - core: fact_payments_ticket - ticket x payment method
        - marts.daily: mart_payments_daily - sucursal x date

    Sales:
        - core: fact_sales_item_line - item/modifier line
        - marts.ticket: mart_sales_by_ticket - one row per ticket
        - marts.group: mart_sales_by_group - category pivot
"""

__version__ = "0.2.0"

from pos_core.config import DataPaths
from pos_core.exceptions import ConfigError, ETLError, ExtractionError

__all__ = [
    "ConfigError",
    "DataPaths",
    "ETLError",
    "ExtractionError",
    "__version__",
]
