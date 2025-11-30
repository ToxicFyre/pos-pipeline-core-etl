"""POS Core ETL - Point of Sale data processing, forecasting, and QA.

This package provides a clean, domain-oriented API for working with
POS data across multiple layers:

- **Bronze (raw)**: Direct Wansoft exports
- **Silver (core facts)**: Cleaned, atomic-grain data
- **Gold (marts)**: Aggregated tables for analysis

Main Modules:
    pos_core.payments: Payment data ETL and queries
    pos_core.sales: Sales data ETL and queries
    pos_core.forecasting: Time series forecasting
    pos_core.qa: Data quality assurance

Quick Start:
    >>> from pos_core import DataPaths
    >>> from pos_core.payments import get_payments
    >>> from pos_core.sales import get_sales
    >>>
    >>> paths = DataPaths.from_root("data", "utils/sucursales.json")
    >>>
    >>> # Get payment data (daily mart by default)
    >>> payments = get_payments(paths, "2025-01-01", "2025-01-31")
    >>>
    >>> # Get sales data (item-line core fact by default)
    >>> sales = get_sales(paths, "2025-01-01", "2025-01-31")

Grain Reference:
    Payments:
        - grain="ticket": Core fact (fact_payments_ticket) - ticket × payment method
        - grain="daily": Daily mart (mart_payments_daily) - sucursal × date

    Sales:
        - grain="item": Core fact (fact_sales_item_line) - item/modifier line
        - grain="ticket": Ticket mart (mart_sales_by_ticket) - one row per ticket
        - grain="group": Group mart (mart_sales_by_group) - category pivot
"""

__version__ = "0.2.0"

from pos_core.config import DataPaths
from pos_core.exceptions import ConfigError, ETLError, ExtractionError

__all__ = [
    "__version__",
    "DataPaths",
    "ConfigError",
    "ETLError",
    "ExtractionError",
]
