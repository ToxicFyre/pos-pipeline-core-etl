"""ETL pipeline module for POS data processing.

This module provides a clean public API for running ETL operations on POS data,
including payments extraction, cleaning, and aggregation.
"""

from pos_core.etl.api import (
    PaymentsETLConfig,
    PaymentsPaths,
    build_payments_dataset,
)
from pos_core.etl.payments import (
    aggregate_payments,
    clean_payments,
    download_payments,
)
from pos_core.etl.queries import (
    get_payments,
    get_payments_forecast,
    get_sales,
)
from pos_core.etl.sales import (
    aggregate_sales,
    clean_sales,
    download_sales,
)
from pos_core.etl.sales_config import (
    SalesETLConfig,
    SalesPaths,
)

__all__ = [
    # Configs
    "PaymentsPaths",
    "PaymentsETLConfig",
    "SalesPaths",
    "SalesETLConfig",
    # High-level orchestration
    "build_payments_dataset",
    # Stage functions - Payments
    "download_payments",
    "clean_payments",
    "aggregate_payments",
    # Stage functions - Sales
    "download_sales",
    "clean_sales",
    "aggregate_sales",
    # Query functions
    "get_payments",
    "get_sales",
    "get_payments_forecast",
]
