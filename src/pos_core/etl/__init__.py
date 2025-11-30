"""ETL pipeline module for POS data processing.

This module provides a clean public API for running ETL operations on POS data,
including payments extraction, cleaning, and aggregation.

Data Layers (Industry Standard Bronze/Silver/Gold)
==================================================

The ETL pipeline follows industry-standard data engineering conventions with
explicit data layers:

**Raw (Bronze)** - ``pos_core.etl.raw/``
    Direct Wansoft HTTP exports, unchanged.
    Data directory: ``data/a_raw/``

**Staging (Silver)** - ``pos_core.etl.staging/``
    Cleaned and standardized tables.
    Data directory: ``data/b_clean/``

**Core (Silver+)** - ``pos_core.etl.core/``
    Granular POS models (one row per ticket line/branch-day).
    Data directory: ``data/c_processed/`` (core models)

**Marts (Gold)** - ``pos_core.etl.marts/``
    Aggregated tables used by forecasting and BI.
    Data directory: ``data/c_processed/`` (aggregated tables)

Internal Layer Subpackages
--------------------------
- ``raw/``: HTTP extraction from Wansoft API
- ``staging/``: Excel cleaning and normalization
- ``core/``: Per-ticket granular aggregation
- ``marts/``: Daily/category-level aggregated tables

High-Level API
--------------
Use the public API (``get_sales()``, ``get_payments()``, etc.) which orchestrates
the appropriate layers automatically. These functions run only the ETL stages
that are needed based on metadata tracking.
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
