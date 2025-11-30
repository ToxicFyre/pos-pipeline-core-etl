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
    Cleaned and standardized tables. This is where the **core facts** live:
    - ``fact_payments_ticket``: One row per ticket × payment method
    - ``fact_sales_item_line``: One row per item/modifier line on a ticket
    Data directory: ``data/b_clean/``

**Core (Silver+)** - ``pos_core.etl.core/``
    The staging layer output IS the core fact. This subpackage documents the
    grain definitions but does not add further transformations.
    Data directory: ``data/b_clean/`` (same as staging - core facts live there)

**Marts (Gold)** - ``pos_core.etl.marts/``
    Aggregated tables built on top of the core facts:
    - ``mart_sales_by_ticket``: Aggregates item-lines to ticket level
    - ``mart_sales_by_group``: Category pivot tables
    - ``mart_payments_daily``: Daily branch-level payment aggregates
    Data directory: ``data/c_processed/``

Grain Definitions (Ground Truth)
--------------------------------
1. **Payments**: Most granular = ticket × payment method (``fact_payments_ticket``)
   - The POS payments export does not expose item-level payment data
   - Ticket-level is the atomic fact for payments

2. **Sales**: Most granular = item/modifier line (``fact_sales_item_line``)
   - Each row represents an item or modifier on a ticket
   - Multiple rows can share the same ``ticket_id``
   - Ticket-level aggregation is a **mart**, not core

Key Rule
--------
- For **sales**: anything aggregated beyond item/modifier line is **gold/mart**
- For **payments**: ticket × payment method is already atomic (silver/core)

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
