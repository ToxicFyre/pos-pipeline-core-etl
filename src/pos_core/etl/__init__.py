"""ETL pipeline module - INTERNAL/DEPRECATED.

.. warning::
    This module is deprecated. Use the new domain-oriented API instead:

    - ``from pos_core.payments import get_payments``
    - ``from pos_core.sales import get_sales``
    - ``from pos_core import DataPaths``

This module contains the internal ETL implementation that the new public API
calls into. It is not part of the public API and may change without notice.

Data Layers (Bronze/Silver/Gold)
================================

**Raw (Bronze)** - ``pos_core.etl.raw/``
    Direct Wansoft HTTP exports, unchanged.
    Data directory: ``data/a_raw/``

**Staging (Silver)** - ``pos_core.etl.staging/``
    Core facts at atomic grain:
    - ``fact_payments_ticket``: One row per ticket × payment method
    - ``fact_sales_item_line``: One row per item/modifier line
    Data directory: ``data/b_clean/``

**Marts (Gold)** - ``pos_core.etl.marts/``
    Aggregated tables:
    - ``mart_payments_daily``: Daily branch-level aggregates
    - ``mart_sales_by_ticket``: Ticket-level aggregates
    - ``mart_sales_by_group``: Category pivot tables
    Data directory: ``data/c_processed/``
"""

# Internal imports - keep for backwards compatibility during transition
# but these are not part of the public API
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
    # Deprecated - use pos_core.DataPaths instead
    "PaymentsPaths",
    "PaymentsETLConfig",
    "SalesPaths",
    "SalesETLConfig",
    # Deprecated - use pos_core.payments.get_payments instead
    "build_payments_dataset",
    "download_payments",
    "clean_payments",
    "aggregate_payments",
    # Deprecated - use pos_core.sales.get_sales instead
    "download_sales",
    "clean_sales",
    "aggregate_sales",
    # Deprecated - use pos_core.payments.get_payments + pos_core.forecasting instead
    "get_payments",
    "get_sales",
    "get_payments_forecast",
]
