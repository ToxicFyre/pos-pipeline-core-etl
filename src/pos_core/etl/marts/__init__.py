"""Marts (Gold) layer - Aggregated / semantic tables.

This layer produces aggregated, analytics-ready tables built on top of the
core facts (Silver+ layer). All tables in this layer represent aggregations
beyond the atomic grain of the underlying data.

Grain and Layers
----------------
- **Core facts (Silver+)** are the atomic grains:
  - Payments: ticket × payment method (``fact_payments_ticket``)
  - Sales: item/modifier line (``fact_sales_item_line``)

- **Marts (Gold)** are aggregations beyond those atomic grains:
  - ``aggregate_by_ticket``: Aggregates sales item-lines to ticket level
  - ``build_category_pivot``: Aggregates ticket sales by category
  - ``aggregate_payments_daily``: Aggregates ticket payments to daily level
  - ``aggregate_transfers``: Aggregates transfer data by branch and category

Data directory mapping:
    data/c_processed/marts/ → Marts (Gold) layer - Aggregated tables for BI/forecasting.
    Currently: data/c_processed/ contains both core and marts outputs.

The marts layer is consumed by:
- pos_core.forecasting for time series predictions
- pos_core.qa for data quality checks
- External BI tools and dashboards
"""

from pos_core.etl.marts.payments_daily import aggregate_payments_daily
from pos_core.etl.marts.sales_by_group import build_category_pivot
from pos_core.etl.marts.sales_by_ticket import aggregate_by_ticket
from pos_core.etl.marts.transfers import aggregate_transfers

__all__ = [
    "aggregate_payments_daily",
    "aggregate_by_ticket",
    "build_category_pivot",
    "aggregate_transfers",
]
