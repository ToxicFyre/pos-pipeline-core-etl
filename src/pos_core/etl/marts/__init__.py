"""Marts (Gold) layer - Aggregated / semantic tables.

This layer produces aggregated, analytics-ready tables:
- Daily/weekly branch-level summaries
- Group-level pivot tables
- Forecast-ready datasets

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
from pos_core.etl.marts.transfers import aggregate_transfers

__all__ = [
    "aggregate_payments_daily",
    "build_category_pivot",
    "aggregate_transfers",
]
