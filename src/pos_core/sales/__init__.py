"""Sales domain module.

This module provides functions to load sales data at different grains:

- **fact_sales_item_line** (grain="item"): Core fact at item/modifier line grain.
  One row per item or modifier on a ticket. This is the atomic grain for sales.

- **mart_sales_by_ticket** (grain="ticket"): Aggregates item-lines to ticket level.
  One row per ticket with group subtotals/totals.

- **mart_sales_by_group** (grain="group"): Category pivot table.
  Rows are product groups, columns are sucursales, values are totals.

Example:
    >>> from pos_core import DataPaths
    >>> from pos_core.sales import get_sales
    >>>
    >>> paths = DataPaths.from_root("data", "utils/sucursales.json")
    >>>
    >>> # Get core fact (item-line grain, default)
    >>> fact_df = get_sales(paths, "2025-01-01", "2025-01-31")
    >>>
    >>> # Get ticket-level mart
    >>> ticket_df = get_sales(paths, "2025-01-01", "2025-01-31", grain="ticket")
    >>>
    >>> # Get group pivot mart
    >>> group_df = get_sales(paths, "2025-01-01", "2025-01-31", grain="group")
"""

from pos_core.sales.api import get_sales

__all__ = ["get_sales"]
