"""Sales domain module.

This module provides access to sales data across bronze/silver/gold layers:

- **Bronze (raw)**: `sales.raw.fetch()` / `load()` - raw Wansoft exports
- **Silver (core)**: `sales.core.fetch()` / `load()` - fact_sales_item_line
  (item/modifier line grain)
- **Gold (marts)**: `sales.marts.fetch_ticket()` / `load_ticket()` - ticket-level aggregations
- **Gold (marts)**: `sales.marts.fetch_group()` / `load_group()` - group-level pivot table

Example:
    >>> from pos_core import DataPaths
    >>> from pos_core.sales import core, marts
    >>>
    >>> paths = DataPaths.from_root("data", "utils/sucursales.json")
    >>>
    >>> # Get core fact (item-line grain, default)
    >>> fact_df = core.fetch(paths, "2025-01-01", "2025-01-31")
    >>>
    >>> # Get ticket-level mart
    >>> ticket_df = marts.fetch_ticket(paths, "2025-01-01", "2025-01-31")
    >>>
    >>> # Get group pivot mart
    >>> group_df = marts.fetch_group(paths, "2025-01-01", "2025-01-31")
"""

from pos_core.sales import core, marts, raw

__all__ = ["core", "marts", "raw"]
