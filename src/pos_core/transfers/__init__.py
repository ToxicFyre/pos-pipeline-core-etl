"""Transfers domain module.

This module provides access to transfer data across bronze/silver/gold layers:

- **Bronze (raw)**: `transfers.raw.fetch()` / `load()` - raw Wansoft exports
- **Silver (core)**: `transfers.core.fetch()` / `load()` - fact_transfers_line
  (transfer line grain)
- **Gold (marts)**: `transfers.marts.fetch_pivot()` / `load_pivot()` - pivot table
  (branch x category aggregation)

Example:
    >>> from pos_core import DataPaths
    >>> from pos_core.transfers import core, marts
    >>>
    >>> paths = DataPaths.from_root("data", "utils/sucursales.json")
    >>>
    >>> # Get core fact (transfer line grain)
    >>> fact_df = core.fetch(paths, "2025-01-01", "2025-01-31")
    >>>
    >>> # Get pivot mart (branch x category)
    >>> pivot_df = marts.fetch_pivot(paths, "2025-01-01", "2025-01-31")

"""

from pos_core.transfers import core, marts, raw

__all__ = ["core", "marts", "raw"]
