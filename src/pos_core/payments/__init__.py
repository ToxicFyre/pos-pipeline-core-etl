"""Payments domain module.

This module provides access to payment data across bronze/silver/gold layers:

- **Bronze (raw)**: `payments.raw.fetch()` / `load()` - raw Wansoft exports
- **Silver (core)**: `payments.core.fetch()` / `load()` - fact_payments_ticket
  (ticket × payment method grain)
- **Gold (marts)**: `payments.marts.fetch_daily()` / `load_daily()` -
  mart_payments_daily (daily aggregations)

Example:
    >>> from pos_core import DataPaths
    >>> from pos_core.payments import core, marts
    >>>
    >>> paths = DataPaths.from_root("data", "utils/sucursales.json")
    >>>
    >>> # Get daily mart (most common use case)
    >>> daily_df = marts.fetch_daily(paths, "2025-01-01", "2025-01-31")
    >>>
    >>> # Get core fact (ticket grain)
    >>> fact_df = core.fetch(paths, "2025-01-01", "2025-01-31")
"""

from pos_core.payments import core, marts, raw

__all__ = ["core", "marts", "raw"]
