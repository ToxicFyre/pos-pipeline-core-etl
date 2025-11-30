"""Payments domain module.

This module provides functions to load payment data at different grains:

- **fact_payments_ticket** (grain="ticket"): Core fact at ticket × payment method grain.
  One row per payment line on a ticket. This is the atomic grain for payments.

- **mart_payments_daily** (grain="daily"): Gold-layer mart aggregated to sucursal × date.
  Contains daily totals by payment method, tips, ticket counts, etc.

Example:
    >>> from pos_core import DataPaths
    >>> from pos_core.payments import get_payments
    >>>
    >>> paths = DataPaths.from_root("data", "utils/sucursales.json")
    >>>
    >>> # Get daily mart (most common use case)
    >>> daily_df = get_payments(paths, "2025-01-01", "2025-01-31")
    >>>
    >>> # Get core fact (ticket grain)
    >>> fact_df = get_payments(paths, "2025-01-01", "2025-01-31", grain="ticket")
"""

from pos_core.payments.api import get_payments

__all__ = ["get_payments"]
