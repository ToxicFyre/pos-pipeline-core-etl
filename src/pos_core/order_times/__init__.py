"""Order times domain module.

This module provides access to order times data across bronze/silver/gold layers:

- **Bronze (raw)**: `order_times.raw.fetch()` / `load()` - raw Wansoft exports

Example:
    >>> from pos_core import DataPaths
    >>> from pos_core.order_times import raw
    >>>
    >>> paths = DataPaths.from_root("data", "utils/sucursales.json")
    >>>
    >>> # Download raw order times data
    >>> raw.fetch(paths, "2025-01-01", "2025-01-31")

"""

from pos_core.order_times import raw

__all__ = ["raw"]
