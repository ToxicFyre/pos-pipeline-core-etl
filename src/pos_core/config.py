"""Unified configuration for POS Core ETL.

This module provides backward compatibility by re-exporting DataPaths from paths.py.
New code should import from pos_core.paths or pos_core directly.
"""

from __future__ import annotations

# Re-export for backward compatibility
from pos_core.paths import DataPaths

__all__ = ["DataPaths"]
