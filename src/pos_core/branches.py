"""Branch registry for resolving branch codes by date.

This module provides utilities for working with branch/sucursal configurations,
including resolving the correct POS code for a branch at a given date based on
validity windows from sucursales.json.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pos_core.etl.branch_config import load_branch_segments_from_json
from pos_core.etl.utils import parse_date

if TYPE_CHECKING:
    from pos_core.paths import DataPaths

EXCLUDED_BRANCHES = {"CEDIS"}


class BranchRegistry:
    """Registry for branch codes with date-based validity windows.

    Loads branch configuration from sucursales.json and provides methods
    to resolve branch codes for specific dates, respecting valid_from/valid_to
    windows.

    Example:
        >>> from pos_core import DataPaths
        >>> from pos_core.branches import BranchRegistry
        >>>
        >>> paths = DataPaths.from_root("data", "utils/sucursales.json")
        >>> registry = BranchRegistry(paths)
        >>> registry.get_code_for_date("Kavia", "2023-01-15")
        '6161'
        >>> registry.list_branches()
        ['Kavia', 'Queen', ...]

    """

    def __init__(self, paths: DataPaths) -> None:
        """Initialize branch registry from DataPaths configuration.

        Args:
            paths: DataPaths instance containing sucursales_json path.

        """
        self.paths = paths
        self._segments = load_branch_segments_from_json(paths.sucursales_json)

    def list_branches(self) -> list[str]:
        """List all available branch names (excluding excluded branches).

        Returns:
            List of branch names (logical names, e.g., "Kavia", "Queen").

        """
        return sorted(self._segments.keys())

    def get_code_for_date(self, branch: str, date_str: str) -> str:
        """Return the correct POS code for a branch at a given date.

        Respects valid_from/valid_to windows from sucursales.json. If multiple
        code windows exist for a branch, returns the code that is valid for
        the specified date.

        Args:
            branch: Branch name (logical name, e.g., "Kavia").
            date_str: Date string in YYYY-MM-DD format.

        Returns:
            Branch code (e.g., "6161", "8777").

        Raises:
            ValueError: If branch is not found or no valid code exists for the date.
            ValueError: If date_str is not in YYYY-MM-DD format.

        Example:
            >>> registry.get_code_for_date("Kavia", "2023-01-15")
            '6161'
            >>> registry.get_code_for_date("Kavia", "2024-03-01")
            '8777'

        """
        if branch not in self._segments:
            raise ValueError(f"Branch '{branch}' not found in registry")

        target_date = parse_date(date_str) if isinstance(date_str, str) else date_str

        windows = self._segments[branch]
        for window in windows:
            if window.valid_from <= target_date and (window.valid_to is None or window.valid_to >= target_date):
                return window.code

        raise ValueError(
            f"No valid code found for branch '{branch}' on date {date_str}. "
            f"Available windows: {windows}"
        )

    def get_all_codes_for_date(self, date_str: str) -> dict[str, str]:
        """Get all branch codes valid for a given date.

        Args:
            date_str: Date string in YYYY-MM-DD format.

        Returns:
            Dictionary mapping branch names to their codes for the given date.

        Example:
            >>> registry.get_all_codes_for_date("2023-01-15")
            {'Kavia': '6161', 'Queen': '6362', ...}

        """
        target_date = parse_date(date_str) if isinstance(date_str, str) else date_str
        result: dict[str, str] = {}

        for branch, windows in self._segments.items():
            for window in windows:
                if window.valid_from <= target_date and (window.valid_to is None or window.valid_to >= target_date):
                    result[branch] = window.code
                    break

        return result
