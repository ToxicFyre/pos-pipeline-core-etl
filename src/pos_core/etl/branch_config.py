"""Branch configuration utilities for payments ETL.

This module provides utilities for loading and working with branch code windows
from sucursales.json configuration files. It is separated from api.py to avoid
circular import issues.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional

from pos_core.etl.utils import parse_date


EXCLUDED_BRANCHES = {"CEDIS"}


@dataclass
class CodeWindow:
    """Represents a time window when a branch code was valid.

    Attributes:
        code: The branch/sucursal code (e.g., "6161").
        valid_from: Start date when this code became active (inclusive).
        valid_to: End date when this code became inactive (inclusive).
            None indicates the code is still active (open-ended).
    """

    code: str
    valid_from: date
    valid_to: Optional[date]  # None = open-ended (inclusive)


def load_branch_segments_from_json(
    sucursales_path: Path,
) -> Dict[str, List[CodeWindow]]:
    """Load branch code windows from sucursales.json configuration file.

    Reads the JSON file containing branch/sucursal definitions and builds
    a mapping from logical branch names to their code windows. Branch names
    with suffixes (e.g., "Kavia" and "Kavia_OLD") are grouped by the part
    before the first underscore.

    Excluded branches (e.g., "CEDIS") are skipped.

    Args:
        sucursales_path: Path to the sucursales.json configuration file.

    Returns:
        Dictionary mapping logical branch names to sorted lists of
        CodeWindow objects (sorted by valid_from date).

    Examples:
        >>> from pathlib import Path
        >>> segments = load_branch_segments_from_json(Path("sucursales.json"))
        >>> segments["Kavia"]
        [CodeWindow(code='6161', valid_from=date(2022, 11, 1), valid_to=date(2023, 4, 29)), ...]
    """
    data = json.loads(sucursales_path.read_text(encoding="utf-8"))

    segments: Dict[str, List[CodeWindow]] = {}

    for key, rec in data.items():
        # logical branch name = part before first underscore
        logical_name = key.split("_", 1)[0]

        # Skip excluded branches (e.g. CEDIS)
        if logical_name in EXCLUDED_BRANCHES:
            continue

        code = str(rec["code"])
        vf = parse_date(rec["valid_from"])
        vt_raw = rec.get("valid_to")
        vt = parse_date(vt_raw) if vt_raw else None

        segments.setdefault(logical_name, []).append(
            CodeWindow(code=code, valid_from=vf, valid_to=vt)
        )

    # sort code windows per logical branch by valid_from, just for readability
    for _, windows in segments.items():
        windows.sort(key=lambda w: w.valid_from)

    return segments

