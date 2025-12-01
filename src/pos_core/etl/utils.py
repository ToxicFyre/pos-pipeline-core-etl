"""Shared utilities for POS ETL pipeline.

This module provides reusable functions for date interval manipulation and
data discovery across ETL modules. It includes:

- Generic interval utilities: merging, subtracting, and checking coverage
- Payments-specific discovery: scanning directories for existing date ranges
- Date parsing: standardized date string parsing

Examples:
    >>> from datetime import date
    >>> from pos_etl.utils import merge_intervals, parse_date
    >>> intervals = [(date(2023, 1, 1), date(2023, 1, 5)),
    ...              (date(2023, 1, 3), date(2023, 1, 10))]
    >>> merge_intervals(intervals)
    [(date(2023, 1, 1), date(2023, 1, 10))]

"""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Iterable
from datetime import date, datetime, timedelta
from pathlib import Path

# Regex patterns for parsing date ranges from file paths and names

# Chunk directory pattern: YYYY-MM-DD_YYYY-MM-DD
CHUNK_DIR_RE = re.compile(r"^(?P<start>\d{4}-\d{2}-\d{2})_(?P<end>\d{4}-\d{2}-\d{2})$")

# Raw payment file pattern: Payments_<label>_YYYY-MM-DD_YYYY-MM-DD.xlsx
FILE_DATE_RE = re.compile(
    r"^Payments_(?P<label>.+?)_(?P<start>\d{4}-\d{2}-\d{2})_(?P<end>\d{4}-\d{2}-\d{2})\.xlsx$",
    re.IGNORECASE,
)

# Clean CSV filename pattern: forma_pago_<sucursal_slug>_<start>_<end>.csv
CLEAN_CSV_RE = re.compile(
    r"^forma_pago_.+?_(?P<start>\d{4}-\d{2}-\d{2})_(?P<end>\d{4}-\d{2}-\d{2})\.csv$",
    re.IGNORECASE,
)


# ============================================================================
# Generic Interval Utilities
# ============================================================================


def parse_date(s: str) -> date:
    """Parse a date string in YYYY-MM-DD format.

    Args:
        s: Date string in ISO format (YYYY-MM-DD).

    Returns:
        Parsed date object.

    Raises:
        ValueError: If the date string is not in YYYY-MM-DD format.

    Examples:
        >>> parse_date("2023-01-15")
        datetime.date(2023, 1, 15)

    """
    return datetime.strptime(s, "%Y-%m-%d").date()


def format_duration(seconds: float) -> str:
    """Format a duration in seconds as a human-readable string.

    Args:
        seconds: Duration in seconds (can be fractional).

    Returns:
        Formatted string like "5m 30.5s" or "45.2s".

    Examples:
        >>> format_duration(90.5)
        '1m 30.5s'
        >>> format_duration(45.2)
        '45.2s'

    """
    mins, secs = divmod(seconds, 60.0)
    if mins >= 1:
        return f"{int(mins)}m {secs:04.1f}s"
    else:
        return f"{secs:.1f}s"


def iter_chunks(start: date, end: date, max_days: int = 180) -> Iterable[tuple[date, date]]:
    """Yield date chunks covering a range in windows of at most max_days.

    Splits a date range into non-overlapping chunks, each containing at most
    max_days (inclusive). The last chunk may be smaller if needed.

    Args:
        start: Start date of the range (inclusive).
        end: End date of the range (inclusive).
        max_days: Maximum number of days per chunk (default: 180).

    Yields:
        Tuples of (chunk_start, chunk_end) covering the range.

    Examples:
        >>> from datetime import date
        >>> list(iter_chunks(date(2023, 1, 1), date(2023, 1, 5), max_days=2))
        [(date(2023, 1, 1), date(2023, 1, 2)), (date(2023, 1, 3), date(2023, 1, 4)),
         (date(2023, 1, 5), date(2023, 1, 5))]

    """
    cur = start
    step = timedelta(days=max_days)
    while cur <= end:
        chunk_end = cur + step - timedelta(days=1)
        if chunk_end > end:
            chunk_end = end
        yield cur, chunk_end
        cur = chunk_end + timedelta(days=1)


def merge_intervals(intervals: list[tuple[date, date]]) -> list[tuple[date, date]]:
    """Merge overlapping or contiguous date intervals.

    Takes a list of date intervals and merges any that overlap or are
    contiguous (touching). Returns a sorted list of non-overlapping intervals.

    Args:
        intervals: List of (start, end) date tuples (both inclusive).

    Returns:
        Sorted list of merged (start, end) intervals.

    Examples:
        >>> from datetime import date
        >>> intervals = [(date(2023, 1, 1), date(2023, 1, 5)),
        ...              (date(2023, 1, 3), date(2023, 1, 10)),
        ...              (date(2023, 1, 15), date(2023, 1, 20))]
        >>> merge_intervals(intervals)
        [(date(2023, 1, 1), date(2023, 1, 10)), (date(2023, 1, 15), date(2023, 1, 20))]

    """
    if not intervals:
        return []
    intervals = sorted(intervals, key=lambda x: x[0])
    merged: list[tuple[date, date]] = []
    cur_start, cur_end = intervals[0]
    for s, e in intervals[1:]:
        if s <= cur_end + timedelta(days=1):  # overlap or touch
            if e > cur_end:
                cur_end = e
        else:
            merged.append((cur_start, cur_end))
            cur_start, cur_end = s, e
    merged.append((cur_start, cur_end))
    return merged


def subtract_intervals(
    target: tuple[date, date],
    covered: list[tuple[date, date]],
) -> list[tuple[date, date]]:
    """Find gaps in a target interval that are not covered.

    Given a target date range and a list of already-covered intervals,
    returns the list of gaps (intervals within the target that are not
    covered by any of the covered intervals).

    Args:
        target: Target interval as (start, end) tuple (both inclusive).
        covered: List of covered intervals as (start, end) tuples.

    Returns:
        List of gap intervals (start, end) that are not covered.

    Examples:
        >>> from datetime import date, timedelta
        >>> target = (date(2023, 1, 1), date(2023, 1, 31))
        >>> covered = [(date(2023, 1, 5), date(2023, 1, 10)),
        ...            (date(2023, 1, 20), date(2023, 1, 25))]
        >>> subtract_intervals(target, covered)
        [(date(2023, 1, 1), date(2023, 1, 4)), (date(2023, 1, 11), date(2023, 1, 19)),
         (date(2023, 1, 26), date(2023, 1, 31))]

    """
    ts, te = target
    if not covered:
        return [(ts, te)]

    covered = merge_intervals(covered)
    gaps: list[tuple[date, date]] = []

    cur = ts
    for cs, ce in covered:
        if ce < cur:
            continue
        if cs > te:
            break
        if cs > cur:
            gaps.append((cur, min(cs - timedelta(days=1), te)))
        cur = max(cur, ce + timedelta(days=1))
        if cur > te:
            break

    if cur <= te:
        gaps.append((cur, te))

    return gaps


def is_interval_covered(
    target: tuple[date, date],
    covered: list[tuple[date, date]],
) -> bool:
    """Check if a target date interval is fully covered by existing intervals.

    Args:
        target: Target interval as (start, end) tuple (both inclusive).
        covered: List of covered intervals as (start, end) tuples.

    Returns:
        True if the target interval is completely covered, False otherwise.

    Examples:
        >>> from datetime import date
        >>> target = (date(2023, 1, 5), date(2023, 1, 10))
        >>> covered = [(date(2023, 1, 1), date(2023, 1, 15))]
        >>> is_interval_covered(target, covered)
        True
        >>> target2 = (date(2023, 1, 1), date(2023, 1, 20))
        >>> is_interval_covered(target2, covered)
        False

    """
    if not covered:
        return False

    ts, te = target
    return any(cs <= ts and ce >= te for cs, ce in covered)


# ============================================================================
# Payments-Specific Discovery Functions
# ============================================================================


def discover_existing_intervals(
    raw_payments_root: Path,
) -> dict[str, list[tuple[date, date]]]:
    """Scan raw payments directory tree for existing date intervals.

    Recursively searches for Payments_*.xlsx files and extracts the date
    ranges they cover, grouped by branch code. Merges overlapping intervals
    per code.

    Expected directory structure:
        data/a_raw/payments/<branch>/<code>/<start>_<end>/Payments_*.xlsx

    The function:
    - Extracts branch code from directory name (e.g., "6161")
    - Extracts dates primarily from chunk directory name "<start>_<end>"
    - Falls back to parsing dates from filename if chunk dir doesn't match

    Args:
        raw_payments_root: Root directory to scan for payment files.

    Returns:
        Dictionary mapping branch codes to lists of merged (start, end)
        date intervals. Returns empty dict if directory doesn't exist.

    Examples:
        >>> from pathlib import Path
        >>> intervals = discover_existing_intervals(Path("data/a_raw/payments"))
        >>> intervals.get("6161", [])
        [(date(2023, 1, 1), date(2023, 1, 31))]

    """
    found: dict[str, list[tuple[date, date]]] = {}
    if not raw_payments_root.exists():
        return found

    for path in raw_payments_root.rglob("Payments_*.xlsx"):
        # Example path parts:
        #   payments / Kavia / 6161 / 2022-11-01_2023-04-29 /
        #   Payments_kavia_2022-11-01_2023-04-29.xlsx
        try:
            chunk_dir = path.parent  # 2022-11-01_2023-04-29
            code_dir = chunk_dir.parent  # 6161
            code = code_dir.name  # "6161"
        except (AttributeError, IndexError):
            # Unexpected layout, skip
            continue

        # 1) try dates from chunk folder name
        m = CHUNK_DIR_RE.match(chunk_dir.name)
        if m:
            start = parse_date(m.group("start"))
            end = parse_date(m.group("end"))
        else:
            # 2) fall back to filename: Payments_<label>_YYYY-MM-DD_YYYY-MM-DD.xlsx
            m2 = FILE_DATE_RE.match(path.name)
            if not m2:
                continue
            start = parse_date(m2.group("start"))
            end = parse_date(m2.group("end"))

        found.setdefault(code, []).append((start, end))

    # merge intervals per code
    for code, intervals in list(found.items()):
        found[code] = merge_intervals(intervals)

    return found


def discover_existing_clean_intervals(
    clean_payments_root: Path,
) -> list[tuple[date, date]]:
    """Scan clean payments directory for existing date intervals.

    Recursively searches for forma_pago_*.csv files and extracts the date
    ranges they cover. Returns a merged list of all intervals (not grouped
    by branch/code since clean files may not preserve that structure).

    Expected filename pattern:
        forma_pago_<sucursal_slug>_<start>_<end>.csv

    Args:
        clean_payments_root: Root directory to scan for clean CSV files.

    Returns:
        List of merged (start, end) date intervals covering all clean files.
        Returns empty list if directory doesn't exist.

    Examples:
        >>> from pathlib import Path
        >>> intervals = discover_existing_clean_intervals(Path("data/b_clean/payments/batch"))
        >>> intervals
        [(date(2023, 1, 1), date(2023, 6, 30))]

    """
    found: list[tuple[date, date]] = []
    if not clean_payments_root.exists():
        return found

    for path in clean_payments_root.rglob("*.csv"):
        m = CLEAN_CSV_RE.match(path.name)
        if m:
            try:
                start = parse_date(m.group("start"))
                end = parse_date(m.group("end"))
                found.append((start, end))
            except (ValueError, KeyError):
                # Invalid date format, skip
                continue

    return merge_intervals(found)


def get_raw_file_date_range(raw_file: Path) -> tuple[date, date] | None:
    """Extract date range from a raw payment Excel file path.

    Tries to extract dates from:
    1. Chunk directory name (format: YYYY-MM-DD_YYYY-MM-DD)
    2. Filename (format: Payments_<label>_YYYY-MM-DD_YYYY-MM-DD.xlsx)

    Args:
        raw_file: Path to a raw Payments_*.xlsx file.

    Returns:
        Tuple of (start, end) dates if found, None otherwise.

    Examples:
        >>> from pathlib import Path
        >>> get_raw_file_date_range(
        ...     Path("data/a_raw/payments/Kavia/6161/2023-01-01_2023-01-31/"
        ...          "Payments_kavia_2023-01-01_2023-01-31.xlsx"))
        (date(2023, 1, 1), date(2023, 1, 31))

    """
    # Try chunk directory first
    chunk_dir = raw_file.parent
    m = CHUNK_DIR_RE.match(chunk_dir.name)
    if m:
        try:
            start = parse_date(m.group("start"))
            end = parse_date(m.group("end"))
            return (start, end)
        except (ValueError, KeyError):
            pass

    # Fall back to filename
    m2 = FILE_DATE_RE.match(raw_file.name)
    if m2:
        try:
            start = parse_date(m2.group("start"))
            end = parse_date(m2.group("end"))
            return (start, end)
        except (ValueError, KeyError):
            pass

    return None


def slugify(value: str) -> str:
    """Convert a string to a URL-friendly slug.

    Normalizes unicode characters, removes special characters, and converts
    spaces/hyphens to single hyphens. Handles unicode combining characters
    properly using NFKD normalization.

    Args:
        value: String to slugify.

    Returns:
        Slugified string (e.g., "My Branch Name" -> "my-branch-name").
        Returns "unknown" if the result would be empty.

    Examples:
        >>> slugify("Punto Valle")
        'punto-valle'
        >>> slugify("Kavia_OLD")
        'kavia_old'
        >>> slugify("Café")
        'cafe'

    """
    value = unicodedata.normalize("NFKD", value)
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = re.sub(r"[^\w\s-]", "", value, flags=re.U)
    value = re.sub(r"[-\s]+", "-", value, flags=re.U).strip("-_")
    return value or "unknown"
