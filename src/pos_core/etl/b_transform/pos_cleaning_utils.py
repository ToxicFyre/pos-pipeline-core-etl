"""Shared utilities for cleaning POS Excel data.

This module provides common functions used across all Excel cleaning modules
for normalizing text, parsing numbers, handling dates, and preventing formula
injection attacks.

Key utilities:
- Text normalization: strip invisible characters, remove accents
- Number parsing: robust handling of various currency and number formats
- Date parsing: multiple date format support
- Security: neutralize formula injection attempts
- Column naming: convert to snake_case, handle duplicates

Examples:
    >>> from pos_etl.b_transform.pos_cleaning_utils import to_float, to_date, to_snake
    >>> to_float("1,234.56")
    1234.56
    >>> to_date("2023-01-15")
    Timestamp('2023-01-15 00:00:00')
    >>> to_snake("Fecha de Operación")
    'fecha_de_operacion'
"""

from __future__ import annotations

import math
import re
import unicodedata
from typing import Any, Iterable, List, Optional, Union

import numpy as np
import pandas as pd

# Unicode characters that should be stripped from text
NBSP = "\u00a0"  # Non-breaking space
NNBSP = "\u202f"  # Narrow non-breaking space
ZW = "".join(chr(c) for c in (0x200B, 0x200C, 0x200D, 0xFEFF))  # Zero-width characters

# Prefixes that could trigger formula injection in spreadsheets
DANGEROUS_PREFIXES = ("=", "+", "@", "-")

# Regex to strip currency symbols while preserving number separators
_CURRENCY_RE = re.compile(r"[^\d,.\-\(\)\s]")


def strip_invisibles(x: Any) -> Optional[str]:
    """Remove invisible and problematic whitespace characters from text.

    Strips:
    - Carriage returns (\r)
    - Tabs (converted to spaces)
    - Non-breaking spaces (NBSP, NNBSP)
    - Zero-width characters (ZWSP, ZWNJ, ZWJ, BOM)
    - Collapses multiple spaces to single space

    Args:
        x: Value to clean (string, number, or None).

    Returns:
        Cleaned string or None if input is None/NaN.

    Examples:
        >>> strip_invisibles("  Hello\u00a0World  ")
        'Hello World'
        >>> strip_invisibles(None)
        None
    """
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    s = str(x)
    s = s.replace("\r", "").replace("\t", " ").replace(NBSP, " ").replace(NNBSP, " ")
    s = re.sub(r"[%s]" % re.escape(ZW), "", s)  # zero-width
    s = re.sub(r"\s+", " ", s).strip()
    return s


def neutralize(text: Any) -> Any:
    """Prevent formula injection by prefixing dangerous characters.

    Spreadsheet applications interpret text starting with =, +, @, or -
    as formulas. This function adds a leading apostrophe to neutralize
    such values, which forces them to be treated as text.

    Args:
        text: Text value to neutralize.

    Returns:
        Text with leading apostrophe if it starts with dangerous prefix,
        otherwise unchanged. Returns None/NaN if input is None/NaN.

    Examples:
        >>> neutralize("=SUM(A1:A10)")
        "'=SUM(A1:A10)"
        >>> neutralize("Hello")
        'Hello'
    """
    if text is None or (isinstance(text, float) and pd.isna(text)):
        return text
    s = str(text)
    return "'" + s if s.startswith(DANGEROUS_PREFIXES) else s


def to_float(x: Any) -> Optional[float]:
    """Robustly parse numbers in various formats.

    Handles multiple number formats commonly found in Excel exports:
    - US format: '1,234.56' (comma thousands, dot decimal)
    - EU format: '1.234,56' (dot thousands, comma decimal)
    - Negative in parentheses: '(1,234.56)'
    - Currency symbols: '$ 1 234,56'
    - Simple numbers: '1.234', '1,234'

    Args:
        x: Value to parse (string, number, or None).

    Returns:
        Parsed float value or None if parsing fails.

    Examples:
        >>> to_float("1,234.56")
        1234.56
        >>> to_float("1.234,56")
        1234.56
        >>> to_float("(1,234.56)")
        -1234.56
        >>> to_float("$ 1 234,56")
        1234.56
    """
    if x is None or (isinstance(x, float) and (math.isnan(x) or math.isinf(x))):
        return None
    s = str(x).strip()
    if not s:
        return None

    neg = False
    if s.startswith("(") and s.endswith(")"):
        neg, s = True, s[1:-1].strip()

    # strip currency and weird symbols but KEEP '.' and ','
    s = _CURRENCY_RE.sub("", s)
    # collapse inner spaces
    s = re.sub(r"\s+", "", s)
    if not s:
        return None

    # now disambiguate separators
    has_dot = "." in s
    has_com = "," in s

    def _finalize(num_str: str, negative: bool) -> Optional[float]:
        try:
            v = float(num_str)
            return -v if negative else v
        except Exception:
            return None

    # Pattern: 1.234,56 (EU)
    if re.fullmatch(r"\d{1,3}(?:\.\d{3})+,\d{1,2}", s):
        return _finalize(s.replace(".", "").replace(",", "."), neg)

    # Pattern: 1,234.56 (US)
    if re.fullmatch(r"\d{1,3}(?:,\d{3})+\.\d{1,2}", s):
        return _finalize(s.replace(",", ""), neg)

    # Only one of them appears -> treat as decimal unless it matches clear thousand-grouping
    if has_com and not has_dot:
        # 1,234,567 (no decimals) -> thousands
        if re.fullmatch(r"\d{1,3}(?:,\d{3})+", s):
            return _finalize(s.replace(",", ""), neg)
        # else comma is decimal
        return _finalize(s.replace(",", "."), neg)

    if has_dot and not has_com:
        # Assume NON-European by default: a single '.' is a decimal separator.
        if s.count(".") == 1:
            return _finalize(s, neg)
        # Multiple dots -> treat as thousand separators only when pattern is clear.
        if re.fullmatch(r"\d{1,3}(?:\.\d{3})+", s):
            return _finalize(s.replace(".", ""), neg)
        # Fallback: keep the dot (prefer preserving decimals over inflating by 10^3).
        return _finalize(s, neg)

    # No separators left, just digits and sign
    if re.fullmatch(r"-?\d+", s):
        return _finalize(s, neg)

    # Fallbacks: try replacing comma with dot
    return _finalize(s.replace(",", "."), neg)


def to_int(val: Any) -> Union[int, float]:
    """Convert value to integer via float parsing and rounding.

    Args:
        val: Value to convert (string, number, or None).

    Returns:
        Integer value or np.nan if conversion fails.

    Examples:
        >>> to_int("123.7")
        124
        >>> to_int("1,234.56")
        1235
    """
    f = to_float(val)
    if f is None or pd.isna(f):
        return np.nan
    try:
        return int(round(f))
    except Exception:
        return np.nan


def to_date(val: Any) -> pd.Timestamp:
    """Parse date from various formats.

    Attempts multiple date formats in order:
    1. ISO format: YYYY-MM-DD
    2. European: DD/MM/YYYY
    3. US: MM/DD/YYYY
    4. Dash format: DD-MM-YYYY
    5. Pandas auto-detection

    Args:
        val: Value to parse (string, Timestamp, datetime64, or None).

    Returns:
        Parsed Timestamp or pd.NaT if parsing fails.

    Examples:
        >>> to_date("2023-01-15")
        Timestamp('2023-01-15 00:00:00')
        >>> to_date("15/01/2023")
        Timestamp('2023-01-15 00:00:00')
    """
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return pd.NaT
    if isinstance(val, (pd.Timestamp, np.datetime64)):
        return pd.to_datetime(val, errors="coerce")
    s = strip_invisibles(val)
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y"):
        try:
            return pd.to_datetime(s, format=fmt, errors="raise")
        except Exception:
            pass
    return pd.to_datetime(s, errors="coerce")


def remove_accents(s: str) -> str:
    """Remove accents and diacritics from string.

    Uses Unicode NFD (Normalization Form Decomposed) to separate base
    characters from combining marks, then filters out the marks.

    Args:
        s: String to process.

    Returns:
        String with accents removed.

    Examples:
        >>> remove_accents("Operación")
        'Operacion'
        >>> remove_accents("José")
        'Jose'
    """
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")


def normalize_spanish_name(s: str) -> str:
    """Normalize a header or name for comparison.

    Process:
    1. Strip invisible characters
    2. Remove accents using NFKD normalization
    3. Collapse whitespace
    4. Convert to lowercase

    Args:
        s: String to normalize.

    Returns:
        Normalized string suitable for fuzzy matching.

    Examples:
        >>> normalize_spanish_name("Forma de Pago")
        'forma de pago'
        >>> normalize_spanish_name("Participación del día")
        'participacion del dia'
    """
    base = strip_invisibles(s or "")
    if base is None:
        return ""
    # Remove accents
    base = unicodedata.normalize("NFKD", base)
    base = "".join(ch for ch in base if not unicodedata.combining(ch))
    # Collapse whitespace + lower
    base = re.sub(r"\s+", " ", base).strip().lower()
    return base


def to_snake(s: str) -> str:
    """Convert string to snake_case.

    Process:
    1. Strip invisible characters
    2. Remove accents
    3. Convert to lowercase
    4. Replace non-word characters with spaces
    5. Replace spaces with underscores
    6. Strip leading/trailing underscores

    Args:
        s: String to convert.

    Returns:
        Snake_case string.

    Examples:
        >>> to_snake("Fecha de Operación")
        'fecha_de_operacion'
        >>> to_snake("No. Mesa")
        'no_mesa'
    """
    s0 = strip_invisibles(s) or ""
    s1 = remove_accents(s0).lower()
    s1 = re.sub(r"[^\w\s]", " ", s1)
    s1 = re.sub(r"\s+", "_", s1).strip("_")
    return s1


def uniquify(cols: Iterable[str]) -> List[str]:
    """Make column names unique by appending .1, .2, etc. to duplicates.

    Args:
        cols: Iterable of column names (may contain duplicates).

    Returns:
        List of unique column names with duplicates numbered.

    Examples:
        >>> uniquify(["col", "col", "other", "col"])
        ['col', 'col.1', 'other', 'col.2']
    """
    seen: dict[str, int] = {}
    out = []
    for c in cols:
        n = seen.get(c, 0)
        out.append(c if n == 0 else f"{c}.{n}")
        seen[c] = n + 1
    return out
