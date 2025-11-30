#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Staging (Silver) layer: POS "Detalle por forma de pago" cleaner.

This module is part of the Staging (Silver) layer in the ETL pipeline.
It transforms raw payment Excel files into clean, normalized CSV files.

Data directory mapping:
    Input: data/a_raw/ → Raw (Bronze) layer
    Output: data/b_clean/ → Staging (Silver) layer

POS "Detalle por forma de pago" cleaner -> normalized CSV.

Single file:
  python -m pos_etl.b_transform.pos_excel_payments_cleaner --input path/to/file.xlsx

Batch (folder):
  python -m pos_etl.b_transform.pos_excel_payments_cleaner \
      --input-dir data/a_raw/payments --recursive --outdir data/b_clean/payments

What it does:
- Reads ONLY the sheet "Detalle por forma de pago" (case-insensitive).
- Detects the header row (the one that contains "Forma de pago").
- Drops these columns, if present:
    - "Participación del día" / "Participacion del día" / "Participacion del dia"
    - "PDV" / "Movimiento                     PDV"
    - "Estatus"
    - "Mesero"
    - "Cajero"
    - "Fecha de pago"
    - "Referencia"
    - "Transacción"
    - "Terminal"
    - "Código de validación"
- Drops any trailing completely-empty rows and unnamed columns.
- Normalizes headers to snake_case and coerces dates/numbers.
- Interprets the duplicated 'Propina' columns as:
    - first 'Propina'  -> total_day_tips
    - second 'Propina' -> ticket_tip
- 'Total Cobrado' is interpreted as ticket_total_plus_tip.
- Sets 'sucursal' primarily from folder/CLI, not from Mesero/Cajero:
    - if --sucursal is provided, use that for all files
    - otherwise, when using --input-dir, infer from the first directory
      under that root (e.g. "Kavia_OLD" -> "Kavia")
    - only if no hint is available, fall back to Mesero/Cajero heuristic.

Output name convention:
  forma_pago_<sucursal_slug>_<start_date>_<end_date>.csv
"""

from __future__ import annotations

import argparse
import csv
import logging
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, List, Optional

import pandas as pd

# Reuse your existing utils from the other cleaner
from pos_core.etl.utils import get_raw_file_date_range, slugify

from .cleaning_utils import (
    neutralize as neutralize_formula_injection,
)
from .cleaning_utils import (
    normalize_spanish_name,
    strip_invisibles,
    to_date,
    to_float,
)

# --------------------------------------------------------------------
# Helpers: sheet finding, header detection, name normalization
# --------------------------------------------------------------------


def find_sheet_case_insensitive(xls: pd.ExcelFile, target: str) -> str:
    """Find a sheet by name (case-insensitive, allowing partial match)."""
    t = target.lower()
    for n in xls.sheet_names:
        if n.lower().strip() == t:
            return n
    for n in xls.sheet_names:
        if t in n.lower():
            return n
    raise ValueError(f"Sheet like '{target}' not found. Available: {xls.sheet_names}")


def detect_header_row(
    df_no_header: pd.DataFrame, sentinels: Iterable[str] = ("Forma de pago",)
) -> int:
    """
    Scan the top rows and find the one that looks like the header.
    For this report, the row containing 'Forma de pago' is the header.
    """
    max_scan = min(40, len(df_no_header))
    for i in range(max_scan):
        row = df_no_header.iloc[i].astype(str).map(strip_invisibles)
        if any(any(token.lower() in cell.lower() for cell in row) for token in sentinels):
            return i
    # Fallback: first row
    return 0


def _to_int_or_none(x: Any) -> Optional[int]:
    """
    Safely convert to int, or return None if blank/non-numeric.
    Used for order_index / Orden keys to avoid merge dtype issues.
    """
    if pd.isna(x):
        return None
    s = str(x).strip()
    if not s:
        return None
    try:
        return int(float(s))
    except Exception:
        return None


# --------------------------------------------------------------------
# Header normalization map
# --------------------------------------------------------------------

# Raw header -> preferred logical name (before snake_case)
# NOTE: 'Propina' is handled specially in normalize_headers to distinguish
# total_day_tips (first Propina) vs ticket_tip (second Propina).
HEADER_MAP = {
    "Total": "day_total",
    "Participacion del día": "day_share",
    "Participación del día": "day_share",
    "Participacion del dia": "day_share",
    "Fecha": "operating_date",
    "Orden": "order_index",
    "Forma de pago": "payment_method",
    "Total.1": "ticket_total",
    "Total Cobrado": "ticket_total_plus_tip",
}

NUMERIC_COLUMNS = {
    "day_total",
    "day_share",
    "ticket_total",
    "ticket_tip",
    "ticket_total_plus_tip",
    "total_day_tips",
}


def normalize_headers(cols: List[str]) -> List[str]:
    """
    Apply HEADER_MAP (with special handling for duplicated 'Propina'),
    then snake_case + de-duplicate.
    - First 'Propina'  -> total_day_tips
    - Second 'Propina' -> ticket_tip
    """
    propina_seen = 0
    mapped_list: List[str] = []

    for c in cols:
        c0 = strip_invisibles(str(c) if c is not None else "")
        raw_no_invisibles = strip_invisibles(c0)

        # Special logic for 'Propina'
        if raw_no_invisibles == "Propina":
            propina_seen += 1
            if propina_seen == 1:
                logical = "total_day_tips"
            else:
                logical = "ticket_tip"
        else:
            logical = HEADER_MAP.get(c0, c0) if c0 is not None else ""

        # snake_case
        logical = re.sub(r"[^\w]+", "_", logical).strip("_").lower()
        logical = logical or "unnamed"
        mapped_list.append(logical)

    # De-duplicate: total, total_2, etc.
    seen = {}
    out: List[str] = []
    for h in mapped_list:
        if h not in seen:
            seen[h] = 1
            out.append(h)
        else:
            seen[h] += 1
            out.append(f"{h}_{seen[h]}")
    return out


# --------------------------------------------------------------------
# Core transform
# --------------------------------------------------------------------


DROP_COLS_LOGICAL_RAW = [
    "Participación del día",
    "Participacion del día",
    "Participacion del dia",
    "PDV",
    "Movimiento                     PDV",
    "Estatus",
    "Mesero",
    "Cajero",
    "Fecha de pago",
    "Referencia",
    "Referencia ",
    "Transacción",
    "Transaccion",
    "Terminal",
    "Código de validación",
    "Codigo de validacion",
]

DROP_COLS_NORMALIZED = {normalize_spanish_name(c) for c in DROP_COLS_LOGICAL_RAW}


def extract_sucursal_like(df: pd.DataFrame) -> str:
    """
    Fallback heuristic for sucursal from data:
    - Try 'Cajero' or 'Mesero' columns before we drop them.
    - Take the most frequent non-empty value.
    If nothing sensible is found, return empty string.

    NOTE: This is now a fallback only; the preferred source is folder/CLI.
    """
    candidates = []
    for col in ("Cajero", "Mesero"):
        if col in df.columns:
            series = df[col].dropna().astype(str).map(strip_invisibles)
            series = series[series != ""]
            if not series.empty:
                value_counts = series.value_counts()
                top = value_counts.index[0]
                candidates.append(top)

    if not candidates:
        return ""

    # If multiple, pick the shortest non-numeric-ish (usually the branch name)
    def score(v: str) -> tuple[int, int]:
        numeric_like = 1 if re.fullmatch(r"\d+", v) else 0
        return (numeric_like, len(v))  # prefer non-numeric, shorter

    candidates.sort(key=score)
    return candidates[0]


def normalize_branch_name(raw: Optional[str]) -> str:
    """
    Normalize folder/CLI branch names for 'sucursal':
    - Kavia -> Kavia
    - Kavia_OLD -> Kavia
    - Punto-Valle -> Punto Valle
    - punto valle -> Punto valle (you can later force title-case if you want)
    """
    if not raw:
        return ""
    name = str(raw).strip()

    # Split off suffixes like _OLD, _TEST, etc.
    base = name.split("_", 1)[0]

    # Replace dashes with spaces for prettier names
    base = base.replace("-", " ")

    # You can decide if you want title-case or keep original
    return base


def transform_detalle_por_forma_pago(
    xlsx_in: Path,
    sucursal_hint: Optional[str] = None,
    verbose: bool = False,
) -> pd.DataFrame:
    """
    Transform a single Excel into a cleaned DataFrame with one row per payment.
    """
    # Log immediately at function entry - this helps catch hangs before any operations
    logging.info("Processing file: %s (sucursal_hint=%r)", xlsx_in, sucursal_hint)

    if verbose:
        logging.debug("Opening Excel file: %s", xlsx_in)

    xls = pd.ExcelFile(xlsx_in)

    if verbose:
        logging.debug("ExcelFile opened successfully: %s", xlsx_in.name)
    try:
        logging.debug("Available sheets: %s", xls.sheet_names)
        sheet = find_sheet_case_insensitive(xls, "Detalle por forma de pago")
        logging.debug("Using sheet: %s", sheet)

        # Read only first 50 rows for header detection to avoid loading entire file
        logging.debug("Reading first 50 rows for header detection...")
        df0 = xls.parse(sheet_name=sheet, header=None, dtype=object, nrows=50)
        logging.debug("Read %d rows for header detection", len(df0))
        header_row = detect_header_row(df0)
        logging.debug("Detected header row at index: %d", header_row)

        # ------------------------------------------------------------------
        # Parse 'Pagos Eliminados' sheet (if present)
        # ------------------------------------------------------------------
        elim_df = pd.DataFrame()

        try:
            logging.debug("Attempting to load 'Pagos Eliminados' sheet...")
            elim_sheet = find_sheet_case_insensitive(xls, "Pagos Eliminados")
            logging.debug("Found 'Pagos Eliminados' sheet: %s", elim_sheet)
            # Read only first 50 rows for header detection
            raw_elim = xls.parse(
                sheet_name=elim_sheet,
                header=None,
                dtype=object,
                nrows=50,
            )
            logging.debug("Read %d rows from 'Pagos Eliminados' sheet", len(raw_elim))

            # Locate header row (row containing "Fecha de operación") - optimized scan
            logging.debug("Scanning for header row in 'Pagos Eliminados'...")
            elim_header_row = 5  # default fallback
            max_scan = min(40, len(raw_elim))
            for i in range(max_scan):
                row_str = raw_elim.iloc[i].astype(str).str.cat(sep=" ")
                if "fecha de operación" in row_str.lower():
                    elim_header_row = i
                    logging.debug("Found 'Pagos Eliminados' header at row %d", i)
                    break

            # Now read full sheet for data extraction
            logging.debug("Reading full 'Pagos Eliminados' sheet...")
            raw_elim_full = xls.parse(
                sheet_name=elim_sheet,
                header=None,
                dtype=object,
            )
            logging.debug("Read %d total rows from 'Pagos Eliminados'", len(raw_elim_full))

            # Extract the block B6:L? -> raw_elim.iloc[elim_header_row:, 1:12]
            elim_df = raw_elim_full.iloc[elim_header_row:, 1:12].copy()
            logging.debug(
                "Extracted block: %d rows, %d columns (before header processing)",
                len(elim_df),
                len(elim_df.columns),
            )
            elim_df.columns = elim_df.iloc[0]
            elim_df = elim_df.drop(elim_df.index[0])  # drop header row
            elim_df = elim_df.dropna(how="all").reset_index(drop=True)
            logging.debug(
                "Extracted %d elimination records (after dropping empty rows)", len(elim_df)
            )

            # Normalize columns
            elim_df = elim_df.rename(
                columns=lambda c: strip_invisibles(str(c)) if not pd.isna(c) else ""
            )

            # Log available columns before filtering (for debugging)
            logging.debug("Available columns in elim_df: %s", list(elim_df.columns))

            # Check if "Forma de pago" or similar payment method column exists
            payment_method_col = None
            for col in elim_df.columns:
                col_lower = normalize_spanish_name(str(col))
                if "forma" in col_lower and "pago" in col_lower:
                    payment_method_col = col
                    logging.debug(
                        "Found payment method column: '%s' (normalized: '%s')", col, col_lower
                    )
                    break

            # Keep only needed columns (but preserve payment method for logging if available)
            cols_to_keep = ["Fecha de operación", "Orden"]
            if payment_method_col:
                cols_to_keep.append(payment_method_col)

            elim_df = elim_df[cols_to_keep].dropna(how="all")
            logging.debug("After filtering, %d elimination records remain", len(elim_df))

            # Standardize types
            logging.debug("Standardizing types for elimination records...")
            # Ensure datetime-like before using .dt accessor
            elim_df["Fecha de operación"] = pd.to_datetime(
                elim_df["Fecha de operación"].map(to_date), errors="coerce"
            ).dt.date
            elim_df["Orden"] = elim_df["Orden"].map(_to_int_or_none)

            # Log details before deduplication for debugging
            rows_before_dedup = len(elim_df)
            logging.debug("Before deduplication: %d records", rows_before_dedup)

            # Check for None/NaT values that might cause issues
            null_dates = elim_df["Fecha de operación"].isna().sum()
            null_orders = elim_df["Orden"].isna().sum()
            logging.debug("  - Records with null dates: %d", null_dates)
            logging.debug("  - Records with null orders: %d", null_orders)

            # Check for actual duplicates
            duplicate_mask = elim_df.duplicated(subset=["Fecha de operación", "Orden"], keep=False)
            duplicate_count = duplicate_mask.sum()
            logging.debug("  - Records that are duplicates: %d", duplicate_count)

            if duplicate_count > 0:
                logging.debug("Sample duplicate records (first 10):")
                duplicates = elim_df[duplicate_mask].head(10)
                for idx, row in duplicates.iterrows():
                    payment_info = ""
                    if payment_method_col and payment_method_col in row:
                        payment_info = f", Forma de pago={row[payment_method_col]}"
                    logging.debug(
                        "    Row %d: Fecha=%s, Orden=%s%s",
                        idx,
                        row["Fecha de operación"],
                        row["Orden"],
                        payment_info,
                    )

            # Show value counts for debugging
            if rows_before_dedup > 0:
                logging.debug("Sample of first 5 records before deduplication:")
                for idx, row in elim_df.head(5).iterrows():
                    logging.debug(
                        "    Row %d: Fecha=%s (type=%s), Orden=%s (type=%s)",
                        idx,
                        row["Fecha de operación"],
                        type(row["Fecha de operación"]).__name__,
                        row["Orden"],
                        type(row["Orden"]).__name__,
                    )

            # Deduplicate possible multi-payment eliminations
            # Remove payment method column if we kept it for logging
            # (we only need date + order for merge)
            if payment_method_col and payment_method_col in elim_df.columns:
                elim_df = elim_df.drop(columns=[payment_method_col])

            elim_df = elim_df.drop_duplicates(subset=["Fecha de operación", "Orden"])
            rows_after_dedup = len(elim_df)
            logging.debug(
                "After deduplication: %d unique elimination records (removed %d duplicates)",
                rows_after_dedup,
                rows_before_dedup - rows_after_dedup,
            )

        except Exception as e:
            logging.warning(
                "Could not load 'Pagos Eliminados' sheet in %s: %s",
                xlsx_in.name,
                e,
            )
            elim_df = pd.DataFrame()

        # Now read main payments sheet with header
        logging.debug("Reading main payments sheet with header at row %d...", header_row)
        df = xls.parse(
            sheet_name=sheet,
            header=header_row,
            dtype=object,
        )
        logging.debug("Read main sheet: %d rows, %d columns", len(df), len(df.columns))

        # Drop unnamed "garbage" columns (e.g., trailing empty one)
        logging.debug("Dropping unnamed columns...")
        df = df.loc[:, ~df.columns.astype(str).str.startswith("Unnamed")]
        logging.debug("After dropping unnamed columns: %d columns", len(df.columns))

        # Drop completely empty rows
        logging.debug("Dropping completely empty rows...")
        df = df.dropna(how="all")
        logging.debug("After dropping empty rows: %d rows", len(df))
        if df.empty:
            logging.warning("DataFrame is empty after initial cleaning")
            return df

        # Drop footer junk where the *first original column* is empty
        logging.debug("Filtering footer rows...")
        first_col = df.columns[0]
        mask_first = df[first_col].map(lambda x: strip_invisibles(str(x)) if not pd.isna(x) else "")
        df = df[mask_first != ""]
        df = df.reset_index(drop=True)
        logging.debug("After filtering footer: %d rows", len(df))

        # Decide sucursal:
        # 1) prefer hint (from CLI or folder)
        # 2) fall back to heuristic from Mesero/Cajero (legacy behaviour)
        logging.debug("Determining sucursal (hint=%r)...", sucursal_hint)
        sucursal = normalize_branch_name(sucursal_hint) if sucursal_hint else ""
        if not sucursal:
            sucursal = extract_sucursal_like(df)
        logging.debug("Using sucursal: %r", sucursal)

        # Clean text-ish columns and neutralize formulas
        logging.debug("Cleaning text columns and neutralizing formulas...")
        object_cols = [col for col in df.columns if df[col].dtype == object]
        logging.debug("Processing %d object columns", len(object_cols))
        for i, col in enumerate(object_cols):
            if (i + 1) % 5 == 0:
                logging.debug("  Processed %d/%d object columns...", i + 1, len(object_cols))
            df[col] = (
                df[col]
                .map(lambda x: strip_invisibles(x) if not pd.isna(x) else x)
                .map(lambda x: neutralize_formula_injection(x) if isinstance(x, str) else x)
            )
        logging.debug("Finished cleaning text columns")

        # Drop the columns you said you don't need (matching by normalized header)
        logging.debug("Filtering columns to keep...")
        keep_cols = []
        for col in df.columns:
            norm = normalize_spanish_name(str(col))
            if norm not in DROP_COLS_NORMALIZED:
                keep_cols.append(col)

        df = df[keep_cols]
        logging.debug("Kept %d columns after filtering", len(df.columns))

        # Add sucursal column, if we have one
        if sucursal:
            df.insert(0, "sucursal", sucursal)

        # Normalize headers to snake_case (with special Propina logic)
        logging.debug("Normalizing headers to snake_case...")
        df.columns = normalize_headers(list(df.columns))
        logging.debug("Normalized headers: %s", list(df.columns[:10]))

        # Coerce date column, if present
        if "operating_date" in df.columns:
            logging.debug("Coercing operating_date column...")
            # Ensure datetime-like before using .dt accessor
            df["operating_date"] = pd.to_datetime(
                df["operating_date"].map(to_date), errors="coerce"
            ).dt.date

        # Filter by chunk date range if available
        # (prevents duplicates from overlapping API responses)
        chunk_range = get_raw_file_date_range(xlsx_in)
        if chunk_range:
            chunk_start, chunk_end = chunk_range
            logging.debug("Filtering by chunk date range: %s to %s", chunk_start, chunk_end)
            # Filter elim_df first if it exists
            if not elim_df.empty and "Fecha de operación" in elim_df.columns:
                elim_mask = elim_df["Fecha de operación"].notna() & (
                    (elim_df["Fecha de operación"] >= chunk_start)
                    & (elim_df["Fecha de operación"] <= chunk_end)
                )
                elim_df = elim_df[elim_mask].copy()
                logging.debug("Filtered elim_df to %d rows", len(elim_df))

            # Filter main df if it has operating_date column
            if "operating_date" in df.columns:
                mask = df["operating_date"].notna() & (
                    (df["operating_date"] >= chunk_start) & (df["operating_date"] <= chunk_end)
                )
                rows_before = len(df)
                df = df[mask].copy()
                rows_after = len(df)
                if rows_before > rows_after:
                    logging.info(
                        "Filtered %d rows to %d rows based on chunk range %s..%s in %s",
                        rows_before,
                        rows_after,
                        chunk_start,
                        chunk_end,
                        xlsx_in.name,
                    )

        # Coerce numeric columns
        logging.debug("Coercing numeric columns...")
        numeric_cols = [c for c in df.columns if c in NUMERIC_COLUMNS]
        logging.debug("Coercing %d numeric columns: %s", len(numeric_cols), numeric_cols)
        for c in numeric_cols:
            df[c] = df[c].map(to_float)
        logging.debug("Finished coercing numeric columns")

        # ------------------------------------------------------------------
        # Attach elimination_present boolean to df (payments)
        # ------------------------------------------------------------------
        if not elim_df.empty and {"operating_date", "order_index"} <= set(df.columns):
            logging.debug(
                "Merging elimination data (%d elim records, %d payment records)...",
                len(elim_df),
                len(df),
            )

            # Log sample of main payments table before merge (to see if duplicates already exist)
            if "operating_date" in df.columns and "order_index" in df.columns:
                logging.debug("  - Sample of main payments table (first 10 rows):")
                for idx, row in df.head(10).iterrows():
                    payment_method = row.get("payment_method", "N/A")
                    logging.debug(
                        "    Row %d: Fecha=%s, Orden=%s, Forma de pago=%s",
                        idx,
                        row.get("operating_date"),
                        row.get("order_index"),
                        payment_method,
                    )
            elim_df["elimination_present"] = True

            # Normalize key types before merging (both sides as ints/None)
            logging.debug("Normalizing order_index types for merge...")
            df["order_index"] = df["order_index"].map(_to_int_or_none)

            key_elim = elim_df.rename(
                columns={
                    "Fecha de operación": "operating_date",
                    "Orden": "order_index",
                }
            )
            key_elim["order_index"] = key_elim["order_index"].map(_to_int_or_none)

            logging.debug("Performing merge on operating_date and order_index...")
            logging.debug("  - Main payments table before merge: %d rows", len(df))
            logging.debug("  - Elimination table (unique date+order): %d rows", len(key_elim))

            # Log sample of what will be marked as eliminated
            if len(key_elim) > 0:
                logging.debug("  - Sample elimination keys (first 5):")
                for idx, row in key_elim.head(5).iterrows():
                    logging.debug(
                        "    Date=%s, Order=%s", row["operating_date"], row["order_index"]
                    )

            df = df.merge(
                key_elim[["operating_date", "order_index", "elimination_present"]],
                on=["operating_date", "order_index"],
                how="left",
            )
            # Ensure boolean type before filling to avoid FutureWarning
            df["elimination_present"] = df["elimination_present"].astype("boolean").fillna(False)
            logging.debug("Merge complete: %d rows in result", len(df))

            # Log details about marked payments
            n_marked = df["elimination_present"].sum()
            if n_marked > 0:
                logging.debug("  - Payments marked as eliminated: %d rows", n_marked)
                # Show sample of marked payments
                marked = df[df["elimination_present"]].head(10)
                logging.debug("  - Sample marked payments (first 10):")
                for idx, row in marked.iterrows():
                    logging.debug(
                        "    Row %d: Fecha=%s, Orden=%s, Forma de pago=%s",
                        idx,
                        row.get("operating_date"),
                        row.get("order_index"),
                        row.get("payment_method", "N/A"),
                    )

        else:
            logging.debug("No elimination data to merge (elim_df empty or missing columns)")
            df["elimination_present"] = False

        # QA summary
        n_elims = df["elimination_present"].sum()
        if n_elims > 0:
            logging.warning(
                "Detected %d eliminated payments in this file (%s).",
                n_elims,
                xlsx_in.name,
            )

        # Final column ordering: put the "useful" ones at the front
        logging.debug("Reordering columns...")
        front = [
            col
            for col in [
                "sucursal",
                "operating_date",
                "order_index",
                "payment_method",
                "ticket_total",
                "ticket_tip",
                "ticket_total_plus_tip",
                "day_total",
                "total_day_tips",
                "day_share",
            ]
            if col in df.columns
        ]
        rest = [c for c in df.columns if c not in front]
        df = df[front + rest]

        logging.debug("Transform complete: %d rows, %d columns", len(df), len(df.columns))
        return df
    finally:
        # Ensure ExcelFile is closed to free resources
        xls.close()


# --------------------------------------------------------------------
# Output naming + CSV writing
# --------------------------------------------------------------------


def output_name_for(xlsx_path: Path, df: pd.DataFrame) -> Path:
    """
    Build something like:
      forma_pago_panem_kavia_2025-01-01_2025-01-01.csv
    Fallbacks if sucursal or dates are missing.
    """
    suc = ""
    if "sucursal" in df.columns and len(df):
        suc = str(df["sucursal"].iloc[0] or "")
    suc_slug = slugify(suc) if suc else "unknown"

    if "operating_date" in df.columns and df["operating_date"].notna().any():
        dates = pd.to_datetime(df["operating_date"].dropna()).dt.date
        start, end = dates.min(), dates.max()
        base = f"forma_pago_{suc_slug}_{start.isoformat()}_{end.isoformat()}.csv"
    else:
        base = f"forma_pago_{suc_slug}_{xlsx_path.stem}.csv"

    return Path(base)


def write_csv(df: pd.DataFrame, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    # utf-8-sig adds BOM so Excel on Windows detects encoding correctly
    df.to_csv(out_path, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_MINIMAL)


# --------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------


@dataclass
class Args:
    input: Optional[Path]
    input_dir: Optional[Path]
    outdir: Path
    recursive: bool
    quiet: bool
    verbose: bool
    sucursal: Optional[str]


def parse_args() -> Args:
    p = argparse.ArgumentParser(description="Clean POS 'Detalle por forma de pago' Excel into CSV")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--input", type=Path, help="Single .xlsx file")
    g.add_argument("--input-dir", type=Path, help="Folder with .xlsx files")
    p.add_argument("--outdir", type=Path, default=Path.cwd(), help="Where to write CSVs")
    p.add_argument(
        "--recursive",
        action="store_true",
        help="Recurse into subdirectories of --input-dir",
    )
    p.add_argument("--quiet", action="store_true", help="Less logging")
    p.add_argument(
        "--verbose",
        action="store_true",
        help="Verbose logging (DEBUG level)",
    )
    p.add_argument(
        "--sucursal",
        type=str,
        default=None,
        help="Override sucursal name (otherwise inferred from path or data).",
    )
    a = p.parse_args()
    return Args(
        input=a.input,
        input_dir=a.input_dir,
        outdir=a.outdir,
        recursive=a.recursive,
        quiet=a.quiet,
        verbose=a.verbose,
        sucursal=a.sucursal,
    )


logger = logging.getLogger(__name__)


def clean_payments_directory(
    input_dir: Path | str,
    output_dir: Path | str,
    recursive: bool = True,
) -> None:
    """Clean all payments Excel files in a directory and write normalized CSVs.

    Reads all .xlsx files from input_dir (recursively if recursive=True),
    processes them using the payments cleaner logic, and writes normalized
    CSV files to output_dir. Handles sucursal inference from directory
    structure.

    Args:
        input_dir: Directory containing raw payment Excel files.
        output_dir: Directory to write cleaned CSV files. Will be created if it doesn't exist.
        recursive: If True, traverse subdirectories recursively (default: True).

    Raises:
        FileNotFoundError: If input_dir doesn't exist.
        ValueError: If input_dir is not a directory.

    Examples:
        >>> from pathlib import Path
        >>> clean_payments_directory(
        ...     Path("data/a_raw/payments/batch"),
        ...     Path("data/b_clean/payments/batch"),
        ...     recursive=True
        ... )
    """
    # Convert string paths to Path
    if isinstance(input_dir, str):
        input_dir = Path(input_dir)
    if isinstance(output_dir, str):
        output_dir = Path(output_dir)

    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")
    if not input_dir.is_dir():
        raise ValueError(f"Input path is not a directory: {input_dir}")

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    root = input_dir.resolve()
    any_found = False

    logger.info(f"Cleaning payments files from {input_dir} -> {output_dir} (recursive={recursive})")

    for xlsx_file in iter_xlsx_files(root, recursive, verbose=False):
        # Skip Excel temp/lock files like "~$Payments_....xlsx"
        if xlsx_file.name.startswith("~$"):
            logging.info("Skipping temp Excel file: %s", xlsx_file)
            continue

        any_found = True

        try:
            # Infer branch from first directory under input_dir
            rel = xlsx_file.resolve().relative_to(root)
            branch_dir = rel.parts[0] if len(rel.parts) > 1 else None
            hint = normalize_branch_name(branch_dir)

            logger.debug(f"Processing {xlsx_file.name} (sucursal_hint={hint})")
            run_single(xlsx_file, output_dir, sucursal_hint=hint, verbose=False)
        except Exception as e:
            logger.error(f"Failed to process {xlsx_file}: {e}", exc_info=True)
            raise

    if not any_found:
        logger.warning(f"No .xlsx files found under {input_dir}")


def iter_xlsx_files(root: Path, recursive: bool, verbose: bool = False) -> Iterable[Path]:
    if verbose:
        logging.debug("Starting to find .xlsx files in: %s (recursive=%s)", root, recursive)
    if recursive:
        files = list(p for p in root.rglob("*.xlsx") if p.is_file())
        if verbose:
            logging.debug("Found %d files recursively", len(files))
        yield from files
    else:
        files = list(p for p in root.glob("*.xlsx") if p.is_file())
        if verbose:
            logging.debug("Found %d files (non-recursive)", len(files))
        yield from files


def run_single(
    xlsx: Path, outdir: Path, sucursal_hint: Optional[str], verbose: bool = False
) -> Path:
    logging.info("Processing %s (sucursal_hint=%r)", xlsx, sucursal_hint)

    try:
        df = transform_detalle_por_forma_pago(xlsx, sucursal_hint=sucursal_hint, verbose=verbose)

        logging.debug(
            "Transform completed, DataFrame shape: %d rows, %d cols", len(df), len(df.columns)
        )
        if df.empty:
            logging.warning("No data found in %s (after cleaning)", xlsx)
        logging.debug("Generating output filename...")
        out_name = output_name_for(xlsx, df if not df.empty else pd.DataFrame())
        out_path = outdir / out_name
        logging.debug("Writing CSV to: %s", out_path)
        write_csv(df, out_path)
        logging.info("Wrote %s (%d rows, %d cols)", out_path, len(df), len(df.columns))
        return out_path
    except Exception as e:
        logging.error("Error processing %s: %s", xlsx, e, exc_info=True)
        raise


def main() -> None:
    args = parse_args()
    if args.verbose:
        log_level = logging.DEBUG
    elif args.quiet:
        log_level = logging.WARNING
    else:
        log_level = logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(levelname)s: %(message)s",
    )

    # Single file: use CLI --sucursal if provided
    if args.input:
        if not args.input.exists():
            raise SystemExit(f"Input file not found: {args.input}")
        run_single(args.input, args.outdir, sucursal_hint=args.sucursal, verbose=args.verbose)
        return

    # Folder mode
    if not args.input_dir or not args.input_dir.exists():
        raise SystemExit(f"Input dir not found: {args.input_dir}")

    root = args.input_dir.resolve()
    any_found = False

    if args.verbose:
        logging.debug("Starting to iterate files in: %s", root)

    for x in iter_xlsx_files(root, args.recursive, verbose=args.verbose):
        any_found = True

        if args.verbose:
            logging.debug("Found file: %s", x)

        try:
            # infer branch from first directory under input_dir if no explicit sucursal
            if args.verbose:
                logging.debug("Resolving relative path for: %s", x)
            rel = x.resolve().relative_to(root)

            if args.verbose:
                logging.debug("Relative path: %s", rel)
            branch_dir = rel.parts[0] if len(rel.parts) > 1 else None

            if args.verbose:
                logging.debug("Branch dir: %s", branch_dir)
            hint = args.sucursal or normalize_branch_name(branch_dir)

            if args.verbose:
                logging.debug("Sucursal hint: %s, about to call run_single", hint)

            run_single(x, args.outdir, sucursal_hint=hint, verbose=args.verbose)

            if args.verbose:
                logging.debug("run_single completed for: %s", x.name)
        except Exception as e:
            logging.error("Failed on %s: %s", x, e)

    if not any_found:
        logging.warning("No .xlsx files found under %s", args.input_dir)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
