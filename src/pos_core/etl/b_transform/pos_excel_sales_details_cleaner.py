#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""POS "Detalle de Ventas" cleaner -> normalized CSV.

This module cleans and normalizes POS sales detail Excel reports into
standardized CSV format. It handles the detailed variant of sales reports
with item-level transaction data.

The cleaning process:
1. Detects the correct sheet and header row
2. Normalizes Spanish column names to snake_case English
3. Handles duplicate column names (Subtotal/IVA/IEPS/Total appear multiple times)
4. Coerces data types (dates, numbers, text)
5. Strips invisible characters and neutralizes formula injection
6. Extracts branch name from metadata if available

Examples:
    Single file:
        python -m pos_etl.b_transform.pos_excel_sales_details_cleaner \\
            --input ./downloads/Detail_CEDIS_2025-08-01_2025-08-31.xlsx

    Batch (folder):
        python -m pos_etl.b_transform.pos_excel_sales_details_cleaner \\
            --input-dir ./downloads --recursive --outdir ./csv

Notes:
    - Handles the *Detailed* variant (sheet like "Detalle de Ventas")
    - Normalizes headers to snake_case, coerces dates/numbers
    - Strips invisible chars and neutralizes spreadsheet formulas
    - Expects 4 amount blocks: ticket, item, cortesia_cancel, anulacion
"""

from __future__ import annotations

import argparse, csv, logging, re, sys, unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from pos_etl.utils import slugify

from .ws_cleaning_utils import (
    strip_invisibles,
    neutralize as neutralize_formula_injection,
    to_float,
    to_int,
    to_date,
)

import numpy as np
import pandas as pd

NBSP = "\u00A0"
ZW = "".join(chr(c) for c in (0x200B, 0x200C, 0x200D, 0xFEFF))

logger = logging.getLogger(__name__)

# --------------------------- header detection ---------------------------
def find_sheet_case_insensitive(xls: pd.ExcelFile, target: str) -> str:
    """Find a sheet by name using case-insensitive matching.

    First tries exact case-insensitive match, then partial substring match.

    Args:
        xls: Excel file object with sheet_names attribute.
        target: Target sheet name to find.

    Returns:
        Actual sheet name from the Excel file.

    Raises:
        ValueError: If no matching sheet is found.
    """
    t = target.lower()
    for n in xls.sheet_names:
        if n.lower().strip() == t:
            return n
    for n in xls.sheet_names:
        if t in n.lower():
            return n
    raise ValueError(f"Sheet like '{target}' not found. Available: {xls.sheet_names}")

def detect_header_row(
    df_no_header: pd.DataFrame,
    sentinels: Iterable[str] = ("Día", "Fecha de operación"),
) -> int:
    """Detect which row contains the column headers.

    Scans the first 30 rows (or fewer if DataFrame is shorter) looking for
    sentinel keywords that indicate a header row. These are typically Spanish
    column names like "Día" or "Fecha de operación".

    Args:
        df_no_header: DataFrame parsed without headers (header=None).
        sentinels: Keywords to search for in header row. Defaults to
            ("Día", "Fecha de operación").

    Returns:
        Row index (0-based) where header is found, or 0 as fallback.
    """
    max_scan = min(30, len(df_no_header))
    for i in range(max_scan):
        row = df_no_header.iloc[i].astype(str).map(strip_invisibles)
        if any(any(token.lower() in cell.lower() for cell in row) for token in sentinels):
            return i
    return 0  # fallback

def parse_sucursal_from_top(df_no_header: pd.DataFrame) -> Optional[str]:
    """Extract branch name from the top rows of the Excel sheet.

    POS reports often include branch metadata in cells near the top
    (typically around C3 or in the first 6 rows/columns). This function
    searches for a pattern like "Sucursal: CEDIS".

    Args:
        df_no_header: DataFrame parsed without headers, containing metadata.

    Returns:
        Branch name if found, None otherwise.
    """
    # Typically appears near C3 or around first rows
    head = df_no_header.iloc[:6, :6].astype(str).applymap(strip_invisibles)
    flat = " | ".join(head.fillna("").astype(str).values.ravel().tolist())
    # e.g., "Sucursal: CEDIS"
    m = re.search(r"Sucursal\s*:\s*([A-Za-z0-9\-\._\s]+)", flat, re.IGNORECASE)
    return m.group(1).strip() if m else None

# --------------------------- normalization map ---------------------------
# Spanish header -> normalized snake_case
HEADER_MAP: Dict[str, str] = {
    "Día": "day_name",
    "Fecha de operación": "operating_date",
    "Hora de cierre": "closing_time",
    "Hora de captura": "captured_time",
    "Semana": "week_number",

    "Movimiento PDV": "pdv_txn_id",
    "Folio PDV": "pdv_txn_id",
    "Folio": "pdv_txn_id",

    "Orden": "order_id",
    "Tipo de Orden": "order_type",
    "Tipo de orden": "order_type",
    "Subtipo de Orden": "order_subtype",
    "Subtipo de orden": "order_subtype",

    "Mesa": "table_number",
    "No. Mesa": "table_number",
    "Comensales": "party_size",
    "No. Personas": "party_size",

    "Mesero": "server",
    "TPV": "terminal",

    "TPV Captura": "capture_terminal",
    "Terminal de captura": "capture_terminal",

    "Acción": "action",

    "Clave": "item_key",
    "Producto": "item",
    "Platillo / Artículo": "item",

    "Modificador": "modifier",

    "Tipo Grupo": "group_type",
    "Tipo de grupo": "group_type",
    "Grupo": "group",
    "Descripción": "description",

    "¿Es modificador?": "is_modifier",
    "Es modificador": "is_modifier",

    "Cantidad": "quantity",

    "Precio unitario": "unit_price",
    "Precio con modificadores": "unit_price_with_mods",
    "Precio unitario con modificador": "unit_price_with_mods",

    "Costo actual": "cost_actual",
    "Costo real": "cost_actual",
    "Costo con modificadores": "cost_with_mods",
    "Costo ideal": "cost_ideal",

    "Descuento": "discount",

    "Subtotal": "subtotal",
    "IVA": "iva",
    "IEPS": "ieps",
    "Total": "total",
}

NUMERIC_COLUMNS_BASE = {
    "quantity",
    "unit_price",
    "unit_price_with_mods",
    "cost_actual",
    "cost_with_mods",
    "cost_ideal",
    "discount",
    "subtotal_ticket",
    "iva_ticket",
    "ieps_ticket",
    "total_ticket",
    "subtotal_item",
    "iva_item",
    "ieps_item",
    "total_item",
    "subtotal_cortesia_cancel",
    "iva_cortesia_cancel",
    "ieps_cortesia_cancel",
    "total_cortesia_cancel",
    "subtotal_anulacion",
    "iva_anulacion",
    "ieps_anulacion",
    "total_anulacion",
}

EXPECTED_AMOUNT_BLOCKS = 4
EXPECTED_AMOUNT_COLS = [
    "subtotal_ticket",
    "iva_ticket",
    "ieps_ticket",
    "total_ticket",
    "subtotal_item",
    "iva_item",
    "ieps_item",
    "total_item",
    "subtotal_cortesia_cancel",
    "iva_cortesia_cancel",
    "ieps_cortesia_cancel",
    "total_cortesia_cancel",
    "subtotal_anulacion",
    "iva_anulacion",
    "ieps_anulacion",
    "total_anulacion",
]

# --------------------------- transform core ---------------------------
def normalize_headers(cols: List[str]) -> List[str]:
    """Normalize column headers to snake_case and handle duplicates.

    POS reports have duplicate column names (Subtotal, IVA, IEPS, Total
    appear 4 times for different contexts: ticket, item, cortesia_cancel,
    anulacion). This function:
    1. Maps Spanish headers to English snake_case via HEADER_MAP
    2. Identifies duplicate amount columns and suffixes them appropriately
    3. Handles pandas auto-numbering of duplicates (e.g., "Subtotal.1")

    Args:
        cols: List of raw column names from Excel.

    Returns:
        List of normalized, unique column names.

    Raises:
        Warning: If expected 4 amount blocks are not found (logged, not raised).
    """
    # Raw headers as strings
    raw = [str(c) for c in cols]
    cleaned = [strip_invisibles(c) or "" for c in raw]

    def cmp_norm(s: str) -> str:
        # Remove trailing ".1", ".2", ".3" that pandas adds to duplicate names
        s = re.sub(r"\.\d+$", "", s)
        # Then normalize whitespace and case
        s = re.sub(r"\s+", " ", s, flags=re.U)
        return s.strip().lower()


    cmp_vals = [cmp_norm(c) for c in cleaned]

    subtotal_idx = [i for i, s in enumerate(cmp_vals) if s == "subtotal"]
    iva_idx = [i for i, s in enumerate(cmp_vals) if s == "iva"]
    ieps_idx = [i for i, s in enumerate(cmp_vals) if s == "ieps"]
    total_idx = [i for i, s in enumerate(cmp_vals) if s == "total"]

    logger.debug("Raw headers: %s", raw)
    logger.debug("Cleaned headers: %s", cleaned)
    logger.debug("cmp headers: %s", cmp_vals)
    logger.debug("Subtotal positions: %s", subtotal_idx)
    logger.debug("IVA positions: %s", iva_idx)
    logger.debug("IEPS positions: %s", ieps_idx)
    logger.debug("Total positions: %s", total_idx)

    if not (
        len(subtotal_idx)
        == len(iva_idx)
        == len(ieps_idx)
        == len(total_idx)
        == EXPECTED_AMOUNT_BLOCKS
    ):
        logger.warning(
            "Expected %d Subtotal/IVA/IEPS/Total blocks "
            "(ticket, item, cortesia_cancel, anulacion) but got: "
            "subtotal=%d, iva=%d, ieps=%d, total=%d. "
            "Cleaned headers: %s",
            EXPECTED_AMOUNT_BLOCKS,
            len(subtotal_idx),
            len(iva_idx),
            len(ieps_idx),
            len(total_idx),
            cleaned,
        )

    # Define semantic block names in order of appearance
    block_labels = ["ticket", "item", "cortesia_cancel", "anulacion"]

    # Index -> final amount column name
    amount_map_by_index: Dict[int, str] = {}

    max_blocks = min(
        len(subtotal_idx), len(iva_idx), len(ieps_idx), len(total_idx), len(block_labels)
    )

    for j in range(max_blocks):
        label = block_labels[j]
        amount_map_by_index[subtotal_idx[j]] = f"subtotal_{label}"
        amount_map_by_index[iva_idx[j]] = f"iva_{label}"
        amount_map_by_index[ieps_idx[j]] = f"ieps_{label}"
        amount_map_by_index[total_idx[j]] = f"total_{label}"

    # Now build normalized names, preserving original column order
    normed: List[str] = []
    for i, c0 in enumerate(cleaned):
        if i in amount_map_by_index:
            mapped = amount_map_by_index[i]
        else:
            mapped = HEADER_MAP.get(c0, c0)
            mapped = re.sub(r"[^\w]+", "_", mapped).strip("_").lower()
            mapped = mapped or "unnamed"
        normed.append(mapped)

    # De-duplicate in case any non-amount headers collide
    seen = {}
    out = []
    for c in normed:
        if c not in seen:
            seen[c] = 1
            out.append(c)
        else:
            seen[c] += 1
            out.append(f"{c}_{seen[c]}")

    logger.debug("Normalized headers: %s", out)
    return out


def transform_detalle_ventas(xlsx_in: Path) -> pd.DataFrame:
    xls = pd.ExcelFile(xlsx_in)
    sheet = find_sheet_case_insensitive(xls, "Detalle de Ventas")
    df0 = pd.read_excel(xlsx_in, sheet_name=sheet, header=None, dtype=object)
    header_row = detect_header_row(df0)
    sucursal = parse_sucursal_from_top(df0)
    df = pd.read_excel(xlsx_in, sheet_name=sheet, header=header_row, dtype=object)

    # drop unnamed columns
    df = df.loc[:, ~df.columns.astype(str).str.startswith("Unnamed")]

    # clean text and neutralize formulas for object columns
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].map(strip_invisibles).map(neutralize_formula_injection)

    # rename headers
    df.columns = normalize_headers([str(c) for c in df.columns])

    # sanity check on amount columns
    missing_amount_cols = [c for c in EXPECTED_AMOUNT_COLS if c not in df.columns]
    if missing_amount_cols:
        logger.warning(
            "Missing expected amount columns in %s: %s. Actual normalized headers: %s",
            xlsx_in,
            ", ".join(missing_amount_cols),
            list(df.columns),
        )

    # add sucursal column (front)
    df.insert(0, "sucursal", strip_invisibles(sucursal) or "")

    # coerce types
    if "operating_date" in df.columns:
        df["operating_date"] = df["operating_date"].map(to_date)

    for c in [c for c in df.columns if c in NUMERIC_COLUMNS_BASE]:
        df[c] = df[c].map(to_float)

    # is_modifier to boole-ish
    if "is_modifier" in df.columns:
        def _coerce_bool(val):
            if pd.isna(val):
                return np.nan
            s = str(val).strip().lower()
            if s in {"si", "sí", "yes", "true", "1"}:
                return True
            if s in {"no", "false", "0"}:
                return False
            try:
                f = float(s)
                return bool(int(f)) if f in (0, 1) else np.nan
            except Exception:
                return np.nan

        df["is_modifier"] = df["is_modifier"].map(_coerce_bool)

    # stable front column order; keep the rest after
    front = [
        c
        for c in [
            "sucursal",
            "operating_date",
            "day_name",
            "closing_time",
            "captured_time",
            "week_number",
            "pdv_txn_id",
            "order_id",
            "order_type",
            "order_subtype",
            "table_number",
            "party_size",
            "server",
            "terminal",
            "capture_terminal",
            "action",
            "item_key",
            "item",
            "modifier",
            "group_type",
            "group",
            "description",
            "is_modifier",
            "quantity",
            "unit_price",
            "unit_price_with_mods",
            "cost_actual",
            "cost_with_mods",
            "cost_ideal",
            "discount",
            "subtotal_ticket",
            "iva_ticket",
            "ieps_ticket",
            "total_ticket",
            "subtotal_item",
            "iva_item",
            "ieps_item",
            "total_item",
            "subtotal_cortesia_cancel",
            "iva_cortesia_cancel",
            "ieps_cortesia_cancel",
            "total_cortesia_cancel",
            "subtotal_anulacion",
            "iva_anulacion",
            "ieps_anulacion",
            "total_anulacion",
        ]
        if c in df.columns
    ]
    rest = [c for c in df.columns if c not in front]
    return df[front + rest]

# --------------------------- CLI ---------------------------
@dataclass
class Args:
    input: Optional[Path]
    input_dir: Optional[Path]
    outdir: Path
    recursive: bool
    quiet: bool

def parse_args() -> Args:
    p = argparse.ArgumentParser(
        description="Clean POS 'Detalle de Ventas' Excel into CSV"
    )
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--input", type=Path, help="Single .xlsx file")
    g.add_argument("--input-dir", type=Path, help="Folder with .xlsx files")
    p.add_argument(
        "--outdir",
        type=Path,
        default=Path.cwd(),
        help="Where to write CSVs",
    )
    p.add_argument("--recursive", action="store_true", help="Recurse input-dir")
    p.add_argument("--quiet", action="store_true", help="Less logging")
    a = p.parse_args()
    return Args(
        input=a.input,
        input_dir=a.input_dir,
        outdir=a.outdir,
        recursive=a.recursive,
        quiet=a.quiet,
    )

def output_name_for(xlsx_path: Path, df: pd.DataFrame) -> Path:
    suc = (df["sucursal"].iloc[0] if "sucursal" in df.columns and len(df) else "") or "unknown"
    # Try to include a date span if present (best effort)
    if "operating_date" in df.columns and df["operating_date"].notna().any():
        dates = sorted(set(pd.to_datetime(df["operating_date"].dropna()).dt.date))
        if dates:
            start, end = min(dates), max(dates)
            base = f"detail_{slugify(suc)}_{start.isoformat()}_{end.isoformat()}.csv"
        else:
            base = f"detail_{slugify(suc)}.csv"
    else:
        base = f"detail_{slugify(suc)}.csv"
    return Path(base)

def write_csv(df: pd.DataFrame, out_path: Path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    # safe CSV writer; avoid excel danger by ensuring text was neutralized already
    df.to_csv(out_path, index=False, encoding="utf-8", quoting=csv.QUOTE_MINIMAL)

def run_single(xlsx: Path, outdir: Path) -> Path:
    logger.info("Processing %s", xlsx)
    df = transform_detalle_ventas(xlsx)
    out_name = output_name_for(xlsx, df)
    out_path = outdir / out_name
    write_csv(df, out_path)
    logger.info(
        "Wrote %s (%d rows, %d cols)", out_path, len(df), len(df.columns)
    )
    return out_path

def iter_xlsx_files(root: Path, recursive: bool) -> Iterable[Path]:
    if recursive:
        yield from (p for p in root.rglob("*.xlsx") if p.is_file())
    else:
        yield from (p for p in root.glob("*.xlsx") if p.is_file())

def main():
    args = parse_args()
    logging.basicConfig(
        level=logging.WARNING if args.quiet else logging.INFO,
        format="%(levelname)s: %(message)s",
    )

    if args.input:
        if not args.input.exists():
            raise SystemExit(f"Input file not found: {args.input}")
        run_single(args.input, args.outdir)
        return

    if not args.input_dir or not args.input_dir.exists():
        raise SystemExit(f"Input dir not found: {args.input_dir}")

    any_found = False
    for x in iter_xlsx_files(args.input_dir, args.recursive):
        any_found = True
        try:
            run_single(x, args.outdir)
        except Exception as e:
            logger.error("Failed on %s: %s", x, e)

    if not any_found:
        logger.warning("No .xlsx files found under %s", args.input_dir)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
