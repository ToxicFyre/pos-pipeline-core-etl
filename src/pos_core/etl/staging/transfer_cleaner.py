r"""Staging (Silver) layer: POS transfer report cleaner.

This module is part of the Staging (Silver) layer in the ETL pipeline.
It transforms raw transfer Excel files into clean, normalized CSV files.

Data directory mapping:
    Input: data/a_raw/ → Raw (Bronze) layer
    Output: data/b_clean/ → Staging (Silver) layer

POS transfer report cleaner -> normalized CSV.

This module cleans and normalizes POS inventory transfer Excel reports
(Inventory ▸ Transfers ▸ Issued) into standardized CSV format.

The cleaning process:
1. Detects the correct sheet (typically "Transferencias")
2. Identifies header row using known column tokens
3. Normalizes column names to snake_case
4. Maps columns to standard field names
5. Coerces data types (numbers, dates)
6. Calculates derived fields (unit costs, totals)

Examples:
    Single file:
        python -m pos_etl.b_transform.pos_excel_transfer_cleaner \\
            --input ./downloads/TransfersIssued_crediclub_2025-09-08_2025-09-14.xlsx

    Batch (folder):
        python -m pos_etl.b_transform.pos_excel_transfer_cleaner \\
            --input-dir ./downloads --recursive --outdir ./csv

"""

import csv
import re
import unicodedata
from pathlib import Path

import numpy as np
import pandas as pd

from .cleaning_utils import neutralize, strip_invisibles, to_float, to_snake, uniquify

NBSP = "\u00a0"
NNBSP = "\u202f"
DANGEROUS_PREFIXES = ("=", "+", "@", "-")

# ---------- small helpers ----------


def remove_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")


# Robust numeric parsing
CURRENCY_RE = re.compile(r"[^\d,.\-\(\)\s]")


# ---------- header detection ----------
KNOWN_HEADER_TOKENS = {
    "orden",
    "sucursal_origen",
    "almacen_origen",
    "sucursal_destino",
    "almacen_destino",
    "descripcion",
    "fecha",
    "estatus",
    "emisor",
    "receptor",
    "costo",
    "costo_con_margen",
    "ieps",
    "iva",
    "costo_total_con_margen",
    "cantidad",
    "departamento",
    "clave",
    "producto",
    "presentacion",
}


def detect_header_row(df_no_header: pd.DataFrame, scan: int = 40) -> int:
    """Detect header row by matching known column tokens.

    Scores each row by counting how many known transfer report column names
    (normalized to snake_case) appear in that row. Returns the row with
    the highest score.

    Args:
        df_no_header: DataFrame parsed without headers.
        scan: Maximum number of rows to scan. Defaults to 40.

    Returns:
        Row index (0-based) with the best match.

    """
    best_row, best_score = 0, -1
    for i in range(min(scan, len(df_no_header))):
        row_vals = [strip_invisibles(x) or "" for x in df_no_header.iloc[i].tolist()]
        norm = {to_snake(v) for v in row_vals if v}
        score = len(norm & KNOWN_HEADER_TOKENS)
        if score > best_score:
            best_row, best_score = i, score
    return best_row


# ---------- core ----------
def clean_to_minimal_csv(input_path: Path, output_csv: Path) -> Path:
    """Clean a POS transfer Excel file and save as normalized CSV.

    Processes the Excel file through the following steps:
    1. Detects the correct sheet (prefers "Transferencias")
    2. Identifies header row using known column tokens
    3. Normalizes column names to snake_case
    4. Maps columns to standard field names
    5. Coerces numeric fields (cantidad, costos, taxes)
    6. Calculates derived fields (unit costs, tax totals)
    7. Neutralizes formula injection in text fields
    8. Saves minimal CSV with standardized column names

    Args:
        input_path: Path to input Excel file.
        output_csv: Path where cleaned CSV will be saved.

    Returns:
        Path to the output CSV file.

    Raises:
        RuntimeError: If required columns are missing from the Excel file.

    """
    xls = pd.ExcelFile(input_path)
    sheet_name = "Transferencias" if "Transferencias" in xls.sheet_names else xls.sheet_names[0]

    df0 = xls.parse(sheet_name=sheet_name, header=None, dtype=object)
    header_row = detect_header_row(df0, scan=40)

    df_raw = xls.parse(sheet_name=sheet_name, header=header_row, dtype=object)
    df_raw = df_raw.dropna(axis=1, how="all")
    if (
        df_raw.columns.size
        and str(df_raw.columns[0]).startswith("Unnamed")
        and df_raw.iloc[:, 0].isna().all()
    ):
        df_raw = df_raw.iloc[:, 1:]
    df_raw = df_raw.dropna(axis=0, how="all")
    if "Orden" in df_raw.columns:
        df_raw = df_raw[df_raw["Orden"].apply(lambda x: (strip_invisibles(x) or "") != "")]

    df_raw.columns = uniquify([to_snake(c) for c in df_raw.columns])

    def pick(*cands: str) -> str | None:
        for cand in cands:
            if cand and cand in df_raw.columns:
                return cand
        return None

    col_map = {
        "orden": pick("orden"),
        "almacen_origen": pick("almacen_origen"),
        "sucursal_destino": pick("sucursal_destino"),
        "almacen_destino": pick("almacen_destino"),
        "fecha": pick("fecha"),
        "estatus": pick("estatus"),
        "cantidad": pick("cantidad"),
        "departamento": pick("departamento"),
        "clave": pick("clave"),
        "producto": pick("producto"),
        "presentacion": pick("presentacion"),
        "costo_ext": pick("costo.1", "costo"),
        "iva_unit": pick("iva.1", "iva_1"),
        "ieps_unit": pick("ieps.1", "ieps_1"),
    }

    required = [
        "orden",
        "almacen_origen",
        "sucursal_destino",
        "almacen_destino",
        "fecha",
        "estatus",
        "cantidad",
        "departamento",
        "clave",
        "producto",
        "presentacion",
    ]
    missing = [k for k in required if col_map[k] is None]
    if missing:
        raise RuntimeError(
            f"Missing required columns: {missing}. Available: {list(df_raw.columns)}"
        )

    df = pd.DataFrame({k: df_raw[v] if v is not None else None for k, v in col_map.items()})

    df["cantidad"] = df["cantidad"].map(to_float)
    for c in ("costo_ext", "iva_unit", "ieps_unit"):
        if c in df.columns:
            df[c] = df[c].map(to_float)

    df["ieps_total"] = df["cantidad"] * df["ieps_unit"] if "ieps_unit" in df.columns else np.nan
    df["iva_total"] = df["cantidad"] * df["iva_unit"] if "iva_unit" in df.columns else np.nan

    def safe_unit_cost(row: pd.Series) -> float | np.floating:
        qty = row.get("cantidad")
        costo = row.get("costo_ext")
        if qty is None or pd.isna(qty) or qty == 0 or costo is None or pd.isna(costo):
            return np.nan
        return float(costo) / float(qty)

    df["costo_unitario_calc"] = df.apply(safe_unit_cost, axis=1)

    for c in (
        "orden",
        "almacen_origen",
        "sucursal_destino",
        "almacen_destino",
        "estatus",
        "departamento",
        "clave",
        "producto",
        "presentacion",
    ):
        if c in df.columns:
            df[c] = df[c].map(neutralize)

    minimal = pd.DataFrame({
        "Orden": df["orden"],
        "Almacén origen": df["almacen_origen"],
        "Sucursal destino": df["sucursal_destino"],
        "Almacén destino": df["almacen_destino"],
        "Fecha": df["fecha"],
        "Estatus": df["estatus"],
        "Cantidad": df["cantidad"],
        "Departamento": df["departamento"],
        "Clave": df["clave"],
        "Producto": df["producto"],
        "Presentación": df["presentacion"],
        "Costo": df["costo_ext"],
        "IEPS": df["ieps_total"],
        "IVA": df["iva_total"],
        "Costo unitario": df["costo_unitario_calc"],
    })

    # Neutralize Excel formula injection on ALL object columns
    obj_cols = df.select_dtypes(include=["object"]).columns
    if len(obj_cols) > 0:
        df[obj_cols] = df[obj_cols].applymap(
            lambda v: ("'" + v) if isinstance(v, str) and v and v[0] in ("=", "+", "-", "@") else v
        )

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    minimal.to_csv(output_csv, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_MINIMAL)
    return output_csv


def main() -> None:
    import argparse

    p = argparse.ArgumentParser(description="Clean CEDIS transfers Excel to ONE minimal CSV")
    p.add_argument("input", type=str, help="Path to TransfersIssued_CEDIS_*.xlsx")
    p.add_argument("--output", type=str, default="", help="Output CSV path (optional)")
    args = p.parse_args()

    input_path = Path(args.input)
    out_csv = (
        Path(args.output)
        if args.output
        else input_path.with_name(f"{input_path.stem}_minimal_final.csv")
    )
    res = clean_to_minimal_csv(input_path, out_csv)
    print(f"Wrote {res}")


if __name__ == "__main__":
    main()
