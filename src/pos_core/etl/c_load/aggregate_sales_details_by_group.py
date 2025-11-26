#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""Aggregate POS sales details by consolidated category (CLI + library)

Overview
--------
This tool reads ticket-wise CSV exports (from aggregate_sales_details_by_ticket.py),
maps the raw group names (from column names like CAFE_subtotal) to a smaller set
of consolidated categories (`Grupo_Nuevo`), sums subtotals, and outputs a pivot
where rows are categories and columns are `sucursal` (branch). Any unmapped group
values are normalized and defaulted to **"EXTRAS y MISC"** so revenue is not lost.

Key columns expected in the input CSV(s):
- `{GROUP}_subtotal` : numeric subtotal columns for each group
  (e.g., CAFE_subtotal, DESAYUNO_subtotal)
- `sucursal`         : (optional) branch name; if missing, a single TOTAL column is written

Note: This now expects ticket-wise CSV format (output of aggregate_sales_details_by_ticket.py),
not the original item-wise format.

Normalization rules:
- Accents are removed; text is uppercased; repeated whitespace is collapsed.
- A single dot `.` with no comma is treated as a decimal (non‑European assumption).
- Multiple dots that match r'\d{1,3}(?:\.\d{3})+' are treated as thousand separators.

Install
-------
Requires Python 3.10+ and pandas:
    pip install pandas

Usage (CLI)
-----------
Basic:
    python aggregate_sales_details_by_group.py -i /path/to/detail_*.csv -o ./Aggregate.csv

Exclude modifiers:
    python aggregate_sales_details_by_group.py -i detail.csv -o Aggregate.csv --no-modifiers

Multiple inputs + show unmapped:
    python aggregate_sales_details_by_group.py \
        -i "detail_2025-09-*.csv" other.csv -o Aggregate.csv --print-unmapped

Batch (folder):
    python aggregate_sales_details_by_group.py \
        --input-dir ./csv --recursive --pattern "*.csv" -o Aggregate.csv

Options:
    -i, --input           One or more input CSV files or globs (default: module's INPUT_CSV)
    -o, --output          Output CSV path (default: module's OUTPUT_CSV)
    --no-modifiers        Exclude rows where `is_modifier == True` (if column exists)
    --print-unmapped      Print normalized raw `group` keys that are not in the mapping

Exit codes:
    0 on success
    2 on schema/argument errors

Library usage
-------------
You can also import and call:

    from aggregate_sales_details_by_group import build_category_pivot
    pivot_df = build_category_pivot(input_csv, output_csv)

This keeps backward compatibility with existing code that imports the constants
and the function defined in this module.

"""

# panem_sales_agg.py
# Python 3.10+
from __future__ import annotations

import argparse
import glob
import logging
import re
import sys
import unicodedata
from pathlib import Path
from typing import Dict, List, Sequence

import pandas as pd

logger = logging.getLogger(__name__)

# ---------- config (kept for backwards-compat with existing imports) ----------
INPUT_CSV = r"detail_Panem-Credi-Club_2025-09-08_2025-09-13.csv"  # default example
OUTPUT_CSV = r"Aggregate-Credi-Club_2025-09-08_2025-09-13.csv"
INCLUDE_MODIFIERS = True  # set False if you want to exclude rows where is_modifier==True

# preferred output row order (what the report expects)
ROW_ORDER: List[str] = [
    "JUGOS Y BEBIDAS FRIAS",
    "CAFE Y  BEBIDAS CALIENTES",
    "DESAYUNOS",
    "COMIDAS",
    "PIZZA",
    "REPOSTERIA",
    "PAN DULCE",
    "PAN SALADO",
    "PRODUCTOS DE TEMPORADA",
    "EXTRAS y MISC",
]

# mapping from raw "group" -> consolidated "Grupo_Nuevo"
RAW_MAP = {
    "CAFE Y  BEBIDAS CALIENTES": "CAFE Y  BEBIDAS CALIENTES",
    " COMIDAS ": "COMIDAS",
    " DESAYUNOS ": "DESAYUNOS",
    "ESPECIALES-": "EXTRAS y MISC",
    "ESTANTERIA": "EXTRAS y MISC",
    " EXTRAS ": "EXTRAS y MISC",
    " JUGOS Y BEBIDAS FRIAS ": "JUGOS Y BEBIDAS FRIAS",
    "PAN DULCE": "PAN DULCE",
    " PAN SALADO ": "PAN SALADO",
    "PANEM  MARKETPLACE": "EXTRAS y MISC",
    " PIZZA ": "PIZZA",
    " PRODUCTOS DE TEMPORADA ": "PRODUCTOS DE TEMPORADA",
    "RAPPI CAFE Y BEBIDAS CALIENTES": "CAFE Y  BEBIDAS CALIENTES",
    "RAPPI COMIDAS": "COMIDAS",
    "RAPPI DESAYUNOS": "DESAYUNOS",
    "RAPPI JUGOS Y BEBIDAS FRIAS": "JUGOS Y BEBIDAS FRIAS",
    "RAPPI PAN DULCE": "PAN DULCE",
    "RAPPI PAN SALADO": "PAN SALADO",
    "RAPPI PIZZA": "PIZZA",
    "RAPPI REPOSTERIA": "REPOSTERIA",
    " REPOSTERIA ": "REPOSTERIA",
    " SUBSIDIO ": "EXTRAS y MISC",
    " UBER CAFE Y BEBIDAS CALIENTES ": "CAFE Y  BEBIDAS CALIENTES",
    "UBER COMIDAS": "COMIDAS",
    " UBER DESAYUNOS ": "DESAYUNOS",
    "UBER JUGOS Y BEBIDAS FRIAS": "JUGOS Y BEBIDAS FRIAS",
    " UBER PAN DULCE ": "PAN DULCE",
    "UBER PAN SALADO": "PAN SALADO",
    "UBER PIZZA": "PIZZA",
    "UBER REPOSTERIA": "REPOSTERIA",
    # sensible extras commonly found in exports:
    "MOD ALIMENTOS": "EXTRAS y MISC",
    "MOD BEBIDAS": "EXTRAS y MISC",
    "NATIVA TEMPORALCOMIDA": "PRODUCTOS DE TEMPORADA",
}


# ---------- helpers ----------
def _strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")


def _normalize_key(s: str) -> str:
    s = str(s).replace("\xa0", " ")
    s = _strip_accents(s).upper()
    s = re.sub(r"\s+", " ", s).strip()
    return s


CATEGORY_MAP: Dict[str, str] = {_normalize_key(k): v for k, v in RAW_MAP.items()}


def _read_any(paths: Sequence[str]) -> pd.DataFrame:
    """Read one or many CSVs (supports globs)."""
    files: list[str] = []
    for p in paths:
        files.extend(sorted(glob.glob(p)))
    if not files:
        raise FileNotFoundError(f"No input files matched: {paths!r}")
    dfs = [pd.read_csv(f, encoding="utf-8", low_memory=False) for f in files]
    if len(dfs) == 1:
        return dfs[0]
    return pd.concat(dfs, ignore_index=True)


# ---------- core ----------
def build_category_pivot(
    input_csv: str, output_csv: str, include_modifiers: bool | None = None, verbose: bool = False
) -> pd.DataFrame:
    """
    Build pivot of group subtotals by Grupo_Nuevo (rows) × sucursal (columns).
    - Reads ticket-wise CSV with {GROUP}_subtotal columns
    - Maps raw group names to consolidated categories
    - Respects ROW_ORDER; any extra categories appear at the bottom.
    - include_modifiers parameter is kept for API compatibility but not used
      (ticket CSV doesn't have modifiers)
    """
    include_modifiers = INCLUDE_MODIFIERS if include_modifiers is None else include_modifiers

    df = _read_any([input_csv])  # supports globs

    if verbose:
        logger.info(f"Loaded DataFrame: {len(df)} rows, {len(df.columns)} columns")
        logger.debug(f"Columns: {list(df.columns)[:20]}...")

    col = {c.lower(): c for c in df.columns}
    sucursal_col = col.get("sucursal")

    # Find all columns ending with _subtotal
    subtotal_cols = [c for c in df.columns if c.endswith("_subtotal")]

    if not subtotal_cols:
        available_cols = [c for c in df.columns if "subtotal" in c.lower() or "total" in c.lower()]
        error_msg = (
            f"Expected columns ending with '_subtotal' (e.g., CAFE_subtotal, DESAYUNO_subtotal) "
            f"in the input CSV. This function expects ticket-wise CSV format.\n"
            f"Found {len(df.columns)} columns total.\n"
        )
        if available_cols:
            error_msg += f"Columns containing 'subtotal' or 'total': {available_cols[:10]}...\n"
        error_msg += f"All columns: {list(df.columns)[:20]}..."
        raise ValueError(error_msg)

    if verbose:
        logger.info(f"Found {len(subtotal_cols)} columns ending with '_subtotal'")
        logger.debug(f"Subtotal columns (first 10): {subtotal_cols[:10]}")

    # Extract group names from column names and map to Grupo_Nuevo
    # Column names are like "CAFE_subtotal" or "DESAYUNO_subtotal"
    group_to_grupo_nuevo = {}
    unmapped_groups = set()
    mapping_details = []  # For verbose output

    for col_name in subtotal_cols:
        # Remove _subtotal suffix to get group name
        # Use removesuffix to safely remove the suffix (works in Python 3.9+)
        # Fallback to string slicing if removesuffix not available
        if hasattr(str, "removesuffix"):
            group_name = col_name.removesuffix("_subtotal")
        else:
            # _subtotal is 9 characters, not 10!
            if col_name.endswith("_subtotal"):
                group_name = col_name[:-9]  # Remove "_subtotal" (9 chars)
            else:
                group_name = col_name  # Shouldn't happen, but be safe
        # Convert underscores back to spaces
        # (since _sanitize_group_name converts spaces to underscores)
        # This allows us to match against the original group names in CATEGORY_MAP
        group_name_with_spaces = group_name.replace("_", " ")
        normalized = _normalize_key(group_name_with_spaces)

        # Try to find a close match in CATEGORY_MAP if exact match fails
        grupo_nuevo = CATEGORY_MAP.get(normalized, "EXTRAS y MISC")

        if grupo_nuevo == "EXTRAS y MISC" and normalized not in CATEGORY_MAP:
            unmapped_groups.add(normalized)

        group_to_grupo_nuevo[col_name] = grupo_nuevo

        if verbose:
            mapping_details.append(
                {
                    "column": col_name,
                    "group_name": group_name,
                    "with_spaces": group_name_with_spaces,
                    "normalized": normalized,
                    "mapped_to": grupo_nuevo,
                    "is_mapped": (normalized in CATEGORY_MAP),
                }
            )

    if verbose:
        logger.info("Group name mapping results:")
        logger.info(f"  Total columns: {len(subtotal_cols)}")
        logger.info(
            f"  Mapped to categories: {len([m for m in mapping_details if m['is_mapped']])}"
        )
        logger.info(f"  Unmapped (EXTRAS y MISC): {len(unmapped_groups)}")
        logger.debug("Sample mappings (first 10):")
        for m in mapping_details[:10]:
            status = "✓" if m["is_mapped"] else "✗ UNMAPPED"
            logger.debug(f"  {status} '{m['column']}' -> '{m['normalized']}' -> '{m['mapped_to']}'")

        if unmapped_groups:
            logger.warning(f"Unmapped groups (will go to EXTRAS y MISC): {sorted(unmapped_groups)}")
            logger.debug("Available CATEGORY_MAP keys (first 20):")
            logger.debug(f"  {sorted(list(CATEGORY_MAP.keys()))[:20]}")

    # Always report all groups that were mapped to EXTRAS y MISC
    extras_misc_groups = []
    for col_name, grupo_nuevo in group_to_grupo_nuevo.items():
        if grupo_nuevo == "EXTRAS y MISC":
            # Extract info for this column
            if hasattr(str, "removesuffix"):
                group_name = col_name.removesuffix("_subtotal")
            else:
                group_name = col_name[:-9] if col_name.endswith("_subtotal") else col_name
            group_name_with_spaces = group_name.replace("_", " ")
            normalized = _normalize_key(group_name_with_spaces)
            extras_misc_groups.append(
                {
                    "column": col_name,
                    "normalized": normalized,
                    "group_name": group_name,
                    "with_spaces": group_name_with_spaces,
                }
            )

    if extras_misc_groups:
        logger.warning(f"Groups mapped to EXTRAS y MISC ({len(extras_misc_groups)} total):")
        for item in extras_misc_groups:
            logger.warning(
                f"  Column: '{item['column']}' -> Normalized: '{item['normalized']}' "
                f"-> EXTRAS y MISC"
            )
            if verbose:
                logger.debug(f"    Original group name: '{item['group_name']}'")
                logger.debug(f"    With spaces: '{item['with_spaces']}'")

    # Melt the dataframe to convert {GROUP}_subtotal columns into rows
    # This creates: order_id, sucursal, group_column, value
    id_vars = [c for c in df.columns if not c.endswith("_subtotal") and not c.endswith("_total")]

    if verbose:
        logger.info(
            f"Melting DataFrame: {len(id_vars)} id columns, {len(subtotal_cols)} value columns"
        )
        logger.debug(f"ID columns: {id_vars[:10]}...")

    melted = df.melt(
        id_vars=id_vars,
        value_vars=subtotal_cols,
        var_name="_group_column",
        value_name="subtotal_value",
    )

    if verbose:
        logger.info(f"After melting: {len(melted)} rows")
        logger.debug(f"Melted columns: {list(melted.columns)}")
        logger.debug(f"Sample melted data:\n{melted.head(10)}")

    # Map group columns to Grupo_Nuevo
    melted["Grupo_Nuevo"] = melted["_group_column"].map(group_to_grupo_nuevo)

    if verbose:
        grupo_counts = melted["Grupo_Nuevo"].value_counts()
        logger.info("Grupo_Nuevo distribution:")
        for grupo, count in grupo_counts.items():
            logger.info(f"  {grupo}: {count} rows")
        # Check for unmapped
        unmapped_count = (melted["Grupo_Nuevo"] == "EXTRAS y MISC").sum()
        if unmapped_count > 0:
            pct = unmapped_count / len(melted) * 100
            logger.warning(f"  EXTRAS y MISC: {unmapped_count} rows ({pct:.1f}%)")

    # Aggregate by Grupo_Nuevo and sucursal
    grp_cols = ["Grupo_Nuevo"] + ([sucursal_col] if sucursal_col else [])

    if verbose:
        logger.info(f"Aggregating by: {grp_cols}")
        if sucursal_col:
            unique_sucursales = melted[sucursal_col].nunique()
            logger.info(f"  Found {unique_sucursales} unique sucursales")

    agg = melted.groupby(grp_cols, dropna=False)["subtotal_value"].sum().reset_index()

    if verbose:
        logger.info(f"After aggregation: {len(agg)} category-sucursal combinations")
        logger.debug(f"Sample aggregated data:\n{agg.head(10)}")

    # Preferred sucursal column order (keywords to match in column names)
    preferred_sucursal_order = [
        "Kavia",
        "Punto Valle",
        "QIN",  # Note: QIN not Qin
        "Zambrano",
        "Carreta",
        "Nativa",
        "Credi Club",  # Note: "Credi Club" not "Crediclub"
    ]

    if sucursal_col:
        out = agg.pivot(index="Grupo_Nuevo", columns=sucursal_col, values="subtotal_value").fillna(
            0.0
        )
        if verbose:
            logger.info(f"Pivot table: {len(out)} categories × {len(out.columns)} sucursales")
            logger.debug(f"Original sucursal columns: {list(out.columns)}")

        # Reorder columns to match preferred order (case-insensitive, partial matching)
        # Match based on keywords in the column names
        # (e.g., "Kavia" matches "Panem - Hotel Kavia N")
        col_list = list(out.columns)
        ordered_cols = []
        matched_indices = set()

        # Add columns in preferred order by finding matching column names
        for preferred in preferred_sucursal_order:
            preferred_lower = preferred.lower()
            # Find the first column that contains this keyword (case-insensitive)
            for idx, col in enumerate(col_list):
                if idx not in matched_indices:
                    col_lower = str(col).lower()
                    if preferred_lower in col_lower:
                        ordered_cols.append(col)
                        matched_indices.add(idx)
                        if verbose:
                            logger.debug(f"  Matched '{preferred}' -> '{col}'")
                        break

        # Add any remaining columns that weren't matched
        for idx, col in enumerate(col_list):
            if idx not in matched_indices:
                ordered_cols.append(col)

        # Reorder the dataframe columns
        out = out[ordered_cols]

        if verbose:
            logger.info(f"Reordered sucursal columns to: {list(out.columns)}")
    else:
        out = agg.set_index("Grupo_Nuevo")[["subtotal_value"]].rename(
            columns={"subtotal_value": "TOTAL"}
        )
        if verbose:
            logger.info(f"Output table: {len(out)} categories (no sucursal breakdown)")

    extras = [r for r in out.index if r not in ROW_ORDER]
    out = out.reindex(ROW_ORDER + extras).fillna(0.0).round(2)

    if verbose:
        logger.info(
            f"Final output: {len(out)} categories "
            f"(ordered: {len(ROW_ORDER)} standard + {len(extras)} extras)"
        )
        if extras:
            logger.debug(f"Extra categories: {extras}")

    out.to_csv(output_csv, encoding="utf-8")
    return out


# ---------- CLI ----------


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="aggregate-sales-by-group",
        description=(
            "Aggregate POS sales details (from ticket-wise CSV with {GROUP}_subtotal columns) "
            "by consolidated category (Grupo_Nuevo), pivoted per sucursal."
        ),
    )
    p.add_argument(
        "-i",
        "--input",
        nargs="+",
        default=[INPUT_CSV],
        help="Input CSV file(s) or glob(s). Defaults to the module's INPUT_CSV.",
    )
    p.add_argument(
        "-o",
        "--output",
        default=OUTPUT_CSV,
        help="Output CSV path (pivot). Defaults to the module's OUTPUT_CSV.",
    )
    p.add_argument(
        "--input-dir",
        default=None,
        help="Directory containing input CSVs. If set, all files matching --pattern are included.",
    )
    p.add_argument(
        "--recursive",
        action="store_true",
        help="When using --input-dir, search subdirectories recursively.",
    )
    p.add_argument(
        "--pattern",
        default="*.csv",
        help="Glob pattern for files under --input-dir (default: *.csv).",
    )
    p.add_argument(
        "--no-modifiers",
        action="store_true",
        help="[Deprecated] Not used with ticket-wise CSV format. Kept for API compatibility.",
    )
    p.add_argument(
        "--print-unmapped",
        action="store_true",
        help="Also print any unmapped raw 'group' values detected.",
    )
    p.add_argument(
        "--verbose",
        "--debug",
        action="store_true",
        dest="verbose",
        help="Verbose/debug logging output with detailed troubleshooting information.",
    )
    return p


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    # Set up logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(levelname)s: %(message)s",
    )

    # Resolve inputs: explicit files/globs plus optional --input-dir scan
    file_specs = list(args.input)
    if args.input_dir:
        base = args.input_dir
        patt = args.pattern
        if args.recursive:
            file_specs.append(str(Path(base) / "**" / patt))
        else:
            file_specs.append(str(Path(base) / patt))
    # Read and aggregate

    df = _read_any(file_specs)
    # Quick pass to discover unmapped groups from column names for optional logging
    subtotal_cols = [c for c in df.columns if c.endswith("_subtotal")]
    if not subtotal_cols:
        print(
            "ERROR: No columns ending with '_subtotal' found in input. "
            "Expected ticket-wise CSV format.",
            file=sys.stderr,
        )
        return 2

    # Extract and normalize group names from column names
    # Note: Need to convert underscores back to spaces before normalizing
    # _subtotal is 9 characters, not 10!
    if hasattr(str, "removesuffix"):
        group_names = [
            _normalize_key(col.removesuffix("_subtotal").replace("_", " ")) for col in subtotal_cols
        ]
    else:
        group_names = [
            _normalize_key((col[:-9] if col.endswith("_subtotal") else col).replace("_", " "))
            for col in subtotal_cols
        ]
    unmapped = sorted(set(group_names) - set(CATEGORY_MAP.keys()))

    if args.verbose:
        logger.info(f"Extracted {len(group_names)} group names from column names")
        logger.debug(f"Sample group names (first 10): {group_names[:10]}")
        logger.debug(f"Unmapped groups: {unmapped}")

    # Save a temp combined file and then call core to reuse code-path
    # (we could pass df directly, but we keep API stable)
    tmp_path = file_specs[0] if file_specs else INPUT_CSV
    if len(file_specs) > 1 or any(("*" in p or "?" in p) for p in file_specs):
        # Compose a single frame and write to a temp CSV in memory-fs-ish path
        tmp_path = args.output + ".__tmp_combined.csv"
        df.to_csv(tmp_path, index=False, encoding="utf-8")

    pivot = build_category_pivot(
        tmp_path, args.output, include_modifiers=(not args.no_modifiers), verbose=args.verbose
    )
    print(f"Wrote: {args.output}")
    # Pretty print
    with pd.option_context("display.float_format", lambda v: f"{v:,.2f}"):
        print(pivot)

    if args.print_unmapped and unmapped:
        print("\nUnmapped raw 'group' keys (normalized):")
        for g in unmapped:
            print(" -", g)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
