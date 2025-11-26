#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Aggregate POS sales details by ticket (CLI + library)

Overview
--------
This tool reads one or more *POS* sales-detail CSV exports (item-wise data),
groups items by ticket (order_id), and creates a ticket-wise CSV with:
- Ticket-level metadata (sucursal, operating_date, order_id, etc.)
- Group-specific subtotal and total columns (e.g., CAFE_subtotal, CAFE_total)
- Total ticket cost

Key columns expected in the input CSV(s):
- `order_id`      : ticket identifier
- `group`         : item category/group
- `subtotal_item` : numeric line subtotal
- `total_item`    : numeric line total
- Ticket-level fields: sucursal, operating_date, day_name, closing_time,
  captured_time, week_number, pdv_txn_id, order_type, table_number,
  party_size, server, terminal

Usage (CLI)
-----------
Basic:
    python aggregate_sales_details_by_ticket.py --input-dir ./b_clean/sales -o ./sales_by_ticket.csv

With recursion:
    python aggregate_sales_details_by_ticket.py --input-dir ./b_clean/sales --recursive -o ./sales_by_ticket.csv

Single file:
    python aggregate_sales_details_by_ticket.py -i detail.csv -o sales_by_ticket.csv

Options:
    -i, --input           One or more input CSV files or globs
    --input-dir           Directory containing input CSVs
    --recursive           When using --input-dir, search subdirectories recursively
    --pattern             Glob pattern for files under --input-dir (default: *.csv)
    -o, --output          Output CSV path

Exit codes:
    0 on success
    2 on schema/argument errors

"""

from __future__ import annotations

import argparse
import csv
import glob
import logging
import sys
from pathlib import Path
from typing import Dict, List, Optional, Sequence

import pandas as pd

logger = logging.getLogger(__name__)

# ---------- helpers ----------
def _read_any(paths: Sequence[str]) -> pd.DataFrame:
    """Read one or many CSVs (supports globs, including recursive with **)."""
    files: list[str] = []
    for p in paths:
        # Check if pattern contains ** for recursive globbing
        if "**" in p:
            # Use recursive glob
            found = glob.glob(p, recursive=True)
        else:
            # Regular glob
            found = glob.glob(p)
        files.extend(sorted(found))
    if not files:
        raise FileNotFoundError(f"No input files matched: {paths!r}")
    logger.info(f"Reading {len(files)} CSV file(s)...")
    dfs = [pd.read_csv(f, encoding="utf-8", low_memory=False) for f in files]
    if len(dfs) == 1:
        return dfs[0]
    return pd.concat(dfs, ignore_index=True)


def _sanitize_group_name(group: str) -> str:
    """Convert group name to a valid column name."""
    if pd.isna(group):
        return "UNKNOWN"
    s = str(group).strip()
    # Replace problematic characters with underscores
    s = s.replace(" ", "_").replace("/", "_").replace("\\", "_")
    s = s.replace("-", "_").replace(".", "_")
    # Remove any remaining non-alphanumeric except underscore
    import re
    s = re.sub(r"[^\w]", "_", s)
    # Collapse multiple underscores
    s = re.sub(r"_+", "_", s).strip("_")
    return s.upper() if s else "UNKNOWN"


# ---------- core ----------
def aggregate_by_ticket(
    input_csv: str | Sequence[str],
    output_csv: str,
    input_dir: Optional[str] = None,
    recursive: bool = False,
    pattern: str = "*.csv",
    verbose: bool = False,
) -> pd.DataFrame:
    """
    Aggregate item-wise sales data by ticket.

    Args:
        input_csv: Input CSV file(s) or glob pattern(s)
        output_csv: Output CSV path
        input_dir: Optional directory to search for CSVs
        recursive: If True, search subdirectories recursively
        pattern: Glob pattern for files under input_dir

    Returns:
        DataFrame with one row per ticket
    """
    # Resolve input files
    file_specs: List[str] = []
    if isinstance(input_csv, str):
        file_specs = [input_csv]
    else:
        file_specs = list(input_csv)

    if input_dir:
        base = Path(input_dir)
        if recursive:
            # Use Path.rglob for recursive search (works cross-platform)
            # rglob expects a pattern like "*.csv" and searches recursively
            found_files = list(base.rglob(pattern))
            if not found_files:
                logger.warning(f"No files found matching pattern '{pattern}' in {base} (recursive)")
            file_specs.extend([str(f) for f in found_files])
        else:
            # Non-recursive: use glob on the directory
            found_files = list(base.glob(pattern))
            if not found_files:
                logger.warning(f"No files found matching pattern '{pattern}' in {base}")
            file_specs.extend([str(f) for f in found_files])

    # Read all CSVs
    df = _read_any(file_specs)
    
    if verbose:
        logger.info(f"Loaded DataFrame: {len(df)} rows, {len(df.columns)} columns")
        logger.info(f"Columns: {list(df.columns)[:10]}...")

    # Normalize column names (case-insensitive lookup)
    col_map = {c.lower(): c for c in df.columns}
    
    if verbose:
        logger.debug(f"Column mapping (first 10): {dict(list(col_map.items())[:10])}")
    
    # Required columns
    order_id_col = col_map.get("order_id")
    group_col = col_map.get("group")
    subtotal_col = col_map.get("subtotal_item")
    total_col = col_map.get("total_item")

    if not order_id_col:
        raise ValueError("Required column 'order_id' not found in input CSV(s).")
    if not group_col:
        raise ValueError("Required column 'group' not found in input CSV(s).")
    if not subtotal_col:
        raise ValueError("Required column 'subtotal_item' not found in input CSV(s).")
    if not total_col:
        raise ValueError("Required column 'total_item' not found in input CSV(s).")

    # Ticket-level fields to preserve
    ticket_fields = [
        "sucursal",
        "operating_date",
        "day_name",
        "week_number",
        "pdv_txn_id",
        "order_id",
        "order_type",
        "table_number",
        "party_size",
        "server",
        "terminal",
    ]
    
    # Special handling fields
    closing_time_col = col_map.get("closing_time")
    captured_time_col = col_map.get("captured_time")

    # Filter to only rows with valid order_id and group
    df = df[df[order_id_col].notna() & df[group_col].notna()].copy()

    if len(df) == 0:
        logger.warning("No valid rows found after filtering.")
        # Create empty output with expected structure
        empty_df = pd.DataFrame(columns=["order_id"])
        empty_df.to_csv(output_csv, index=False, encoding="utf-8")
        return empty_df

    # Get all unique groups to create columns for
    unique_groups = sorted(df[group_col].dropna().unique())
    logger.info(f"Found {len(unique_groups)} unique groups: {unique_groups[:10]}...")
    
    if verbose:
        logger.info(f"All unique groups ({len(unique_groups)}): {unique_groups}")
        logger.info(f"Group value counts (top 10):\n{df[group_col].value_counts().head(10)}")

    # Group by ticket identifier
    # order_id is only unique within the same sucursal on the same day
    # pdv_txn_id is unique overall, so prefer that if available
    groupby_cols = []
    pdv_txn_id_col = col_map.get("pdv_txn_id")
    sucursal_col = col_map.get("sucursal")
    date_col = col_map.get("operating_date")
    
    if pdv_txn_id_col and pdv_txn_id_col in df.columns and df[pdv_txn_id_col].notna().any():
        # Use pdv_txn_id if available (it's globally unique)
        logger.info("Using pdv_txn_id for grouping (globally unique)")
        groupby_cols = [pdv_txn_id_col]
        if verbose:
            unique_pdv = df[pdv_txn_id_col].nunique()
            total_rows = len(df)
            logger.info(f"  pdv_txn_id: {unique_pdv} unique values in {total_rows} rows")
    else:
        # Use order_id + sucursal + operating_date (order_id is only unique within sucursal+date)
        logger.info("Using order_id + sucursal + operating_date for grouping")
        groupby_cols = [order_id_col]
        if sucursal_col and sucursal_col in df.columns:
            groupby_cols.append(sucursal_col)
        if date_col and date_col in df.columns:
            groupby_cols.append(date_col)
        if verbose:
            unique_orders = df[order_id_col].nunique()
            if sucursal_col:
                unique_sucursal = df[sucursal_col].nunique()
                logger.info(f"  order_id: {unique_orders} unique values")
                logger.info(f"  sucursal: {unique_sucursal} unique values")
                if date_col:
                    unique_dates = df[date_col].nunique()
                    logger.info(f"  operating_date: {unique_dates} unique values")
                    # Check for duplicates
                    combo_unique = df.groupby(groupby_cols).size()
                    duplicates = combo_unique[combo_unique > 1]
                    if len(duplicates) > 0:
                        logger.warning(f"  Found {len(duplicates)} order_id combinations with multiple rows")
                        if verbose:
                            logger.debug(f"  Sample duplicates:\n{duplicates.head(10)}")

    # Aggregate subtotal_item and total_item by group within each ticket
    # Create pivot-like structure: for each ticket, sum subtotal/total per group
    if verbose:
        logger.info(f"Aggregating by: {groupby_cols + [group_col]}")
        logger.info(f"  Using columns: subtotal={subtotal_col}, total={total_col}")
    
    ticket_groups = df.groupby(groupby_cols + [group_col], dropna=False).agg({
        subtotal_col: "sum",
        total_col: "sum",
    }).reset_index()
    
    if verbose:
        logger.info(f"After grouping: {len(ticket_groups)} ticket-group combinations")
        logger.debug(f"Sample ticket_groups:\n{ticket_groups.head(10)}")

    # Pivot to create group columns
    # Handle case where there might be no groups or empty pivot
    if len(ticket_groups) == 0:
        # No groups found - create empty pivots with just the index
        # Get unique tickets from the original df
        unique_tickets = df.groupby(groupby_cols).size().reset_index(name='_count')
        unique_tickets = unique_tickets[groupby_cols]
        if len(groupby_cols) == 1:
            subtotal_pivot = pd.DataFrame(index=unique_tickets[groupby_cols[0]].values)
            total_pivot = pd.DataFrame(index=unique_tickets[groupby_cols[0]].values)
        else:
            subtotal_pivot = pd.DataFrame(index=pd.MultiIndex.from_frame(unique_tickets))
            total_pivot = pd.DataFrame(index=pd.MultiIndex.from_frame(unique_tickets))
    else:
        subtotal_pivot = ticket_groups.pivot_table(
            index=groupby_cols,
            columns=group_col,
            values=subtotal_col,
            aggfunc="sum",
            fill_value=0.0
        )
        
        total_pivot = ticket_groups.pivot_table(
            index=groupby_cols,
            columns=group_col,
            values=total_col,
            aggfunc="sum",
            fill_value=0.0
        )

        # Rename columns to {GROUP}_subtotal and {GROUP}_total
        # Note: The pivot table columns are the original group values from the data
        if verbose:
            logger.debug(f"Original group values (pivot columns) before sanitization:")
            for orig_group in list(subtotal_pivot.columns)[:10]:
                logger.debug(f"  Original: '{orig_group}' (len={len(str(orig_group))})")
                sanitized = _sanitize_group_name(orig_group)
                logger.debug(f"  Sanitized: '{sanitized}' (len={len(sanitized)})")
        
        subtotal_cols = {col: f"{_sanitize_group_name(col)}_subtotal" for col in subtotal_pivot.columns}
        total_cols = {col: f"{_sanitize_group_name(col)}_total" for col in total_pivot.columns}
        
        if verbose:
            logger.info(f"Renaming {len(subtotal_cols)} subtotal columns and {len(total_cols)} total columns")
            logger.debug(f"Sample column mappings (first 5):")
            for orig, new in list(subtotal_cols.items())[:5]:
                logger.debug(f"  '{orig}' -> '{new}'")
        
        subtotal_pivot = subtotal_pivot.rename(columns=subtotal_cols)
        total_pivot = total_pivot.rename(columns=total_cols)

    # Combine pivots (handle empty case)
    if len(subtotal_pivot.columns) > 0 or len(total_pivot.columns) > 0:
        ticket_agg = pd.concat([subtotal_pivot, total_pivot], axis=1)
    else:
        # Both pivots are empty - just use one as the base
        ticket_agg = subtotal_pivot.copy()

    # Get ticket-level metadata (first non-null value for most fields)
    # Exclude groupby columns from aggregation since they're already in the groupby
    # and will be included in the result via reset_index()
    groupby_cols_set = set(groupby_cols)
    fields_to_agg = {
        col_map.get(field): "first" 
        for field in ticket_fields 
        if col_map.get(field) and col_map.get(field) not in groupby_cols_set
    }
    if closing_time_col and closing_time_col not in groupby_cols_set:
        fields_to_agg[closing_time_col] = "max"
    if captured_time_col and captured_time_col not in groupby_cols_set:
        fields_to_agg[captured_time_col] = "min"
    
    # Handle case where all fields are in groupby_cols (empty aggregation dict)
    if verbose:
        logger.info(f"Aggregating ticket metadata with {len(fields_to_agg)} fields")
        if fields_to_agg:
            logger.debug(f"Fields to aggregate: {list(fields_to_agg.keys())}")
        else:
            logger.warning("No fields to aggregate (all are in groupby_cols)")
    
    if fields_to_agg:
        ticket_metadata = df.groupby(groupby_cols).agg(fields_to_agg).reset_index()
    else:
        # If no fields to aggregate, just get unique groupby combinations
        ticket_metadata = df.groupby(groupby_cols).size().reset_index(name='_count')
        ticket_metadata = ticket_metadata.drop(columns=['_count'])
    
    if verbose:
        logger.info(f"Ticket metadata: {len(ticket_metadata)} tickets")
        logger.debug(f"Ticket metadata columns: {list(ticket_metadata.columns)}")

    # Reset index on ticket_agg and merge with metadata
    # The index contains the groupby columns, so reset_index() will add them as columns
    ticket_agg_reset = ticket_agg.reset_index()
    
    if verbose:
        logger.info(f"Ticket aggregation: {len(ticket_agg_reset)} tickets")
        logger.info(f"  Group columns created: {len([c for c in ticket_agg_reset.columns if c.endswith('_subtotal')])} subtotals, "
                   f"{len([c for c in ticket_agg_reset.columns if c.endswith('_total')])} totals")
        logger.debug(f"  Ticket agg columns: {list(ticket_agg_reset.columns)[:15]}...")
        logger.debug(f"  Merging on: {groupby_cols}")
    
    # Merge on groupby columns, but avoid duplicate columns
    # ticket_metadata already has the groupby columns, and ticket_agg_reset will have them from reset_index()
    # Use suffixes to handle any conflicts, but there shouldn't be any since we're merging on the same columns
    result = ticket_metadata.merge(ticket_agg_reset, on=groupby_cols, how="left", suffixes=("", "_agg"))
    
    if verbose:
        logger.info(f"After merge: {len(result)} tickets, {len(result.columns)} columns")
        if len(result) != len(ticket_metadata):
            logger.warning(f"Merge changed row count: {len(ticket_metadata)} -> {len(result)}")
    
    # Drop any duplicate columns created by the merge (shouldn't happen, but just in case)
    result = result.loc[:, ~result.columns.duplicated()]
    
    if verbose and result.columns.duplicated().any():
        logger.warning(f"Found duplicate columns after merge: {result.columns[result.columns.duplicated()].tolist()}")

    # Calculate total ticket cost (sum of all total_item values)
    # This is the sum of all {GROUP}_total columns (exclude total_ticket_cost itself)
    total_cols_list = [col for col in result.columns if col.endswith("_total") and col != "total_ticket_cost"]
    if total_cols_list:
        # Fill NaN values with 0 before summing to avoid issues
        result["total_ticket_cost"] = result[total_cols_list].fillna(0.0).sum(axis=1)
    else:
        result["total_ticket_cost"] = 0.0

    # Reorder columns: ticket metadata first, then group columns, then total
    metadata_cols = [c for c in ticket_fields if c in result.columns]
    if closing_time_col and closing_time_col in result.columns:
        metadata_cols.append(closing_time_col)
    if captured_time_col and captured_time_col in result.columns:
        metadata_cols.append(captured_time_col)
    
    # Sort group columns: subtotals first, then totals, both alphabetically
    group_subtotal_cols = sorted([c for c in result.columns if c.endswith("_subtotal")])
    group_total_cols = sorted([c for c in result.columns if c.endswith("_total")])
    
    final_cols = metadata_cols + group_subtotal_cols + group_total_cols + ["total_ticket_cost"]
    # Add any remaining columns
    remaining_cols = [c for c in result.columns if c not in final_cols]
    final_cols = final_cols + remaining_cols

    result = result[final_cols]

    # Write output
    output_path = Path(output_csv)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(output_path, index=False, encoding="utf-8")
    logger.info(f"Wrote {len(result)} tickets to {output_path}")

    return result


# ---------- CLI ----------
def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="aggregate-sales-by-ticket",
        description="Aggregate POS sales details (item-wise) into ticket-wise CSV with group columns."
    )
    p.add_argument(
        "-i", "--input",
        nargs="+",
        default=None,
        help="Input CSV file(s) or glob(s)."
    )
    p.add_argument(
        "--input-dir",
        default=None,
        help="Directory containing input CSVs. If set, all files matching --pattern are included."
    )
    p.add_argument(
        "--recursive",
        action="store_true",
        help="When using --input-dir, search subdirectories recursively."
    )
    p.add_argument(
        "--pattern",
        default="*.csv",
        help="Glob pattern for files under --input-dir (default: *.csv)."
    )
    p.add_argument(
        "-o", "--output",
        required=True,
        help="Output CSV path."
    )
    p.add_argument(
        "--quiet",
        action="store_true",
        help="Less logging output."
    )
    p.add_argument(
        "--verbose", "--debug",
        action="store_true",
        dest="verbose",
        help="Verbose/debug logging output with detailed troubleshooting information."
    )
    return p


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    log_level = logging.DEBUG if args.verbose else (logging.WARNING if args.quiet else logging.INFO)
    logging.basicConfig(
        level=log_level,
        format="%(levelname)s: %(message)s",
    )

    if not args.input and not args.input_dir:
        print("ERROR: Must provide either --input or --input-dir", file=sys.stderr)
        return 2

    try:
        aggregate_by_ticket(
            input_csv=args.input or [],
            output_csv=args.output,
            input_dir=args.input_dir,
            recursive=args.recursive,
            pattern=args.pattern,
            verbose=args.verbose,
        )
        print(f"Successfully wrote ticket aggregation to: {args.output}")
        return 0
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        print(f"ERROR: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

