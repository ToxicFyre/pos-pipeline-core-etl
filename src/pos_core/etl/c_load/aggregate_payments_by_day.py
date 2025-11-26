#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Aggregate POS 'Detalle por forma de pago' clean CSVs -> branch/day income table.

USAGE GUIDE
===========

Goal
----
This script takes the *clean* payments CSVs produced by
`pos_excel_payments_cleaner.py` and builds a centralized daily-income table
per sucursal (branch), split by payment type and tips.

Typical pipeline
----------------
1. Download raw Excel reports (another script).
2. Clean them into normalized CSVs:
       python pos_excel_payments_cleaner.py --input-dir ./downloads --outdir ./clean_csv
3. Aggregate all clean CSVs into a single table:
       python aggregate_payments_by_day.py --input-dir ./clean_csv --out ./aggregated_payments.csv

Command-line examples
---------------------
Single clean CSV:
    python aggregate_payments_by_day.py \
        --input ./clean_csv/forma_pago_Panem-Kavia_2025-01-01_2025-06-30.csv \
        --out ./aggregated_payments.csv

Folder with many clean CSVs:
    python aggregate_payments_by_day.py \
        --input-dir ./clean_csv \
        --recursive \
        --out ./aggregated_payments.csv

Quieter logging:
    python aggregate_payments_by_day.py --input-dir ./clean_csv --out ./aggregated_payments.csv --quiet

Input contract
--------------
Each input CSV must be the output of the cleaner script and include at least:

    sucursal          (string)
    operating_date    (date or text parseable as date)
    payment_method    (string)
    ticket_total      (numeric)
    ticket_tip        (numeric)

If present, the column `total_day_tips` is used as a reference for a tips
sanity check. If present, the column `elimination_present` (boolean) is used
to count tickets with eliminations.

Output
------
A single CSV with one row per (sucursal, fecha):

    sucursal,
    fecha,
    ingreso_efectivo,
    ingreso_credito,
    ingreso_debito,
    ingreso_amex,
    ingreso_ubereats,
    ingreso_rappi,
    ingreso_transferencia,
    ingreso_SubsidioTEC,
    ingreso_otros,
    propinas,
    num_tickets,
    tickets_with_eliminations,
    pct_tickets_with_eliminations,
    is_national_holiday

- Each ingreso_* column is the sum of `ticket_total` for that day and payment bucket.
- `propinas` is the sum of `ticket_tip` for that day (all methods).
- `num_tickets` is the count of unique tickets (order_index) for that day.
- `tickets_with_eliminations` is the count of unique tickets that had eliminations associated with them.
- `pct_tickets_with_eliminations` is the percentage of tickets with eliminations (tickets_with_eliminations / num_tickets * 100), rounded to 2 decimal places. Returns 0.0 when num_tickets is 0.
- `is_national_holiday` is True if the date is a Mexican national holiday, False otherwise. Holiday data is fetched from the Nager.Date Public Holiday API (https://date.nager.at/).

Payment-method bucketing
------------------------
`payment_method` strings are normalized (lowercased, accents removed, spaces collapsed)
and then mapped as follows:

- Contains "efectivo"                    -> ingreso_efectivo
- Contains "credito" / "crédito"        -> ingreso_credito
- Contains "debito" / "débito"          -> ingreso_debito
- Contains "amex" / "american express"  -> ingreso_amex
- Contains "rappi"                      -> ingreso_rappi
- Contains "uber"                       -> ingreso_ubereats
- Contains "transfer"                   -> ingreso_transferencia
- Contains "subsidio" and "tec"         -> ingreso_SubsidioTEC
- Anything else                         -> ingreso_otros

Tip sanity check
----------------
If the column `total_day_tips` exists in the inputs:

- For each (sucursal, fecha), the script compares:
    - `sum(ticket_tip)` from rows
    - `max(total_day_tips)` for that day

- If the absolute difference is greater than 0.05, it raises a ValueError with
  a short sample table of mismatches. This protects you from silent data issues.

Encoding
--------
All CSVs are read and written as UTF-8 with BOM (`utf-8-sig`) so Excel on
Windows shows accents correctly.

Testing suggestions
-------------------
1. Run the aggregation on a single small CSV and open the output in Excel.
2. For a given sucursal + fecha:
   - Manually sum `ticket_total` by payment_method in the clean file and
     compare to the corresponding ingreso_* columns.
   - Manually sum `ticket_tip` and compare to `propinas`.
   - If `total_day_tips` exists, confirm it matches `propinas`.

"""

from __future__ import annotations

import argparse
import csv
import logging
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable, List, Optional, Set

import pandas as pd
import requests

from pos_etl.b_transform.pos_cleaning_utils import normalize_spanish_name


# ------------------------------------------------------------
# Normalization + payment-method bucketing
# ------------------------------------------------------------

def bucket_for_payment_method(method: str) -> str:
    """
    Map raw payment_method to one of the ingreso_* columns.
    Falls back to 'ingreso_otros' if none of the patterns match.
    """
    s = normalize_spanish_name(method)
    if not s:
        return "ingreso_otros"

    # Cash
    if "efectivo" in s:
        return "ingreso_efectivo"

    # Delivery apps
    if "rappi" in s:
        return "ingreso_rappi"
    if "uber" in s:
        # catches 'uber', 'ubereats', 'uber eats'
        return "ingreso_ubereats"

    # Internal subsidy
    if "subsidio" in s and "tec" in s:
        return "ingreso_SubsidioTEC"

    # Bank transfers
    if "transfer" in s:
        return "ingreso_transferencia"

    # Cards
    if "amex" in s or "american express" in s:
        return "ingreso_amex"
    if "debito" in s:
        return "ingreso_debito"
    if "credito" in s:
        return "ingreso_credito"

    # Anything else
    return "ingreso_otros"


BUCKET_COLS = [
    "ingreso_efectivo",
    "ingreso_credito",
    "ingreso_debito",
    "ingreso_amex",
    "ingreso_ubereats",
    "ingreso_rappi",
    "ingreso_transferencia",
    "ingreso_SubsidioTEC",
    "ingreso_otros",
]


# ------------------------------------------------------------
# Mexican holidays API
# ------------------------------------------------------------

# Cache for holiday dates (key: year, value: set of date objects)
_HOLIDAY_CACHE: dict[int, Set[date]] = {}


def fetch_mexican_holidays(year: int) -> Set[date]:
    """
    Fetch Mexican national holidays for a given year using the Nager.Date API.
    Uses caching to avoid redundant API calls.
    
    Args:
        year: The year to fetch holidays for
        
    Returns:
        A set of date objects representing Mexican national holidays
        
    Raises:
        requests.RequestException: If the API request fails
        ValueError: If the API response is invalid
    """
    # Check cache first
    if year in _HOLIDAY_CACHE:
        return _HOLIDAY_CACHE[year]
    
    url = f"https://date.nager.at/api/v3/PublicHolidays/{year}/MX"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        holidays_data = response.json()
        
        # Extract dates from the response
        holiday_dates = set()
        for holiday in holidays_data:
            # API returns dates in 'YYYY-MM-DD' format
            holiday_date = date.fromisoformat(holiday["date"])
            holiday_dates.add(holiday_date)
        
        # Cache the result
        _HOLIDAY_CACHE[year] = holiday_dates
        logging.info("Fetched %d Mexican holidays for year %d", len(holiday_dates), year)
        return holiday_dates
        
    except requests.RequestException as e:
        logging.warning(
            "Failed to fetch Mexican holidays for year %d: %s. "
            "Holiday flag will be False for all dates.", year, e
        )
        # Return empty set on error, so holiday flag will be False
        _HOLIDAY_CACHE[year] = set()
        return set()
    except (KeyError, ValueError) as e:
        logging.warning(
            "Invalid response format from holidays API for year %d: %s. "
            "Holiday flag will be False for all dates.", year, e
        )
        _HOLIDAY_CACHE[year] = set()
        return set()


def get_all_mexican_holidays(df: pd.DataFrame) -> Set[date]:
    """
    Fetch Mexican holidays for all years present in the dataframe.
    
    Args:
        df: DataFrame with 'operating_date' column
        
    Returns:
        A set of all holiday dates across all years in the dataframe
    """
    if "operating_date" not in df.columns:
        return set()
    
    # Convert to datetime if needed
    dates = pd.to_datetime(df["operating_date"], errors="coerce")
    dates = dates.dropna()
    
    if dates.empty:
        return set()
    
    # Get unique years
    years = dates.dt.year.unique()
    
    # Fetch holidays for all years and combine
    all_holidays = set()
    for year in years:
        holidays = fetch_mexican_holidays(int(year))
        all_holidays.update(holidays)
    
    return all_holidays


# ------------------------------------------------------------
# Core aggregation
# ------------------------------------------------------------

def aggregate_payments(dfs: List[pd.DataFrame]) -> pd.DataFrame:
    """
    Main aggregation:
    - concat all clean CSVs
    - group by sucursal + fecha + payment bucket
    - pivot to wide ingreso_* columns
    - sum tips into 'propinas'
    - derive num_tickets
    - validate that tips per day align with total_day_tips from source
    """
    if not dfs:
        return pd.DataFrame(
            columns=["sucursal", "fecha"] + BUCKET_COLS + ["propinas", "num_tickets", "tickets_with_eliminations", "pct_tickets_with_eliminations", "is_national_holiday"]
        )

    df = pd.concat(dfs, ignore_index=True)

    required = ["sucursal", "operating_date", "payment_method", "ticket_total", "ticket_tip"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in input CSVs: {missing}")

    # include optional columns (for sanity check + ticket count)
    cols = required.copy()
    has_total_day_tips = "total_day_tips" in df.columns
    if has_total_day_tips:
        cols.append("total_day_tips")
    has_order_index = "order_index" in df.columns
    if has_order_index:
        cols.append("order_index")
    has_elimination_present = "elimination_present" in df.columns
    if has_elimination_present:
        cols.append("elimination_present")

    sub = df[cols].copy()

    # Normalize date -> date type
    sub["operating_date"] = pd.to_datetime(sub["operating_date"]).dt.date

    # Payment bucket
    sub["bucket"] = sub["payment_method"].map(
        lambda x: bucket_for_payment_method(x) if pd.notna(x) else "ingreso_otros"
    )

    # --- Sanity check: tips per day vs total_day_tips ------------------
    if has_total_day_tips:
        # Expected tips from daily total
        daily_totals = (
            sub.dropna(subset=["total_day_tips"])
               .groupby(["sucursal", "operating_date"], as_index=False)["total_day_tips"]
               .max()
               .rename(columns={"total_day_tips": "expected_tips"})
        )

        # Actual tips from sum of ticket_tip
        daily_actual = (
            sub.groupby(["sucursal", "operating_date"], as_index=False)["ticket_tip"]
               .sum()
               .rename(columns={"ticket_tip": "actual_tips"})
        )

        cmp = daily_actual.merge(daily_totals, on=["sucursal", "operating_date"], how="left")
        cmp["expected_tips"] = cmp["expected_tips"].fillna(0.0)
        cmp["diff"] = (cmp["actual_tips"] - cmp["expected_tips"]).abs()

        mismatches = cmp[cmp["diff"] > 0.05]  # small tolerance for rounding
        if not mismatches.empty:
            sample = mismatches.head(10)
            raise ValueError(
                "Tip sanity check failed for some (sucursal, fecha). "
                "Differences between sum(ticket_tip) and total_day_tips exceed tolerance.\n"
                f"Sample rows:\n{sample}"
            )
    # -------------------------------------------------------------------

    # Sum ticket_total per sucursal/fecha/bucket
    agg = (
        sub.groupby(["sucursal", "operating_date", "bucket"], as_index=False)
           .agg(ingreso=("ticket_total", "sum"))
    )

    # Pivot to wide table
    wide = agg.pivot_table(
        index=["sucursal", "operating_date"],
        columns="bucket",
        values="ingreso",
        aggfunc="sum",
        fill_value=0.0,
    ).reset_index()

    # Make sure all ingreso_* columns exist
    for col in BUCKET_COLS:
        if col not in wide.columns:
            wide[col] = 0.0

    # Sum tips per sucursal/fecha
    tips = (
        sub.groupby(["sucursal", "operating_date"], as_index=False)["ticket_tip"]
           .sum()
           .rename(columns={"ticket_tip": "propinas"})
    )

    # Derive num_tickets
    if has_order_index:
        tickets = (
            sub.groupby(["sucursal", "operating_date"], as_index=False)["order_index"]
               .nunique()
               .rename(columns={"order_index": "num_tickets"})
        )
    else:
        tickets = (
            sub.groupby(["sucursal", "operating_date"], as_index=False)
               .size()
               .rename(columns={"size": "num_tickets"})
        )

    # Derive tickets_with_eliminations (count of unique tickets with eliminations)
    if has_elimination_present and has_order_index:
        # Filter to rows with eliminations, then count unique order_index per day
        elim_tickets = (
            sub[sub["elimination_present"].fillna(False)]
            .groupby(["sucursal", "operating_date"], as_index=False)["order_index"]
            .nunique()
            .rename(columns={"order_index": "tickets_with_eliminations"})
        )
    else:
        elim_tickets = pd.DataFrame(
            columns=["sucursal", "operating_date", "tickets_with_eliminations"]
        )

    # Merge tips and tickets into wide table
    result = wide.merge(tips, on=["sucursal", "operating_date"], how="left")
    result = result.merge(tickets, on=["sucursal", "operating_date"], how="left")
    result = result.merge(elim_tickets, on=["sucursal", "operating_date"], how="left")

    result["propinas"] = result["propinas"].fillna(0.0)
    result["num_tickets"] = result["num_tickets"].fillna(0).astype(int)
    result["tickets_with_eliminations"] = result["tickets_with_eliminations"].fillna(0).astype(int)

    # Calculate percentage of tickets with eliminations
    # Avoid division by zero: if num_tickets is 0, percentage is 0.0
    result["pct_tickets_with_eliminations"] = (
        result["tickets_with_eliminations"] / result["num_tickets"] * 100
    ).fillna(0.0).replace([float('inf'), -float('inf')], 0.0).round(2)

    # Add Mexican national holiday flag
    # Fetch holidays for all years present in the dataset
    # Use the sub dataframe before aggregation to get all years efficiently
    holiday_dates = get_all_mexican_holidays(sub)
    # operating_date is already a date object, so we can directly compare
    result["is_national_holiday"] = result["operating_date"].apply(
        lambda d: d in holiday_dates if isinstance(d, date) else False
    )

    # Rename date column
    result = result.rename(columns={"operating_date": "fecha"})

    # Order columns
    final_cols = ["sucursal", "fecha"] + BUCKET_COLS + ["propinas", "num_tickets", "tickets_with_eliminations", "pct_tickets_with_eliminations", "is_national_holiday"]
    result = result[final_cols].sort_values(["sucursal", "fecha"]).reset_index(drop=True)

    return result


# ------------------------------------------------------------
# IO + CLI
# ------------------------------------------------------------

def read_clean_csv(path: Path) -> pd.DataFrame:
    """
    Read a clean payments CSV.
    Uses utf-8-sig to handle BOM safely.
    """
    return pd.read_csv(path, encoding="utf-8-sig")


def iter_csv_files(root: Path, recursive: bool) -> Iterable[Path]:
    if recursive:
        yield from (p for p in root.rglob("*.csv") if p.is_file())
    else:
        yield from (p for p in root.glob("*.csv") if p.is_file())


def write_csv(df: pd.DataFrame, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_MINIMAL)


@dataclass
class Args:
    input: Optional[Path]
    input_dir: Optional[Path]
    out: Path
    recursive: bool
    quiet: bool


def parse_args() -> Args:
    p = argparse.ArgumentParser(
        description="Aggregate clean POS payments CSVs into branch/day income table"
    )
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--input", type=Path, help="Single clean CSV file")
    g.add_argument("--input-dir", type=Path, help="Folder with clean CSV files")

    p.add_argument(
        "--out",
        type=Path,
        default=Path("aggregated_payments.csv"),
        help="Output CSV path (default: ./aggregated_payments.csv)",
    )
    p.add_argument(
        "--recursive",
        action="store_true",
        help="Recurse into subdirectories of --input-dir when searching for CSVs",
    )
    p.add_argument("--quiet", action="store_true", help="Less logging")

    a = p.parse_args()
    return Args(
        input=a.input,
        input_dir=a.input_dir,
        out=a.out,
        recursive=a.recursive,
        quiet=a.quiet,
    )


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=logging.WARNING if args.quiet else logging.INFO,
        format="%(levelname)s: %(message)s",
    )

    dfs: List[pd.DataFrame] = []

    if args.input:
        if not args.input.exists():
            raise SystemExit(f"Input file not found: {args.input}")
        logging.info("Reading %s", args.input)
        dfs.append(read_clean_csv(args.input))
    else:
        if not args.input_dir or not args.input_dir.exists():
            raise SystemExit(f"Input dir not found: {args.input_dir}")
        any_found = False
        for csv_path in iter_csv_files(args.input_dir, args.recursive):
            any_found = True
            logging.info("Reading %s", csv_path)
            dfs.append(read_clean_csv(csv_path))
        if not any_found:
            logging.warning("No .csv files found under %s", args.input_dir)

    result = aggregate_payments(dfs)
    write_csv(result, args.out)
    logging.info(
        "Wrote %s (%d rows, %d cols)", args.out, len(result), len(result.columns)
    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)