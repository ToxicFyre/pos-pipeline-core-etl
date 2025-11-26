"""Build payments dataset from POS ETL pipeline.

This module orchestrates a three-step ETL process for payments data:
1. Downloads missing payment reports from POS HTTP API
2. Cleans the raw Excel files
3. Aggregates cleaned data into a daily dataset

The module intelligently skips already-downloaded date ranges by scanning
existing files and only downloading missing intervals. It respects branch
code windows (periods when specific branch codes were valid) and handles
chunking large date ranges into smaller HTTP requests.

Examples:
    Command-line usage:
        # Download and process payments from 2022-11-01 to today
        python -m pos_etl.build_payments_dataset

        # Process a specific date range
        python -m pos_etl.build_payments_dataset \\
            --start 2023-01-01 --end 2023-12-31

        # Dry run to see what would be downloaded
        python -m pos_etl.build_payments_dataset --dry-run

        # Use smaller chunks (90 days instead of default 180)
        python -m pos_etl.build_payments_dataset \\
            --max-days-per-chunk 90

    Programmatic usage:
        from datetime import date
        from pos_etl.build_payments_dataset import build_payments_dataset

        build_payments_dataset(
            global_start=date(2023, 1, 1),
            global_end=date(2023, 12, 31),
            max_days_per_chunk=180,
            dry_run=False
        )
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Optional

from pos_etl import config
from pos_etl.utils import (
    discover_existing_clean_intervals,
    discover_existing_intervals,
    format_duration,
    get_raw_file_date_range,
    is_interval_covered,
    iter_chunks,
    parse_date,
    subtract_intervals,
)


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


EXCLUDED_BRANCHES = {"CEDIS"}


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



def run_cmd(cmd: List[str], dry_run: bool) -> None:
    """Execute a subprocess command from the project root directory.

    Runs a command using the same Python environment. In dry-run mode,
    only prints the command without executing it.

    Args:
        cmd: Command to run as a list of strings (e.g., ["python", "-m", "module"]).
        dry_run: If True, print command but don't execute it.

    Raises:
        subprocess.CalledProcessError: If the command fails (only in non-dry-run mode).
    """
    print("      CMD:", " ".join(cmd))
    if dry_run:
        print("      (dry-run: not executing)")
        return
    subprocess.run(cmd, cwd=str(config.ROOT), check=True)


def build_payments_dataset(
    global_start: date,
    global_end: date,
    max_days_per_chunk: int = 180,
    dry_run: bool = False,
    verbose: bool = False,
    reclean: bool = False,
) -> None:
    """Orchestrate the complete payments ETL pipeline.

    Executes a three-step process:
    1. Downloads missing payments data for all branches, respecting code
       windows and skipping already-downloaded date ranges
    2. Cleans raw payment Excel files (only missing ones unless reclean=True)
    3. Aggregates cleaned data into a single daily dataset

    The function intelligently determines which date ranges need to be
    downloaded and cleaned by comparing the requested range against existing
    files and branch code validity windows.

    Args:
        global_start: Start date for the entire dataset (inclusive).
        global_end: End date for the entire dataset (inclusive).
        max_days_per_chunk: Maximum days per HTTP request chunk (default: 180).
        dry_run: If True, print actions without executing downloads/processing.
        verbose: If True, enable verbose logging.
        reclean: If True, re-clean all raw files even if clean versions exist.
            If False, only clean files that are missing from the clean dataset.

    Prints:
        Progress information and timing for each step. Final output path
        and timing summary.

    Examples:
        >>> from datetime import date
        >>> build_payments_dataset(
        ...     global_start=date(2023, 1, 1),
        ...     global_end=date(2023, 12, 31),
        ...     max_days_per_chunk=90,
        ...     dry_run=True,
        ...     reclean=False
        ... )
    """

    overall_start = time.perf_counter()

    raw_payments_root = config.RAW_PAYMENTS_DIR_BATCH
    clean_payments_dir = config.CLEAN_PAYMENTS_DIR_BATCH
    proc_payments_dir = config.PROC_PAYMENTS_DIR
    if not dry_run:
        proc_payments_dir.mkdir(parents=True, exist_ok=True)
    aggregate_path = proc_payments_dir / "aggregated_payments_daily.csv"

    print(f"Project ROOT:       {config.ROOT}")
    print(f"Raw payments dir:   {raw_payments_root}")
    print(f"Clean payments dir: {clean_payments_dir}")
    print(f"Proc payments dir:  {proc_payments_dir}")
    print(f"Sucursales JSON:    {config.SUCURSALES_PATH}")

    raw_payments_root.mkdir(parents=True, exist_ok=True)

    # 1) load logical branches + code windows from JSON
    branch_segments = load_branch_segments_from_json(config.SUCURSALES_PATH)

    # 2) discover what we already have on disk
    existing_by_code = discover_existing_intervals(raw_payments_root)
    print("\nExisting intervals by code:")
    if not existing_by_code:
        print("  (none)")
    else:
        for code, ivs in existing_by_code.items():
            for s, e in ivs:
                print(f"  code={code}: {s}..{e}")

    python_exe = sys.executable

    # -------------------------------------------------
    # STEP 1: DOWNLOAD MISSING CHUNKS
    # -------------------------------------------------
    download_start = time.perf_counter()

    for branch_name, windows in branch_segments.items():
        print(f"\n=== Branch: {branch_name} ===")
        for seg in windows:
            # Calculate the intersection of the code window with the requested date range
            # This ensures we only download data for periods when this code was actually valid
            seg_start = max(global_start, seg.valid_from)
            seg_end = min(global_end, seg.valid_to or global_end)
            if seg_start > seg_end:
                continue

            code = seg.code
            # Get already-downloaded intervals for this code
            already = existing_by_code.get(code, [])
            # Find gaps: date ranges that need to be downloaded
            missing_ranges = subtract_intervals((seg_start, seg_end), already)

            if not missing_ranges:
                msg = (
                    f"  code={code} window {seg_start}..{seg_end}: "
                    "already fully covered, skipping."
                )
                print(msg)
                continue

            code_root = raw_payments_root / branch_name / code
            code_root.mkdir(parents=True, exist_ok=True)

            print(f"  code={code} window {seg_start}..{seg_end}")
            print(f"    existing: {already or 'none'}")
            print(f"    missing ranges: {missing_ranges}")

            for mr_start, mr_end in missing_ranges:
                chunks = iter_chunks(
                    mr_start, mr_end, max_days_per_chunk
                )
                for chunk_start, chunk_end in chunks:
                    chunk_dir = code_root / f"{chunk_start}_{chunk_end}"
                    if not dry_run:
                        chunk_dir.mkdir(parents=True, exist_ok=True)

                    print(
                        f"    downloading {chunk_start}..{chunk_end} "
                        f"-> {chunk_dir}"
                    )
                    # POS API treats end date as exclusive, so we add 1 day
                    # to ensure we get data for the full chunk_end date
                    api_end_date = chunk_end + timedelta(days=1)
                    cmd = [
                        python_exe,
                        "-m",
                        "pos_etl.a_extract.HTTP_extraction",
                        "--report",
                        "Payments",
                        "--sucursal-id",
                        code,
                        "--start",
                        chunk_start.isoformat(),
                        "--end",
                        api_end_date.isoformat(),
                        "--outdir",
                        str(chunk_dir),
                    ]
                    run_cmd(cmd, dry_run=dry_run)

    download_secs = time.perf_counter() - download_start
    print(f"\nSTEP 1 (download) finished in {format_duration(download_secs)}.")


    # -------------------------------------------------
    # STEP 2: CLEAN
    # -------------------------------------------------
    print(
        f"\nCleaning payments from {raw_payments_root} "
        f"-> {clean_payments_dir}"
    )
    
    # Check for raw Excel files
    raw_files = list(raw_payments_root.rglob("Payments_*.xlsx")) if raw_payments_root.exists() else []
    
    if not raw_files:
        print(f"  WARNING: No raw Excel files found in {raw_payments_root}")
        print("  Skipping cleaning step (nothing to clean)")
        clean_secs = 0.0
    else:
        print(f"  Found {len(raw_files)} raw Excel file(s)")
        
        # Discover existing clean intervals (unless reclean is True)
        files_to_clean: List[Path] = []
        if reclean:
            print("  --reclean flag set: will re-clean all files")
            files_to_clean = raw_files
        else:
            # Discover what's already cleaned
            existing_clean_intervals = discover_existing_clean_intervals(clean_payments_dir)
            print(f"  Existing clean intervals: {len(existing_clean_intervals)} date range(s)")
            if existing_clean_intervals:
                for s, e in existing_clean_intervals:
                    print(f"    {s}..{e}")
            
            # Filter raw files to only those that need cleaning
            for raw_file in raw_files:
                file_range = get_raw_file_date_range(raw_file)
                if file_range is None:
                    # Can't determine date range, include it to be safe
                    files_to_clean.append(raw_file)
                    if verbose:
                        print(f"    Including {raw_file.name} (unknown date range)")
                elif not is_interval_covered(file_range, existing_clean_intervals):
                    files_to_clean.append(raw_file)
                    if verbose:
                        print(f"    Including {raw_file.name} ({file_range[0]}..{file_range[1]})")
                elif verbose:
                    print(f"    Skipping {raw_file.name} ({file_range[0]}..{file_range[1]} - already covered)")
        
        if not files_to_clean:
            print("  All raw files already have clean versions. Skipping cleaning step.")
            clean_secs = 0.0
        else:
            print(f"  Will clean {len(files_to_clean)} file(s) (out of {len(raw_files)} total)")
            
            clean_start = time.perf_counter()
            
            if not dry_run:
                clean_payments_dir.mkdir(parents=True, exist_ok=True)
                
                # If we need to clean only a subset, create a temporary directory
                # with only those files (preserving directory structure for cleaner)
                if len(files_to_clean) < len(raw_files):
                    with tempfile.TemporaryDirectory(prefix="pos_clean_") as temp_dir:
                        temp_root = Path(temp_dir)
                        
                        # Create directory structure and copy only files that need cleaning
                        for raw_file in files_to_clean:
                            # Preserve relative path structure
                            rel_path = raw_file.relative_to(raw_payments_root)
                            temp_file = temp_root / rel_path
                            temp_file.parent.mkdir(parents=True, exist_ok=True)
                            shutil.copy2(raw_file, temp_file)
                        
                        if verbose:
                            print(f"  Created temporary directory with {len(files_to_clean)} file(s): {temp_root}")
                        
                        cmd_clean = [
                            python_exe,
                            "-m",
                            "pos_etl.b_transform.pos_excel_payments_cleaner",
                            "--input-dir",
                            str(temp_root),
                            "--recursive",
                            "--outdir",
                            str(clean_payments_dir),
                        ]
                        if verbose:
                            cmd_clean.append("--verbose")
                        run_cmd(cmd_clean, dry_run=dry_run)
                else:
                    # Clean all files - use original directory
                    cmd_clean = [
                        python_exe,
                        "-m",
                            "pos_etl.b_transform.pos_excel_payments_cleaner",
                        "--input-dir",
                        str(raw_payments_root),
                        "--recursive",
                        "--outdir",
                        str(clean_payments_dir),
                    ]
                    if verbose:
                        cmd_clean.append("--verbose")
                    run_cmd(cmd_clean, dry_run=dry_run)
            else:
                # Dry run - just show what would be cleaned
                cmd_clean = [
                    python_exe,
                    "-m",
                            "pos_etl.b_transform.pos_excel_payments_cleaner",
                    "--input-dir",
                    str(raw_payments_root) if len(files_to_clean) == len(raw_files) else "<temp_dir>",
                    "--recursive",
                    "--outdir",
                    str(clean_payments_dir),
                ]
                if verbose:
                    cmd_clean.append("--verbose")
                run_cmd(cmd_clean, dry_run=dry_run)
            
            clean_secs = time.perf_counter() - clean_start

    print(f"STEP 2 (clean) finished in {format_duration(clean_secs)}.")


    # -------------------------------------------------
    # STEP 3: AGGREGATE
    # -------------------------------------------------
    print(
        f"\nAggregating cleaned payments from {clean_payments_dir} "
        f"-> {aggregate_path}"
    )

    # Check for clean CSV files
    clean_files = list(clean_payments_dir.rglob("*.csv")) if clean_payments_dir.exists() else []
    
    if not clean_files:
        print(f"  WARNING: No clean CSV files found in {clean_payments_dir}")
        print("  Skipping aggregation step (nothing to aggregate)")
        agg_secs = 0.0
    else:
        print(f"  Found {len(clean_files)} clean CSV file(s) to aggregate")
        agg_start = time.perf_counter()

        cmd_agg = [
            python_exe,
            "-m",
            "pos_etl.c_load.aggregate_payments_by_day",
            "--input-dir",
            str(clean_payments_dir),
            "--recursive",
            "--out",
            str(aggregate_path),
        ]
        run_cmd(cmd_agg, dry_run=dry_run)

        agg_secs = time.perf_counter() - agg_start
    
    print(f"STEP 3 (aggregate) finished in {format_duration(agg_secs)}.")


    overall_secs = time.perf_counter() - overall_start

    print("\nDONE. Final aggregated payments daily dataset at:")
    print(f"  {aggregate_path}")
    print("\nTiming summary:")
    print(f"  Download:    {format_duration(download_secs)}")
    print(f"  Cleaning:    {format_duration(clean_secs)}")
    print(f"  Aggregation: {format_duration(agg_secs)}")
    print(f"  TOTAL:       {format_duration(overall_secs)}")




def main() -> None:
    """Main entry point for the build_payments_dataset command-line tool.

    Parses command-line arguments and executes the payments ETL pipeline.
    Changes working directory to project root before running subprocesses.

    Command-line arguments:
        --start: Global start date (YYYY-MM-DD, default: 2022-11-01)
        --end: Global end date (YYYY-MM-DD, default: today)
        --max-days-per-chunk: Maximum days per HTTP request (default: 180)
        --dry-run: Print actions without executing
        --reclean: Re-clean all raw files even if clean versions exist

    Raises:
        SystemExit: If start date is after end date.

    Examples:
        $ python -m pos_etl.build_payments_dataset
        $ python -m pos_etl.build_payments_dataset --start 2023-01-01
        $ python -m pos_etl.build_payments_dataset --dry-run
    """
    parser = argparse.ArgumentParser(
        description=(
            "Backfill payments dataset from POS (all branches, "
            "skipping already-downloaded ranges)."
        )
    )
    parser.add_argument(
        "--start",
        type=str,
        default="2022-11-01",
        help="Global start date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--end",
        type=str,
        default=date.today().isoformat(),
        help="Global end date (YYYY-MM-DD, default: today).",
    )
    parser.add_argument(
        "--max-days-per-chunk",
        type=int,
        default=180,
        help=(
            "Maximum number of days per HTTP request chunk (inclusive)."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Print what would be downloaded/processed without calling "
            "HTTP_extraction or other scripts."
        ),
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging (DEBUG level).",
    )
    parser.add_argument(
        "--reclean",
        action="store_true",
        help=(
            "Re-clean all raw files even if clean versions already exist. "
            "By default, only missing files are cleaned."
        ),
    )

    args = parser.parse_args()

    global_start = parse_date(args.start)
    global_end = parse_date(args.end)

    if global_start > global_end:
        raise SystemExit("ERROR: start date is after end date.")

    # Make sure we run from ROOT (subprocess cwd)
    os.chdir(config.ROOT)

    build_payments_dataset(
        global_start=global_start,
        global_end=global_end,
        max_days_per_chunk=args.max_days_per_chunk,
        dry_run=args.dry_run,
        verbose=args.verbose,
        reclean=args.reclean,
    )



if __name__ == "__main__":
    main()
