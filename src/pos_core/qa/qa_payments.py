"""Quick QA checks for aggregated daily payments.

This module performs quality assurance checks on aggregated daily payment data
from the POS ETL pipeline. It validates data integrity, checks for anomalies,
and generates summary reports including monthly sales tables with elimination
percentages.

Usage (from repo root):

    python -m pos_qa.qa_payments \
        --file aggregated_payments_daily.csv \
        --sample-months 5 \
        --seed 42

Examples:
    # Default file in processed payments directory
    python -m pos_qa.qa_payments

    # Only sample Carreta, 3 random months
    python -m pos_qa.qa_payments --sucursal Carreta --sample-months 3

    # Use a different file
    python -m pos_qa.qa_payments --file my_payments.csv

    # Disable random sampling
    python -m pos_qa.qa_payments --sample-months 0

    # Use custom data root
    python -m pos_qa.qa_payments --data-root /path/to/data

What this script does:

1. Loads aggregated_payments_daily.csv from:
       <data-root>/c_processed/payments/<file>
2. Validates:
   - required columns present
   - fecha is parseable as date
   - sucursal + fecha has no duplicates
   - money and ticket columns are numeric and non-negative
   - rows with tickets > 0 have positive revenue, and vice versa (within a small tolerance)
3. Computes:
   - total revenue per row (excluding propinas)
   - per-sucursal summary stats
   - monthly sales table with elimination percentages (if available)
4. Prints random sucursal-month samples for manual QA (totals, avg ticket,
   and monthly breakdown by payment form).

Output:
    The script prints:
    - Validation errors and warnings
    - Per-sucursal summary statistics
    - Monthly sales table (one column per sucursal, rows for each month showing
      total sales and percentage of eliminated tickets)
    - Random month samples for manual review

"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

REQUIRED_COLUMNS = [
    "sucursal",
    "fecha",
    "ingreso_efectivo",
    "ingreso_credito",
    "ingreso_debito",
    "ingreso_amex",
    "ingreso_ubereats",
    "ingreso_rappi",
    "ingreso_transferencia",
    "ingreso_SubsidioTEC",
    "ingreso_otros",
    "propinas",
    "num_tickets",
]

MONEY_COLUMNS = [
    "ingreso_efectivo",
    "ingreso_credito",
    "ingreso_debito",
    "ingreso_amex",
    "ingreso_ubereats",
    "ingreso_rappi",
    "ingreso_transferencia",
    "ingreso_SubsidioTEC",
    "ingreso_otros",
    "propinas",
]

TICKET_COLUMN = "num_tickets"

TOTAL_NO_TIPS_COLUMN = "total_sin_propinas"


@dataclass
class QAResult:
    """Represents a single QA check result.

    Attributes:
        level: Severity level, either "ERROR" or "WARN".
        message: Human-readable message describing the issue or information.

    Examples:
        >>> result = QAResult("ERROR", "Found duplicate rows")
        >>> result.level
        'ERROR'
        >>> result.message
        'Found duplicate rows'

    """

    level: str  # "ERROR" or "WARN"
    message: str


# --------------------------------------------------------------------------- #
# Loading
# --------------------------------------------------------------------------- #


def load_payments(path: Path) -> pd.DataFrame:
    """Load and preprocess aggregated payments CSV file.

    Reads a CSV file containing aggregated daily payment data, validates required
    columns are present, parses dates, and computes helper fields including
    total revenue (excluding tips) and year-month grouping.

    Args:
        path: Path to the CSV file to load.

    Returns:
        DataFrame with loaded and preprocessed payment data. Includes computed
        columns: 'total_sin_propinas', 'year_month', and 'weekday'.

    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError: If required columns are missing.

    Examples:
        >>> from pathlib import Path
        >>> df = load_payments(Path("data/c_processed/payments/aggregated_payments_daily.csv"))
        >>> "total_sin_propinas" in df.columns
        True
        >>> "year_month" in df.columns
        True

    """
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    df = pd.read_csv(path)

    # Basic column check
    missing = sorted(set(REQUIRED_COLUMNS) - set(df.columns))
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Parse fecha
    df["fecha"] = pd.to_datetime(df["fecha"], format="%Y-%m-%d", errors="raise")

    # Enforce dtypes for numeric columns
    for col in [*MONEY_COLUMNS, TICKET_COLUMN]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Compute helper fields
    df[TOTAL_NO_TIPS_COLUMN] = df[
        [
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
    ].sum(axis=1)

    df["year_month"] = df["fecha"].dt.to_period("M").astype(str)
    df["weekday"] = df["fecha"].dt.day_name()

    return df


def prepare_payments_df(df: pd.DataFrame) -> pd.DataFrame:
    """Prepare a payments DataFrame for QA checks.

    Parses dates, enforces numeric types, and computes helper fields including
    total revenue (excluding tips) and year-month grouping.

    Args:
        df: DataFrame with payment data. Must have required columns.

    Returns:
        DataFrame with prepared data. Includes computed columns:
        'total_sin_propinas', 'year_month', and 'weekday'.

    Examples:
        >>> df = pd.DataFrame({
        ...     'fecha': ['2023-01-01', '2023-01-02'],
        ...     'ingreso_efectivo': [100, 200],
        ...     'num_tickets': [10, 20]
        ... })
        >>> prepared = prepare_payments_df(df)
        >>> 'total_sin_propinas' in prepared.columns
        True

    """
    # Parse fecha if it's not already datetime
    if not pd.api.types.is_datetime64_any_dtype(df["fecha"]):
        df["fecha"] = pd.to_datetime(df["fecha"], format="%Y-%m-%d", errors="raise")

    # Enforce dtypes for numeric columns
    for col in [*MONEY_COLUMNS, TICKET_COLUMN]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Compute helper fields
    payment_cols = [
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
    available_cols = [col for col in payment_cols if col in df.columns]
    if available_cols:
        df[TOTAL_NO_TIPS_COLUMN] = df[available_cols].sum(axis=1)
    else:
        df[TOTAL_NO_TIPS_COLUMN] = 0.0

    df["year_month"] = df["fecha"].dt.to_period("M").astype(str)
    df["weekday"] = df["fecha"].dt.day_name()

    return df


# --------------------------------------------------------------------------- #
# Detection helpers (used by API)
# --------------------------------------------------------------------------- #


def detect_missing_days(df: pd.DataFrame) -> pd.DataFrame | None:
    """Detect missing days per sucursal.

    For each sucursal, builds a full date range from min(fecha) to max(fecha)
    and identifies missing dates.

    Args:
        df: DataFrame with 'sucursal' and 'fecha' columns.

    Returns:
        DataFrame with columns: sucursal, fecha (missing dates), or None if no missing days.

    Examples:
        >>> df = pd.DataFrame({
        ...     'sucursal': ['A', 'A', 'A'],
        ...     'fecha': pd.to_datetime(['2023-01-01', '2023-01-03', '2023-01-05'])
        ... })
        >>> missing = detect_missing_days(df)
        >>> len(missing)
        2

    """
    if df.empty or "sucursal" not in df.columns or "fecha" not in df.columns:
        return None

    missing_rows = []
    for sucursal in df["sucursal"].unique():
        sucursal_df = df[df["sucursal"] == sucursal].copy()
        if sucursal_df.empty:
            continue

        min_date = sucursal_df["fecha"].min()
        max_date = sucursal_df["fecha"].max()
        date_range = pd.date_range(start=min_date, end=max_date, freq="D")
        existing_dates = set(sucursal_df["fecha"].dt.date)

        for date in date_range:
            if date.date() not in existing_dates:
                missing_rows.append({"sucursal": sucursal, "fecha": date.date()})

    if not missing_rows:
        return None

    return pd.DataFrame(missing_rows)


def detect_duplicate_days(df: pd.DataFrame) -> pd.DataFrame | None:
    """Detect duplicate (sucursal, fecha) rows.

    Finds all rows where the (sucursal, fecha) combination appears more than once.

    Args:
        df: DataFrame with 'sucursal' and 'fecha' columns.

    Returns:
        DataFrame with all duplicate rows, or None if no duplicates found.

    Examples:
        >>> df = pd.DataFrame({
        ...     'sucursal': ['A', 'A', 'B'],
        ...     'fecha': pd.to_datetime(['2023-01-01', '2023-01-01', '2023-01-01'])
        ... })
        >>> duplicates = detect_duplicate_days(df)
        >>> len(duplicates)
        2

    """
    if df.empty:
        return None

    dup_mask = df.duplicated(subset=["sucursal", "fecha"], keep=False)
    if not dup_mask.any():
        return None

    return df[dup_mask].copy()


def detect_zscore_anomalies(
    df: pd.DataFrame, window: int = 60, threshold: float = 4.0
) -> pd.DataFrame | None:
    """Detect z-score anomalies in payment methods.

    For each sucursal and each payment method column, computes rolling mean and
    std over a window, then flags values where |z_score| >= threshold.

    Args:
        df: DataFrame with 'sucursal', 'fecha', and payment method columns.
        window: Rolling window size in days (default: 60).
        threshold: Z-score threshold for flagging anomalies (default: 4.0).

    Returns:
        DataFrame with columns: sucursal, fecha, method, value, z_score,
        or None if no anomalies found.

    Examples:
        >>> df = pd.DataFrame({
        ...     'sucursal': ['A'] * 100,
        ...     'fecha': pd.date_range('2023-01-01', periods=100),
        ...     'ingreso_efectivo': np.random.normal(1000, 100, 100)
        ... })
        >>> anomalies = detect_zscore_anomalies(df, window=30, threshold=3.0)

    """
    if df.empty:
        return None

    payment_methods = [col for col in MONEY_COLUMNS if col != "propinas" and col in df.columns]

    if not payment_methods:
        return None

    anomalies = []
    for sucursal in df["sucursal"].unique():
        sucursal_df = df[df["sucursal"] == sucursal].copy()
        if sucursal_df.empty:
            continue

        # Sort by fecha
        sucursal_df = sucursal_df.sort_values("fecha").reset_index(drop=True)

        for method in payment_methods:
            if method not in sucursal_df.columns:
                continue

            values = sucursal_df[method].values
            dates = sucursal_df["fecha"].values

            # Compute rolling mean and std
            rolling_mean = pd.Series(values).rolling(window=window, min_periods=1).mean()
            rolling_std = pd.Series(values).rolling(window=window, min_periods=1).std()

            # Compute z-scores
            rolling_std_safe = rolling_std.replace(0, np.nan)
            z_scores = (values - rolling_mean) / rolling_std_safe

            # Flag anomalies (filter out NaN values)
            z_scores_array = z_scores.values
            valid_mask = ~np.isnan(z_scores_array)
            anomaly_mask = valid_mask & (np.abs(z_scores_array) >= threshold)
            anomaly_indices = np.where(anomaly_mask)[0]

            for idx in anomaly_indices:
                anomalies.append({
                    "sucursal": sucursal,
                    "fecha": pd.Timestamp(dates[idx]).date(),
                    "method": method,
                    "value": float(values[idx]),
                    "z_score": float(z_scores_array[idx]),
                })

    if not anomalies:
        return None

    return pd.DataFrame(anomalies)


def detect_zero_method_flags(df: pd.DataFrame) -> pd.DataFrame | None:
    """Detect rows with tickets > 0 but certain payment methods are zero.

    Flags rows where num_tickets > 0 but payment methods like credito or debito
    are exactly zero, which may indicate data issues.

    Args:
        df: DataFrame with 'num_tickets' and payment method columns.

    Returns:
        DataFrame with flagged rows (all original columns), or None if no flags found.

    Examples:
        >>> df = pd.DataFrame({
        ...     'num_tickets': [10, 5, 0],
        ...     'ingreso_credito': [0, 100, 0],
        ...     'ingreso_debito': [0, 50, 0]
        ... })
        >>> flags = detect_zero_method_flags(df)

    """
    if df.empty or TICKET_COLUMN not in df.columns:
        return None

    # Check for rows with tickets > 0
    has_tickets = df[TICKET_COLUMN] > 0
    if not has_tickets.any():
        return None

    # Payment methods that should typically be non-zero if there are tickets
    suspicious_methods = [
        "ingreso_credito",
        "ingreso_debito",
    ]

    # Check which methods exist in the DataFrame
    available_methods = [m for m in suspicious_methods if m in df.columns]

    if not available_methods:
        return None

    # Find rows where tickets > 0 but all suspicious methods are zero
    flags = []
    for idx, row in df[has_tickets].iterrows():
        all_zero = all(row[method] == 0.0 for method in available_methods)
        if all_zero:
            flags.append(idx)

    if not flags:
        return None

    return df.loc[flags].copy()


# --------------------------------------------------------------------------- #
# QA checks
# --------------------------------------------------------------------------- #


def check_duplicates(df: pd.DataFrame) -> list[QAResult]:
    """Check for duplicate rows by sucursal and fecha.

    Validates that each (sucursal, fecha) combination appears only once in the
    dataset. Duplicates indicate data quality issues that need investigation.

    Args:
        df: DataFrame with 'sucursal' and 'fecha' columns.

    Returns:
        List of QAResult objects. Contains one ERROR result if duplicates are
        found, otherwise empty list.

    Examples:
        >>> df = pd.DataFrame({
        ...     'sucursal': ['A', 'A', 'B'],
        ...     'fecha': pd.to_datetime(['2023-01-01', '2023-01-01', '2023-01-01'])
        ... })
        >>> results = check_duplicates(df)
        >>> len(results)
        1
        >>> results[0].level
        'ERROR'

    """
    out: list[QAResult] = []
    dup_mask = df.duplicated(subset=["sucursal", "fecha"])
    dup_count = dup_mask.sum()
    if dup_count > 0:
        out.append(
            QAResult(
                "ERROR",
                f"Found {dup_count} duplicate rows for (sucursal, fecha).",
            )
        )
    return out


def check_non_negative(df: pd.DataFrame) -> list[QAResult]:
    """Check that money and ticket columns contain only non-negative values.

    Validates that all revenue columns (ingreso_*) and ticket counts are
    non-negative. Negative values indicate data errors that need correction.

    Args:
        df: DataFrame with money and ticket columns.

    Returns:
        List of QAResult objects. Contains one ERROR result per column with
        negative values, otherwise empty list.

    Examples:
        >>> df = pd.DataFrame({
        ...     'ingreso_efectivo': [100, -50, 200],
        ...     'num_tickets': [10, 5, 20]
        ... })
        >>> results = check_non_negative(df)
        >>> len(results)
        1
        >>> 'ingreso_efectivo' in results[0].message
        True

    """
    out: list[QAResult] = []
    for col in [*MONEY_COLUMNS, TICKET_COLUMN]:
        neg = df[df[col] < -1e-6]  # small tolerance
        if not neg.empty:
            out.append(
                QAResult(
                    "ERROR",
                    f"Column '{col}' has {len(neg)} negative values (min={neg[col].min():.2f}).",
                )
            )
    return out


def check_nulls(df: pd.DataFrame) -> list[QAResult]:
    """Check for null values in required columns.

    Validates that required columns do not contain null values. Nulls in
    'sucursal' or 'fecha' are treated as errors, while nulls in other columns
    are warnings.

    Args:
        df: DataFrame with required columns.

    Returns:
        List of QAResult objects. Contains ERROR results for nulls in
        'sucursal' or 'fecha', WARN results for other columns with nulls.

    Examples:
        >>> df = pd.DataFrame({
        ...     'sucursal': ['A', None, 'B'],
        ...     'fecha': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03']),
        ...     'ingreso_efectivo': [100, 200, None]
        ... })
        >>> results = check_nulls(df)
        >>> any(r.level == 'ERROR' for r in results)
        True
        >>> any(r.level == 'WARN' for r in results)
        True

    """
    out: list[QAResult] = []
    for col in REQUIRED_COLUMNS:
        null_count = df[col].isna().sum()
        if null_count > 0:
            level = "ERROR" if col in ("sucursal", "fecha") else "WARN"
            out.append(
                QAResult(
                    level,
                    f"Column '{col}' has {null_count} null values.",
                )
            )
    return out


def check_ticket_revenue_consistency(df: pd.DataFrame) -> list[QAResult]:
    """Check logical consistency between ticket counts and revenue.

    Validates that:
    - Rows with tickets > 0 have positive revenue (excluding tips)
    - Rows with revenue > 0 have positive ticket counts

    These checks help identify data quality issues such as missing ticket counts
    or closure days with zero revenue.

    Args:
        df: DataFrame with 'num_tickets' and 'total_sin_propinas' columns.

    Returns:
        List of QAResult objects. Contains WARN results for inconsistent rows.

    Examples:
        >>> df = pd.DataFrame({
        ...     'num_tickets': [10, 0, 5],
        ...     'total_sin_propinas': [1000, 500, 0]
        ... })
        >>> results = check_ticket_revenue_consistency(df)
        >>> len(results) >= 1
        True

    """
    out: list[QAResult] = []

    # If there are tickets, expect some revenue (excluding tips)
    mask_tickets_positive = df[TICKET_COLUMN] > 0
    weird_zero_rev = df[mask_tickets_positive & (df[TOTAL_NO_TIPS_COLUMN] <= 1e-6)]
    if not weird_zero_rev.empty:
        out.append(
            QAResult(
                "WARN",
                f"{len(weird_zero_rev)} rows have tickets > 0 but almost zero revenue "
                f"({TOTAL_NO_TIPS_COLUMN} <= 0). Check closure days or data errors.",
            )
        )

    # If there is revenue, expect some tickets
    mask_rev_positive = df[TOTAL_NO_TIPS_COLUMN] > 1e-6
    weird_zero_tickets = df[mask_rev_positive & (df[TICKET_COLUMN] <= 0)]
    if not weird_zero_tickets.empty:
        out.append(
            QAResult(
                "WARN",
                f"{len(weird_zero_tickets)} rows have revenue > 0 but zero tickets. "
                f"Check if num_tickets is missing or mis-mapped.",
            )
        )

    return out


def check_per_sucursal_ranges(df: pd.DataFrame) -> list[QAResult]:
    """Generate per-sucursal summary statistics.

    Computes aggregate statistics for each sucursal including:
    - Number of days (rows)
    - Date range (min and max fecha)
    - Total revenue (excluding tips)
    - Total ticket count
    - Average ticket value

    Args:
        df: DataFrame with 'sucursal', 'fecha', 'total_sin_propinas', and
            'num_tickets' columns.

    Returns:
        List containing one QAResult with WARN level containing the summary table.

    Examples:
        >>> df = pd.DataFrame({
        ...     'sucursal': ['A', 'A', 'B'],
        ...     'fecha': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-01']),
        ...     'total_sin_propinas': [1000, 2000, 1500],
        ...     'num_tickets': [10, 20, 15]
        ... })
        >>> results = check_per_sucursal_ranges(df)
        >>> len(results)
        1
        >>> 'Per-sucursal summary' in results[0].message
        True

    """
    out: list[QAResult] = []

    # Base per-sucursal aggregates
    base = df.groupby("sucursal").agg(
        rows=("fecha", "size"),
        fecha_min=("fecha", "min"),
        fecha_max=("fecha", "max"),
        total_sin_propinas=(TOTAL_NO_TIPS_COLUMN, "sum"),
        total_tickets=(TICKET_COLUMN, "sum"),
    )

    # Compute avg ticket explicitly
    base["avg_ticket"] = base["total_sin_propinas"] / base["total_tickets"].replace(0, np.nan)

    group = base.reset_index()

    out.append(
        QAResult(
            "WARN",
            "Per-sucursal summary (rows, date range, total revenue, total tickets, avg ticket):\n"
            + group.to_string(index=False),
        )
    )

    return out


# --------------------------------------------------------------------------- #
# Monthly sales table with elimination percentages
# --------------------------------------------------------------------------- #


def generate_monthly_sales_table(
    df: pd.DataFrame, output_dir: Path
) -> tuple[list[QAResult], Path | None]:
    """Generate a monthly sales table with elimination percentages and save to CSV.

    Creates a CSV file with:
    - One column per sucursal for sales
    - One column per sucursal for elimination percentages
    - Rows for each month

    If elimination columns are not present in the data, the CSV will only
    show sales totals.

    Args:
        df: DataFrame with 'sucursal', 'year_month', 'total_sin_propinas',
            and optionally 'pct_tickets_with_eliminations' columns.
        output_dir: Directory where the CSV file should be saved.

    Returns:
        Tuple of (list of QAResult objects, path to created CSV file or None if failed).

    Examples:
        >>> from pathlib import Path
        >>> df = pd.DataFrame({
        ...     'sucursal': ['A', 'A', 'B', 'B'],
        ...     'year_month': ['2023-01', '2023-02', '2023-01', '2023-02'],
        ...     'total_sin_propinas': [1000, 2000, 1500, 2500],
        ...     'pct_tickets_with_eliminations': [5.0, 3.0, 2.0, 4.0]
        ... })
        >>> results, csv_path = generate_monthly_sales_table(df, Path("."))
        >>> csv_path is not None
        True

    """
    out: list[QAResult] = []

    # Check if elimination columns exist
    has_eliminations = "pct_tickets_with_eliminations" in df.columns
    has_tickets_with_elim = "tickets_with_eliminations" in df.columns

    # Aggregate by sucursal and month
    if has_eliminations and has_tickets_with_elim:
        # For percentage, compute from aggregated ticket counts
        df_monthly = (
            df.groupby(["sucursal", "year_month"])
            .agg(
                total_sin_propinas=("total_sin_propinas", "sum"),
                total_tickets=("num_tickets", "sum"),
                tickets_with_eliminations=("tickets_with_eliminations", "sum"),
            )
            .reset_index()
        )

        # Calculate percentage: (sum of tickets_with_eliminations / sum of total_tickets) * 100
        df_monthly["pct_eliminations"] = (
            (df_monthly["tickets_with_eliminations"] / df_monthly["total_tickets"] * 100)
            .fillna(0.0)
            .replace([float("inf"), -float("inf")], 0.0)
            .round(2)
        )
    else:
        df_monthly = (
            df.groupby(["sucursal", "year_month"])
            .agg(
                total_sin_propinas=("total_sin_propinas", "sum"),
            )
            .reset_index()
        )
        df_monthly["pct_eliminations"] = None
        has_eliminations = False  # Don't show elimination section if we can't compute it

    # Pivot: months as rows, sucursales as columns
    # First pivot for sales
    sales_pivot = df_monthly.pivot_table(
        index="year_month",
        columns="sucursal",
        values="total_sin_propinas",
        aggfunc="sum",
        fill_value=0.0,
    )

    # Build output DataFrame for CSV
    csv_data = {"Month": sorted(sales_pivot.index)}

    # Add sales columns
    for suc in sorted(sales_pivot.columns):
        csv_data[f"Sales_{suc}"] = [sales_pivot.loc[month, suc] for month in csv_data["Month"]]

    # Add elimination percentage columns if available
    if has_eliminations:
        elim_pivot = df_monthly.pivot_table(
            index="year_month",
            columns="sucursal",
            values="pct_eliminations",
            aggfunc="mean",  # Should be same value per month/sucursal, but use mean for safety
            fill_value=0.0,
        )
        for suc in sorted(elim_pivot.columns):
            csv_data[f"ElimPct_{suc}"] = [elim_pivot.loc[month, suc] for month in csv_data["Month"]]

    # Create DataFrame and save to CSV
    csv_df = pd.DataFrame(csv_data)
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / "monthly_sales_table.csv"

    try:
        csv_df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        out.append(QAResult("WARN", f"Monthly sales table saved to: {csv_path}"))
        return out, csv_path
    except OSError as e:
        out.append(QAResult("ERROR", f"Failed to write monthly sales table CSV: {e}"))
        return out, None


# --------------------------------------------------------------------------- #
# Sampling random months for manual QA
# --------------------------------------------------------------------------- #


def sample_months(
    df: pd.DataFrame,
    n_months: int,
    sucursal: str | None,
    seed: int | None,
) -> list[QAResult]:
    """Sample random sucursal-month combinations for manual QA review.

    Randomly selects n_months sucursal-month combinations and generates detailed
    summaries including totals, averages, payment method breakdowns, and sample
    daily rows for manual inspection.

    Args:
        df: DataFrame with payment data including 'sucursal', 'year_month',
            and payment columns.
        n_months: Number of random sucursal-month combinations to sample.
            If <= 0, returns empty list.
        sucursal: Optional sucursal name to filter before sampling. If None,
            samples from all sucursales.
        seed: Random seed for reproducible sampling. If None, uses random seed.

    Returns:
        List of QAResult objects. Contains one WARN result with detailed
        summaries, or warning messages if filtering/sampling fails.

    Examples:
        >>> df = pd.DataFrame({
        ...     'sucursal': ['A', 'A', 'B'],
        ...     'year_month': ['2023-01', '2023-02', '2023-01'],
        ...     'total_sin_propinas': [1000, 2000, 1500],
        ...     'num_tickets': [10, 20, 15]
        ... })
        >>> results = sample_months(df, n_months=2, sucursal=None, seed=42)
        >>> len(results)
        1
        >>> 'Random sample' in results[0].message
        True

    """
    out: list[QAResult] = []
    if n_months <= 0:
        return out

    subset = df.copy()
    if sucursal is not None:
        subset = subset[subset["sucursal"] == sucursal]
        if subset.empty:
            out.append(
                QAResult(
                    "WARN",
                    f"No rows found for sucursal='{sucursal}' when sampling months.",
                )
            )
            return out

    unique_pairs = subset[["sucursal", "year_month"]].drop_duplicates()
    if unique_pairs.empty:
        out.append(QAResult("WARN", "No (sucursal, month) pairs available to sample."))
        return out

    rng = np.random.default_rng(seed)
    n_sample = min(n_months, len(unique_pairs))
    sample_idx = rng.choice(len(unique_pairs), size=n_sample, replace=False)
    sampled = unique_pairs.iloc[sample_idx]

    lines: list[str] = []
    lines.append(f"\nRandom sample of {n_sample} sucursal-month combinations:")
    for _, row in sampled.iterrows():
        suc = row["sucursal"]
        ym = row["year_month"]
        mdf = subset[(subset["sucursal"] == suc) & (subset["year_month"] == ym)].copy()

        # Month totals
        total_sin_propinas = mdf[TOTAL_NO_TIPS_COLUMN].sum()
        total_propinas = mdf["propinas"].sum()
        total_tickets = mdf[TICKET_COLUMN].sum()

        # Average ticket
        avg_ticket = total_sin_propinas / total_tickets if total_tickets > 0 else np.nan
        avg_ticket_str = f"{avg_ticket:.2f}" if not np.isnan(avg_ticket) else "NA"

        # Elimination data
        has_eliminations = "tickets_with_eliminations" in mdf.columns
        if has_eliminations:
            total_tickets_with_elim = mdf["tickets_with_eliminations"].sum()
            pct_eliminations = (
                (total_tickets_with_elim / total_tickets * 100) if total_tickets > 0 else 0.0
            )
            elim_info = (
                f"Tickets with eliminations: {int(total_tickets_with_elim)}\n"
                f"Elimination percentage:    {pct_eliminations:.2f}%\n"
            )
        else:
            elim_info = "Elimination data: Not available\n"

        # Monthly breakdown by payment form (excluding propinas)
        monthly_forms = mdf[
            [
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
        ].sum()

        lines.append(
            f"\n=== {suc} — {ym} ===\n"
            f"Days: {len(mdf)}\n"
            f"Total sin propinas: {total_sin_propinas:.2f}\n"
            f"Total propinas:     {total_propinas:.2f}\n"
            f"Total tickets:      {int(total_tickets)}\n"
            f"Avg ticket (sin propinas): {avg_ticket_str}\n"
            f"{elim_info}"
            f"Breakdown por forma de pago (sin propinas):\n"
            f"  efectivo        : {monthly_forms['ingreso_efectivo']:.2f}\n"
            f"  credito         : {monthly_forms['ingreso_credito']:.2f}\n"
            f"  debito          : {monthly_forms['ingreso_debito']:.2f}\n"
            f"  amex            : {monthly_forms['ingreso_amex']:.2f}\n"
            f"  uber eats       : {monthly_forms['ingreso_ubereats']:.2f}\n"
            f"  rappi           : {monthly_forms['ingreso_rappi']:.2f}\n"
            f"  transferencia   : {monthly_forms['ingreso_transferencia']:.2f}\n"
            f"  subsidio TEC    : {monthly_forms['ingreso_SubsidioTEC']:.2f}\n"
            f"  otros           : {monthly_forms['ingreso_otros']:.2f}\n"
            f"First 5 rows:\n"
            + mdf.sort_values("fecha")
            .head(5)[
                [
                    "fecha",
                    TOTAL_NO_TIPS_COLUMN,
                    "propinas",
                    TICKET_COLUMN,
                ]
            ]
            .to_string(index=False)
        )

    out.append(QAResult("WARN", "\n".join(lines)))
    return out


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #


def run_qa(
    file_name: str,
    sample_months_n: int,
    sucursal: str | None,
    seed: int | None,
    proc_payments_dir: Path | None = None,
) -> tuple[list[QAResult], Path, Path | None]:
    """Run all QA checks on aggregated payments data.

    Executes a comprehensive set of quality assurance checks including:
    - Duplicate detection
    - Null value checks
    - Non-negative value validation
    - Ticket-revenue consistency checks
    - Per-sucursal summary statistics
    - Monthly sales table with elimination percentages (saved to CSV)
    - Random month sampling for manual review

    Args:
        file_name: Name of the CSV file in proc_payments_dir to load.
        sample_months_n: Number of random sucursal-month combinations to sample.
        sucursal: Optional sucursal name to filter for sampling.
        seed: Random seed for reproducible sampling.
        proc_payments_dir: Directory containing processed payments CSV files.
            If None, defaults to "data/c_processed/payments" relative to current directory.

    Returns:
        Tuple of (list of QAResult objects, path to loaded CSV file,
            path to monthly sales CSV or None).

    Examples:
        >>> from pathlib import Path
        >>> results, input_path, sales_csv_path = run_qa(
        ...     file_name="aggregated_payments_daily.csv",
        ...     sample_months_n=3,
        ...     sucursal=None,
        ...     seed=42,
        ...     proc_payments_dir=Path("data/c_processed/payments")
        ... )
        >>> len(results) > 0
        True
        >>> input_path.exists()
        True

    """
    if proc_payments_dir is None:
        # Default to standard directory structure
        proc_payments_dir = Path("data/c_processed/payments")

    csv_path = proc_payments_dir / file_name
    df = load_payments(csv_path)

    results: list[QAResult] = []
    results.extend(check_duplicates(df))
    results.extend(check_nulls(df))
    results.extend(check_non_negative(df))
    results.extend(check_ticket_revenue_consistency(df))
    results.extend(check_per_sucursal_ranges(df))

    # Generate monthly sales table CSV
    monthly_results, sales_csv_path = generate_monthly_sales_table(df, proc_payments_dir)
    results.extend(monthly_results)

    results.extend(sample_months(df, sample_months_n, sucursal, seed))

    return results, csv_path, sales_csv_path


def main(argv: list[str] | None = None) -> None:
    """Execute the QA payments command-line tool.

    Parses command-line arguments and executes QA checks on aggregated payments
    data. Prints validation results, summary statistics, monthly sales table,
    and random month samples.

    Command-line arguments:
        --file: CSV file name in processed payments directory
            (default: aggregated_payments_daily.csv)
        --sucursal: Optional sucursal name to filter sampling (default: None)
        --sample-months: Number of random months to sample (default: 3)
        --seed: Random seed for sampling (default: 42)
        --data-root: Root directory for ETL data (default: "data")

    Exits with code 1 if errors are found, 0 otherwise.

    Examples:
        $ python -m pos_qa.qa_payments
        $ python -m pos_qa.qa_payments --sucursal Carreta --sample-months 5
        $ python -m pos_qa.qa_payments --file my_payments.csv --sample-months 0
        $ python -m pos_qa.qa_payments --data-root /path/to/data

    """
    parser = argparse.ArgumentParser(description="QA checks for aggregated daily payments (POS).")
    parser.add_argument(
        "--file",
        default="aggregated_payments_daily.csv",
        help="CSV file name inside processed payments directory. Default: "
        "aggregated_payments_daily.csv",
    )
    parser.add_argument(
        "--data-root",
        type=str,
        default="data",
        help="Root directory for ETL data (default: 'data'). "
        "Processed payments will be in <data-root>/c_processed/payments",
    )
    parser.add_argument(
        "--sucursal",
        default=None,
        help="Optional sucursal name to focus sampling on (e.g. 'Carreta').",
    )
    parser.add_argument(
        "--sample-months",
        type=int,
        default=3,
        help="Number of random sucursal-month combinations to print for manual QA.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for month sampling.",
    )

    args = parser.parse_args(argv)

    # Build proc_payments_dir from data_root
    data_root = Path(args.data_root)
    proc_payments_dir = data_root / "c_processed" / "payments"

    print("Running payments QA...")
    print(f"  file        : {args.file}")
    print(f"  data-root   : {data_root}")
    print(f"  sucursal    : {args.sucursal or 'ALL'}")
    print(f"  sampleMonths: {args.sample_months}")
    print(f"  seed        : {args.seed}")

    try:
        results, path, sales_csv_path = run_qa(
            file_name=args.file,
            sample_months_n=args.sample_months,
            sucursal=args.sucursal,
            seed=args.seed,
            proc_payments_dir=proc_payments_dir,
        )
    except Exception as e:
        import traceback

        print("\nFATAL error while running QA.")
        print(f"  Exception type   : {type(e).__name__}")
        print(f"  Exception message: {e}")
        print("  Full traceback:")
        traceback.print_exc()
        raise SystemExit(1) from e

    errors = [r for r in results if r.level == "ERROR"]
    warns = [r for r in results if r.level == "WARN"]

    print(f"\nLoaded file: {path}")
    if sales_csv_path:
        print(f"Monthly sales table CSV: {sales_csv_path}")
    print(f"QA results: {len(errors)} ERROR(S), {len(warns)} WARNING(S)\n")

    for r in results:
        prefix = "[ERROR]" if r.level == "ERROR" else "[WARN ]"
        print(prefix, r.message)

    if errors:
        raise SystemExit(1)
    else:
        raise SystemExit(0)


if __name__ == "__main__":
    main()
