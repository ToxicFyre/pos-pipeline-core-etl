r"""Marts (Gold) layer: Aggregate transfer data into weekly sucursal table.

This module is part of the Marts (Gold) layer in the ETL pipeline.
It produces aggregated transfer cost summaries by branch and category.

Data directory mapping:
    Input: data/b_clean/ → Staging (Silver) layer (cleaned transfers)
    Output: data/c_processed/ → Marts (Gold) - Transfer pivot tables

This module processes cleaned transfer CSV files (from staging/transfer_cleaner.py)
and creates a pivot table showing transfer costs by branch and product category.

The aggregation:
1. Maps branch names to codes (K, N, C, Q, PV, HZ, CC)
2. Buckets transfers by origin warehouse and department into categories
3. Creates a "Gasto de Insumos" pivot: categories as rows, branches as columns
4. Includes totals row and column

Examples:
    Basic usage:
        python -m pos_etl.c_load.aggregate_transfer_data \\
            transfers_clean.csv -o output.xlsx

    Include CEDIS as destination:
        python -m pos_etl.c_load.aggregate_transfer_data \\
            transfers_clean.csv --include-cedis -o output.xlsx

"""

#!/usr/bin/env python3
import argparse

import pandas as pd

# Gasto de Insumos layout: categories as ROWS, branches as COLUMNS
# Category row order (display labels)
CATEGORY_ROW_ORDER = [
    "No-Procesados (Abarrotes)",
    "No-Procesados (Harinas)",
    "No-Procesados (Bebidas)",
    "No-Procesados (Deshechables)",
    "No-Procesados (Papelería)",
    "No-Procesados (Químicos)",
    "No-Procesados (Verdura)",
    "No-Procesados (Refri y Conge)",
    "Cafe",
    "Comida Salada",
    "Repostería",
    "Panadería Dulce y Salada",
]

# Branch column order: (code, display_name)
BRANCH_COL_ORDER = ["Kavia", "PV", "Qin", "Zambrano", "Carreta", "Nativa", "Crediclub"]
SUC_TO_DISPLAY = {
    "K": "Kavia",
    "PV": "PV",
    "Q": "Qin",
    "HZ": "Zambrano",
    "C": "Carreta",
    "N": "Nativa",
    "CC": "Crediclub",
}

# Internal bucket key -> display row label
BUCKET_TO_ROW_LABEL = {
    "ABARROTES (No-PROC)": "No-Procesados (Abarrotes)",
    "HARINAS (No-PROC)": "No-Procesados (Harinas)",
    "BEBIDAS (No-PROC)": "No-Procesados (Bebidas)",
    "DESECHABLE (No-PROC)": "No-Procesados (Deshechables)",
    "PAPELERIA (No-PROC)": "No-Procesados (Papelería)",
    "QUIMICOS (No-PROC)": "No-Procesados (Químicos)",
    "VERDURA (No-PROC)": "No-Procesados (Verdura)",
    "REFRICONGE": "No-Procesados (Refri y Conge)",
    "TOSTADOR": "Cafe",
    "COMIDA SALADA": "Comida Salada",
    "REPO": "Repostería",
    "PAN DULCE Y SALADA": "Panadería Dulce y Salada",
}

# Internal category keys (for pivot aggregation)
INTERNAL_BUCKET_ORDER = list(BUCKET_TO_ROW_LABEL.keys())

# Department -> No-PROC column mapping
DEPT_TO_NO_PROC_COL = {
    "ABARROTES": "ABARROTES (No-PROC)",
    "AZUCAR Y HARINA": "HARINAS (No-PROC)",
    "BEBIDAS": "BEBIDAS (No-PROC)",
    "DESECHABLE": "DESECHABLE (No-PROC)",
    "DESECHABLES": "DESECHABLE (No-PROC)",
    "PAPELERIA": "PAPELERIA (No-PROC)",
    "QUIMICOS": "QUIMICOS (No-PROC)",
    "VERDURA": "VERDURA (No-PROC)",
}

# Mapping from full branch names to codes
SUC_MAP = {
    "PANEM - HOTEL KAVIA N": "K",
    "PANEM - PLAZA NATIVA": "N",
    "PANEM - LA CARRETA N": "C",
    "PANEM - PLAZA QIN N": "Q",
    "PANEM - PUNTO VALLE": "PV",
    "PANEM - HOSPITAL ZAMBRANO N": "HZ",
    "PANEM - CREDI CLUB": "CC",
}


def normalize(s: pd.Series) -> pd.Series:
    """Normalize a pandas Series to uppercase, stripped strings.

    Args:
        s: Series to normalize.

    Returns:
        Series with values converted to uppercase, stripped strings.

    """
    return s.astype(str).str.strip().str.upper()


def bucket_row(origen: str, depto: str) -> str | None:
    """Categorize a transfer row based on origin warehouse and department.

    Maps combinations of origin warehouse and department to product categories:
    - ALMACEN PRODUCTO TERMINADO + COCINA -> COMIDA SALADA
    - ALMACEN PRODUCTO TERMINADO + REPOSTERIA -> REPO
    - ALMACEN PRODUCTO TERMINADO + PANADERIA DULCE Y SALADA -> PAN DULCE Y SALADA
    - ALMACEN GENERAL + (each department) -> specific (No-PROC) column
    - ALMACEN GENERAL + REFRIGERADOS Y CONGELADOS -> REFRICONGE
    - ALMACEN GENERAL + TOSTADOR -> TOSTADOR

    Args:
        origen: Origin warehouse name (normalized).
        depto: Department name (normalized).

    Returns:
        Category name if mapped, None if unmapped.

    """
    if origen == "ALMACEN PRODUCTO TERMINADO":
        if depto == "COCINA":
            return "COMIDA SALADA"
        if depto == "REPOSTERIA":
            return "REPO"
        if depto == "PANADERIA DULCE Y SALADA":
            return "PAN DULCE Y SALADA"
    elif origen == "ALMACEN GENERAL":
        if depto in DEPT_TO_NO_PROC_COL:
            return DEPT_TO_NO_PROC_COL[depto]
        if depto == "REFRIGERADOS Y CONGELADOS":
            return "REFRICONGE"
        if depto == "TOSTADOR":
            return "TOSTADOR"
    return None  # unmapped


def build_table(csv_path: str, include_cedis: bool = False) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build pivot table from transfer CSV data.

    Produces "Gasto de Insumos" layout: categories as rows, branches as columns.
    Also returns unmapped rows that couldn't be categorized.

    Args:
        csv_path: Path to cleaned transfer CSV file.
        include_cedis: If True, include rows where destination is CEDIS.
            Defaults to False.

    Returns:
        Tuple of (pivot_table, unmapped_rows):
        - pivot_table: DataFrame with categories as rows, branches as columns
        - unmapped_rows: DataFrame with rows that couldn't be categorized

    Raises:
        SystemExit: If required columns are missing from the CSV.

    """
    df = pd.read_csv(csv_path, encoding="utf-8-sig")

    # Normalize
    for col in ["Almacén origen", "Sucursal destino", "Departamento"]:
        if col not in df.columns:
            raise SystemExit(f"Missing required column: {col}")
        df[col] = normalize(df[col])

    # Destino -> code
    df["SUC"] = df["Sucursal destino"].map(SUC_MAP)
    if not include_cedis:
        df = df[df["SUC"].notna()].copy()

    # Bucket
    df["BUCKET"] = [
        bucket_row(o, d) for o, d in zip(df["Almacén origen"], df["Departamento"], strict=False)
    ]

    # Unmapped report
    unmapped = df[df["BUCKET"].isna()].copy()

    # Money (Costo already equals Cantidad * Costo unitario)
    df["Monto"] = pd.to_numeric(df["Costo"], errors="coerce").fillna(0)

    # Aggregate: categories as rows, branches as columns (Gasto de Insumos layout)
    piv = df.dropna(subset=["SUC", "BUCKET"]).pivot_table(
        index="BUCKET", columns="SUC", values="Monto", aggfunc="sum", fill_value=0.0
    )

    # Ensure all expected buckets exist
    for b in INTERNAL_BUCKET_ORDER:
        if b not in piv.index:
            piv.loc[b] = 0.0
    piv = piv.reindex(INTERNAL_BUCKET_ORDER).fillna(0.0)

    # Rename index to display labels
    piv = piv.rename(index=BUCKET_TO_ROW_LABEL)

    # Rename columns from branch codes to display names
    piv = piv.rename(columns=SUC_TO_DISPLAY)

    # Ensure all expected branch columns exist and are in order
    for col in BRANCH_COL_ORDER:
        if col not in piv.columns:
            piv[col] = 0.0
    piv = piv.reindex(columns=BRANCH_COL_ORDER).fillna(0.0)

    # Totals
    piv["TOTAL"] = piv.sum(axis=1)
    total_row = piv.sum(numeric_only=True)
    piv.loc["TOTAL"] = total_row

    # Row order (categories + TOTAL)
    piv = piv.reindex([*CATEGORY_ROW_ORDER, "TOTAL"]).fillna(0.0)

    # Round to 2 decimals
    piv = piv.round(2)

    return piv, unmapped[["Almacén origen", "Departamento", "Sucursal destino", "Costo"]]


def aggregate_transfers(
    csv_path: str, output_path: str | None = None, include_cedis: bool = False
) -> pd.DataFrame:
    """Aggregate transfer data into pivot table.

    High-level wrapper for build_table that handles output writing.

    Args:
        csv_path: Path to cleaned transfer CSV file.
        output_path: Optional output path (Excel or CSV based on extension).
        include_cedis: If True, include rows where destination is CEDIS.

    Returns:
        DataFrame with the aggregated pivot table.

    """
    table, _ = build_table(csv_path, include_cedis=include_cedis)

    if output_path:
        if output_path.lower().endswith(".xlsx"):
            with pd.ExcelWriter(output_path, engine="xlsxwriter") as xw:
                table.to_excel(xw, sheet_name="Tabla", index=True)
        else:
            table.to_csv(output_path, index=True)

    return table


def main() -> None:
    """Execute the transfer aggregation command-line tool.

    Parses arguments, builds the pivot table, and writes output to file
    (Excel or CSV based on extension) or prints to stdout.
    """
    ap = argparse.ArgumentParser(
        description="Aggregate CEDIS transfers into the weekly sucursal table."
    )
    ap.add_argument("csv_path", help="Path to TransfersIssued_CEDIS_*.csv")
    ap.add_argument(
        "-o", "--output", help="Write Excel/CSV at this path (by extension)", default=None
    )
    ap.add_argument("--include-cedis", action="store_true", help="Keep rows whose destino is CEDIS")
    args = ap.parse_args()

    table, unmapped = build_table(args.csv_path, include_cedis=args.include_cedis)

    # Print quick summary
    print("\n=== Aggregated Table ===")
    print(table.to_string())

    if len(unmapped):
        lost = pd.to_numeric(unmapped["Costo"], errors="coerce").fillna(0).sum()
        print(f"\nWARNING: {len(unmapped)} unmapped rows (total ${lost:,.2f}). Top 10:")
        print(unmapped.head(10).to_string())
    else:
        print("\nAll rows mapped cleanly.")

    if args.output:
        if args.output.lower().endswith(".xlsx"):
            with pd.ExcelWriter(args.output, engine="xlsxwriter") as xw:
                table.to_excel(xw, sheet_name="Tabla", index=True)
        else:
            table.to_csv(args.output, index=True)
        print(f"\nWrote {args.output}")


if __name__ == "__main__":
    main()
