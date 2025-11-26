"""Aggregate CEDIS transfer data into weekly sucursal table.

This module processes cleaned transfer CSV files (from pos_excel_transfer_cleaner.py)
and creates a pivot table showing transfer costs by branch and product category.

The aggregation:
1. Maps branch names to codes (K, N, C, Q, PV, HZ, CC)
2. Buckets transfers by origin warehouse and department into categories
3. Creates a pivot table with branches as rows and categories as columns
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
import sys
import pandas as pd

# Row order for output pivot table (branch codes)
ROW_ORDER = ['K', 'N', 'C', 'Q', 'PV', 'HZ', 'CC', 'TOTAL']

# Column order for output pivot table (product categories)
COL_ORDER = ['NO-PROC', 'REFRICONGE', 'TOSTADOR', 'COMIDA SALADA', 'REPO', 'PAN DULCE Y SALADA']

# Mapping from full branch names to codes
SUC_MAP = {
    'PANEM - HOTEL KAVIA N': 'K',
    'PANEM - PLAZA NATIVA': 'N',
    'PANEM - LA CARRETA N': 'C',
    'PANEM - PLAZA QIN N': 'Q',
    'PANEM - PUNTO VALLE': 'PV',
    'PANEM - HOSPITAL ZAMBRANO N': 'HZ',
    'PANEM - CREDI CLUB': 'CC',
}

# Departments that map to NO-PROC category
NO_PROC_SET = {
    'ABARROTES','AZUCAR Y HARINA','BEBIDAS','DESECHABLE','DESECHABLES',
    'PAPELERIA','QUIMICOS','VERDURA'
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
    - ALMACEN GENERAL + (departments in NO_PROC_SET) -> NO-PROC
    - ALMACEN GENERAL + REFRIGERADOS Y CONGELADOS -> REFRICONGE
    - ALMACEN GENERAL + TOSTADOR -> TOSTADOR

    Args:
        origen: Origin warehouse name (normalized).
        depto: Department name (normalized).

    Returns:
        Category name if mapped, None if unmapped.
    """
    if origen == 'ALMACEN PRODUCTO TERMINADO':
        if depto == 'COCINA':
            return 'COMIDA SALADA'
        if depto == 'REPOSTERIA':
            return 'REPO'
        if depto == 'PANADERIA DULCE Y SALADA':
            return 'PAN DULCE Y SALADA'
    elif origen == 'ALMACEN GENERAL':
        if depto in NO_PROC_SET:
            return 'NO-PROC'
        if depto == 'REFRIGERADOS Y CONGELADOS':
            return 'REFRICONGE'
        if depto == 'TOSTADOR':
            return 'TOSTADOR'
    return None  # unmapped

def build_table(csv_path: str, include_cedis: bool=False) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build pivot table from transfer CSV data.

    Processes the transfer CSV file and creates a pivot table showing transfer
    costs by branch (rows) and product category (columns). Also returns unmapped
    rows that couldn't be categorized.

    Args:
        csv_path: Path to cleaned transfer CSV file.
        include_cedis: If True, include rows where destination is CEDIS.
            Defaults to False.

    Returns:
        Tuple of (pivot_table, unmapped_rows):
        - pivot_table: DataFrame with branches as rows, categories as columns
        - unmapped_rows: DataFrame with rows that couldn't be categorized

    Raises:
        SystemExit: If required columns are missing from the CSV.
    """
    df = pd.read_csv(csv_path)

    # Normalize
    for col in ['Almacén origen','Sucursal destino','Departamento']:
        if col not in df.columns:
            raise SystemExit(f"Missing required column: {col}")
        df[col] = normalize(df[col])

    # Destino -> code
    df['SUC'] = df['Sucursal destino'].map(SUC_MAP)
    if not include_cedis:
        df = df[df['SUC'].notna()].copy()

    # Bucket
    df['BUCKET'] = [bucket_row(o, d) for o, d in zip(df['Almacén origen'], df['Departamento'])]

    # Unmapped report
    unmapped = df[df['BUCKET'].isna()].copy()

    # Money (Costo already equals Cantidad * Costo unitario)
    df['Monto'] = pd.to_numeric(df['Costo'], errors='coerce').fillna(0)

    # Aggregate
    piv = (df.dropna(subset=['SUC','BUCKET'])
             .pivot_table(index='SUC', columns='BUCKET', values='Monto',
                          aggfunc='sum', fill_value=0.0))

    # Ensure all expected columns exist and in order
    for c in COL_ORDER:
        if c not in piv.columns:
            piv[c] = 0.0
    piv = piv[COL_ORDER]

    # Totals
    piv['TOTAL'] = piv.sum(axis=1)
    total_row = piv.sum(numeric_only=True)
    piv.loc['TOTAL'] = total_row

    # Row order
    piv = piv.reindex(ROW_ORDER)

    # Round to 2 decimals
    piv = piv.round(2)

    return piv, unmapped[['Almacén origen','Departamento','Sucursal destino','Costo']]

def main() -> None:
    """Main entry point for transfer aggregation command-line tool.

    Parses arguments, builds the pivot table, and writes output to file
    (Excel or CSV based on extension) or prints to stdout.
    """
    ap = argparse.ArgumentParser(description="Aggregate CEDIS transfers into the weekly sucursal table.")
    ap.add_argument("csv_path", help="Path to TransfersIssued_CEDIS_*.csv")
    ap.add_argument("-o","--output", help="Write Excel/CSV at this path (by extension)", default=None)
    ap.add_argument("--include-cedis", action="store_true", help="Keep rows whose destino is CEDIS")
    args = ap.parse_args()

    table, unmapped = build_table(args.csv_path, include_cedis=args.include_cedis)

    # Print quick summary
    print("\n=== Aggregated Table ===")
    print(table.to_string())

    if len(unmapped):
        lost = pd.to_numeric(unmapped['Costo'], errors='coerce').fillna(0).sum()
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
