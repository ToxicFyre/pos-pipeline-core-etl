"""Data loading utilities for forecasting pipeline."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd

from pos_etl import config


def load_payments_data(csv_path: Optional[Path] = None) -> pd.DataFrame:
    """Load payments data from CSV file.
    
    Args:
        csv_path: Path to aggregated_payments_daily.csv. If None, uses default from config.
        
    Returns:
        DataFrame with payments data, sorted by sucursal and fecha
        
    Raises:
        FileNotFoundError: If the CSV file does not exist
    """
    if csv_path is None:
        csv_path = config.PROC_PAYMENTS_DIR / "aggregated_payments_daily.csv"
    
    if not csv_path.exists():
        raise FileNotFoundError(f"Payments data not found at {csv_path}")
    
    # Load data
    df = pd.read_csv(csv_path)
    df["fecha"] = pd.to_datetime(df["fecha"])
    df = df.sort_values(["sucursal", "fecha"])
    
    return df

