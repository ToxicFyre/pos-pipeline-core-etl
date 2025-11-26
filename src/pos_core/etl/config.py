"""Path configuration for POS ETL pipeline.

This module defines all directory paths used throughout the ETL pipeline.
Paths are resolved relative to the project root (two levels up from this file).

The directory structure follows an ETL naming convention:
- a_raw: Raw data files downloaded from POS API
- b_clean: Cleaned and normalized data files
- c_processed: Aggregated and processed datasets ready for analysis

Examples:
    >>> from pos_etl import config
    >>> # Access raw payments directory
    >>> raw_payments = config.RAW_PAYMENTS_DIR
    >>> # Access processed sales directory
    >>> processed_sales = config.PROC_SALES_DIR
    >>> # Load sucursales configuration
    >>> import json
    >>> sucursales = json.loads(config.SUCURSALES_PATH.read_text())
"""

from pathlib import Path

# Project root: two levels up from this file (src/pos_etl/config.py -> project root)
ROOT = Path(__file__).resolve().parents[2]

# Top-level directories
DATA_DIR = ROOT / "data"
MODELS_DIR = ROOT / "models"

# ETL stage directories (following a_raw, b_clean, c_processed convention)
RAW_DIR = DATA_DIR / "a_raw"
CLEAN_DIR = DATA_DIR / "b_clean"
PROC_DIR = DATA_DIR / "c_processed"

# Raw data directories by data type
RAW_SALES_DIR = RAW_DIR / "sales"
RAW_PAYMENTS_DIR = RAW_DIR / "payments"
RAW_TRANSFERS_DIR = RAW_DIR / "transfers"

# Clean data directories by data type
CLEAN_SALES_DIR = CLEAN_DIR / "sales"
CLEAN_PAYMENTS_DIR = CLEAN_DIR / "payments"
CLEAN_TRANSFERS_DIR = CLEAN_DIR / "transfers"

# Processed/aggregated data directories by data type
PROC_SALES_DIR = PROC_DIR / "sales"
PROC_PAYMENTS_DIR = PROC_DIR / "payments"
PROC_TRANSFERS_DIR = PROC_DIR / "transfers"

# Batch processing directories for payments (organized by branch/code/date ranges)
RAW_PAYMENTS_DIR_BATCH = RAW_PAYMENTS_DIR / "batch"
CLEAN_PAYMENTS_DIR_BATCH = CLEAN_PAYMENTS_DIR / "batch"

# Configuration and utility files
UTILS_DIR = ROOT / "utils"
SUCURSALES_PATH = UTILS_DIR / "sucursales.json"
