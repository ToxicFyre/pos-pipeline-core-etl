"""Staging (Silver) layer - Cleaning and normalizing into b_clean.

This layer handles data cleaning and standardization:
- Parse Excel files and extract structured data
- Normalize column names and data types
- Remove invalid rows and handle missing values
- Standardize date formats and encodings

Data directory mapping:
    data/b_clean/ → Staging (Silver) layer - Cleaned and standardized tables.

The staging layer produces clean, normalized data ready for further modeling.
"""

from pos_core.etl.staging.cleaning_utils import (
    neutralize,
    normalize_spanish_name,
    strip_invisibles,
    to_date,
    to_float,
)
from pos_core.etl.staging.payments_cleaner import (
    clean_payments_directory,
    transform_detalle_por_forma_pago,
)
from pos_core.etl.staging.payments_cleaner import (
    output_name_for as payments_output_name_for,
)
from pos_core.etl.staging.sales_cleaner import (
    output_name_for as sales_output_name_for,
)
from pos_core.etl.staging.sales_cleaner import (
    transform_detalle_ventas,
)

__all__ = [
    # Cleaning utilities
    "strip_invisibles",
    "normalize_spanish_name",
    "to_date",
    "to_float",
    "neutralize",
    # Payments cleaner
    "transform_detalle_por_forma_pago",
    "clean_payments_directory",
    "payments_output_name_for",
    # Sales cleaner
    "transform_detalle_ventas",
    "sales_output_name_for",
]
