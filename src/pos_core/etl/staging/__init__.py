"""Staging (Silver) layer - Cleaning and normalizing into b_clean.

This layer handles data cleaning and standardization:
- Parse Excel files and extract structured data
- Normalize column names and data types
- Remove invalid rows and handle missing values
- Standardize date formats and encodings

Data directory mapping:
    data/b_clean/ → Staging (Silver) layer - Cleaned and standardized tables.

Core Facts (Silver+ grain)
--------------------------
The staging layer output IS the **core fact** for each domain:

1. **Payments** (``fact_payments_ticket``):
   - Grain: ticket × payment method
   - Key: ``(sucursal, operating_date, order_index, payment_method)``
   - One row per payment line on a ticket
   - The POS does not expose item-level payment data

2. **Sales** (``fact_sales_item_line``):
   - Grain: item/modifier line
   - Key: ``(sucursal, operating_date, order_id, item_key, [modifier fields])``
   - One row per item or modifier on a ticket
   - Multiple rows can share the same ticket_id/order_id

The staging layer produces clean, normalized data at the atomic grain for each
domain. All aggregations beyond these grains are **Marts (Gold)**.
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
