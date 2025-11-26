"""ETL pipeline module for POS data processing.

This module provides a clean public API for running ETL operations on POS data,
including payments extraction, cleaning, and aggregation.
"""

from pos_core.etl.api import (
    PaymentsETLConfig,
    PaymentsPaths,
    build_payments_dataset,
)

__all__ = ["PaymentsPaths", "PaymentsETLConfig", "build_payments_dataset"]
