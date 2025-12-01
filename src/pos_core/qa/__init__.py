"""QA module for data quality assurance.

This module provides quality assurance checks for POS data.

Example:
    >>> from pos_core import DataPaths
    >>> from pos_core.payments import get_payments
    >>> from pos_core.qa import run_payments_qa
    >>>
    >>> paths = DataPaths.from_root("data", "utils/sucursales.json")
    >>>
    >>> # Get payment data
    >>> df = get_payments(paths, "2025-01-01", "2025-01-31")
    >>>
    >>> # Run QA checks
    >>> result = run_payments_qa(df)
    >>>
    >>> # Check results
    >>> print(result.summary)
    >>> if result.has_anomalies:
    ...     print(result.zscore_anomalies)

"""

from pos_core.qa.api import PaymentsQAResult, run_payments_qa

__all__ = ["PaymentsQAResult", "run_payments_qa"]
