"""Shared types for forecasting models."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class ModelDebugInfo:
    """Generic container for model-specific debug information.

    This provides a standard way for forecasting models to expose
    introspection/debug information in a consistent format.

    Attributes:
        model_name: Short identifier for the model, e.g. "naive_last_week", "arima".
        version: Optional version string if model behavior changes over time.
        data: Arbitrary model-specific payload (dict of JSON-like values).
            Each model can populate this with its own schema.
    """

    model_name: str
    version: Optional[str] = None
    data: Dict[str, Any] = field(default_factory=dict)
