"""Shared types for forecasting models."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol


@dataclass(frozen=True)
class ModelDebugInfo:
    """Generic container for model-specific debug information.

    This provides a standard way for forecasting models to expose
    introspection/debug information in a consistent format.

    Attributes:
        model_name: Short identifier for the model, e.g. "naive_last_week", "arima".
        version: Optional version string if model behavior changes over time.
        data: Arbitrary model-specific payload (dict of JSON-like values).
            Each model can populate this with its own schema.

    Note:
        This dataclass is frozen to prevent accidental mutations after creation.
        The ModelDebugInfo instance cannot be reassigned, and the data dict
        should be populated at creation time with all required information.

    """

    model_name: str
    version: str | None = None
    data: dict[str, Any] = field(default_factory=dict)


class HasDebugInfo(Protocol):
    """Protocol for models that expose debug information.

    This protocol allows type checkers (MyPy, Pyright) to recognize models
    that have a debug_ attribute, without requiring a concrete base class.

    Example:
        def inspect_model_debug(model: HasDebugInfo) -> Optional[ModelDebugInfo]:
            return model.debug_

    """

    debug_: ModelDebugInfo | None
