"""Simple baseline forecasting models."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import pandas as pd
from pandas.tseries.frequencies import to_offset

from pos_core.forecasting.models.base import ForecastModel


@dataclass(frozen=True)
class NaiveModelState:
    """Container for the fitted state of the naive model."""

    last_value: float
    last_date: pd.Timestamp
    freq: str


def _infer_frequency(index: pd.DatetimeIndex) -> str:
    """Infer the frequency of a DateTimeIndex, defaulting to daily."""

    if index.freqstr:
        return index.freqstr

    try:
        inferred = pd.infer_freq(index)
    except ValueError:
        inferred = None

    return inferred or "D"


class NaiveLastValueModel(ForecastModel):
    """Forecasts future values by repeating the last observed value."""

    def train(self, series: pd.Series, **kwargs: Any) -> NaiveModelState:
        """Store the last observed value/date as the model state."""

        if not isinstance(series.index, pd.DatetimeIndex):
            raise ValueError("series must use a DateTimeIndex")

        clean_series = series.dropna()
        if clean_series.empty:
            raise ValueError("NaiveLastValueModel requires at least one observation")

        last_value = float(clean_series.iloc[-1])
        last_date = pd.Timestamp(clean_series.index[-1])
        freq = _infer_frequency(clean_series.index)

        return NaiveModelState(last_value=last_value, last_date=last_date, freq=freq)

    def forecast(
        self,
        model: NaiveModelState,
        steps: int,
        *,
        last_date: Optional[pd.Timestamp] = None,
        freq: Optional[str] = None,
        **kwargs: Any,
    ) -> pd.Series:
        """Repeat the stored value for the requested horizon."""

        if steps <= 0:
            raise ValueError("steps must be a positive integer")

        forecast_freq = freq or model.freq or "D"
        offset = to_offset(forecast_freq)

        base_date = pd.Timestamp(last_date) if last_date is not None else model.last_date
        future_index = pd.date_range(
            start=base_date + offset,
            periods=steps,
            freq=forecast_freq,
        )

        return pd.Series(
            data=[model.last_value] * steps,
            index=future_index,
            dtype=float,
            name="forecast",
        )

