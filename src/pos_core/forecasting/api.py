"""Public API for payments forecasting pipeline.

This module provides a clean, configurable API for running the payments forecasting
pipeline with in-memory DataFrames and no side effects.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date
from typing import Dict, List, Optional, Set

import numpy as np
import pandas as pd

from pos_core.exceptions import DataQualityError
from pos_core.forecasting.cash_flow import calculate_cash_flow_deposits
from pos_core.forecasting.data.preparation import (
    build_daily_series,
    calculate_ingreso_total,
)
from pos_core.forecasting.models.arima import LogARIMAModel
from pos_core.forecasting.models.base import ForecastModel
from pos_core.forecasting.types import ModelDebugInfo

logger = logging.getLogger(__name__)


@dataclass
class ForecastConfig:
    """Configuration for payments forecasting.

    Attributes:
        horizon_days: Number of days ahead to forecast (default: 7).
        metrics: List of metrics to forecast (default: cash, credit, debit, total).
        branches: Optional list of branch names to forecast. If None, infers from payments_df.
        model: Optional forecast model instance. If None, uses LogARIMAModel.
    """

    horizon_days: int = 7
    metrics: List[str] = field(
        default_factory=lambda: [
            "ingreso_efectivo",
            "ingreso_credito",
            "ingreso_debito",
            "ingreso_total",
        ]
    )
    branches: Optional[List[str]] = None  # if None, infer from payments_df
    model: Optional[ForecastModel] = None  # if None, use LogARIMAModel


@dataclass
class ForecastResult:
    """Result of the payments forecasting pipeline.

    Attributes:
        forecast: DataFrame with columns: sucursal, fecha, metric, valor
        deposit_schedule: DataFrame with cash-flow deposit schedule
        metadata: Dictionary with additional metadata (branches, metrics, horizon_days, etc.)
        debug: Optional nested dictionary of debug info.
            Structure: debug[model_name][branch][metric] = ModelDebugInfo
            Only populated when run_payments_forecast is called with debug=True.
            Allows tracking debug info per model, branch, and metric combination.
    """

    forecast: pd.DataFrame  # per branch/metric/date forecast
    deposit_schedule: pd.DataFrame  # cash-flow / banking schedule view
    metadata: Dict[str, object] = field(default_factory=dict)
    # Debug info structure: debug[model_name][branch][metric] = ModelDebugInfo
    # Allows tracking debug info per model, branch, and metric combination
    debug: Optional[Dict[str, Dict[str, Dict[str, ModelDebugInfo]]]] = None


def _forecast_dict_to_dataframe(forecasts: Dict[str, Dict[str, pd.Series]]) -> pd.DataFrame:
    """Convert nested forecast dictionary to a structured DataFrame.

    Args:
        forecasts: Nested dictionary {branch: {metric: forecast_series}}

    Returns:
        DataFrame with columns: sucursal, fecha, metric, valor
    """
    rows = []
    for branch, metrics_dict in forecasts.items():
        for metric, forecast_series in metrics_dict.items():
            for fecha, valor in forecast_series.items():
                # Convert Timestamp to date if needed
                fecha_date = fecha.date() if isinstance(fecha, pd.Timestamp) else fecha
                rows.append(
                    {
                        "sucursal": branch,
                        "fecha": fecha_date,
                        "metric": metric,
                        "valor": float(valor),
                    }
                )

    if not rows:
        # Return empty DataFrame with correct columns
        return pd.DataFrame(columns=["sucursal", "fecha", "metric", "valor"])

    df = pd.DataFrame(rows)
    df["fecha"] = pd.to_datetime(df["fecha"])
    return df.sort_values(["sucursal", "fecha", "metric"]).reset_index(drop=True)


def _build_deposit_schedule_dataframe(
    forecast_df: pd.DataFrame,
    historical_df: pd.DataFrame,
    horizon_days: int,
) -> pd.DataFrame:
    """Build deposit schedule DataFrame from forecast and historical data.

    Args:
        forecast_df: DataFrame with forecast data (columns: sucursal, fecha, metric, valor)
        historical_df: DataFrame with historical payment data
        horizon_days: Number of forecast days (to determine date range)

    Returns:
        DataFrame with columns: fecha, efectivo, credito, debito, total
    """
    # Get forecast dates
    if forecast_df.empty:
        return pd.DataFrame(columns=["fecha", "efectivo", "credito", "debito", "total"])

    forecast_dates = sorted(list(forecast_df["fecha"].dt.date.unique()))
    last_historical_date = (
        historical_df["fecha"].max().date() if not historical_df.empty else date.today()
    )

    # Build daily_totals dictionary: {metric: {date: total_value}}
    daily_totals: Dict[str, Dict[date, float]] = {}
    for metric in ["ingreso_efectivo", "ingreso_credito", "ingreso_debito"]:
        daily_totals[metric] = {}
        metric_forecasts = forecast_df[forecast_df["metric"] == metric]
        for _, row in metric_forecasts.iterrows():
            fecha_date = (
                row["fecha"].date() if isinstance(row["fecha"], pd.Timestamp) else row["fecha"]
            )
            valor = row["valor"]
            if fecha_date not in daily_totals[metric]:
                daily_totals[metric][fecha_date] = 0.0
            daily_totals[metric][fecha_date] += valor

    # Calculate cash flow deposits
    cash_flow = calculate_cash_flow_deposits(
        forecast_dates=forecast_dates,
        daily_totals=daily_totals,
        historical_df=historical_df,
        last_historical_date=last_historical_date,
    )

    # Convert cash flow dict to DataFrame
    deposit_rows = []
    for deposit_date, deposits in sorted(cash_flow.items()):
        efectivo = deposits.get("efectivo", 0.0)
        credito = deposits.get("credito", 0.0)
        debito = deposits.get("debito", 0.0)
        total = efectivo + credito + debito
        deposit_rows.append(
            {
                "fecha": deposit_date,
                "efectivo": efectivo,
                "credito": credito,
                "debito": debito,
                "total": total,
            }
        )

    if not deposit_rows:
        return pd.DataFrame(columns=["fecha", "efectivo", "credito", "debito", "total"])

    deposit_df = pd.DataFrame(deposit_rows)
    deposit_df["fecha"] = pd.to_datetime(deposit_df["fecha"])
    return deposit_df.sort_values("fecha").reset_index(drop=True)


def run_payments_forecast(
    payments_df: pd.DataFrame,
    config: Optional[ForecastConfig] = None,
    debug: bool = False,
) -> ForecastResult:
    """Run the payments forecasting pipeline in memory.

    This function:
    - does NOT read or write any files,
    - does NOT send Telegram messages,
    - does NOT parse CLI arguments or read environment variables,
    - MAY log progress via the logging module.

    Args:
        payments_df: Aggregated payments data, typically the output of
            the ETL step (e.g. aggregated_payments_daily).
            Expected columns include at least:
            - 'sucursal' (branch name)
            - 'fecha' (date or datetime)
            - the metrics in config.metrics (e.g. ingreso_efectivo, ingreso_credito, ...)
        config: ForecastConfig for horizon, metrics, and branches. If None, uses defaults.
        debug: If True, collects debug information from models and includes it in result.debug.
            Default is False to keep the API simple for normal use.

    Returns:
        ForecastResult containing:
        - forecast: per-branch, per-metric predictions for the next horizon_days
        - deposit_schedule: computed cash-flow deposit schedule using existing logic.
        - metadata: additional information about the forecast
        - debug: model debug information (only if debug=True)

    Raises:
        DataQualityError: If required columns are missing.
        DataQualityError: If no forecasts are generated.
    """
    if config is None:
        config = ForecastConfig()

    # Validate input DataFrame
    required_columns = ["sucursal", "fecha"] + config.metrics
    missing_columns = [col for col in required_columns if col not in payments_df.columns]
    if missing_columns:
        raise DataQualityError(
            f"Missing required columns in payments_df: {missing_columns}. "
            f"Required: {required_columns}"
        )

    # Prepare data
    df = payments_df.copy()
    df["fecha"] = pd.to_datetime(df["fecha"])

    # Calculate ingreso_total if not present and needed
    if "ingreso_total" in config.metrics and "ingreso_total" not in df.columns:
        df = calculate_ingreso_total(df)

    # Determine branches
    if config.branches is None:
        branches = sorted(df["sucursal"].unique().tolist())
    else:
        branches = config.branches
        # Validate branches exist
        available_branches = set(df["sucursal"].unique())
        missing_branches = [b for b in branches if b not in available_branches]
        if missing_branches:
            logger.warning(
                f"Branches not found in data: {missing_branches}. "
                f"Available branches: {sorted(available_branches)}"
            )

    metrics = config.metrics
    horizon_days = config.horizon_days

    logger.info(
        f"Running forecast for {len(branches)} branches, {len(metrics)} metrics, "
        f"{horizon_days} days"
    )

    # Get model instance (use configured model or default to LogARIMAModel)
    model = config.model if config.model is not None else LogARIMAModel()

    # Extract holidays from payments_df if is_national_holiday column exists
    holidays: Set[date] = set()
    if "is_national_holiday" in df.columns:
        holiday_rows = df[df["is_national_holiday"] == True]  # noqa: E712
        for fecha in holiday_rows["fecha"].unique():
            if isinstance(fecha, pd.Timestamp):
                holidays.add(fecha.date())
            else:
                holidays.add(fecha)

    # Collect debug info if requested
    # Structure: debug_info[model_name][branch][metric] = ModelDebugInfo
    # This allows tracking debug info per model, branch, and metric combination
    debug_info: Optional[Dict[str, Dict[str, Dict[str, ModelDebugInfo]]]] = {} if debug else None

    # Generate forecasts: {branch: {metric: forecast_series}}
    forecasts: Dict[str, Dict[str, pd.Series]] = {}
    successful_forecasts = 0
    failed_forecasts = 0

    for branch in branches:
        if branch not in df["sucursal"].values:
            logger.warning(f"Branch '{branch}' not found in data, skipping")
            continue

        forecasts[branch] = {}

        for metric in metrics:
            try:
                # Build series (missing days are already filled with 0.0)
                series = build_daily_series(df, branch, metric)

                # Series should have no NaN values (missing days are 0.0)
                # But check for any remaining NaN/inf just in case
                series = series.replace([np.inf, -np.inf], np.nan).fillna(0.0)

                if len(series) < 30:
                    logger.warning(
                        f"{branch} - {metric}: insufficient data ({len(series)} obs), skipping"
                    )
                    failed_forecasts += 1
                    continue

                # Train model and forecast
                logger.debug(f"Training {branch} - {metric}...")
                trained_model = model.train(series, holidays=holidays)
                last_date = series.index[-1]
                forecast = model.forecast(trained_model, steps=horizon_days, last_date=last_date)
                forecasts[branch][metric] = forecast
                successful_forecasts += 1

                # Collect debug info if requested
                if debug and hasattr(model, "debug_") and model.debug_ is not None:
                    # Store debug info in nested structure: model_name -> branch -> metric
                    # This allows tracking debug info per model, branch, and metric combination
                    # Type narrowing: debug_info is {} when debug=True
                    assert debug_info is not None
                    model_name = model.debug_.model_name
                    if model_name not in debug_info:
                        debug_info[model_name] = {}
                    if branch not in debug_info[model_name]:
                        debug_info[model_name][branch] = {}
                    debug_info[model_name][branch][metric] = model.debug_

            except Exception as e:
                logger.warning(f"Error forecasting {branch} - {metric}: {e}")
                failed_forecasts += 1
                continue

    logger.info(f"Forecast summary: {successful_forecasts} successful, {failed_forecasts} failed")

    # Check if we have any forecasts at all
    total_forecasts = sum(len(metrics) for metrics in forecasts.values())
    if total_forecasts == 0:
        raise DataQualityError(
            "No forecasts were generated. Check data availability and model training errors."
        )

    # Convert forecasts dict to DataFrame
    forecast_df = _forecast_dict_to_dataframe(forecasts)

    # Build deposit schedule
    deposit_schedule_df = _build_deposit_schedule_dataframe(forecast_df, df, horizon_days)

    # Get last historical date for metadata
    last_historical_date = df["fecha"].max().date() if not df.empty else None

    # Build result
    result = ForecastResult(
        forecast=forecast_df,
        deposit_schedule=deposit_schedule_df,
        metadata={
            "branches": branches,
            "metrics": metrics,
            "horizon_days": horizon_days,
            "last_historical_date": last_historical_date,
            "successful_forecasts": successful_forecasts,
            "failed_forecasts": failed_forecasts,
        },
        debug=debug_info if debug else None,
    )

    return result
