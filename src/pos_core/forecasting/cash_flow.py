"""Cash flow deposit calculation for forecasting pipeline.

This module calculates expected deposit amounts based on banking schedules,
combining historical and forecasted payment data.
"""

from __future__ import annotations

from datetime import date

import pandas as pd

from pos_core.forecasting.deposit_schedule import (
    calculate_card_deposits,
    calculate_cash_deposit,
)


def calculate_cash_flow_deposits(
    forecast_dates: list[date],
    daily_totals: dict[str, dict[date, float]],
    historical_df: pd.DataFrame,
    last_historical_date: date,
) -> dict[date, dict[str, float]]:
    """Calculate cash flow deposits for forecast dates using banking schedule.

    Args:
        forecast_dates: List of forecast dates to calculate deposits for
        daily_totals: Dictionary {metric: {date: total_value}} with forecast totals
        historical_df: DataFrame with historical payment data
        last_historical_date: Last date in historical data

    Returns:
        Dictionary {deposit_date: {"efectivo": float, "credito": float, "debito": float}}

    """
    # Get historical data for dates before/on last_historical_date
    historical_by_date = {}
    for _, row in historical_df.iterrows():
        fecha = row["fecha"].date()
        if fecha not in historical_by_date:
            historical_by_date[fecha] = {
                "ingreso_efectivo": 0.0,
                "ingreso_credito": 0.0,
                "ingreso_debito": 0.0,
            }
        historical_by_date[fecha]["ingreso_efectivo"] += row.get("ingreso_efectivo", 0.0) or 0.0
        historical_by_date[fecha]["ingreso_credito"] += row.get("ingreso_credito", 0.0) or 0.0
        historical_by_date[fecha]["ingreso_debito"] += row.get("ingreso_debito", 0.0) or 0.0

    # Helper function to get value (historical or forecast)
    # Missing dates are treated as zero-sale days (branch closed, holiday, etc.)
    def get_value(target_date: date, metric: str) -> float:
        if target_date <= last_historical_date:
            # Use historical data - missing dates return 0.0 (branch closed)
            return historical_by_date.get(target_date, {}).get(metric, 0.0)
        else:
            # Use forecast data - daily_totals now uses date keys
            # If date not in forecast, return 0.0 (shouldn't happen, but safety)
            return daily_totals[metric].get(target_date, 0.0)

    # Create cash flow dictionary: {deposit_date: {type: amount}}
    cash_flow = {}

    # Calculate deposits for each day in the forecast period
    # Note: We need to look back at dates that might be before the forecast period
    # (e.g., Monday deposits need Fri/Sat/Sun which might be historical)
    for forecast_date in forecast_dates:
        # Initialize entry if needed
        if forecast_date not in cash_flow:
            cash_flow[forecast_date] = {"efectivo": 0.0, "credito": 0.0, "debito": 0.0}

        # Calculate cash deposit
        cash_deposit = calculate_cash_deposit(forecast_date, get_value)
        if cash_deposit > 0:
            cash_flow[forecast_date]["efectivo"] += cash_deposit

        # Calculate card deposits (credit and debit)
        credit_deposit, debit_deposit = calculate_card_deposits(forecast_date, get_value)
        if credit_deposit > 0:
            cash_flow[forecast_date]["credito"] += credit_deposit
        if debit_deposit > 0:
            cash_flow[forecast_date]["debito"] += debit_deposit

    return cash_flow
