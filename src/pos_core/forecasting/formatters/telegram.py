"""Telegram message formatting for forecasts."""

from __future__ import annotations

from datetime import date
from typing import Dict

import pandas as pd

from pos_forecasting.cash_flow import calculate_cash_flow_deposits
from pos_forecasting.config import BRANCHES, METRICS
from pos_forecasting.date_formatters import SPANISH_DAYS


def format_telegram_message(
    forecasts: Dict[str, Dict[str, pd.Series]],
    historical_df: pd.DataFrame,
    last_historical_date: date,
) -> str:
    """Format forecasts as a Telegram-friendly message using HTML formatting.
    
    Args:
        forecasts: Nested dictionary {branch: {metric: forecast_series}}
        historical_df: DataFrame with historical payment data
        last_historical_date: Last date in historical data
        
    Returns:
        Formatted message string with Telegram HTML formatting
        
    Raises:
        ValueError: If no forecasts are provided
    """
    if not forecasts or not any(forecasts.values()):
        raise ValueError("No forecasts to format")
    
    # Metric display names
    metric_names = {
        "ingreso_efectivo": "Efectivo",
        "ingreso_credito": "Crédito",
        "ingreso_debito": "Débito",
        "ingreso_total": "Total",
    }
    
    lines = ["📊 <b>Forecast de Pagos - Próximos 7 Días</b>\n"]
    
    # Track totals across all branches for each metric and each day
    # Structure: {metric: {date: total_value}}
    daily_totals = {metric: {} for metric in METRICS}
    
    for branch in BRANCHES:
        if branch not in forecasts or not forecasts[branch]:
            continue
        
        lines.append(f"<b>{branch}</b>")
        
        for metric in METRICS:
            if metric not in forecasts[branch]:
                continue
            
            forecast_series = forecasts[branch][metric]
            metric_display = metric_names[metric]
            
            lines.append(f"{metric_display}:")
            
            # Daily breakdown
            total = 0.0
            for forecast_date, value in forecast_series.items():
                day_name = SPANISH_DAYS[forecast_date.weekday()]
                date_str = forecast_date.strftime("%Y-%m-%d")
                # Format currency - $ is safe in HTML
                value_str = f"${value:,.2f}"
                lines.append(f"  {day_name} {date_str}: {value_str}")
                total += value
                
                # Accumulate daily totals across branches
                # Convert forecast_date to date if it's a Timestamp
                forecast_date_key = forecast_date.date() if isinstance(forecast_date, pd.Timestamp) else forecast_date
                if forecast_date_key not in daily_totals[metric]:
                    daily_totals[metric][forecast_date_key] = 0.0
                daily_totals[metric][forecast_date_key] += value
            
            # Total for this metric
            total_str = f"${total:,.2f}"
            lines.append(f"  <b>Total: {total_str}</b>\n")
        
        lines.append("")  # Blank line between branches
    
    # Add TOTAL section with daily breakdown
    lines.append("<b>TOTAL:</b>")
    
    # Get all unique dates from forecasts (sorted)
    all_dates = set()
    for metric in METRICS:
        all_dates.update(daily_totals[metric].keys())
    all_dates = sorted([d.date() if isinstance(d, pd.Timestamp) else d for d in all_dates])
    
    for metric in METRICS:
        metric_display = metric_names[metric]
        lines.append(f"{metric_display}:")
        
        for forecast_date in all_dates:
            if forecast_date in daily_totals[metric]:
                day_name = SPANISH_DAYS[forecast_date.weekday()]
                date_str = forecast_date.strftime("%Y-%m-%d")
                value = daily_totals[metric][forecast_date]
                value_str = f"${value:,.2f}"
                lines.append(f"  {day_name} {date_str}: {value_str}")
        
        # Calculate and show total for this metric
        metric_total = sum(daily_totals[metric].values())
        total_str = f"${metric_total:,.2f}"
        lines.append(f"  <b>Total: {total_str}</b>\n")
    
    # Add Cash Flow section
    lines.append("<b>Cash Flow (Depósitos Reales):</b>")
    
    # Calculate cash flow deposits using shared deposit schedule functions
    cash_flow = calculate_cash_flow_deposits(
        all_dates, daily_totals, historical_df, last_historical_date
    )
    
    # Format cash flow section
    for deposit_date in sorted(cash_flow.keys()):
        day_name = SPANISH_DAYS[deposit_date.weekday()]
        date_str = deposit_date.strftime("%Y-%m-%d")
        cf = cash_flow[deposit_date]
        
        lines.append(f"{day_name} {date_str}:")
        
        if cf["efectivo"] > 0:
            lines.append(f"  Efectivo: ${cf['efectivo']:,.2f}")
        if cf["credito"] > 0:
            lines.append(f"  Crédito: ${cf['credito']:,.2f}")
        if cf["debito"] > 0:
            lines.append(f"  Débito: ${cf['debito']:,.2f}")
        
        total_deposit = cf["efectivo"] + cf["credito"] + cf["debito"]
        lines.append(f"  <b>Total: ${total_deposit:,.2f}</b>\n")
    
    return "\n".join(lines)

