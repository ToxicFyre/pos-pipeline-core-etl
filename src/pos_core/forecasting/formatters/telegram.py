"""Telegram message formatting for forecasts."""

from __future__ import annotations

from typing import Any

import pandas as pd

from pos_core.forecasting.api import ForecastResult
from pos_core.forecasting.date_formatters import SPANISH_DAYS


def format_telegram_message(result: ForecastResult) -> str:
    """Format forecasts as a Telegram-friendly message using HTML formatting.

    This function accepts a ForecastResult and returns a formatted HTML string.
    It does NOT send any messages or touch the network.

    Args:
        result: ForecastResult containing forecast and deposit_schedule DataFrames

    Returns:
        Formatted message string with Telegram HTML formatting

    Raises:
        ValueError: If ForecastResult is empty or invalid
    """
    if result.forecast.empty:
        raise ValueError("No forecasts to format: forecast DataFrame is empty")

    # Metric display names
    metric_names = {
        "ingreso_efectivo": "Efectivo",
        "ingreso_credito": "Crédito",
        "ingreso_debito": "Débito",
        "ingreso_total": "Total",
    }

    horizon_days = result.metadata.get("horizon_days", 7)
    lines = [f"📊 <b>Forecast de Pagos - Próximos {horizon_days} Días</b>\n"]

    # Get branches and metrics from forecast DataFrame
    branches = sorted(result.forecast["sucursal"].unique())
    metrics = sorted(result.forecast["metric"].unique())

    # Track totals across all branches for each metric and each day
    # Structure: {metric: {date: total_value}}
    daily_totals: dict[str, dict[Any, float]] = {metric: {} for metric in metrics}

    # Process each branch
    for branch in branches:
        branch_forecasts = result.forecast[result.forecast["sucursal"] == branch]
        if branch_forecasts.empty:
            continue

        lines.append(f"<b>{branch}</b>")

        for metric in metrics:
            metric_forecasts = branch_forecasts[branch_forecasts["metric"] == metric]
            if metric_forecasts.empty:
                continue

            metric_display = metric_names.get(metric, metric)
            lines.append(f"{metric_display}:")

            # Daily breakdown
            total = 0.0
            for _, row in metric_forecasts.iterrows():
                fecha = row["fecha"]
                valor = row["valor"]

                # Convert to date if needed
                fecha_date = fecha.date() if isinstance(fecha, pd.Timestamp) else fecha
                day_name = SPANISH_DAYS[fecha_date.weekday()]
                date_str = fecha_date.strftime("%Y-%m-%d")

                # Format currency - $ is safe in HTML
                value_str = f"${valor:,.2f}"
                lines.append(f"  {day_name} {date_str}: {value_str}")
                total += valor

                # Accumulate daily totals across branches
                if fecha_date not in daily_totals[metric]:
                    daily_totals[metric][fecha_date] = 0.0
                daily_totals[metric][fecha_date] += valor

            # Total for this metric
            total_str = f"${total:,.2f}"
            lines.append(f"  <b>Total: {total_str}</b>\n")

        lines.append("")  # Blank line between branches

    # Add TOTAL section with daily breakdown
    lines.append("<b>TOTAL:</b>")

    # Get all unique dates from forecasts (sorted)
    all_dates_set: set[Any] = set()
    for metric in metrics:
        all_dates_set.update(daily_totals[metric].keys())
    all_dates = sorted(all_dates_set)

    for metric in metrics:
        metric_display = metric_names.get(metric, metric)
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

    # Add Cash Flow section using deposit_schedule from result
    lines.append("<b>Cash Flow (Depósitos Reales):</b>")

    if not result.deposit_schedule.empty:
        for _, row in result.deposit_schedule.iterrows():
            fecha = row["fecha"]
            fecha_date = fecha.date() if isinstance(fecha, pd.Timestamp) else fecha
            day_name = SPANISH_DAYS[fecha_date.weekday()]
            date_str = fecha_date.strftime("%Y-%m-%d")

            efectivo = row.get("efectivo", 0.0)
            credito = row.get("credito", 0.0)
            debito = row.get("debito", 0.0)
            total_deposit = row.get("total", efectivo + credito + debito)

            lines.append(f"{day_name} {date_str}:")

            if efectivo > 0:
                lines.append(f"  Efectivo: ${efectivo:,.2f}")
            if credito > 0:
                lines.append(f"  Crédito: ${credito:,.2f}")
            if debito > 0:
                lines.append(f"  Débito: ${debito:,.2f}")

            lines.append(f"  <b>Total: ${total_deposit:,.2f}</b>\n")

    return "\n".join(lines)
