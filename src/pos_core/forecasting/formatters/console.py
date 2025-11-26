"""Console output formatting utilities."""

from __future__ import annotations

import re

import pandas as pd

from pos_core.forecasting.api import ForecastResult
from pos_core.forecasting.date_formatters import SPANISH_DAYS


def sanitize_for_console(text: str) -> str:
    """Sanitize text for console output by removing emojis and HTML tags.

    This prevents UnicodeEncodeError on Windows console which uses cp1252 encoding.

    Args:
        text: Text that may contain emojis and HTML tags

    Returns:
        Sanitized text safe for console output
    """
    # Remove emojis (Unicode characters outside ASCII range)
    # This regex matches emojis and other non-ASCII characters
    text = re.sub(r'[^\x00-\x7F]+', '', text)
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', '', text)
    return text


def format_forecast_for_console(result: ForecastResult) -> str:
    """Build a human-readable string representation of the forecast and deposit schedule for console output.

    Args:
        result: ForecastResult containing forecast and deposit_schedule DataFrames

    Returns:
        Human-readable text string for console output
    """
    if result.forecast.empty:
        return "No forecasts available."

    lines = []
    horizon_days = result.metadata.get("horizon_days", 7)
    lines.append(f"Forecast de Pagos - Proximos {horizon_days} Dias")
    lines.append("=" * 60)
    lines.append("")

    # Metric display names
    metric_names = {
        "ingreso_efectivo": "Efectivo",
        "ingreso_credito": "Credito",
        "ingreso_debito": "Debito",
        "ingreso_total": "Total",
    }

    # Get branches and metrics from forecast DataFrame
    branches = sorted(result.forecast["sucursal"].unique())
    metrics = sorted(result.forecast["metric"].unique())

    # Process each branch
    for branch in branches:
        branch_forecasts = result.forecast[result.forecast["sucursal"] == branch]
        if branch_forecasts.empty:
            continue

        lines.append(f"{branch}:")

        for metric in metrics:
            metric_forecasts = branch_forecasts[branch_forecasts["metric"] == metric]
            if metric_forecasts.empty:
                continue

            metric_display = metric_names.get(metric, metric)
            lines.append(f"  {metric_display}:")

            total = 0.0
            for _, row in metric_forecasts.iterrows():
                fecha = row["fecha"]
                valor = row["valor"]

                # Convert to date if needed
                fecha_date = fecha.date() if isinstance(fecha, pd.Timestamp) else fecha
                day_name = SPANISH_DAYS[fecha_date.weekday()]
                date_str = fecha_date.strftime("%Y-%m-%d")

                value_str = f"${valor:,.2f}"
                lines.append(f"    {day_name} {date_str}: {value_str}")
                total += valor

            total_str = f"${total:,.2f}"
            lines.append(f"    Total: {total_str}")

        lines.append("")  # Blank line between branches

    # Add deposit schedule section
    if not result.deposit_schedule.empty:
        lines.append("Cash Flow (Depositos Reales):")
        lines.append("-" * 60)

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
                lines.append(f"  Credito: ${credito:,.2f}")
            if debito > 0:
                lines.append(f"  Debito: ${debito:,.2f}")

            lines.append(f"  Total: ${total_deposit:,.2f}")
            lines.append("")

    return "\n".join(lines)

