"""Spanish date formatting utilities for forecasting modules.

This module provides constants and functions for formatting dates in Spanish,
centralizing date formatting logic that was previously duplicated across modules.
"""

from datetime import date


# Spanish day names (Monday through Sunday)
SPANISH_DAYS = [
    "Lunes", "Martes", "Miércoles", "Jueves", "Viernes",
    "Sábado", "Domingo"
]

# Spanish month names (January through December)
SPANISH_MONTHS = [
    "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
    "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"
]

# Spanish day name abbreviations (Monday through Sunday)
SPANISH_DAY_ABBREVIATIONS = [
    "Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom"
]


def format_date_spanish(d: date) -> str:
    """Format date in Spanish format like 'Jueves 20 de Noviembre'.
    
    Args:
        d: Date object to format
        
    Returns:
        Formatted date string in Spanish (e.g., "Jueves 20 de Noviembre")
    """
    return f"{SPANISH_DAYS[d.weekday()]} {d.day} de {SPANISH_MONTHS[d.month - 1]}"


def format_date_short(d: date) -> str:
    """Format date in short Spanish format like 'Jue 20'.
    
    Args:
        d: Date object to format
        
    Returns:
        Formatted date string in short Spanish format (e.g., "Jue 20")
    """
    day_abbrev = SPANISH_DAY_ABBREVIATIONS[d.weekday()]
    return f"{day_abbrev} {d.day}"

