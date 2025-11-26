"""Banking deposit schedule calculation for cash flow forecasting.

This module provides unified functions for calculating deposit schedules based on
banking deposit patterns:
- Cash deposits: Batched deposits on Mon/Wed/Fri
- Card deposits: 1-day lag (next business day)
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Callable, List, Tuple


def is_holiday_or_adjacent(d: date, holidays: set[date]) -> bool:
    """Check if a date is a holiday or adjacent to a holiday.

    Args:
        d: Date to check
        holidays: Set of holiday dates

    Returns:
        True if date is a holiday or adjacent to a holiday
    """
    return (
        d in holidays
        or (d - timedelta(days=1)) in holidays
        or (d + timedelta(days=1)) in holidays
    )


def get_dates_needed_for_cash_deposit(forecast_date: date) -> List[date]:
    """Get the dates needed for cash deposit calculation based on deposit date.

    Cash deposits are batched:
      - Monday: deposits from Fri+Sat+Sun (weekend cash)
      - Wednesday: deposits from Mon+Tue
      - Friday: deposits from Wed+Thu
      - Tuesday/Thursday: No cash deposits

    Args:
        forecast_date: The deposit date when cash is deposited

    Returns:
        List of dates whose cash sales are included in this deposit.
        Returns empty list if no cash deposit on this weekday.
    """
    weekday = forecast_date.weekday()  # 0=Monday, 4=Friday

    if weekday == 0:  # Monday: Fri+Sat+Sun
        return [
            forecast_date - timedelta(days=3),  # Friday
            forecast_date - timedelta(days=2),  # Saturday
            forecast_date - timedelta(days=1),  # Sunday
        ]
    elif weekday == 2:  # Wednesday: Mon+Tue
        return [
            forecast_date - timedelta(days=2),  # Monday
            forecast_date - timedelta(days=1),  # Tuesday
        ]
    elif weekday == 4:  # Friday: Wed+Thu
        return [
            forecast_date - timedelta(days=2),  # Wednesday
            forecast_date - timedelta(days=1),  # Thursday
        ]
    else:
        return []  # No cash deposits on Tue/Thu


def get_dates_needed_for_card_deposit(forecast_date: date) -> List[date]:
    """Get the dates needed for card deposit calculation based on deposit date.

    Credit/Debit deposits: 1-day lag (next business day)
      - Monday: deposits from previous Fri+Sat+Sun
      - Tuesday: deposits from Monday
      - Wednesday: deposits from Tuesday
      - Thursday: deposits from Wednesday
      - Friday: deposits from Thursday

    Args:
        forecast_date: The deposit date when card payments are deposited

    Returns:
        List of dates whose card sales are included in this deposit.
    """
    weekday = forecast_date.weekday()  # 0=Monday, 4=Friday

    if weekday == 0:  # Monday: previous Fri+Sat+Sun
        return [
            forecast_date - timedelta(days=3),  # Friday
            forecast_date - timedelta(days=2),  # Saturday
            forecast_date - timedelta(days=1),  # Sunday
        ]
    else:  # Tue-Fri: previous business day
        prev_business_day = forecast_date - timedelta(days=1)
        # Skip weekends
        while prev_business_day.weekday() >= 5:
            prev_business_day -= timedelta(days=1)
        return [prev_business_day]


def calculate_cash_deposit(
    forecast_date: date,
    get_value_fn: Callable[[date, str], float],
) -> float:
    """Calculate cash deposit amount for a given deposit date.

    Uses the provided get_value_fn to retrieve cash sales values.

    Args:
        forecast_date: The deposit date when cash is deposited
        get_value_fn: Function that takes (date, metric) and returns float value.
                     Metric will be "ingreso_efectivo" for cash.

    Returns:
        Total cash deposit amount
    """
    needed_dates = get_dates_needed_for_cash_deposit(forecast_date)

    total = 0.0
    for needed_date in needed_dates:
        total += get_value_fn(needed_date, "ingreso_efectivo")

    return total


def calculate_card_deposits(
    forecast_date: date,
    get_value_fn: Callable[[date, str], float],
) -> Tuple[float, float]:
    """Calculate credit and debit deposit amounts for a given deposit date.

    Uses the provided get_value_fn to retrieve card sales values.

    Args:
        forecast_date: The deposit date when card payments are deposited
        get_value_fn: Function that takes (date, metric) and returns float value.
                     Metrics will be "ingreso_credito" and "ingreso_debito".

    Returns:
        Tuple of (credit_deposit, debit_deposit)
    """
    needed_dates = get_dates_needed_for_card_deposit(forecast_date)

    credit_total = 0.0
    debit_total = 0.0

    for needed_date in needed_dates:
        credit_total += get_value_fn(needed_date, "ingreso_credito")
        debit_total += get_value_fn(needed_date, "ingreso_debito")

    return credit_total, debit_total

