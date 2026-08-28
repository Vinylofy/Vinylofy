from __future__ import annotations

from datetime import date, timedelta


def friday_for_week(anchor: date | None = None) -> date:
    today = anchor or date.today()
    return today + timedelta(days=(4 - today.weekday()) % 7)


def release_fridays(past_weeks: int = 2, future_weeks: int = 4, anchor: date | None = None) -> list[date]:
    base = friday_for_week(anchor)
    return [base + timedelta(weeks=i) for i in range(-past_weeks, future_weeks + 1)]


def nl_release_query(d: date) -> str:
    return f"{d.day}-{d.month}-{d.year}"
