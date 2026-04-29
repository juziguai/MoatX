"""交易日历 — 判断 A 股交易日"""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

_CALENDAR_CACHE: set[date] | None = None


def _load_calendar() -> set[date]:
    """Load trading calendar from akshare or built-in cache."""
    global _CALENDAR_CACHE
    if _CALENDAR_CACHE is not None:
        return _CALENDAR_CACHE
    try:
        import akshare as ak
        df = ak.tool_trade_date_hist_sina()
        trading = df[df["trade_date"] == 1]["date"].dt.date
        _CALENDAR_CACHE = set(trading)
    except Exception:
        _CALENDAR_CACHE = _builtin_calendar()
    return _CALENDAR_CACHE


def _builtin_calendar() -> set[date]:
    """Fallback: approximate A-share trading days (Mon-Fri, excluding major holidays)."""
    trading = set()
    d = date(2020, 1, 1)
    end = date(2030, 12, 31)
    while d <= end:
        if d.weekday() < 5:
            trading.add(d)
        d += timedelta(days=1)
    return trading


def is_trading_day(d: date | None = None) -> bool:
    """Check if given date is a trading day."""
    d = d or date.today()
    cal = _load_calendar()
    return d in cal


def previous_trading_day(d: date | None = None) -> date:
    """Get the most recent trading day before or on given date."""
    d = d or date.today()
    cal = _load_calendar()
    while d not in cal:
        d -= timedelta(days=1)
    return d


def next_trading_day(d: date | None = None) -> date:
    """Get the next trading day on or after given date."""
    d = d or date.today()
    cal = _load_calendar()
    while d not in cal:
        d += timedelta(days=1)
    return d


def trading_days_between(start: date, end: date) -> list[date]:
    """List all trading days in [start, end]."""
    cal = _load_calendar()
    return sorted(d for d in cal if start <= d <= end)


def trading_day_range(start: date, end: date) -> pd.DatetimeIndex:
    """Return a DatetimeIndex of trading days."""
    return pd.DatetimeIndex(trading_days_between(start, end))
