"""A-share trading calendar and market hours."""

from __future__ import annotations

import json
from datetime import date, time, datetime
from pathlib import Path

import pandas as pd

from modules.backtest.calendar import (
    is_trading_day as _backtest_is_trading_day,
    previous_trading_day,
    next_trading_day as _next_trading_day_impl,
)

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_CACHE_FILE = _PROJECT_ROOT / "data" / "trading_calendar.json"


def _load_cache() -> dict:
    if _CACHE_FILE.exists():
        try:
            return json.loads(_CACHE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def _save_cache(data: dict) -> None:
    _CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    _CACHE_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _refresh_calendar() -> set[date]:
    """Fetch trading calendar from akshare, cache to JSON."""
    try:
        import akshare as ak
        df = ak.tool_trade_date_hist_sina()
        trading = set(pd.to_datetime(df[df["trade_date"] == 1]["date"]).dt.date)
        today = date.today()
        _save_cache({
            "dates": [str(d) for d in sorted(trading)],
            "updated": today.isoformat(),
        })
        return trading
    except Exception:
        pass
    cached = _load_cache()
    if cached.get("dates"):
        return {date.fromisoformat(d) for d in cached["dates"]}
    from modules.backtest.calendar import _builtin_calendar
    return _builtin_calendar()


def _get_calendar() -> set[date]:
    cached = _load_cache()
    today_str = date.today().isoformat()
    if cached.get("updated") == today_str and cached.get("dates"):
        return {date.fromisoformat(d) for d in cached["dates"]}
    return _refresh_calendar()


def is_trading_day(d: date | None = None) -> bool:
    """Check if given date is an A-share trading day."""
    d = d or date.today()
    return _backtest_is_trading_day(d)


def last_trading_day(d: date | None = None) -> date:
    """Return the most recent trading day on or before given date."""
    return previous_trading_day(d)


def next_trading_day(d: date | None = None) -> date:
    """Return the next trading day on or after given date."""
    return _next_trading_day_impl(d)


def is_trading_time() -> bool:
    """Check if current time is within A-share trading hours (09:30-11:30, 13:00-15:00)."""
    now = datetime.now()
    t = now.time()
    d = now.date()
    if not is_trading_day(d):
        return False
    morning_start = time(9, 30)
    morning_end = time(11, 30)
    afternoon_start = time(13, 0)
    afternoon_end = time(15, 0)
    if morning_start <= t <= morning_end:
        return True
    if afternoon_start <= t <= afternoon_end:
        return True
    return False
