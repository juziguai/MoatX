"""Persistent cache for akshare-backed financial data.

When akshare is unavailable (Python 3.14, network issues, etc.), returns
stale-but-usable cached data instead of empty errors.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

_logger = logging.getLogger("moatx.akshare_cache")

CACHE_DIR = Path("data/akshare_cache")
MAX_AGE_DAYS = 90  # Financial data is valid for 90 days


def _cache_path(symbol: str, func_name: str) -> Path:
    """Return cache file path for a given symbol + function."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    safe_symbol = symbol.replace(".", "_").replace("/", "_")
    return CACHE_DIR / f"{safe_symbol}_{func_name}.json"


def read_cache(symbol: str, func_name: str) -> Any | None:
    """Read cached result if not expired. Returns None on miss/expiry."""
    path = _cache_path(symbol, func_name)
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data.get("result")
    except Exception as exc:
        _logger.debug("akshare cache read failed [%s/%s]: %s", symbol, func_name, exc)
        return None


def write_cache(symbol: str, func_name: str, result: Any) -> None:
    """Write result to cache with timestamp."""
    path = _cache_path(symbol, func_name)
    try:
        payload = {
            "symbol": symbol,
            "func": func_name,
            "result": result,
            "cached_at": __import__("datetime").datetime.now().isoformat(),
        }
        path.write_text(json.dumps(payload, ensure_ascii=False, default=str), encoding="utf-8")
    except Exception as exc:
        _logger.debug("akshare cache write failed [%s/%s]: %s", symbol, func_name, exc)
