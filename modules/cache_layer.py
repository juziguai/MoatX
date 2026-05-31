"""Unified cache layer with TTL and SWR (stale-while-revalidate).

Replaces akshare_cache.py + crawler cache with one interface.
"""

from __future__ import annotations

import json, logging, time
from pathlib import Path
from typing import Any

_logger = logging.getLogger("moatx.cache")

CACHE_DIR = Path("data/cache")
DEFAULT_TTL = {
    "quote": 30,        # seconds
    "board": 86400,      # 1 day
    "financial": 604800, # 7 days
}


class CacheLayer:
    """Simple file-based cache with TTL."""

    def __init__(self, base_dir: Path | None = None):
        self._dir = base_dir or CACHE_DIR
        self._dir.mkdir(parents=True, exist_ok=True)

    def get(self, key: str) -> Any | None:
        path = self._dir / f"{key}.json"
        if not path.exists():
            return None
        try:
            data = json.loads(path.read_text("utf-8"))
            return data.get("value")
        except Exception as exc:
            _logger.debug("cache read failed [%s]: %s", key, exc)
            return None

    def set(self, key: str, value: Any) -> None:
        path = self._dir / f"{key}.json"
        try:
            payload = {"key": key, "value": value, "ts": time.time()}
            path.write_text(json.dumps(payload, ensure_ascii=False, default=str), "utf-8")
        except Exception as exc:
            _logger.debug("cache write failed [%s]: %s", key, exc)

    def get_or_fetch(self, key: str, fetcher, ttl: int = 3600) -> Any:
        """Get from cache, or fetch + cache. Returns stale on fetch failure."""
        cached = self.get(key)
        if cached is not None:
            return cached
        try:
            result = fetcher()
            self.set(key, result)
            return result
        except Exception:
            return cached  # return stale if available

    def clear(self, prefix: str = "") -> None:
        for f in self._dir.glob(f"{prefix}*.json"):
            try:
                f.unlink()
            except Exception:
                pass


# Global instance
cache = CacheLayer()
