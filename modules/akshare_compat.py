"""Compatibility helpers for optional akshare imports."""

from __future__ import annotations

import logging
from functools import lru_cache
from typing import Any

_logger = logging.getLogger("moatx.akshare")


class AkshareUnavailable:
    """Placeholder that raises only when an akshare API is actually used."""

    def __init__(self, error: Exception):
        self._error = error

    def __bool__(self) -> bool:
        return False

    @property
    def error(self) -> Exception:
        return self._error

    def __getattr__(self, name: str) -> Any:
        raise RuntimeError(f"akshare unavailable while calling {name}: {self._error}") from self._error


@lru_cache(maxsize=1)
def import_akshare() -> Any:
    """Import akshare with graceful degradation for incompatible runtimes."""
    try:
        import akshare as ak

        return ak
    except Exception as exc:
        _logger.warning("akshare import failed; akshare-backed data will degrade: %s", exc)
        return AkshareUnavailable(exc)
