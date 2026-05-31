"""Crawler result models and error constants."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


NETWORK_TIMEOUT = "NETWORK_TIMEOUT"
REMOTE_DISCONNECTED = "REMOTE_DISCONNECTED"
PROXY_ERROR = "PROXY_ERROR"
HTTP_ERROR_4XX = "HTTP_ERROR_4XX"
HTTP_ERROR_5XX = "HTTP_ERROR_5XX"
PARSE_ERROR = "PARSE_ERROR"
EMPTY_RESPONSE = "EMPTY_RESPONSE"
CACHE_MISS = "CACHE_MISS"
CACHE_EXPIRED = "CACHE_EXPIRED"
CACHE_VERSION_MISMATCH = "CACHE_VERSION_MISMATCH"
CIRCUIT_OPEN = "CIRCUIT_OPEN"
SOURCE_UNAVAILABLE = "SOURCE_UNAVAILABLE"

RETRYABLE = {
    NETWORK_TIMEOUT,
    REMOTE_DISCONNECTED,
    EMPTY_RESPONSE,
    PROXY_ERROR,
    HTTP_ERROR_5XX,
}

NON_RETRYABLE = {
    HTTP_ERROR_4XX,
    PARSE_ERROR,
    CACHE_MISS,
    CACHE_VERSION_MISMATCH,
    CIRCUIT_OPEN,
}


@dataclass
class CrawlResult:
    ok: bool
    data: Any = None
    source: str = ""
    from_cache: bool = False
    error: str = ""
    error_detail: str = ""
    user_message: str = ""
    elapsed_ms: int = 0
    cached_at: str = ""
    trade_date: str = ""
    warnings: list[str] = field(default_factory=list)


class BoardSource(ABC):
    """Board data source abstract base class.

    To add a new board source:
    1. Subclass BoardSource
    2. Implement name / fetch_industry_boards / fetch_concept_boards
    3. Register in BoardManager registry
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Source identifier, e.g. 'ths', 'sina', 'local'."""
        ...

    @abstractmethod
    def fetch_industry_boards(self, use_cache: bool = True):
        """Fetch industry board data."""
        ...

    @abstractmethod
    def fetch_concept_boards(self, use_cache: bool = True):
        """Fetch concept board data."""
        ...

    def health_check(self) -> dict:
        """Quick health check."""
        import time
        t0 = time.time()
        try:
            r = self.fetch_industry_boards(use_cache=False)
            return {
                "healthy": r.ok,
                "latency_ms": (time.time() - t0) * 1000,
                "error": r.error if not r.ok else "",
            }
        except Exception as exc:
            return {
                "healthy": False,
                "latency_ms": (time.time() - t0) * 1000,
                "error": str(exc),
            }
