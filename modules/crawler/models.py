"""Crawler result models and error constants."""

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

