"""Sina HTTP session with ban detection and exponential backoff.

All Sina API callers (crawler/sina, market_index, stock_data) MUST use
this module instead of raw requests.get to benefit from unified 456/429/503
protection.
"""

from __future__ import annotations

import logging
import time

import requests

_logger = logging.getLogger("moatx.sina_http")

BAN_HTTP_CODES = {429, 456, 503}
MAX_RETRIES = 3


def sina_session() -> requests.Session:
    """Create a requests.Session pre-configured for Sina APIs."""
    s = requests.Session()
    s.trust_env = False
    s.proxies = {"http": None, "https": None}
    s.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://finance.sina.com.cn",
    })
    return s


def sina_get(
    url: str,
    params: dict | None = None,
    timeout: int = 10,
    encoding: str | None = None,
    session: requests.Session | None = None,
) -> requests.Response:
    """GET a Sina API endpoint with ban detection and exponential backoff.

    Returns the response on success. Raises RuntimeError if all retries
    exhausted due to ban (456/429/503) or persistent non-200 status.
    """
    s = session or sina_session()

    for attempt in range(MAX_RETRIES):
        try:
            r = s.get(url, params=params or {}, timeout=timeout)
        except requests.RequestException as exc:
            if attempt < MAX_RETRIES - 1:
                wait = (2 ** attempt) * 2
                _logger.warning("Sina request failed (attempt %d/%d): %s, retrying in %ds",
                                attempt + 1, MAX_RETRIES, exc, wait)
                time.sleep(wait)
                s = sina_session()  # fresh session
                continue
            raise RuntimeError(f"Sina request failed after {MAX_RETRIES} attempts: {exc}") from exc

        if r.status_code in BAN_HTTP_CODES:
            if attempt < MAX_RETRIES - 1:
                wait = (2 ** attempt) * 3  # 3s, 6s
                _logger.warning("Sina API HTTP %d (ban), retrying in %ds (attempt %d/%d)",
                                r.status_code, wait, attempt + 1, MAX_RETRIES)
                time.sleep(wait)
                s = sina_session()
                continue
            raise RuntimeError(f"Sina API blocked (HTTP {r.status_code}) after {MAX_RETRIES} retries")

        if r.status_code != 200:
            if attempt < MAX_RETRIES - 1:
                _logger.warning("Sina API HTTP %d, retrying (attempt %d/%d)", r.status_code, attempt + 1, MAX_RETRIES)
                time.sleep(1)
                continue
            raise RuntimeError(f"Sina API returned HTTP {r.status_code}")

        if encoding:
            r.encoding = encoding
        return r

    raise RuntimeError("unreachable")
