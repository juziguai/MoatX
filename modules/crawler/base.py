"""Crawler HTTP client, proxy handling, and lightweight circuit breaker."""

from __future__ import annotations

import json
import os
import time
from http.client import RemoteDisconnected
from urllib.parse import urlparse

import requests
from requests import Response
from requests.exceptions import ConnectionError, ProxyError, ReadTimeout, Timeout

from .models import (
    CIRCUIT_OPEN,
    CrawlResult,
    EMPTY_RESPONSE,
    HTTP_ERROR_4XX,
    HTTP_ERROR_5XX,
    NETWORK_TIMEOUT,
    PARSE_ERROR,
    PROXY_ERROR,
    REMOTE_DISCONNECTED,
    RETRYABLE,
    SOURCE_UNAVAILABLE,
)

from modules.utils import _clear_all_proxy


class CircuitBreaker:
    def __init__(self, threshold: int = 3, cooldown_seconds: int = 300):
        self.threshold = threshold
        self.cooldown_seconds = cooldown_seconds
        self._state: dict[str, dict[str, float | int]] = {}

    def is_open(self, key: str) -> bool:
        state = self._state.get(key)
        if not state:
            return False
        fail_count = int(state.get("fail_count", 0))
        last_trip_time = float(state.get("last_trip_time", 0))
        if fail_count < self.threshold:
            return False
        if time.time() - last_trip_time > self.cooldown_seconds:
            self._state[key] = {"fail_count": 0, "last_trip_time": 0}
            return False
        return True

    def record_failure(self, key: str) -> None:
        state = self._state.setdefault(key, {"fail_count": 0, "last_trip_time": 0})
        state["fail_count"] = int(state.get("fail_count", 0)) + 1
        if int(state["fail_count"]) >= self.threshold:
            state["last_trip_time"] = time.time()

    def record_success(self, key: str) -> None:
        self._state[key] = {"fail_count": 0, "last_trip_time": 0}

    def retry_after_seconds(self, key: str) -> int:
        state = self._state.get(key)
        if not state:
            return 0
        remaining = self.cooldown_seconds - (time.time() - float(state.get("last_trip_time", 0)))
        return max(0, int(remaining))


_GLOBAL_BREAKER = CircuitBreaker()


class CrawlerClient:
    """
    source: 数据源名称，写入 CrawlResult.source。
    host_key: 熔断器键名；为空时使用 source，不为空时使用 host_key。
    """

    def __init__(
        self,
        timeout: int = 8,
        retries: int = 2,
        proxy_mode: str = "clear",
        circuit_breaker: CircuitBreaker | None = None,
    ):
        self.timeout = timeout
        self.retries = retries
        self.proxy_mode = proxy_mode
        self.circuit_breaker = circuit_breaker or _GLOBAL_BREAKER
        self.session = requests.Session()
        if proxy_mode == "clear":
            self._clear_proxy()
            self.session.trust_env = False

    @staticmethod
    def _clear_proxy() -> None:
        _clear_all_proxy()

    @staticmethod
    def _default_headers() -> dict[str, str]:
        return {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json,text/plain,*/*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Connection": "close",
        }

    @staticmethod
    def _breaker_key(url: str, source: str, host_key: str) -> str:
        if host_key:
            return host_key
        if source:
            parsed = urlparse(url)
            return f"{source}:{parsed.netloc}" if parsed.netloc else source
        return urlparse(url).netloc or url

    def get_json(
        self,
        url: str,
        params: dict | None = None,
        headers: dict | None = None,
        cookies: dict | None = None,
        data: str | bytes | dict | None = None,
        json_body: dict | list | None = None,
        source: str = "",
        host_key: str = "",
    ) -> CrawlResult:
        result = self._request(
            "GET",
            url,
            params=params,
            headers=headers,
            cookies=cookies,
            data=data,
            json_body=json_body,
            source=source,
            host_key=host_key,
        )
        if not result.ok:
            return result
        try:
            if isinstance(result.data, Response):
                result.data = result.data.json()
            elif isinstance(result.data, str):
                result.data = json.loads(result.data)
        except Exception as exc:
            return CrawlResult(
                ok=False,
                source=source,
                error=PARSE_ERROR,
                error_detail=str(exc),
                elapsed_ms=result.elapsed_ms,
                user_message=f"{source or '数据源'} 响应解析失败",
            )
        return result

    def get_text(
        self,
        url: str,
        params: dict | None = None,
        headers: dict | None = None,
        cookies: dict | None = None,
        data: str | bytes | dict | None = None,
        json_body: dict | list | None = None,
        source: str = "",
        host_key: str = "",
    ) -> CrawlResult:
        result = self._request(
            "GET",
            url,
            params=params,
            headers=headers,
            cookies=cookies,
            data=data,
            json_body=json_body,
            source=source,
            host_key=host_key,
        )
        if result.ok and isinstance(result.data, Response):
            result.data = result.data.text
        return result

    def _request(
        self,
        method: str,
        url: str,
        params: dict | None,
        headers: dict | None = None,
        cookies: dict | None = None,
        data: str | bytes | dict | None = None,
        json_body: dict | list | None = None,
        source: str = "",
        host_key: str = "",
    ) -> CrawlResult:
        breaker_key = self._breaker_key(url, source, host_key)
        if self.circuit_breaker.is_open(breaker_key):
            retry_after = self.circuit_breaker.retry_after_seconds(breaker_key)
            return CrawlResult(
                ok=False,
                source=source,
                error=CIRCUIT_OPEN,
                error_detail=breaker_key,
                user_message=f"{source or breaker_key} 已熔断，约 {retry_after} 秒后重试",
            )

        attempts = max(1, self.retries + 1)
        last_result: CrawlResult | None = None
        for _ in range(attempts):
            started = time.perf_counter()
            try:
                response = self.session.request(
                    method,
                    url,
                    params=params,
                    headers={**self._default_headers(), **(headers or {})},
                    cookies=cookies,
                    data=data,
                    json=json_body,
                    timeout=self.timeout,
                )
                elapsed_ms = int((time.perf_counter() - started) * 1000)
                result = self._result_from_response(response, source, elapsed_ms)
            except (Timeout, ReadTimeout) as exc:
                elapsed_ms = int((time.perf_counter() - started) * 1000)
                result = CrawlResult(
                    ok=False,
                    source=source,
                    error=NETWORK_TIMEOUT,
                    error_detail=str(exc),
                    elapsed_ms=elapsed_ms,
                    user_message=f"{source or '数据源'} 请求超时",
                )
            except ProxyError as exc:
                elapsed_ms = int((time.perf_counter() - started) * 1000)
                self._clear_proxy()
                result = CrawlResult(
                    ok=False,
                    source=source,
                    error=PROXY_ERROR,
                    error_detail=str(exc),
                    elapsed_ms=elapsed_ms,
                    user_message=f"{source or '数据源'} 代理连接失败",
                )
            except ConnectionError as exc:
                elapsed_ms = int((time.perf_counter() - started) * 1000)
                error = REMOTE_DISCONNECTED if "RemoteDisconnected" in repr(exc) else REMOTE_DISCONNECTED
                result = CrawlResult(
                    ok=False,
                    source=source,
                    error=error,
                    error_detail=str(exc),
                    elapsed_ms=elapsed_ms,
                    user_message=f"{source or '数据源'} 远端断开连接",
                )
            except RemoteDisconnected as exc:
                elapsed_ms = int((time.perf_counter() - started) * 1000)
                result = CrawlResult(
                    ok=False,
                    source=source,
                    error=REMOTE_DISCONNECTED,
                    error_detail=str(exc),
                    elapsed_ms=elapsed_ms,
                    user_message=f"{source or '数据源'} 远端断开连接",
                )
            except Exception as exc:
                elapsed_ms = int((time.perf_counter() - started) * 1000)
                result = CrawlResult(
                    ok=False,
                    source=source,
                    error=SOURCE_UNAVAILABLE,
                    error_detail=str(exc),
                    elapsed_ms=elapsed_ms,
                    user_message=f"{source or '数据源'} 请求失败",
                )

            last_result = result
            if result.ok:
                self.circuit_breaker.record_success(breaker_key)
                return result
            if result.error not in RETRYABLE:
                break

        if last_result is None:
            last_result = CrawlResult(ok=False, source=source, error=EMPTY_RESPONSE)
        self.circuit_breaker.record_failure(breaker_key)
        return last_result

    @staticmethod
    def _result_from_response(response: Response, source: str, elapsed_ms: int) -> CrawlResult:
        if 400 <= response.status_code < 500:
            return CrawlResult(
                ok=False,
                source=source,
                error=HTTP_ERROR_4XX,
                error_detail=f"HTTP {response.status_code}",
                elapsed_ms=elapsed_ms,
                user_message=f"{source or '数据源'} 请求被拒绝",
            )
        if response.status_code >= 500:
            return CrawlResult(
                ok=False,
                source=source,
                error=HTTP_ERROR_5XX,
                error_detail=f"HTTP {response.status_code}",
                elapsed_ms=elapsed_ms,
                user_message=f"{source or '数据源'} 服务端错误",
            )
        if not response.content:
            return CrawlResult(
                ok=False,
                source=source,
                error=EMPTY_RESPONSE,
                elapsed_ms=elapsed_ms,
                user_message=f"{source or '数据源'} 返回空响应",
            )
        return CrawlResult(ok=True, data=response, source=source, elapsed_ms=elapsed_ms)
