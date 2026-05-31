"""Observability — rate limiting, health tracking, and metrics for data sources."""

from __future__ import annotations

import logging, time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime

_logger = logging.getLogger("moatx.obs")


@dataclass
class RateLimiter:
    """Per-source rate limiter (token bucket)."""
    rate: float  # requests per second
    _tokens: float = field(default=0.0, init=False)
    _last: float = field(default=0.0, init=False)

    def __post_init__(self):
        self._tokens = self.rate

    def acquire(self) -> bool:
        now = time.time()
        elapsed = now - self._last
        self._tokens = min(self.rate, self._tokens + elapsed * self.rate)
        self._last = now
        if self._tokens >= 1.0:
            self._tokens -= 1.0
            return True
        return False

    def wait_and_acquire(self, timeout: float = 10.0) -> bool:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.acquire():
                return True
            time.sleep(0.1)
        return False


class RateLimitRegistry:
    """Registry of per-source rate limiters."""

    def __init__(self, defaults: dict[str, float] | None = None):
        self._limiters: dict[str, RateLimiter] = {}
        defaults = defaults or {"sina": 3.0, "tencent": 10.0, "eastmoney": 10.0, "ths": 5.0}
        for name, rate in defaults.items():
            self._limiters[name] = RateLimiter(rate)

    def get(self, source: str) -> RateLimiter:
        if source not in self._limiters:
            self._limiters[source] = RateLimiter(5.0)  # default 5 req/s
        return self._limiters[source]

    def acquire(self, source: str) -> bool:
        return self.get(source).acquire()


@dataclass
class HealthTracker:
    """Track consecutive failures per source, trigger alerts at threshold."""

    alert_threshold: int = 3
    _failures: dict[str, int] = field(default_factory=dict)
    _last_alerted: dict[str, float] = field(default_factory=dict)

    def record_success(self, source: str):
        self._failures[source] = 0

    def record_failure(self, source: str, error: str = ""):
        prev = self._failures.get(source, 0)
        self._failures[source] = prev + 1
        if self._failures[source] >= self.alert_threshold:
            self._maybe_alert(source, error)

    def _maybe_alert(self, source: str, error: str):
        now = time.time()
        last = self._last_alerted.get(source, 0)
        if now - last > 300:  # at most once per 5 min
            self._last_alerted[source] = now
            _logger.warning("[ALERT] %s: %d consecutive failures. Last: %s",
                            source, self._failures[source], error)

    def is_healthy(self, source: str) -> bool:
        return self._failures.get(source, 0) < self.alert_threshold

    def status(self) -> dict[str, dict]:
        return {
            src: {"failures": n, "healthy": n < self.alert_threshold}
            for src, n in self._failures.items()
        }


@dataclass
class MetricsCollector:
    """Collect per-source latency and success metrics."""

    _latencies: dict[str, list[float]] = field(default_factory=lambda: defaultdict(list))
    _successes: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    _failures: dict[str, int] = field(default_factory=lambda: defaultdict(int))

    def record(self, source: str, latency_ms: float, success: bool):
        self._latencies[source].append(latency_ms)
        if success:
            self._successes[source] += 1
        else:
            self._failures[source] += 1

    def stats(self, source: str) -> dict:
        lats = self._latencies.get(source, [])
        total = self._successes.get(source, 0) + self._failures.get(source, 0)
        if not lats:
            return {"p50": 0, "p99": 0, "success_rate": 0, "calls": 0}
        sorted_lats = sorted(lats[-100:])  # last 100 samples
        return {
            "p50": sorted_lats[len(sorted_lats) // 2],
            "p99": sorted_lats[int(len(sorted_lats) * 0.99)],
            "success_rate": round(self._successes[source] / total * 100, 1) if total else 0,
            "calls": total,
        }
