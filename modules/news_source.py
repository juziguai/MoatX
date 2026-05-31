"""Unified NewsSource abstract base class with observability components.

Pattern mirrors DataSource ABC + observability from the market data module.
"""

from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

_logger = logging.getLogger("moatx.news_source")


# ─── Enums & Data Classes ──────────────────────────

class NewsCapability(Enum):
    """What a news source can provide."""
    RSS_FETCH = "rss"
    HTTP_JSON_FETCH = "http_json"
    JSONP_FETCH = "jsonp"
    HTML_SCRAPE = "html"


@dataclass
class NewsHealth:
    """Health check result for a news source."""
    source: str
    healthy: bool
    latency_ms: float = 0.0
    error: str = ""
    items_fetched: int = 0


# ─── Observability Components ──────────────────────

@dataclass
class NewsRateLimiter:
    """Per-source-domain rate limiter (token bucket)."""
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


class NewsRateLimitRegistry:
    """Per-source-domain rate limit registry.

    Defaults: RSS sources 2 req/s, HTTP/JSON 5 req/s, HTML scrape 1 req/s.
    """

    def __init__(self, defaults: dict[str, float] | None = None):
        self._defaults = defaults or {
            "rss": 2.0,
            "http_json": 5.0,
            "jsonp": 5.0,
            "html": 1.0,
        }

    def get(self, source_type: str) -> NewsRateLimiter:
        rate = self._defaults.get(source_type, 3.0)
        return NewsRateLimiter(rate)

    def acquire(self, source_type: str) -> bool:
        return self.get(source_type).acquire()


@dataclass
class NewsHealthTracker:
    """Track consecutive failures, auto-disable, and auto-recover news sources."""

    alert_threshold: int = 3
    _failures: dict[str, int] = field(default_factory=dict)
    _last_alerted: dict[str, float] = field(default_factory=dict)
    _disabled: set[str] = field(default_factory=set)
    _recovery_probe_at: dict[str, float] = field(default_factory=dict)

    def record_success(self, source: str):
        was_disabled = source in self._disabled
        self._failures[source] = 0
        if was_disabled:
            self._disabled.discard(source)
            self._recovery_probe_at.pop(source, None)
            _logger.info("[NEWS_RECOVERY] %s recovered, re-enabled", source)

    def record_failure(self, source: str, error: str = ""):
        prev = self._failures.get(source, 0)
        self._failures[source] = prev + 1
        if self._failures[source] >= self.alert_threshold:
            self._disabled.add(source)
            self._recovery_probe_at[source] = time.time() + 300
            self._maybe_alert(source, error)

    def _maybe_alert(self, source: str, error: str):
        now = time.time()
        last = self._last_alerted.get(source, 0)
        if now - last > 300:
            self._last_alerted[source] = now
            _logger.warning(
                "[NEWS_ALERT] %s: %d consecutive failures, auto-disabled. Last: %s",
                source, self._failures[source], error,
            )

    def is_healthy(self, source: str) -> bool:
        return source not in self._disabled

    def is_disabled(self, source: str) -> bool:
        return source in self._disabled

    def disabled_sources(self) -> set[str]:
        return set(self._disabled)

    def due_for_recovery_probe(self) -> list[str]:
        """Return disabled sources due for a recovery probe."""
        now = time.time()
        return [s for s, t in self._recovery_probe_at.items() if now >= t and s in self._disabled]

    def status(self) -> dict[str, dict]:
        return {
            src: {
                "failures": n,
                "healthy": src not in self._disabled,
                "disabled": src in self._disabled,
            }
            for src, n in self._failures.items()
        }

class NewsMetricsCollector:
    """Per-source latency / item count / success rate metrics."""

    _latencies: dict[str, list[float]] = field(default_factory=lambda: defaultdict(list))
    _item_counts: dict[str, list[int]] = field(default_factory=lambda: defaultdict(list))
    _successes: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    _failures: dict[str, int] = field(default_factory=lambda: defaultdict(int))

    def record(self, source: str, latency_ms: float, items: int, success: bool):
        self._latencies[source].append(latency_ms)
        self._item_counts[source].append(items)
        if success:
            self._successes[source] += 1
        else:
            self._failures[source] += 1

    def stats(self, source: str | None = None) -> dict:
        """Return per-source or aggregate stats."""
        sources = [source] if source else list(self._latencies.keys())
        result = {}
        for src in sources:
            lats = self._latencies.get(src, [])
            items = self._item_counts.get(src, [])
            total = self._successes.get(src, 0) + self._failures.get(src, 0)
            if not lats:
                result[src] = {"p50_ms": 0, "p99_ms": 0, "success_rate": 0, "calls": 0, "total_items": 0}
                continue
            sorted_lats = sorted(lats[-100:])
            result[src] = {
                "p50_ms": round(sorted_lats[len(sorted_lats) // 2], 1),
                "p99_ms": round(sorted_lats[min(int(len(sorted_lats) * 0.99), len(sorted_lats) - 1)], 1),
                "success_rate": round(self._successes[src] / total * 100, 1) if total else 0,
                "calls": total,
                "total_items": sum(items[-100:]),
            }
        if source:
            return result.get(source, {})
        # Aggregate
        all_items = sum(sum(v[-100:]) for v in self._item_counts.values())
        total_calls = sum(self._successes.values()) + sum(self._failures.values())
        total_success = sum(self._successes.values())
        result["_aggregate"] = {
            "sources": len(sources),
            "total_calls": total_calls,
            "total_items": all_items,
            "overall_success_rate": round(total_success / total_calls * 100, 1) if total_calls else 0,
        }
        return result


# ─── NewsSource ABC ───────────────────────────────

class NewsSource(ABC):
    """Abstract base for all news data providers.

    Subclass and implement:
      - name: str property
      - capabilities() -> set[NewsCapability]
      - fetch(capability, **params) -> NewsResult
      - health() -> NewsHealth (optional, default provided)
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique provider identifier."""
        ...

    @abstractmethod
    def capabilities(self) -> set[NewsCapability]:
        """Return the set of capabilities this provider supports."""
        ...

    @abstractmethod
    def fetch(self, capability: NewsCapability, **params: Any) -> Any:
        """Fetch news items for a capability.

        Args:
            capability: What to fetch
            **params: source-specific parameters (url, headers, field_map, etc.)

        Returns:
            NewsResult with list[NewsItem] or error
        """
        ...

    def health(self) -> NewsHealth:
        """Default health check."""
        caps = self.capabilities()
        if not caps:
            return NewsHealth(source=self.name, healthy=True, error="no capabilities")

        t0 = time.time()
        try:
            cap = next(iter(caps))
            from modules.event_intelligence.models import EventSource
            dummy = EventSource(
                id=f"{self.name}_health", name=f"{self.name}_health",
                type=cap.value, url="", enabled=False, category="health",
                weight=0.0, headers={}, field_map={}, record_path="",
            )
            result = self.fetch(cap, source=dummy)
            healthy = getattr(result, "ok", False)
            return NewsHealth(
                source=self.name,
                healthy=healthy,
                latency_ms=(time.time() - t0) * 1000,
                items_fetched=len(getattr(result, "data", [])) if healthy else 0,
            )
        except Exception as exc:
            return NewsHealth(
                source=self.name,
                healthy=False,
                latency_ms=(time.time() - t0) * 1000,
                error=str(exc),
            )
