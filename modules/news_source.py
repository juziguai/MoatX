"""Unified NewsSource abstract base class for event intelligence.

Pattern mirrors DataSource ABC from the market data module.
"""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any


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
