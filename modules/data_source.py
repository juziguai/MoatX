"""Unified DataSource abstract base class.

One interface for all data providers: quotes, boards, financials.

Usage:
    class MySource(DataSource):
        name = "my_source"
        
        def capabilities(self) -> set[Capability]:
            return {Capability.QUOTE, Capability.BOARD_INDUSTRY}
        
        def fetch(self, cap: Capability, **params) -> Result:
            ...
        
        def health(self) -> Health:
            ...

"""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any



class Capability(Enum):
    """What a data source can provide."""
    QUOTE = "quote"
    BOARD_INDUSTRY = "board_industry"
    BOARD_CONCEPT = "board_concept"
    DIVIDEND = "dividend"
    PROFIT_FORECAST = "profit_forecast"
    MAJOR_SHAREHOLDERS = "major_shareholders"
    SHAREHOLDER_CHANGES = "shareholder_changes"
    PROFIT_SHEET = "profit_sheet"
    CASH_FLOW = "cash_flow"
    FUND_FLOW = "fund_flow"
    VALUATION = "valuation"
    INDEX_QUOTE = "index_quote"
    STOCK_INFO = "stock_info"


@dataclass
class Health:
    """Health check result for a data source."""
    source: str
    healthy: bool
    latency_ms: float = 0.0
    error: str = ""
    sample_count: int = 0
    checked_at: str = ""

    def __post_init__(self):
        if not self.checked_at:
            from datetime import datetime
            self.checked_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class DataSource(ABC):
    """Abstract base for all data providers.

    Subclass and implement:
      - name: str property
      - capabilities() -> set[Capability]
      - fetch(capability, **params) -> Result
      - health() -> Health (optional, default provided)
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique provider identifier, e.g. 'tencent', 'sina'."""
        ...

    @abstractmethod
    def capabilities(self) -> set[Capability]:
        """Return the set of capabilities this provider supports."""
        ...

    @abstractmethod
    def fetch(self, capability: Capability, **params: Any) -> Any:
        """Fetch data for a capability.

        Args:
            capability: What to fetch
            **params: Capability-specific parameters
                - QUOTE: symbols=list[str]
                - BOARD_INDUSTRY/CONCEPT: use_cache=bool
                - DIVIDEND etc.: symbol=str

        Returns:
            Result[Any]
        """
        ...

    def health(self) -> Health:
        """Default health check using QUOTE or first capability."""

        caps = self.capabilities()
        test_cap = (
            Capability.QUOTE if Capability.QUOTE in caps
            else next(iter(caps)) if caps else None
        )
        if test_cap is None:
            return Health(source=self.name, healthy=True, error="no capabilities")

        t0 = time.time()
        try:
            params = {"symbols": ["600519"]} if test_cap == Capability.QUOTE else {"use_cache": False}
            result = self.fetch(test_cap, **params)
            healthy = getattr(result, "ok", False)
            return Health(
                source=self.name,
                healthy=healthy,
                latency_ms=(time.time() - t0) * 1000,
                error="" if healthy else str(getattr(result, "error", "")),
                sample_count=1 if healthy else 0,
            )
        except Exception as exc:
            return Health(
                source=self.name,
                healthy=False,
                latency_ms=(time.time() - t0) * 1000,
                error=str(exc),
                sample_count=0,
            )

    # Backward compatibility with old QuoteSource interface

    def fetch_quotes(self, symbols: list[str]) -> dict[str, dict]:
        """Backward-compatible quote fetch for old QuoteManager callers."""
        result = self.fetch(Capability.QUOTE, symbols=symbols)
        if getattr(result, "ok", False) and getattr(result, "data", None):
            return result.data
        return {}

    def health_check(self):
        """Backward-compatible health check for old QuoteSource interface."""
        from modules.datasource import SourceHealth
        h = self.health()
        return SourceHealth(
            source=h.source,
            healthy=h.healthy,
            latency_ms=h.latency_ms,
            error=h.error,
            sample_count=h.sample_count,
        )
