"""Data models for macro event intelligence."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal


Direction = Literal["bullish", "bearish", "neutral"]
EventStatus = Literal["watching", "escalating", "confirmed", "pricing", "resolved", "expired"]
SourceType = Literal["http_json", "rss", "html", "api", "jsonp"]


def now_ts() -> str:
    """Return local timestamp in the format used by the warehouse."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


@dataclass(slots=True)
class NewsItem:
    """Normalized news record collected from an event source."""

    source: str
    title: str
    summary: str = ""
    url: str = ""
    published_at: str = ""
    fetched_at: str = field(default_factory=now_ts)
    language: str = "zh"
    raw_hash: str = ""


@dataclass(slots=True)
class EventSource:
    """Configured news source for event collection."""

    id: str
    name: str
    type: SourceType = "http_json"
    url: str = ""
    enabled: bool = False
    category: str = "general"
    weight: float = 0.5
    headers: dict = field(default_factory=dict)
    field_map: dict = field(default_factory=dict)
    record_path: str = ""


@dataclass(slots=True)
class EventSignal:
    """Structured signal extracted from one news item."""

    event_id: str
    news_id: int | None = None
    event_type: str = ""
    entities: dict = field(default_factory=dict)
    matched_keywords: list[str] = field(default_factory=list)
    matched_actions: list[str] = field(default_factory=list)
    severity: float = 0.0
    confidence: float = 0.0
    direction: Direction = "neutral"
    created_at: str = field(default_factory=now_ts)


@dataclass(slots=True)
class EventState:
    """Aggregated state for an evolving macro event."""

    event_id: str
    name: str
    probability: float = 0.0
    impact_strength: float = 0.0
    status: EventStatus = "watching"
    evidence_count: int = 0
    sources_count: int = 0
    last_signal_at: str = ""
    updated_at: str = field(default_factory=now_ts)


@dataclass(slots=True)
class TransmissionEffect:
    """One event-to-asset/sector/concept transmission rule."""

    event_id: str
    target: str
    target_type: Literal["asset", "sector", "concept", "stock"] = "sector"
    direction: Direction = "neutral"
    impact: float = 0.0


@dataclass(slots=True)
class EventDefinition:
    """Rule definition for one macro event and its transmission effects."""

    id: str
    name: str
    event_types: list[str] = field(default_factory=list)
    keywords: list[str] = field(default_factory=list)
    actions: list[str] = field(default_factory=list)
    base_probability: float = 0.0
    effects: list[TransmissionEffect] = field(default_factory=list)


@dataclass(slots=True)
class EventOpportunity:
    """A stock opportunity generated from an event state and transmission map."""

    event_id: str
    symbol: str
    name: str = ""
    sector_tags: list[str] = field(default_factory=list)
    opportunity_score: float = 0.0
    event_score: float = 0.0
    exposure_score: float = 0.0
    underpricing_score: float = 0.0
    timing_score: float = 0.0
    risk_penalty: float = 0.0
    recommendation: str = ""
    evidence: dict = field(default_factory=dict)
    created_at: str = field(default_factory=now_ts)
