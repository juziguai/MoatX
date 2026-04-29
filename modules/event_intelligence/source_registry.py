"""News source registry for event intelligence."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from modules.config import tomllib

from .models import EventSource

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_DEFAULT_PATH = _PROJECT_ROOT / "data" / "event_sources.toml"


class SourceRegistry:
    """Load and expose configured event news sources."""

    def __init__(self, path: str | Path | None = None):
        self._path = Path(path) if path else _DEFAULT_PATH
        self._sources: list[EventSource] | None = None

    def load(self) -> list[EventSource]:
        """Load sources from TOML config."""
        if self._sources is not None:
            return self._sources

        if not self._path.exists():
            self._sources = []
            return self._sources

        raw = tomllib.loads(self._path.read_text(encoding="utf-8"))
        sources: list[EventSource] = []
        for item in raw.get("sources", []):
            sources.append(self._parse_source(item))
        self._sources = sources
        return sources

    def enabled(self) -> list[EventSource]:
        """Return enabled sources with a non-empty URL."""
        return [s for s in self.load() if s.enabled and bool(s.url)]

    @staticmethod
    def _parse_source(item: dict[str, Any]) -> EventSource:
        return EventSource(
            id=str(item.get("id", "")),
            name=str(item.get("name", item.get("id", ""))),
            type=item.get("type", "http_json"),
            url=str(item.get("url", "")),
            enabled=bool(item.get("enabled", False)),
            category=str(item.get("category", "general") or "general"),
            weight=float(item.get("weight", 0.5) or 0.5),
            headers=dict(item.get("headers", {}) or {}),
            field_map=dict(item.get("field_map", {}) or {}),
            record_path=str(item.get("record_path", "") or ""),
        )
