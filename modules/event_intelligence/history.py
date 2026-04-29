"""Configured historical macro event samples."""

from __future__ import annotations

from pathlib import Path
from typing import Any


_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_DEFAULT_PATH = _PROJECT_ROOT / "data" / "event_history.toml"


class EventHistoryRegistry:
    """Load manually curated historical event trigger points."""

    def __init__(self, path: str | Path | None = None):
        self._path = Path(path) if path else _DEFAULT_PATH
        self._events: list[dict[str, Any]] | None = None

    def load(self) -> list[dict[str, Any]]:
        """Return configured historical events."""
        if self._events is not None:
            return self._events
        if not self._path.exists():
            self._events = []
            return self._events

        import tomllib

        raw = tomllib.loads(self._path.read_text(encoding="utf-8"))
        events: list[dict[str, Any]] = []
        for item in raw.get("events", []):
            event_id = str(item.get("event_id") or "")
            trigger_date = str(item.get("trigger_date") or "")
            if not event_id or not trigger_date:
                continue
            events.append(
                {
                    "event_id": event_id,
                    "name": str(item.get("name") or event_id),
                    "trigger_date": trigger_date,
                    "category": str(item.get("category") or "general"),
                    "severity": float(item.get("severity") or 0),
                    "summary": str(item.get("summary") or ""),
                    "source_url": str(item.get("source_url") or ""),
                    "related_sectors": [str(x) for x in item.get("related_sectors", [])],
                }
            )
        self._events = events
        return events

    def list(self, event_id: str = "", limit: int = 100) -> list[dict[str, Any]]:
        """List historical events, optionally filtered by event id."""
        rows = [row for row in self.load() if not event_id or row["event_id"] == event_id]
        rows = sorted(rows, key=lambda row: row["trigger_date"], reverse=True)
        return rows[:limit]

    def count(self, event_id: str = "") -> int:
        """Return historical event count."""
        return len(self.list(event_id=event_id, limit=10_000))
