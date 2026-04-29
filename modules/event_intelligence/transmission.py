"""Event transmission map loading and matching."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .models import EventDefinition, TransmissionEffect

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_DEFAULT_PATH = _PROJECT_ROOT / "data" / "event_transmission_map.toml"


class EventTransmissionMap:
    """Load macro event rules and their asset/sector/concept effects."""

    def __init__(self, path: str | Path | None = None):
        self._path = Path(path) if path else _DEFAULT_PATH
        self._events: list[EventDefinition] | None = None

    def load(self) -> list[EventDefinition]:
        """Load event definitions from TOML config."""
        if self._events is not None:
            return self._events

        if not self._path.exists():
            self._events = []
            return self._events

        import tomllib

        raw = tomllib.loads(self._path.read_text(encoding="utf-8"))
        events = [self._parse_event(item) for item in raw.get("events", [])]
        self._events = events
        return events

    def get(self, event_id: str) -> EventDefinition | None:
        """Return one event definition by id."""
        for event in self.load():
            if event.id == event_id:
                return event
        return None

    def match_text(self, text: str) -> list[EventDefinition]:
        """Return event definitions whose keywords appear in text."""
        content = str(text or "")
        content_lower = content.lower()
        matched: list[EventDefinition] = []
        for event in self.load():
            if any(str(keyword).lower() in content_lower for keyword in event.keywords):
                matched.append(event)
        return matched

    @staticmethod
    def matched_keywords(text: str, event: EventDefinition) -> list[str]:
        """Return matched event keywords in text."""
        content = str(text or "").lower()
        return [keyword for keyword in event.keywords if str(keyword).lower() in content]

    @staticmethod
    def matched_actions(text: str, event: EventDefinition) -> list[str]:
        """Return matched event action words in text."""
        content = str(text or "").lower()
        return [action for action in event.actions if str(action).lower() in content]

    @staticmethod
    def _parse_event(item: dict[str, Any]) -> EventDefinition:
        event_id = str(item.get("id", ""))
        effects = [
            TransmissionEffect(
                event_id=event_id,
                target=str(effect.get("target", "")),
                target_type=effect.get("target_type", "sector"),
                direction=effect.get("direction", "neutral"),
                impact=float(effect.get("impact", 0.0) or 0.0),
            )
            for effect in item.get("effects", [])
        ]
        return EventDefinition(
            id=event_id,
            name=str(item.get("name", event_id)),
            event_types=[str(v) for v in item.get("event_types", [])],
            keywords=[str(v) for v in item.get("keywords", [])],
            actions=[str(v) for v in item.get("actions", [])],
            base_probability=float(item.get("base_probability", 0.0) or 0.0),
            effects=effects,
        )
