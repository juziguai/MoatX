"""Event probability and state updates."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

import pandas as pd

from modules.config import cfg
from modules.db import DatabaseManager

from .models import EventState, now_ts
from .transmission import EventTransmissionMap


class EventProbabilityEngine:
    """Aggregate event signals into persistent event states."""

    def __init__(
        self,
        db: DatabaseManager | None = None,
        transmission_map: EventTransmissionMap | None = None,
    ):
        self._db = db or DatabaseManager(cfg().data.warehouse_path)
        self._map = transmission_map or EventTransmissionMap()

    def update_states(self, limit_per_event: int = 200) -> dict[str, Any]:
        """Update all states for events with known definitions."""
        updated = 0
        for definition in self._map.load():
            signals = self._db.event().list_signals(event_id=definition.id, limit=limit_per_event)
            if signals.empty:
                continue
            state = self._build_state(definition.id, definition.name, definition.base_probability, signals)
            self._db.event().upsert_state(state)
            updated += 1
        return {"updated": updated}

    def _build_state(
        self,
        event_id: str,
        name: str,
        base_probability: float,
        signals: pd.DataFrame,
    ) -> EventState:
        now = datetime.now()
        evidence_score = 0.0
        source_values: set[str] = set()
        last_signal_at = ""

        for _, row in signals.iterrows():
            created_at = str(row.get("created_at") or "")
            last_signal_at = max(last_signal_at, created_at)
            age_weight = self._age_weight(created_at, now)
            severity = float(row.get("severity") or 0)
            confidence = float(row.get("confidence") or 0)
            entities = self._entities(row)
            source = entities.get("source")
            source_weight = self._source_weight(str(source or ""))
            stage_weight = self._stage_weight(str(entities.get("stage") or "watching"))
            evidence_score += severity * confidence * age_weight * source_weight * stage_weight

            if source:
                source_values.add(str(source))

        cross_source_bonus = min(len(source_values), 4) * 0.04
        probability = min(1.0, base_probability + evidence_score * 0.18 + cross_source_bonus)
        impact_strength = min(1.0, max(float(x or 0) for x in signals["severity"].tolist()))

        return EventState(
            event_id=event_id,
            name=name,
            probability=round(probability, 3),
            impact_strength=round(impact_strength, 3),
            status=self._status(probability),
            evidence_count=len(signals),
            sources_count=len(source_values),
            last_signal_at=last_signal_at,
            updated_at=now_ts(),
        )

    @staticmethod
    def _age_weight(created_at: str, now: datetime) -> float:
        try:
            ts = datetime.strptime(created_at[:19], "%Y-%m-%d %H:%M:%S")
        except Exception:
            return 0.5
        hours = max((now - ts).total_seconds() / 3600, 0)
        if hours <= 6:
            return 1.0
        if hours <= 24:
            return 0.8
        if hours <= 72:
            return 0.5
        if hours <= 168:
            return 0.25
        return 0.1

    @staticmethod
    def _status(probability: float) -> str:
        if probability >= 0.75:
            return "confirmed"
        if probability >= 0.55:
            return "escalating"
        return "watching"

    @staticmethod
    def _entities(row: pd.Series) -> dict[str, Any]:
        try:
            payload = json.loads(row.get("entities_json") or "{}")
            return payload if isinstance(payload, dict) else {}
        except Exception:
            return {}

    def _source_weight(self, source_id: str) -> float:
        if not source_id:
            return 1.0
        quality = self._db.event().get_source_quality(source_id) or {}
        hit_rate = max(0.0, min(float(quality.get("hit_rate") or 0.0), 0.8))
        errors = int(quality.get("errors") or 0)
        error_penalty = 0.15 if errors else 0.0
        return max(0.7, min(1.25, 0.9 + hit_rate * 0.45 - error_penalty))

    @staticmethod
    def _stage_weight(stage: str) -> float:
        return {
            "confirmed": 1.25,
            "escalating": 1.05,
            "rumor": 0.7,
            "watching": 0.85,
            "resolved": 0.35,
            "denied": 0.2,
        }.get(stage, 0.85)


def update_event_states() -> dict[str, Any]:
    """Convenience entry point for scheduler/CLI."""
    return EventProbabilityEngine().update_states()
