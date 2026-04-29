"""Event intelligence context export for downstream extensions."""

from __future__ import annotations

from typing import Any

import pandas as pd

from modules.config import cfg
from modules.db import DatabaseManager
from modules.sector_tags import SectorTagProvider

from .history import EventHistoryRegistry
from .models import now_ts
from .source_quality import source_recommendation


class EventContextBuilder:
    """Build a stable JSON context for future automation and model adapters."""

    SCHEMA_VERSION = "event_context_v1"

    def __init__(self, db: DatabaseManager | None = None):
        self._db = db or DatabaseManager(cfg().data.warehouse_path)

    def build(self, limit: int = 20) -> dict[str, Any]:
        """Return a read-only event intelligence snapshot."""
        event_store = self._db.event()
        states = event_store.list_states(limit=limit)
        opportunities = event_store.list_opportunities(limit=limit)
        news = event_store.list_news(limit=limit)
        notifications = event_store.list_notifications(limit=limit)
        source_quality = event_store.list_source_quality(limit=limit)
        signal_evidence = event_store.list_signal_evidence(limit=limit)
        elasticity_runs = event_store.list_elasticity_runs(limit=5)
        history_registry = EventHistoryRegistry()
        history = history_registry.list(limit=limit)

        return {
            "schema_version": self.SCHEMA_VERSION,
            "generated_at": now_ts(),
            "states": self._records(states),
            "opportunities": self._records(opportunities),
            "latest_news": self._records(news),
            "notifications": self._records(notifications),
            "source_quality": self._source_quality_records(source_quality),
            "signal_evidence": self._records(signal_evidence),
            "elasticity_summary": self._records(elasticity_runs),
            "historical_events": history,
            "historical_event_count": history_registry.count(),
            "sector_graph_version": SectorTagProvider().graph_version(),
            "extension_points": {
                "auto_trading": {
                    "status": "prepared_disabled",
                    "contract": "consume states/opportunities, never place orders without explicit trading adapter",
                },
                "external_llm": {
                    "status": "prepared_disabled",
                    "contract": "send latest_news/states to a user-approved model adapter",
                },
                "complex_nlp": {
                    "status": "prepared_disabled",
                    "contract": "replace or enrich rule extractor with entity/event classifiers",
                },
                "elasticity_backtest": {
                    "status": "prepared_disabled",
                    "contract": "join event timelines with sector/stock forward returns",
                },
            },
        }

    @staticmethod
    def _records(df: pd.DataFrame) -> list[dict[str, Any]]:
        if df.empty:
            return []
        normalized = df.where(pd.notna(df), None)
        return normalized.to_dict(orient="records")

    @classmethod
    def _source_quality_records(cls, df: pd.DataFrame) -> list[dict[str, Any]]:
        records = cls._records(df)
        for row in records:
            row.update(source_recommendation(row))
        return records


def build_event_context(limit: int = 20) -> dict[str, Any]:
    """Convenience entry point for CLI and integrations."""
    return EventContextBuilder().build(limit=limit)
