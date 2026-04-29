import sqlite3

from modules.db.event_store import EventStore
from modules.db.migrations import run_migrations
from modules.event_intelligence.context import EventContextBuilder
from modules.event_intelligence.models import EventSignal, EventState, NewsItem


class MemoryDB:
    def __init__(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.execute("PRAGMA foreign_keys=ON")
        run_migrations(self.conn)
        self._event = EventStore(self.conn)

    def event(self):
        return self._event

    def close(self):
        self.conn.close()


def test_event_context_exports_extension_contracts():
    db = MemoryDB()
    try:
        news_id = db.event().insert_news(NewsItem(source="test", title="Hormuz closure risk rises"))
        db.event().insert_signal(EventSignal(
            event_id="hormuz_closure_risk",
            news_id=news_id,
            event_type="energy_supply_risk",
            entities={"stage": "escalating"},
            severity=0.8,
            confidence=0.7,
        ))
        db.event().upsert_state(
            EventState(
                event_id="hormuz_closure_risk",
                name="霍尔木兹关闭风险",
                probability=0.62,
                impact_strength=0.85,
                status="escalating",
            )
        )
        db.event().upsert_source_quality(
            source_id="test",
            name="Test Source",
            category="geopolitics",
            type="rss",
            enabled=True,
            fetched=20,
            inserted=20,
            duplicates=0,
            errors=0,
            last_success_at="2026-04-27 10:00:00",
            last_error="",
        )
        db.event().refresh_source_quality_signal_hits(["test"])

        context = EventContextBuilder(db=db).build(limit=5)

        assert context["schema_version"] == "event_context_v1"
        assert context["states"][0]["event_id"] == "hormuz_closure_risk"
        assert context["latest_news"][0]["title"] == "Hormuz closure risk rises"
        assert "source_quality" in context
        assert context["source_quality"][0]["source_recommendation"] in {
            "keep",
            "watch",
            "watch_low_signal",
            "promote",
            "disable_candidate",
        }
        assert "source_recommendation_reason" in context["source_quality"][0]
        assert context["signal_evidence"][0]["event_id"] == "hormuz_closure_risk"
        assert "elasticity_summary" in context
        assert "historical_events" in context
        assert context["historical_event_count"] >= len(context["historical_events"])
        assert "sector_graph_version" in context
        assert context["extension_points"]["auto_trading"]["status"] == "prepared_disabled"
        assert context["extension_points"]["external_llm"]["status"] == "prepared_disabled"
    finally:
        db.close()
