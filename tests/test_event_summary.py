import sqlite3

from modules.db.event_store import EventStore
from modules.db.migrations import run_migrations
from modules.event_intelligence.models import EventOpportunity, EventState
from modules.event_intelligence.summary import build_event_monitor_summary, format_event_monitor_summary


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


def test_event_monitor_summary_returns_top_events_with_sectors_and_stocks():
    db = MemoryDB()
    try:
        db.event().upsert_state(
            EventState(
                event_id="hormuz_closure_risk",
                name="霍尔木兹关闭风险",
                probability=0.70,
                impact_strength=0.80,
                status="escalating",
            )
        )
        db.event().insert_opportunity(
            EventOpportunity(
                event_id="hormuz_closure_risk",
                symbol="600938",
                name="中国海油",
                sector_tags=["石油行业"],
                opportunity_score=88.0,
                recommendation="重点关注",
            )
        )

        summary = build_event_monitor_summary(db=db, top_n=3)
        lines = format_event_monitor_summary(summary)

        assert summary["top_events"][0]["event_id"] == "hormuz_closure_risk"
        assert summary["top_events"][0]["alert"] is True
        assert summary["top_events"][0]["sectors"] == ["石油行业"]
        assert summary["top_events"][0]["opportunities"][0]["symbol"] == "600938"
        assert any("霍尔木兹关闭风险" in line for line in lines)
    finally:
        db.close()
