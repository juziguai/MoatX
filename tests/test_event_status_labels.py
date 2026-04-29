import sqlite3

from modules.db.event_store import EventStore
from modules.db.migrations import run_migrations
from modules.event_intelligence.models import EventState, event_status_label
from modules.event_intelligence.reporter import EventReporter
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


def test_event_status_label_maps_internal_codes_to_chinese():
    assert event_status_label("confirmed") == "已确认"
    assert event_status_label("escalating") == "升级中"
    assert event_status_label("watching") == "观察中"
    assert event_status_label("rumor") == "传闻"
    assert event_status_label("denied") == "已否认"
    assert event_status_label("resolved") == "已缓和"


def test_event_report_renders_chinese_status_labels():
    db = MemoryDB()
    try:
        db.event().upsert_state(
            EventState(
                event_id="hormuz_closure_risk",
                name="霍尔木兹关闭风险",
                probability=0.95,
                impact_strength=0.9,
                status="confirmed",
            )
        )

        report = EventReporter(db=db).report(limit=5)

        assert "已确认" in report
        assert "| 霍尔木兹关闭风险 | 95% | 90% | confirmed |" not in report
    finally:
        db.close()


def test_event_summary_exposes_chinese_status_labels():
    db = MemoryDB()
    try:
        db.event().upsert_state(
            EventState(
                event_id="hormuz_closure_risk",
                name="霍尔木兹关闭风险",
                probability=0.95,
                impact_strength=0.9,
                status="escalating",
            )
        )

        summary = build_event_monitor_summary(db=db, top_n=1)
        lines = format_event_monitor_summary(summary)

        assert summary["top_events"][0]["status"] == "escalating"
        assert summary["top_events"][0]["status_label"] == "升级中"
        assert any("状态=升级中" in line for line in lines)
    finally:
        db.close()
