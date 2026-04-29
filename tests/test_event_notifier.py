import sqlite3

from modules.db.event_store import EventStore
from modules.db.migrations import run_migrations
from modules.event_intelligence.models import EventOpportunity, EventState
from modules.event_intelligence.notifier import EventNotifier


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


class FakeAlerter:
    def __init__(self):
        self.sent = []

    def send(self, report, title=None):
        self.sent.append((title, report))
        return True


def _insert_state(db, probability=0.60):
    db.event().upsert_state(
        EventState(
            event_id="hormuz_closure_risk",
            name="霍尔木兹关闭风险",
            probability=probability,
            impact_strength=0.85,
            status="escalating",
            evidence_count=2,
            sources_count=1,
            last_signal_at="2026-04-26 10:00:00",
        )
    )


def _insert_opportunity(db, score=80.0):
    db.event().insert_opportunity(
        EventOpportunity(
            event_id="hormuz_closure_risk",
            symbol="600028",
            name="Sinopec",
            sector_tags=["石油行业"],
            opportunity_score=score,
            recommendation="重点关注",
        )
    )


def test_notifier_dry_run_does_not_send_or_write_cooldown():
    db = MemoryDB()
    alerter = FakeAlerter()
    try:
        _insert_state(db, probability=0.60)
        notifier = EventNotifier(db=db, alerter=alerter)

        result = notifier.notify(send=False)

        assert result["dry_run"] is True
        assert result["candidates"] == 1
        assert result["sent"] == 0
        assert alerter.sent == []
        assert db.event().get_notification("hormuz_closure_risk") is None
    finally:
        db.close()


def test_notifier_send_writes_cooldown_and_skips_duplicate_report():
    db = MemoryDB()
    alerter = FakeAlerter()
    try:
        _insert_state(db, probability=0.60)
        _insert_opportunity(db, score=80.0)
        notifier = EventNotifier(db=db, alerter=alerter)

        first = notifier.notify(send=True)
        second = notifier.notify(send=True)

        assert first["sent"] == 1
        assert len(alerter.sent) == 1
        notification = db.event().get_notification("hormuz_closure_risk")
        assert notification is not None
        assert notification["status"] == "sent"
        assert second["sent"] == 0
        assert second["skipped"] == 1
        assert second["items"][0]["reason"] == "duplicate_report"
        assert len(alerter.sent) == 1
    finally:
        db.close()


def test_notifier_filters_below_threshold_events():
    db = MemoryDB()
    try:
        _insert_state(db, probability=0.40)
        notifier = EventNotifier(db=db, alerter=FakeAlerter())

        result = notifier.notify(send=False)

        assert result["candidates"] == 0
        assert result["sent"] == 0
    finally:
        db.close()
