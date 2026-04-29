import sqlite3

import pandas as pd

from modules.db.event_store import EventStore
from modules.db.migrations import run_migrations
from modules.db.price_store import PriceStore
from modules.event_intelligence.elasticity import EventElasticityBacktester
from modules.event_intelligence.models import EventOpportunity, EventSignal, EventState


class MemoryDB:
    def __init__(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.execute("PRAGMA foreign_keys=ON")
        run_migrations(self.conn)
        self._event = EventStore(self.conn)
        self._price = PriceStore(self.conn)

    def event(self):
        return self._event

    def price(self):
        return self._price

    def close(self):
        self.conn.close()


def test_elasticity_uses_daily_event_windows():
    db = MemoryDB()
    try:
        db.event().insert_signal(
            EventSignal(
                event_id="test_supply_risk",
                news_id=None,
                event_type="energy_supply_risk",
                created_at="2026-04-20 10:00:00",
            )
        )
        db.event().insert_opportunity(
            EventOpportunity(
                event_id="test_supply_risk",
                symbol="600028",
                name="中国石化",
                opportunity_score=80,
            )
        )
        daily = pd.DataFrame(
            {
                "date": pd.to_datetime([
                    "2026-04-20",
                    "2026-04-21",
                    "2026-04-22",
                    "2026-04-23",
                ]),
                "open": [10, 10.5, 11, 10.8],
                "high": [10.5, 11.2, 12.5, 11.5],
                "low": [9.8, 10.3, 10.7, 10.6],
                "close": [10, 11, 12, 11],
            }
        )
        db.price().save_daily_batch(daily, "600028")

        result = EventElasticityBacktester(db=db).run(
            event_id="test_supply_risk",
            windows=[1, 3],
            limit=10,
        )

        assert result["triggers"] == 1
        assert result["samples"] == 2
        summary = {row["window_days"]: row for row in result["summary"]}
        assert summary[1]["avg_forward_return"] == 10.0
        assert summary[3]["avg_forward_return"] == 10.0
        assert summary[1]["win_rate"] == 1.0
    finally:
        db.close()


def test_elasticity_empty_events_return_empty_result():
    db = MemoryDB()
    try:
        result = EventElasticityBacktester(db=db).run(event_id="missing", windows=[1])

        assert result["triggers"] == 0
        assert result["samples"] == 0
        assert result["summary"] == []
    finally:
        db.close()


def test_elasticity_falls_back_to_history_related_sectors():
    class FakeHistory:
        def list(self, event_id="", limit=100):
            return [
                {
                    "event_id": "history_event",
                    "trigger_date": "2026-04-20",
                    "related_sectors": ["石油行业"],
                }
            ]

    class FakeSectorProvider:
        def get_members(self, target, target_type):
            return pd.DataFrame([{"code": "600028", "name": "中国石化"}])

    db = MemoryDB()
    try:
        db.event().insert_signal(
            EventSignal(
                event_id="history_event",
                news_id=None,
                event_type="energy_supply_risk",
                created_at="2026-04-20 10:00:00",
            )
        )
        daily = pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-04-20", "2026-04-21"]),
                "open": [10, 10.5],
                "high": [10.5, 11.2],
                "low": [9.8, 10.3],
                "close": [10, 11],
            }
        )
        db.price().save_daily_batch(daily, "600028")

        result = EventElasticityBacktester(
            db=db,
            sector_provider=FakeSectorProvider(),
            history_registry=FakeHistory(),
        ).run(event_id="history_event", windows=[1], limit=10)

        assert result["samples"] == 1
        assert result["summary"][0]["avg_forward_return"] == 10.0
    finally:
        db.close()
