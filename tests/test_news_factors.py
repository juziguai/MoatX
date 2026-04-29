import sqlite3

from modules.db.event_store import EventStore
from modules.db.migrations import run_migrations
from modules.event_intelligence.models import NewsItem
from modules.event_intelligence.news_factors import NewsFactorEngine


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


def test_news_factor_engine_aggregates_ai_news_to_sector_boosts():
    db = MemoryDB()
    try:
        db.event().upsert_source_quality(
            source_id="manual_ai",
            name="Manual AI",
            category="technology",
            type="manual",
            enabled=True,
            fetched=1,
            inserted=1,
            duplicates=0,
            errors=0,
            last_success_at="2026-04-29 09:30:00",
        )
        db.event().insert_news(
            NewsItem(
                source="manual_ai",
                title="OpenAI 发布 GPT-5.5，DeepSeek V4 升级推理能力",
                summary="大模型 API 和多模态能力升级，带动算力、AI 应用、软件服务关注。",
                published_at="2026-04-29 09:35:00",
            )
        )

        payload = NewsFactorEngine(db=db).build(limit=20, min_score=0)
        boosts = NewsFactorEngine(db=db).sector_boosts(limit=20, min_score=0)

        assert payload["engine"] == "news_factor_v1"
        assert any(row["sector"] == "算力" for row in payload["factors"])
        assert boosts["算力"] > 0
        assert payload["topic_summary"][0]["topic"] == "AI大模型"
    finally:
        db.close()
