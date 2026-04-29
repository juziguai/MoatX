import sqlite3

from modules.db.event_store import EventStore
from modules.db.migrations import run_migrations
from modules.event_intelligence.models import NewsItem
from modules.event_intelligence.topic_memory import TopicMemoryEngine


class MemoryDB:
    def __init__(self):
        self._conn = sqlite3.connect(":memory:")
        self._conn.execute("PRAGMA foreign_keys=ON")
        run_migrations(self._conn)
        self._event = EventStore(self._conn)

    def event(self):
        return self._event

    @property
    def conn(self):
        return self._conn

    def close(self):
        self._conn.close()


def test_topic_memory_tracks_new_and_stable_topics():
    db = MemoryDB()
    try:
        db.event().insert_news(
            NewsItem(
                source="manual_ai",
                title="OpenAI 发布 GPT-5.5，DeepSeek V4 升级推理能力",
                summary="大模型 API 和多模态能力升级，带动算力、AI 应用、软件服务关注。",
                published_at="2026-04-29 09:35:00",
            )
        )

        first = TopicMemoryEngine(db=db).update(limit=20, min_score=0, top_n=10)
        second = TopicMemoryEngine(db=db).update(limit=20, min_score=0, top_n=10)
        snapshots = TopicMemoryEngine(db=db).snapshots(topic="AI大模型", limit=10)

        assert first["updated"] >= 1
        assert first["topics"][0]["trend"] == "new"
        assert second["topics"][0]["topic"] == "AI大模型"
        assert second["topics"][0]["trend"] in {"stable", "rising", "cooling"}
        assert len(snapshots) >= 2
    finally:
        db.close()
