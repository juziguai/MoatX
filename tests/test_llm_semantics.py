import json
import sqlite3

from modules.db.event_store import EventStore
from modules.db.migrations import run_migrations
from modules.event_intelligence.llm_semantics import LLMSettings, LLMSemanticReviewer
from modules.event_intelligence.models import NewsItem


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


def test_llm_review_dry_run_does_not_call_transport():
    db = MemoryDB()
    try:
        db.event().insert_news(
            NewsItem(
                source="manual_ai",
                title="OpenAI 发布 GPT-5.5，DeepSeek V4 升级推理能力",
                summary="大模型能力升级，利好算力和 AI 应用。",
                published_at="2026-04-29 09:35:00",
            )
        )

        def fail_transport(url, headers, payload, timeout):
            raise AssertionError("transport should not be called in dry-run")

        reviewer = LLMSemanticReviewer(db=db, transport=fail_transport)
        payload = reviewer.review(limit=20, min_score=0, send=False)

        assert payload["status"] == "dry_run"
        assert payload["candidate_count"] >= 1
        assert payload["reviews"] == []
        assert "request_preview" in payload
    finally:
        db.close()


def test_llm_review_send_parses_and_persists_reviews():
    db = MemoryDB()
    try:
        news_id = db.event().insert_news(
            NewsItem(
                source="manual_energy",
                title="国际油价大涨，原油供给风险升温",
                summary="原油供需紧张，可能影响石油、油服和航运板块。",
                published_at="2026-04-29 09:35:00",
            )
        )

        def fake_transport(url, headers, payload, timeout):
            assert headers["Authorization"] == "Bearer test-key"
            content = {
                "reviews": [
                    {
                        "news_id": news_id,
                        "decision": "use",
                        "llm_score": 86,
                        "rationale": "供给冲击与 A 股石油链条高度相关。",
                        "sentiment": "bullish",
                        "time_horizon": "short",
                        "sectors": ["石油行业", "油服工程"],
                        "risks": ["油价快速回落"],
                    }
                ]
            }
            return {"choices": [{"message": {"content": json.dumps(content, ensure_ascii=False)}}], "usage": {"total_tokens": 123}}

        settings = LLMSettings(enabled=True, model="fake-model", api_key_env="MOATX_TEST_LLM_KEY")
        reviewer = LLMSemanticReviewer(db=db, settings=settings, transport=fake_transport, api_key="test-key")
        payload = reviewer.review(limit=20, min_score=0, send=True)
        stored = LLMSemanticReviewer(db=db).list_reviews(limit=5)

        assert payload["status"] == "reviewed"
        assert payload["reviews"][0]["decision"] == "use"
        assert payload["reviews"][0]["llm_score"] == 86
        assert stored[0]["news_id"] == news_id
        assert stored[0]["decision"] == "use"
        assert stored[0]["review"]["sectors"] == ["石油行业", "油服工程"]
    finally:
        db.close()
