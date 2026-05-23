import sqlite3
from datetime import datetime, timedelta

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


def test_news_factor_engine_persists_materialized_factors():
    db = MemoryDB()
    try:
        db.event().insert_news(
            NewsItem(
                source="manual_ai",
                title="OpenAI GPT-5.5 launch boosts AI compute demand",
                summary="GPU server, CPO optical module and data center demand may increase.",
                published_at="2026-04-29 09:35:00",
            )
        )

        payload = NewsFactorEngine(db=db).build(limit=20, min_score=0)
        rows = NewsFactorEngine(db=db).list_persisted(limit=20)

        assert payload["factors"]
        assert rows
        assert {row["sector"] for row in rows} >= {row["sector"] for row in payload["factors"]}
        assert all("top_titles" in row for row in rows)
    finally:
        db.close()


def test_news_factor_engine_uses_latest_llm_review_decision():
    db = MemoryDB()
    try:
        news_id = db.event().insert_news(
            NewsItem(
                source="manual_ai",
                title="OpenAI GPT-5.5 launch boosts AI compute demand",
                summary="GPU server, CPO optical module and data center demand may increase.",
                published_at="2026-04-29 09:35:00",
            )
        )

        baseline = NewsFactorEngine(db=db).build(limit=20, min_score=0)
        top_sector = baseline["factors"][0]["sector"]
        baseline_score = next(row["factor_score"] for row in baseline["factors"] if row["sector"] == top_sector)

        topics = {row["topic"] for row in NewsFactorEngine(db=db).build(limit=20, min_score=0)["topic_summary"]}
        for topic in topics:
            db.conn.execute(
                """INSERT INTO event_llm_reviews
                   (news_id, title, topic, value_score, llm_score, decision, rationale, review_json, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (news_id, "reviewed", topic, 80, 95, "ignore", "not market relevant", "{}", "2026-04-29 09:36:00"),
            )
        db.conn.commit()
        ignored = NewsFactorEngine(db=db).build(limit=20, min_score=0)
        assert all(row["sector"] != top_sector for row in ignored["factors"])

        for topic in topics:
            db.conn.execute(
                """INSERT INTO event_llm_reviews
                   (news_id, title, topic, value_score, llm_score, decision, rationale, review_json, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (news_id, "reviewed", topic, 80, 95, "use", "high value", "{}", "2026-04-29 09:37:00"),
            )
        db.conn.commit()
        boosted = NewsFactorEngine(db=db).build(limit=20, min_score=0)
        boosted_score = next(row["factor_score"] for row in boosted["factors"] if row["sector"] == top_sector)
        boosted_adjustment = next(row["llm_adjustment"] for row in boosted["factors"] if row["sector"] == top_sector)

        assert boosted_score >= baseline_score
        assert boosted_adjustment > 1.0
    finally:
        db.close()


def test_news_factor_time_decay_reduces_stale_news_contribution():
    recent = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    old = (datetime.now() - timedelta(days=8)).strftime("%Y-%m-%d %H:%M:%S")
    insights = [
        {
            "affected_sectors": ["recent_sector"],
            "sentiment": "bullish",
            "value_score": 90,
            "confidence": 1,
            "impact_strength": 1,
            "published_at": recent,
            "topic": "AI",
            "title": "recent",
        },
        {
            "affected_sectors": ["old_sector"],
            "sentiment": "bullish",
            "value_score": 90,
            "confidence": 1,
            "impact_strength": 1,
            "published_at": old,
            "topic": "AI",
            "title": "old",
        },
    ]

    factors = {item.sector: item for item in NewsFactorEngine._aggregate(insights, {})}

    assert factors["recent_sector"].factor_score > factors["old_sector"].factor_score
    assert factors["recent_sector"].avg_time_decay == 1.0
    assert factors["old_sector"].avg_time_decay == 0.1


def test_news_factor_bearish_keywords_override_bullish_rule_sentiment():
    insights = [
        {
            "affected_sectors": ["risk_sector"],
            "sentiment": "bullish",
            "value_score": 90,
            "confidence": 1,
            "impact_strength": 1,
            "published_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "topic": "AI",
            "title": "\u4e8f\u635f \u5236\u88c1 risk",
            "summary": "",
            "reason": "",
        }
    ]

    factor = NewsFactorEngine._aggregate(insights, {})[0]

    assert factor.sector == "risk_sector"
    assert factor.direction == "bearish"
    assert factor.factor_score < 0
