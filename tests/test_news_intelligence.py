import sqlite3

from modules.db.event_store import EventStore
from modules.db.migrations import run_migrations
from modules.event_intelligence.models import NewsItem
from modules.event_intelligence.news_intelligence import NewsIntelligenceEngine


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


def test_news_intelligence_detects_ai_model_news_and_maps_sectors():
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
                title="OpenAI 发布 GPT-5.5，DeepSeek V4 同步升级推理能力",
                summary="大模型 API、推理能力和多模态能力升级，带动算力、AI 应用、软件服务关注。",
                published_at="2026-04-29 09:35:00",
            )
        )

        payload = NewsIntelligenceEngine(db=db).analyze(limit=20, min_score=0)

        ai_items = [item for item in payload["insights"] if item["topic"] == "AI大模型"]
        assert ai_items
        assert "算力" in ai_items[0]["affected_sectors"]
        assert ai_items[0]["value_score"] > 50
        assert payload["topic_summary"][0]["topic"] == "AI大模型"
    finally:
        db.close()


def test_news_intelligence_skips_unmatched_noise_by_default():
    db = MemoryDB()
    try:
        db.event().insert_news(
            NewsItem(
                source="noise",
                title="某地举办普通文化活动",
                summary="社区活动顺利完成，暂无明显 A 股产业映射。",
                published_at="2026-04-29 09:35:00",
            )
        )

        payload = NewsIntelligenceEngine(db=db).analyze(limit=20, min_score=0)

        assert payload["news_scanned"] == 1
        assert payload["insights"] == []
        assert "未识别" in payload["message"] or "暂无" in payload["message"]
    finally:
        db.close()


def test_news_intelligence_report_contains_chinese_sections():
    db = MemoryDB()
    try:
        db.event().insert_news(
            NewsItem(
                source="manual_energy",
                title="国际油价大幅上涨，原油供给风险升温",
                summary="能源价格上行带动石油、油服、煤化工等方向关注。",
                published_at="2026-04-29 09:35:00",
            )
        )

        report = NewsIntelligenceEngine(db=db).report(limit=20, min_score=0)

        assert "MoatX 新闻价值发现报告" in report
        assert "高价值主题" in report
        assert "能源商品" in report
    finally:
        db.close()
