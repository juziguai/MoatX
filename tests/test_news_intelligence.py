import sqlite3
from datetime import datetime

from modules.db.event_store import EventStore
from modules.db.migrations import run_migrations
from modules.event_intelligence.models import NewsItem
from modules.event_intelligence.news_intelligence import NewsIntelligenceEngine
from modules.event_intelligence.reporter import EventReporter


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


def test_news_intelligence_selects_one_primary_topic_for_real_estate_news():
    db = MemoryDB()
    try:
        db.event().insert_news(
            NewsItem(
                source="manual_policy",
                title="天津发布房地产新政 优化房地产供给促进住房消费",
                summary="通知提出用好金融、公积金和住房消费政策，优化房地产供给。",
                published_at="2026-04-30 18:35:00",
            )
        )

        payload = NewsIntelligenceEngine(db=db).analyze(limit=20, min_score=0)

        topics = [item["topic"] for item in payload["insights"]]
        assert topics == ["金融地产政策"]
        assert "消费出海" not in topics
    finally:
        db.close()


def test_news_intelligence_does_not_classify_oil_api_as_ai_model():
    db = MemoryDB()
    try:
        db.event().insert_news(
            NewsItem(
                source="oilprice_main_rss",
                title="EIA: US Crude Oil Inventories Fall Sharply",
                summary="API and EIA data show crude oil inventories decreased by 6.2 million barrels.",
                published_at="2026-04-30 14:20:00",
            )
        )

        payload = NewsIntelligenceEngine(db=db).analyze(limit=20, min_score=0)

        topics = [item["topic"] for item in payload["insights"]]
        assert topics == ["能源商品"]
        assert "AI大模型" not in topics
    finally:
        db.close()


def test_news_intelligence_prioritizes_today_news_over_overnight_catalyst():
    db = MemoryDB()
    try:
        db.event().insert_news(
            NewsItem(
                source="cls_telegraph_json",
                title="麦格米特：GB300电源产品已成功获取批量订单",
                summary="AI服务器电源领域已取得GB300批量订单，相关业绩贡献已体现。",
                published_at="2026-04-29 22:32:23",
            )
        )
        db.event().insert_news(
            NewsItem(
                source="chinanews_finance_rss",
                title="依托京东JoyAI大模型 京办、京通APP智能体亮相",
                summary="可信AI与政务智能体亮相，展示AI助力数字政务建设的创新实践。",
                published_at="2026-04-30 17:39:00",
            )
        )

        payload = NewsIntelligenceEngine(db=db).analyze(limit=20, min_score=0)

        assert payload["insights"][0]["title"].startswith("依托京东JoyAI大模型")
        assert NewsIntelligenceEngine._time_bucket("2026-04-30 17:39:00", datetime(2026, 4, 30, 18, 0)) == 5
        assert NewsIntelligenceEngine._time_bucket("2026-04-29 22:32:23", datetime(2026, 4, 30, 18, 0)) == 4
        assert NewsIntelligenceEngine._time_bonus("2026-04-30 17:39:00") > NewsIntelligenceEngine._time_bonus(
            "2026-04-29 22:32:23"
        )
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


def test_news_intelligence_persists_insights_and_topic_events_without_duplicates():
    db = MemoryDB()
    try:
        db.event().insert_news(
            NewsItem(
                source="manual_ai",
                title="OpenAI releases GPT-5.5 and DeepSeek V4 upgrades reasoning API",
                summary="AI model upgrade drives compute, CPO, optical module and software demand.",
                published_at="2026-04-29 09:35:00",
            )
        )

        first = NewsIntelligenceEngine(db=db).analyze(limit=20, min_score=0)
        second = NewsIntelligenceEngine(db=db).analyze(limit=20, min_score=0)

        insight_count = db.conn.execute("SELECT COUNT(*) FROM event_news_insights").fetchone()[0]
        topic_count = db.conn.execute("SELECT COUNT(*) FROM event_news_topic_events").fetchone()[0]
        topic_row = db.conn.execute(
            "SELECT heat, insight_count FROM event_news_topic_events ORDER BY heat DESC LIMIT 1"
        ).fetchone()

        assert first["insights"]
        assert second["insights"]
        assert insight_count == len({(row["news_id"], row["topic"]) for row in first["insights"]})
        assert topic_count >= 1
        assert float(topic_row[0]) > 0
        assert int(topic_row[1]) >= 1
    finally:
        db.close()


def test_event_report_explains_why_high_value_news_matters_to_a_share():
    db = MemoryDB()
    try:
        db.event().insert_news(
            NewsItem(
                source="manual_ai",
                title="OpenAI 发布 GPT-5.5，DeepSeek V4 同步升级推理能力",
                summary="AI 算力需求上升，CPO、光模块、数据中心和软件服务产业链景气度提升。",
                published_at="2026-04-29 09:35:00",
            )
        )

        report = EventReporter(db=db).report(limit=5)

        assert "选中理由：" in report
        assert "AI大模型" in report or "算力基础设施" in report
        assert any(sector in report for sector in ["算力", "光模块", "CPO"])
        assert "可能影响" in report or "命中" in report
        assert "热点速览 |" in report
        assert "本时段扫描" in report
        assert "今日高热聚焦" in report
        assert "热度 ⭐" in report
        assert " | " in report
        assert "核心看点：" in report
        assert "传导路径：" in report
        assert "选中理由：" in report
        assert "可能涉及的A股：" in report
        assert "一句话：" in report
        assert "| 分数 | 主题 | 新闻 | 关联板块 |" not in report
        assert "# MoatX" not in report
        assert "```" not in report
    finally:
        db.close()


def test_event_report_uses_specific_power_order_mapping():
    item = {
        "source": "cls_telegraph_json",
        "title": "麦格米特：GB300电源产品已成功获取批量订单",
        "summary": "麦格米特发布投资者关系活动记录表，在AI服务器电源领域已取得GB300批量订单。",
        "topic": "算力基础设施",
        "affected_sectors": ["算力", "光模块", "CPO", "液冷", "数据中心", "半导体"],
        "reason": "命中“数据中心”，归入算力基础设施，可能影响算力、光模块、CPO、液冷、数据中心。",
        "value_score": 85,
    }

    text = "\n".join(EventReporter._hotspot_tuple(1, item))

    assert "核心看点：麦格米特：GB300电源产品已成功获取批量订单" not in text
    assert "国产电源厂首次获得AI服务器高端平台批量配套订单" in text
    assert "GB300批量订单 ➔ 服务器电源" in text
    assert "麦格米特(002851)" in text
    assert "工业富联(601138)" not in text
    assert "出现边际变化" not in text
    assert "AI服务器电源国产替代取得实质订单" in text


def test_event_report_uses_shortage_and_policy_specific_templates():
    shortage = {
        "source": "cls_telegraph_json",
        "title": "该细分领域是光通信系统中的关键无源器件，其全球供应紧张",
        "summary": "光通信关键无源器件全球供应紧张，已成为制约高速光模块等下游产业扩产的卡脖子环节。",
        "topic": "算力基础设施",
        "affected_sectors": ["算力", "光模块", "CPO", "液冷", "数据中心", "半导体"],
        "reason": "命中“光模块”，归入算力基础设施，可能影响算力、光模块、CPO、液冷、数据中心。",
        "value_score": 82,
    }
    policy = {
        "source": "chinanews_finance_rss",
        "title": "六部委联合发布绿色算力设施榜单 大同绿色算力迈入国家第一梯队",
        "summary": "工信部、国家发改委等六部委联合发布绿色算力设施榜单。",
        "topic": "算力基础设施",
        "affected_sectors": ["算力", "光模块", "CPO", "液冷", "数据中心", "半导体"],
        "reason": "命中“算力”，归入算力基础设施，可能影响算力、光模块、CPO、液冷、数据中心。",
        "value_score": 82,
    }

    shortage_text = "\n".join(EventReporter._hotspot_tuple(1, shortage))
    policy_text = "\n".join(EventReporter._hotspot_tuple(2, policy))

    assert "800G/1.6T光模块扩产拉动 ➔ 光无源器件" in shortage_text
    assert "天孚通信(300394)" in shortage_text
    assert "上游光无源器件产能受限" in shortage_text
    assert "六部委发布绿色算力榜单" in policy_text
    assert "绿色算力设施榜单 ➔ 绿色算力" in policy_text
    assert "政策首次明确绿色算力官方梯队" in policy_text


def test_event_report_keeps_final_editorial_template_regressions():
    sodium = {
        "source": "cls_telegraph_json",
        "title": "钠电订单落地，规模化量产进入加速阶段",
        "summary": "钠电大订单落地并给出量产时间表，验证新型储能从示范走向规模化交付。",
        "topic": "算力基础设施",
        "affected_sectors": ["算力", "储能", "数据中心", "电力"],
        "reason": "命中“钠电”，归入算力基础设施，可能影响储能、电池材料、数据中心备电。",
        "value_score": 82,
    }

    text = "\n".join(EventReporter._hotspot_tuple(4, sodium))

    assert EventReporter._module_title("算力基础设施", 3) == "🔥 算力基础设施 · 硬件突破与政策双轮驱动"
    assert "自动归入\"储能新能源\"模块" in text
    assert "储能（" in text
    assert "宁德时代(300750)" in text
    assert "出现边际变化" not in text


def test_event_report_marks_after_close_news_as_yesterday_catalyst():
    item = {
        "source": "cls_telegraph_json",
        "title": "麦格米特：GB300电源产品已成功获取批量订单",
        "summary": "麦格米特发布投资者关系活动记录表，在AI服务器电源领域已取得GB300批量订单。",
        "topic": "算力基础设施",
        "affected_sectors": ["算力", "光模块", "CPO", "液冷", "数据中心", "半导体"],
        "reason": "命中“数据中心”，归入算力基础设施，可能影响算力、光模块、CPO、液冷、数据中心。",
        "value_score": 85,
        "published_at": "2026-04-29 22:32:23",
    }

    text = "\n".join(EventReporter._hotspot_tuple(1, item))

    assert "【昨日盘后 22:32】" in text
    assert EventReporter._time_badge(item, "2026-04-30 10:00:00") == "【昨日盘后 22:32】"
    assert (
        EventReporter._time_badge(
            {"published_at": "Wed, 29 Apr 2026 16:08:00 +0800"},
            "2026-04-30 10:00:00",
        )
        == "【昨日盘后 16:08】"
    )
