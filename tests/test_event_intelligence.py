import sqlite3

import pandas as pd

from modules.db.event_store import EventStore
from modules.db.migrations import run_migrations
from modules.event_intelligence.collector import EventNewsCollector
from modules.event_intelligence.extractor import EventExtractor
from modules.event_intelligence.manual_ingest import ingest_manual_news, ingest_news_file
from modules.event_intelligence.models import EventOpportunity, EventSource, EventState, NewsItem
from modules.event_intelligence.opportunity import EventOpportunityScanner
from modules.event_intelligence.probability import EventProbabilityEngine
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


class FakeSectorProvider:
    def get_members(self, target, target_type):
        return pd.DataFrame([{"code": "600028", "name": "Sinopec"}])


class EmptyRegistry:
    def enabled(self):
        return []


def test_collect_with_no_enabled_sources_is_not_error():
    db = MemoryDB()
    try:
        stats = EventNewsCollector(db=db, registry=EmptyRegistry()).collect()

        assert stats["sources"] == 0
        assert stats["errors"] == []
        assert stats["message"] == "no enabled event news sources"
        assert stats["source_stats"] == []
    finally:
        db.close()


def test_collect_records_source_quality(monkeypatch):
    class OneSourceRegistry:
        def enabled(self):
            return [
                EventSource(
                    id="fake_energy",
                    name="Fake Energy",
                    type="rss",
                    category="energy",
                    url="https://example.test/rss",
                    enabled=True,
                )
            ]

    db = MemoryDB()
    try:
        collector = EventNewsCollector(db=db, registry=OneSourceRegistry())
        monkeypatch.setattr(
            collector,
            "fetch_source",
            lambda source: [NewsItem(source=source.id, title="Iran oil supply disruption")],
        )

        stats = collector.collect()
        quality = db.event().list_source_quality()

        assert stats["source_stats"][0]["category"] == "energy"
        assert stats["inserted"] == 1
        assert quality.iloc[0]["source_id"] == "fake_energy"
        assert quality.iloc[0]["category"] == "energy"
        assert int(quality.iloc[0]["fetched"]) == 1
        assert "quality_score" in quality.columns
        assert "reliability" in quality.columns
    finally:
        db.close()


def test_event_cycle_generates_states_and_mocked_opportunities():
    db = MemoryDB()
    try:
        title = "\u4f0a\u6717\u5a01\u80c1\u5c01\u9501\u970d\u5c14\u6728\u5179\u6d77\u5ce1\uff0c\u7f8e\u519b\u8fdb\u5165\u6ce2\u65af\u6e7e"
        summary = "\u539f\u6cb9\u4f9b\u5e94\u98ce\u9669\u5347\u9ad8"

        inserted = EventNewsCollector(db=db).ingest_items(
            [NewsItem(source="manual", title=title, summary=summary)]
        )
        assert inserted["inserted"] == 1

        extracted = EventExtractor(db=db).extract_unprocessed()
        assert extracted["signals"] >= 1

        states = EventProbabilityEngine(db=db).update_states()
        assert states["updated"] >= 1

        scanner = EventOpportunityScanner(db=db, sector_provider=FakeSectorProvider())
        scanner._spot_cache = pd.DataFrame(
            [
                {
                    "code": "600028",
                    "name": "Sinopec",
                    "price": 6.5,
                    "amount": 500_000_000,
                    "pct_change": 1.2,
                    "turnover": 2.5,
                }
            ]
        )

        scanned = scanner.scan(min_probability=0.30, per_effect_limit=2)
        assert scanned["opportunities"] >= 1

        opportunities = db.event().list_opportunities()
        assert "600028" in set(opportunities["symbol"])
        assert opportunities["opportunity_score"].max() > 0
    finally:
        db.close()


def test_extractor_classifies_denial_as_low_confidence_neutral():
    db = MemoryDB()
    try:
        text = "伊朗否认将封锁霍尔木兹海峡，称相关传闻不实"
        signals = EventExtractor(db=db).extract_text(text, source="manual")

        assert signals
        signal = [s for s in signals if s.event_id == "hormuz_closure_risk"][0]
        assert signal.entities["stage"] == "denied"
        assert signal.direction == "neutral"
        assert signal.severity < 0.35
    finally:
        db.close()


def test_extractor_classifies_confirmed_escalation():
    db = MemoryDB()
    try:
        text = "伊朗宣布封锁霍尔木兹海峡，原油供应中断风险升级"
        signals = EventExtractor(db=db).extract_text(text, source="manual")

        signal = [s for s in signals if s.event_id == "hormuz_closure_risk"][0]
        assert signal.entities["stage"] == "confirmed"
        assert signal.severity > 0.8
        assert signal.confidence > 0.5
    finally:
        db.close()


def test_extractor_marks_stale_review_as_lower_severity():
    db = MemoryDB()
    try:
        fresh = EventExtractor(db=db).extract_text("突发：伊朗宣布封锁霍尔木兹海峡，危机升级", source="manual")
        stale = EventExtractor(db=db).extract_text("历史上伊朗曾经威胁封锁霍尔木兹海峡的复盘", source="manual")

        fresh_signal = [s for s in fresh if s.event_id == "hormuz_closure_risk"][0]
        stale_signal = [s for s in stale if s.event_id == "hormuz_closure_risk"][0]
        assert fresh_signal.entities["time_sensitivity"] == "urgent"
        assert fresh_signal.entities["intensity"] == "high"
        assert stale_signal.entities["time_sensitivity"] == "stale"
        assert fresh_signal.severity > stale_signal.severity
    finally:
        db.close()


def test_extractor_skips_old_dated_news():
    db = MemoryDB()
    try:
        inserted = EventNewsCollector(db=db).ingest_items([
            NewsItem(
                source="old_feed",
                title="伊朗宣布封锁霍尔木兹海峡，原油供应中断风险升级",
                summary="这是一条旧新闻，不应驱动当前事件状态",
                published_at="2021-12-27",
            )
        ])
        assert inserted["inserted"] == 1

        extracted = EventExtractor(db=db).extract_unprocessed()
        signals = db.event().list_signals()

        assert extracted["processed"] == 1
        assert extracted["signals"] == 0
        assert extracted["stale_skipped"] == 1
        assert signals.empty
    finally:
        db.close()


def test_extractor_keeps_recent_dated_news_metadata():
    db = MemoryDB()
    try:
        inserted = EventNewsCollector(db=db).ingest_items([
            NewsItem(
                source="fresh_feed",
                title="突发：伊朗宣布封锁霍尔木兹海峡，原油供应中断风险升级",
                published_at="2026-04-27 09:30:00",
            )
        ])
        assert inserted["inserted"] == 1

        extracted = EventExtractor(db=db).extract_unprocessed()
        signals = db.event().list_signals()

        assert extracted["signals"] >= 1
        assert extracted["stale_skipped"] == 0
        entities = signals.iloc[0]["entities_json"]
        assert "published_at" in entities
        assert "news_age_days" in entities
    finally:
        db.close()


def test_event_report_contains_state_and_opportunity_sections():
    db = MemoryDB()
    try:
        db.event().upsert_state(EventState(
            event_id="hormuz_closure_risk",
            name="\u970d\u5c14\u6728\u5179\u5173\u95ed\u98ce\u9669",
            probability=0.45,
            impact_strength=0.85,
            status="watching",
            evidence_count=2,
            sources_count=1,
            last_signal_at="2026-04-26 10:00:00",
        ))
        db.event().insert_opportunity(EventOpportunity(
            event_id="hormuz_closure_risk",
            symbol="600028",
            name="Sinopec",
            sector_tags=["\u77f3\u6cb9\u884c\u4e1a"],
            opportunity_score=78.5,
            event_score=30.5,
            exposure_score=17.0,
            underpricing_score=11.0,
            timing_score=10.0,
            risk_penalty=0.0,
            recommendation="\u91cd\u70b9\u5173\u6ce8",
            evidence={},
        ))
        db.event().upsert_source_quality(
            source_id="manual",
            name="Manual",
            category="manual",
            type="api",
            enabled=True,
            fetched=5,
            inserted=5,
            duplicates=0,
            errors=0,
            last_success_at="2026-04-27 10:00:00",
            last_error="",
        )

        report = EventReporter(db=db).report(limit=5)
        assert "MoatX" in report
        assert "hormuz_closure_risk" in report
        assert "600028" in report
        assert "最新证据链" in report
        assert "源质量" in report
        assert "建议" in report
    finally:
        db.close()


def test_opportunity_uses_positive_elasticity_prior():
    db = MemoryDB()
    try:
        run_id = db.event().insert_elasticity_run(
            event_id="hormuz_closure_risk",
            windows=[1, 3],
            trigger_count=1,
            sample_count=0,
            summary={},
        )
        db.event().insert_elasticity_sample(
            {
                "run_id": run_id,
                "event_id": "hormuz_closure_risk",
                "symbol": "600028",
                "name": "Sinopec",
                "trigger_date": "2024-04-13",
                "entry_date": "2024-04-15",
                "window_days": 3,
                "entry_close": 10,
                "exit_date": "2024-04-18",
                "exit_close": 10.8,
                "forward_return": 8.0,
                "benchmark_return": 0,
                "excess_return": 8.0,
                "max_drawdown": -1.0,
                "success": True,
                "source": "test",
            }
        )
        scanner = EventOpportunityScanner(db=db, sector_provider=FakeSectorProvider())
        scanner._spot_cache = pd.DataFrame([
            {
                "code": "600028",
                "name": "Sinopec",
                "price": 6.5,
                "amount": 500_000_000,
                "pct_change": 1.2,
                "turnover": 2.5,
            }
        ])

        opportunity = scanner._build_opportunity(
            event_id="hormuz_closure_risk",
            event_name="霍尔木兹关闭风险",
            probability=0.7,
            impact=0.85,
            target="石油行业",
            member={"code": "600028", "name": "Sinopec"},
        )

        assert opportunity is not None
        assert opportunity.evidence["elasticity_sample_count"] == 1.0
        assert opportunity.evidence["elasticity_score"] > 0
    finally:
        db.close()


def test_opportunity_scan_refreshes_existing_event_rows():
    db = MemoryDB()
    try:
        db.event().upsert_state(EventState(
            event_id="hormuz_closure_risk",
            name="霍尔木兹关闭风险",
            probability=0.7,
            impact_strength=0.8,
            status="escalating",
        ))
        db.event().insert_opportunity(EventOpportunity(
            event_id="hormuz_closure_risk",
            symbol="600028",
            name="old",
            sector_tags=["石油行业"],
            opportunity_score=50,
        ))

        scanner = EventOpportunityScanner(db=db, sector_provider=FakeSectorProvider())
        scanner._spot_cache = pd.DataFrame([
            {
                "code": "600028",
                "name": "Sinopec",
                "price": 6.5,
                "amount": 500_000_000,
                "pct_change": 1.2,
                "turnover": 2.5,
            }
        ])

        result = scanner.scan(min_probability=0.35, per_effect_limit=2)
        rows = db.event().list_opportunities(event_id="hormuz_closure_risk", limit=50)

        assert result["deleted_old"] == 1
        assert "old" not in set(rows["name"])
        assert len(rows) == result["opportunities"]
    finally:
        db.close()


def test_manual_ingest_single_news_and_json_file(tmp_path):
    db = MemoryDB()
    try:
        single = ingest_manual_news(
            title="\u4f0a\u6717\u5a01\u80c1\u5c01\u9501\u970d\u5c14\u6728\u5179\u6d77\u5ce1",
            summary="\u539f\u6cb9\u4f9b\u5e94\u98ce\u9669\u5347\u9ad8",
            source="manual_test",
            db=db,
        )
        assert single["inserted"] == 1

        payload = tmp_path / "news.json"
        payload.write_text(
            '[{"title":"OPEC减产推动油价上涨","summary":"能源供给收紧","url":"https://example.test/a"}]',
            encoding="utf-8",
        )
        batch = ingest_news_file(payload, source="manual_file_test", db=db)
        assert batch["inserted"] == 1

        news = db.event().list_news(limit=10)
        assert set(news["source"]) >= {"manual_test", "manual_file_test"}
    finally:
        db.close()


def test_http_json_source_supports_record_path_and_field_map(monkeypatch):
    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "data": {
                    "items": [
                        {
                            "headline": "\u7f8e\u519b\u90e8\u7f72\u6ce2\u65af\u6e7e",
                            "desc": "\u4e2d\u4e1c\u7d27\u5f20\u5c40\u52bf\u5347\u7ea7",
                            "href": "https://example.test/news",
                            "published": "2026-04-26",
                        }
                    ]
                }
            }

    class FakeSession:
        trust_env = False

        def get(self, *args, **kwargs):
            return FakeResponse()

    monkeypatch.setattr("modules.event_intelligence.collector.requests.Session", lambda: FakeSession())

    db = MemoryDB()
    try:
        collector = EventNewsCollector(db=db)
        source = EventSource(
            id="fake_json",
            name="Fake JSON",
            type="http_json",
            url="https://example.test/api",
            enabled=True,
            field_map={
                "title": "headline",
                "summary": "desc",
                "url": "href",
                "published_at": "published",
            },
            record_path="data.items",
        )

        items = collector.fetch_source(source)

        assert len(items) == 1
        assert items[0].title == "\u7f8e\u519b\u90e8\u7f72\u6ce2\u65af\u6e7e"
        assert items[0].summary == "\u4e2d\u4e1c\u7d27\u5f20\u5c40\u52bf\u5347\u7ea7"
        assert items[0].url == "https://example.test/news"
    finally:
        db.close()


def test_jsonp_source_supports_callback_payload(monkeypatch):
    class FakeResponse:
        text = 'news({"data":{"list":[{"title":"央视消息：原油供应风险升温","brief":"能源运输受扰","url":"https://example.test/cctv","focus_date":"2026-04-26 10:00:00"}]}});'

        def raise_for_status(self):
            return None

        def json(self):
            raise ValueError("jsonp")

    class FakeSession:
        trust_env = False

        def get(self, *args, **kwargs):
            return FakeResponse()

    monkeypatch.setattr("modules.event_intelligence.collector.requests.Session", lambda: FakeSession())

    db = MemoryDB()
    try:
        collector = EventNewsCollector(db=db)
        source = EventSource(
            id="fake_jsonp",
            name="Fake JSONP",
            type="jsonp",
            url="https://example.test/news.jsonp",
            enabled=True,
            record_path="data.list",
            field_map={"summary": "brief", "published_at": "focus_date"},
        )

        items = collector.fetch_source(source)

        assert len(items) == 1
        assert items[0].title == "央视消息：原油供应风险升温"
        assert items[0].summary == "能源运输受扰"
    finally:
        db.close()


def test_html_source_extracts_configured_links(monkeypatch):
    class FakeResponse:
        content = b""
        encoding = "utf-8"
        apparent_encoding = "utf-8"
        text = """
        <html><body>
          <a href="/article/detail/1.html">霍尔木兹海峡，再传新动态！</a>
          <a href="/about.html">关于我们</a>
        </body></html>
        """

        def raise_for_status(self):
            return None

    class FakeSession:
        trust_env = False

        def get(self, *args, **kwargs):
            return FakeResponse()

    monkeypatch.setattr("modules.event_intelligence.collector.requests.Session", lambda: FakeSession())

    db = MemoryDB()
    try:
        collector = EventNewsCollector(db=db)
        source = EventSource(
            id="fake_html",
            name="Fake HTML",
            type="html",
            url="https://example.test/list.html",
            enabled=True,
            field_map={
                "base_url": "https://example.test/",
                "link_contains": "/article/detail/",
                "max_items": 10,
            },
        )

        items = collector.fetch_source(source)

        assert len(items) == 1
        assert items[0].title == "霍尔木兹海峡，再传新动态！"
        assert items[0].url == "https://example.test/article/detail/1.html"
    finally:
        db.close()
