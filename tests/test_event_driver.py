from modules.event_driver import EventDriver
from modules.event_intelligence.exposure import StockTopicExposureProvider


class FakeSectorProvider:
    def __init__(self, tags=None):
        self.tags = set(tags or {"AI_APP"})

    def normalize_code(self, symbol):
        return str(symbol or "").split(".")[0].zfill(6)

    def _graph_tags_for_code(self, code):
        return set()

    def get_tags(self, symbol):
        return set(self.tags)

    def build_code_to_tags(self):
        return {"600000": set(self.tags)}

    def market_fallback_tag(self, code):
        return "fallback"


def test_event_tag_matches_alias_and_suffix_forms():
    assert EventDriver._tag_matches("黄金概念", "黄金")
    assert EventDriver._tag_matches("半导体及元件", "半导体")
    assert EventDriver._tag_matches("光伏设备", "光伏")
    assert EventDriver._tag_matches("石油行业", "石油")


def test_event_tag_matches_contains_forms():
    assert EventDriver._tag_matches("贵金属概念", "贵金属")
    assert EventDriver._tag_matches("国防军工", "军工")


def test_event_driver_applies_news_factor_sector_boost(monkeypatch):
    class FakeNewsFactorEngine:
        def build(self, limit=200, min_score=55.0, top_n=100):
            return {
                "factors": [
                    {
                        "sector": "AI_APP",
                        "factor_score": 12.5,
                        "top_topic": "AI",
                        "direction": "bullish",
                    }
                ]
            }

    monkeypatch.setattr("modules.event_intelligence.news_factors.NewsFactorEngine", FakeNewsFactorEngine)
    driver = EventDriver(sector_provider=FakeSectorProvider())
    driver._events = []
    monkeypatch.setattr(driver, "_market_validation_by_sector", lambda sectors: {})
    monkeypatch.setattr(driver, "_scan_announcement_sentiment", lambda symbol: 0)

    assert driver.score_single("600000") == 12.5


def test_event_driver_dedupes_same_topic_near_synonym_tags(monkeypatch):
    driver = EventDriver(sector_provider=FakeSectorProvider(tags={"chip"}))
    driver._sector_boost_details = {
        "chip": {"top_topic": "AI"},
        "chip_equipment": {"top_topic": "AI"},
    }
    monkeypatch.setattr(driver, "_scan_announcement_sentiment", lambda symbol: 0)

    result = driver.explain_single("600000", {"chip": 25.0, "chip_equipment": 25.0})

    assert result["boost"] == 25.0
    assert len(result["matched_factors"]) == 1
    assert result["matched_factors"][0]["deduped_count"] == 2


def test_event_driver_market_validation_discounts_unconfirmed_positive_news(monkeypatch):
    class FakeNewsFactorEngine:
        def build(self, limit=200, min_score=55.0, top_n=100):
            return {
                "factors": [
                    {
                        "sector": "AI_APP",
                        "factor_score": 25.0,
                        "top_topic": "AI",
                        "direction": "bullish",
                    }
                ]
            }

    monkeypatch.setattr("modules.event_intelligence.news_factors.NewsFactorEngine", FakeNewsFactorEngine)
    driver = EventDriver(sector_provider=FakeSectorProvider(tags={"AI_APP"}))
    driver._events = []
    monkeypatch.setattr(
        driver,
        "_market_validation_by_sector",
        lambda sectors: {"AI_APP": {"avg_pct": -1.0, "up": 0, "down": 3}},
    )
    monkeypatch.setattr(driver, "_scan_announcement_sentiment", lambda symbol: 0)

    result = driver.explain_single("600000")

    assert result["boost"] == 11.2
    assert result["matched_factors"][0]["market_validation"] == "unconfirmed"
    assert result["matched_factors"][0]["market_multiplier"] == 0.45


def test_event_driver_topic_exposure_table_lists_stock_topic_matches(monkeypatch):
    driver = EventDriver(sector_provider=FakeSectorProvider(tags={"AI_APP"}))
    driver._sector_boost_details = {"AI_APP": {"top_topic": "AI"}}
    monkeypatch.setattr(driver, "_active_sector_boosts", lambda: {"AI_APP": 10.0})

    rows = driver.topic_exposure_table(["600000"])

    assert rows == [
        {
            "symbol": "600000",
            "stock_tag": "AI_APP",
            "event_tag": "AI_APP",
            "topic": "AI",
            "exposure": 1.0,
            "exposure_source": "tag_match",
            "boost": 10.0,
        }
    ]


def test_event_driver_uses_configured_topic_exposure_weight(tmp_path, monkeypatch):
    path = tmp_path / "exposure.toml"
    path.write_text(
        """
[[exposures]]
symbol = "600000"
topic = "AI"
sector_tag = "AI_APP"
exposure = 0.5
confidence = 1.0
source = "unit"
""",
        encoding="utf-8",
    )
    driver = EventDriver(
        sector_provider=FakeSectorProvider(tags={"AI_APP"}),
        exposure_provider=StockTopicExposureProvider(path),
    )
    driver._sector_boost_details = {"AI_APP": {"top_topic": "AI"}}
    monkeypatch.setattr(driver, "_scan_announcement_sentiment", lambda symbol: 0)

    result = driver.explain_single("600000", {"AI_APP": 20.0})

    assert result["boost"] == 10.0
    assert result["matched_factors"][0]["exposure"] == 0.5
    assert result["matched_factors"][0]["exposure_source"] == "unit"


def test_event_driver_market_confirmation_scores_breadth_liquidity_and_concentration():
    strong = {
        "sample_count": 5,
        "avg_pct": 2.0,
        "up_ratio": 0.8,
        "down_ratio": 0.0,
        "amount_yi": 80.0,
        "leader_share": 0.3,
    }
    concentrated = dict(strong, leader_share=0.8)
    weak = {
        "sample_count": 5,
        "avg_pct": -0.5,
        "up_ratio": 0.2,
        "down_ratio": 0.6,
        "amount_yi": 1.0,
        "leader_share": 0.2,
    }

    assert EventDriver._market_validation_multiplier(25.0, strong) == (1.0, "confirmed")
    assert EventDriver._market_confirmation_score(25.0, concentrated) < EventDriver._market_confirmation_score(25.0, strong)
    assert EventDriver._market_validation_multiplier(25.0, weak) == (0.45, "unconfirmed")


def test_event_driver_news_factor_flow_dedupes_discounts_and_exposes(monkeypatch):
    class FakeNewsFactorEngine:
        def build(self, limit=200, min_score=55.0, top_n=100):
            return {
                "factors": [
                    {"sector": "AI_APP", "factor_score": 25.0, "top_topic": "AI", "direction": "bullish"},
                    {"sector": "AI_APP_NEAR", "factor_score": 25.0, "top_topic": "AI", "direction": "bullish"},
                    {"sector": "AI_COMPUTE", "factor_score": 10.0, "top_topic": "Compute", "direction": "bullish"},
                ]
            }

    monkeypatch.setattr("modules.event_intelligence.news_factors.NewsFactorEngine", FakeNewsFactorEngine)
    driver = EventDriver(sector_provider=FakeSectorProvider(tags={"AI_APP", "AI_APP_NEAR", "AI_COMPUTE"}))
    driver._events = []
    monkeypatch.setattr(
        driver,
        "_market_validation_by_sector",
        lambda sectors: {
            "AI_APP": {"avg_pct": -1.0, "up": 0, "down": 3},
            "AI_APP_NEAR": {"avg_pct": -1.0, "up": 0, "down": 3},
            "AI_COMPUTE": {"avg_pct": 1.2, "up": 4, "down": 0},
        },
    )
    monkeypatch.setattr(driver, "_scan_announcement_sentiment", lambda symbol: 0)

    explained = driver.explain_batch(["600000"])["600000"]
    exposure = driver.topic_exposure_table(["600000"])

    assert explained["boost"] == 15.2
    assert explained["matched_factors"][0]["topic"] == "AI"
    assert explained["matched_factors"][0]["deduped_count"] == 2
    assert explained["matched_factors"][0]["market_validation"] == "unconfirmed"
    assert any(row["topic"] == "Compute" and row["exposure"] == 1.0 for row in exposure)
