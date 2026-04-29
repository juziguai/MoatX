from modules.event_driver import EventDriver


class FakeSectorProvider:
    def get_tags(self, symbol):
        return {"AI应用"}

    def build_code_to_tags(self):
        return {"600000": {"AI应用"}}


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
        def sector_boosts(self):
            return {"AI应用": 12.5}

    monkeypatch.setattr("modules.event_intelligence.news_factors.NewsFactorEngine", FakeNewsFactorEngine)
    driver = EventDriver(sector_provider=FakeSectorProvider())
    driver._events = []
    monkeypatch.setattr(driver, "_scan_announcement_sentiment", lambda symbol: 0)

    assert driver.score_single("600000") == 12.5
