from modules.event_intelligence.source_registry import SourceRegistry


def test_event_sources_enable_verified_domestic_xinhua_feeds():
    sources = {source.id: source for source in SourceRegistry().load()}

    assert sources["xinhua_world_rss"].enabled is True
    assert sources["xinhua_fortune_rss"].enabled is True
    assert sources["xinhua_politics_rss"].enabled is True
    assert sources["xinhua_world_rss"].type == "rss"


def test_event_sources_disable_known_slow_optional_feeds_by_default():
    sources = {source.id: source for source in SourceRegistry().load()}

    assert sources["bbc_chinese_rss"].enabled is False
    assert sources["rfi_chinese_rss"].enabled is False
    assert sources["dw_chinese_rss"].enabled is False
    assert sources["qhrb_home_html"].enabled is False
