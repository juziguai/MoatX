from modules.event_intelligence.exposure import StockTopicExposureProvider


def test_stock_topic_exposure_provider_reads_configured_weight(tmp_path):
    path = tmp_path / "exposure.toml"
    path.write_text(
        """
[[exposures]]
symbol = "600000"
topic = "AI"
sector_tag = "AI_APP"
exposure = 0.6
confidence = 0.5
source = "unit"
""",
        encoding="utf-8",
    )
    provider = StockTopicExposureProvider(path)

    weight, row = provider.weight(
        "600000",
        topic="AI",
        event_tag="AI_APP",
        stock_tag="AI_APP",
        fallback=1.0,
    )

    assert weight == 0.3
    assert row["source"] == "unit"


def test_stock_topic_exposure_provider_falls_back_when_not_configured(tmp_path):
    provider = StockTopicExposureProvider(tmp_path / "missing.toml")

    weight, row = provider.weight("600000", topic="AI", event_tag="AI_APP", fallback=0.75)

    assert weight == 0.75
    assert row is None
