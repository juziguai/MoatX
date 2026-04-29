from modules.datasource import QuoteManager, QuoteSource, SinaSource
from modules.config import close, set


class FakeSource(QuoteSource):
    def __init__(self, name, rows):
        self._name = name
        self._rows = rows

    @property
    def name(self):
        return self._name

    def fetch_quotes(self, symbols):
        return self._rows


def _quote(code, price, pct, source_name=""):
    return {
        "code": code,
        "name": "测试股票",
        "price": price,
        "change_pct": pct,
        "volume": 1000,
        "amount": 10000,
        "turnover": 1.0,
        "pe": 10,
        "high": price + 1,
        "low": price - 1,
        "open": price,
        "prev_close": price / (1 + pct / 100),
        "source": source_name,
    }


def test_quote_manager_aggregates_multiple_sources_as_verified():
    manager = QuoteManager(
        sources=[
            FakeSource("tencent", {"600519.SH": _quote("600519.SH", 100.00, 1.00)}),
            FakeSource("sina", {"600519.SH": _quote("600519.SH", 100.02, 1.03)}),
        ],
        tolerance_pct=0.15,
    )

    rows = manager.fetch_quotes(["600519"])
    row = rows["600519.SH"]

    assert row["validation_status"] == "verified"
    assert row["sources"] == ["tencent", "sina"]
    assert row["max_pct_diff"] == 0.03


def test_quote_manager_marks_diverged_sources():
    manager = QuoteManager(
        sources=[
            FakeSource("tencent", {"600519.SH": _quote("600519.SH", 100.00, 1.00)}),
            FakeSource("sina", {"600519.SH": _quote("600519.SH", 101.00, 2.00)}),
        ],
        tolerance_pct=0.15,
    )

    row = manager.fetch_quotes(["600519"])["600519.SH"]

    assert row["validation_status"] == "diverged"
    assert row["warning"]


def test_quote_manager_keeps_single_source_when_others_miss():
    manager = QuoteManager(
        sources=[
            FakeSource("tencent", {}),
            FakeSource("sina", {"000858.SZ": _quote("000858.SZ", 150.00, -0.5)}),
        ],
    )

    row = manager.fetch_quotes(["000858"])["000858.SZ"]

    assert row["validation_status"] == "single_source"
    assert row["sources"] == ["sina"]


def test_quote_manager_default_sources_follow_runtime_config():
    try:
        close()
        set("datasource.primary", "tencent")
        set("datasource.validation", ["sina"])
        set("datasource.supplement", ["eastmoney"])

        manager = QuoteManager()

        assert [source.name for source in manager.sources] == ["tencent", "sina", "eastmoney"]
    finally:
        set("datasource.primary", "sina")
        set("datasource.validation", ["tencent"])
        set("datasource.supplement", ["eastmoney"])
        close()


def test_quote_manager_single_mode_uses_only_primary():
    try:
        close()
        set("datasource.primary", "sina")
        set("datasource.mode", "single")
        set("datasource.validation", ["tencent"])
        set("datasource.supplement", ["eastmoney"])

        manager = QuoteManager()

        assert [source.name for source in manager.sources] == ["sina"]
    finally:
        set("datasource.primary", "sina")
        set("datasource.mode", "validate")
        set("datasource.validation", ["tencent"])
        set("datasource.supplement", ["eastmoney"])
        close()


def test_quote_manager_explicit_source_names_override_config():
    try:
        close()
        set("datasource.primary", "sina")
        set("datasource.mode", "validate")

        manager = QuoteManager(source_names=["tencent"])

        assert [source.name for source in manager.sources] == ["tencent"]
    finally:
        close()


def test_sina_source_parses_hq_str_symbol(monkeypatch):
    class FakeResponse:
        status_code = 200
        text = (
            'var hq_str_sh600111="北方稀土,47.500,46.320,50.280,50.500,'
            '47.490,50.270,50.280,123634308,6119430676.000,0,0,0,0,0,'
            '0,0,0,0,0,0,0,0,0,0,0,0,0,0,2026-04-29,09:58:00,00";'
        )

    class FakeSession:
        trust_env = False
        proxies = {}

        def get(self, *args, **kwargs):
            return FakeResponse()

    monkeypatch.setattr("requests.Session", lambda: FakeSession())

    rows = SinaSource().fetch_quotes(["600111"])

    assert "600111.SH" in rows
    assert rows["600111.SH"]["name"] == "北方稀土"
    assert rows["600111.SH"]["price"] == 50.28
