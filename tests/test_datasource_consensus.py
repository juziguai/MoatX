"""DataSource consensus tests — migrated from QuoteManager to DataSourceManager."""

from modules.data_source import Capability, DataSource, Health
from modules.data_source_manager import DataSourceManager
from modules.result import Result
from modules.config import close, set


class FakeProvider(DataSource):
    """Fake DataSource that returns pre-set quote data."""

    def __init__(self, name, rows):
        self._name = name
        self._rows = rows

    @property
    def name(self) -> str:
        return self._name

    def capabilities(self) -> set[Capability]:
        return {Capability.QUOTE}

    def fetch(self, capability: Capability, **params):
        if capability == Capability.QUOTE:
            return Result.ok(self._rows, source=self._name)
        return Result.fail("unsupported", source=self._name)


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


def _make_manager(**providers):
    """Create DataSourceManager with fake providers injected."""
    mgr = DataSourceManager()
    mgr._providers.update(providers)
    return mgr


def test_manager_aggregates_multiple_sources_as_verified():
    mgr = _make_manager(
        tencent=FakeProvider("tencent", {"600519.SH": _quote("600519.SH", 100.00, 1.00)}),
        sina=FakeProvider("sina", {"600519.SH": _quote("600519.SH", 100.02, 1.03)}),
    )

    rows = mgr.fetch_quotes(["600519"], mode="validate", source_names=["tencent", "sina"], tolerance_pct=0.15)
    row = rows["600519.SH"]

    assert row["validation_status"] == "verified"
    assert row["sources"] == ["tencent", "sina"]
    assert row["max_pct_diff"] == 0.03


def test_manager_marks_diverged_sources():
    mgr = _make_manager(
        tencent=FakeProvider("tencent", {"600519.SH": _quote("600519.SH", 100.00, 1.00)}),
        sina=FakeProvider("sina", {"600519.SH": _quote("600519.SH", 101.00, 2.00)}),
    )

    rows = mgr.fetch_quotes(["600519"], mode="validate", source_names=["tencent", "sina"], tolerance_pct=0.15)
    row = rows["600519.SH"]

    assert row["validation_status"] == "diverged"
    assert row["warning"]


def test_manager_keeps_single_source_when_others_miss():
    mgr = _make_manager(
        tencent=FakeProvider("tencent", {}),
        sina=FakeProvider("sina", {"000858.SZ": _quote("000858.SZ", 150.00, -0.5)}),
    )

    rows = mgr.fetch_quotes(["000858"], mode="validate", source_names=["tencent", "sina"])
    row = rows["000858.SZ"]

    assert row["validation_status"] == "single_source"
    assert row["sources"] == ["sina"]


def test_manager_default_sources_follow_runtime_config():
    try:
        close()
        set("datasource.primary", "tencent")
        set("datasource.validation", ["sina"])
        set("datasource.supplement", ["eastmoney"])

        mgr = DataSourceManager()

        # Default chain should reflect config
        chain = mgr._policy.chain_for("quote")
        assert chain == ["tencent", "sina", "eastmoney"]
    finally:
        set("datasource.primary", "sina")
        set("datasource.validation", ["tencent"])
        set("datasource.supplement", ["eastmoney"])
        close()


def test_manager_single_mode_uses_only_primary():
    try:
        close()
        set("datasource.primary", "sina")
        set("datasource.mode", "single")
        set("datasource.validation", ["tencent"])
        set("datasource.supplement", ["eastmoney"])

        mgr = DataSourceManager()

        chain = mgr._policy.chain_for("quote")
        assert chain == ["sina"]
    finally:
        set("datasource.primary", "sina")
        set("datasource.mode", "validate")
        set("datasource.validation", ["tencent"])
        set("datasource.supplement", ["eastmoney"])
        close()


def test_manager_explicit_source_names_override_config():
    try:
        close()
        set("datasource.primary", "sina")
        set("datasource.mode", "validate")

        mgr = DataSourceManager()

        chain = mgr._policy.chain_for("quote")
        assert chain == ["sina", "tencent", "eastmoney"]
    finally:
        close()


def test_sina_provider_parses_hq_str_symbol(monkeypatch):
    """Integration test: SinaProvider correctly parses Sina HQ API response."""
    from modules.data_sources.sina import SinaProvider

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
        headers = {}

        def get(self, *args, **kwargs):
            return FakeResponse()

    monkeypatch.setattr("requests.Session", lambda: FakeSession())
    monkeypatch.setattr("modules.sina_http.sina_session", lambda **kw: FakeSession())

    provider = SinaProvider()
    result = provider.fetch(Capability.QUOTE, symbols=["600111"])

    assert result.ok
    assert "sh600111" in result.data
    assert result.data["sh600111"]["name"] == "北方稀土"
    assert result.data["sh600111"]["price"] == 50.28
