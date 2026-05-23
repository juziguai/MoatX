import pandas as pd
import pytest


def test_fundflow_normalizer_keeps_newest_first():
    from modules.crawler.fundflow import _normalize_fund_flow_frame

    df = pd.DataFrame(
        [
            {"date": "2026-05-20", "main_net_inflow": -1},
            {"date": "2026-05-22", "main_net_inflow": 3, "main_net_inflow_pct": 6.5},
            {"date": "2026-05-21", "main_net_inflow": 2},
        ]
    )

    out = _normalize_fund_flow_frame(df)

    assert out["date"].tolist() == ["2026-05-22", "2026-05-21", "2026-05-20"]
    assert out.iloc[0]["main_net_inflow_pct"] == 6.5
    assert {"super_large_net", "small_net_pct"} <= set(out.columns)


def test_fundflow_prefers_eastmoney_direct(monkeypatch):
    from modules.crawler import fundflow

    realtime_called = False

    def fake_realtime(code, market):
        nonlocal realtime_called
        realtime_called = True
        return pd.DataFrame([{"date": "2026-05-22", "main_net_inflow": 1}])

    monkeypatch.setattr(
        fundflow,
        "_fetch_eastmoney_individual_fund_flow",
        lambda code, market: pd.DataFrame([{"date": "2026-05-22", "main_net_inflow": 10}]),
    )
    monkeypatch.setattr(fundflow, "_fetch_eastmoney_realtime_fund_flow", fake_realtime)
    monkeypatch.setattr(fundflow.cache, "write_df_cache", lambda *args, **kwargs: None)

    result = fundflow.get_individual_fund_flow("000001", use_cache=False, days=1)

    assert result.ok
    assert result.source == "eastmoney_direct"
    assert result.data.iloc[0]["main_net_inflow"] == 10
    assert realtime_called is False


def test_fundflow_uses_eastmoney_realtime_before_akshare(monkeypatch):
    from modules.crawler import fundflow

    class FakeAk:
        def stock_individual_fund_flow(self, stock, market):
            raise AssertionError("akshare should not be called when realtime fallback works")

    monkeypatch.setattr(
        fundflow,
        "_fetch_eastmoney_individual_fund_flow",
        lambda code, market: (_ for _ in ()).throw(RuntimeError("eastmoney history down")),
    )
    monkeypatch.setattr(
        fundflow,
        "_fetch_eastmoney_realtime_fund_flow",
        lambda code, market: pd.DataFrame([{"date": "2026-05-22", "main_net_inflow": 7}]),
    )
    monkeypatch.setattr(fundflow, "ak", FakeAk())
    monkeypatch.setattr(fundflow.cache, "write_df_cache", lambda *args, **kwargs: None)

    result = fundflow.get_individual_fund_flow("000001", use_cache=False, days=1)

    assert result.ok
    assert result.source == "eastmoney_realtime"
    assert result.data.iloc[0]["main_net_inflow"] == 7


def test_fundflow_uses_akshare_only_as_fallback(monkeypatch):
    from modules.crawler import fundflow

    class FakeAk:
        def stock_individual_fund_flow(self, stock, market):
            return pd.DataFrame([{"date": "2026-05-22", "main_net_inflow": 5}])

    monkeypatch.setattr(
        fundflow,
        "_fetch_eastmoney_individual_fund_flow",
        lambda code, market: (_ for _ in ()).throw(RuntimeError("eastmoney down")),
    )
    monkeypatch.setattr(
        fundflow,
        "_fetch_eastmoney_realtime_fund_flow",
        lambda code, market: (_ for _ in ()).throw(RuntimeError("eastmoney realtime down")),
    )
    monkeypatch.setattr(fundflow, "ak", FakeAk())
    monkeypatch.setattr(fundflow.cache, "write_df_cache", lambda *args, **kwargs: None)

    result = fundflow.get_individual_fund_flow("000001", use_cache=False, days=1)

    assert result.ok
    assert result.source == "akshare"
    assert result.data.iloc[0]["main_net_inflow"] == 5


def test_fundflow_failure_degrades_to_empty_summary(monkeypatch):
    from modules.crawler import fundflow
    from modules.crawler.models import CrawlResult

    class BrokenAk:
        def stock_individual_fund_flow(self, stock, market):
            raise RuntimeError("akshare down")

    monkeypatch.setattr(
        fundflow,
        "_fetch_eastmoney_individual_fund_flow",
        lambda code, market: (_ for _ in ()).throw(RuntimeError("eastmoney down")),
    )
    monkeypatch.setattr(
        fundflow,
        "_fetch_eastmoney_realtime_fund_flow",
        lambda code, market: (_ for _ in ()).throw(RuntimeError("eastmoney realtime down")),
    )
    monkeypatch.setattr(fundflow, "ak", BrokenAk())
    monkeypatch.setattr(fundflow.cache, "read_df_cache", lambda *args, **kwargs: CrawlResult(ok=False))

    summary = fundflow.get_money_flow_summary("000001", use_cache=False)

    assert summary["main_net_inflow"] == 0
    assert summary["main_net_inflow_pct"] == 0
    assert "数据不可用" in summary["_note"]


def test_ths_fund_flow_import_does_not_require_mini_racer(monkeypatch):
    import modules.crawler.ths_fund_flow as ths_fund_flow

    monkeypatch.setattr(ths_fund_flow, "MiniRacer", None)
    monkeypatch.setattr(ths_fund_flow, "_IMPORT_ERROR", RuntimeError("broken runtime"))

    with pytest.raises(RuntimeError, match="mini-racer unavailable"):
        ths_fund_flow.get_hexin_v_header()


def test_sector_tags_fall_back_when_akshare_getattr_fails():
    from modules.sector_tags import SectorTagProvider

    class BrokenAk:
        def __getattr__(self, name):
            raise RuntimeError("akshare import failed")

    provider = SectorTagProvider(ak=BrokenAk())

    assert provider.build_code_to_tags() == {}
    assert provider.get_members("石油", "sector").empty


def test_default_sector_tags_use_graph_when_optional_akshare_fails(monkeypatch):
    import modules.sector_tags as sector_tags

    class BrokenAk:
        def __getattr__(self, name):
            raise RuntimeError("akshare import failed")

    monkeypatch.setattr(sector_tags, "import_akshare", lambda: BrokenAk())

    provider = sector_tags.SectorTagProvider()
    tags = provider.get_tags("600028")
    members = provider.get_members(next(iter(tags)), "sector")

    assert tags
    assert tags != {"上海主板"}
    assert not members.empty


def test_sector_boards_use_local_snapshot_when_realtime_sources_fail(monkeypatch):
    from modules.crawler import sector
    from modules.crawler.models import CrawlResult

    local_df = pd.DataFrame(
        [
            {
                "sector_type": "industry",
                "sector": "Local Industry",
                "pct_change": 1.2,
                "source": "sector_graph_quote",
            }
        ]
    )

    monkeypatch.setattr(
        sector.ths,
        "fetch_industry_boards",
        lambda use_cache=True: CrawlResult(ok=False, error="THS_DOWN", warnings=["ths failed"]),
    )
    monkeypatch.setattr(
        sector.sina,
        "fetch_industry_boards",
        lambda use_cache=True: CrawlResult(ok=False, error="SINA_DOWN", warnings=["sina failed"]),
    )
    monkeypatch.setattr(
        sector.local_sector,
        "fetch_industry_boards",
        lambda use_cache=True: CrawlResult(ok=True, data=local_df, source="sector_graph_quote"),
    )

    result = sector.get_industry_boards(use_cache=False)

    assert result.ok
    assert result.data.iloc[0]["sector"] == "Local Industry"
    assert result.data.iloc[0]["source"] == "sector_graph_quote"
    assert any("local sector graph" in warning for warning in result.warnings)


def test_concept_boards_use_local_snapshot_when_ths_fails(monkeypatch):
    from modules.crawler import sector
    from modules.crawler.models import CrawlResult

    local_df = pd.DataFrame(
        [
            {
                "sector_type": "concept",
                "sector": "Local Theme",
                "pct_change": 2.3,
                "source": "sector_graph_quote",
            }
        ]
    )

    monkeypatch.setattr(
        sector.ths,
        "fetch_concept_boards",
        lambda use_cache=True: CrawlResult(ok=False, error="THS_DOWN", warnings=["ths failed"]),
    )
    monkeypatch.setattr(
        sector.local_sector,
        "fetch_concept_boards",
        lambda use_cache=True: CrawlResult(ok=True, data=local_df, source="sector_graph_quote"),
    )

    result = sector.get_concept_boards(use_cache=False)

    assert result.ok
    assert result.data.iloc[0]["sector"] == "Local Theme"
    assert result.data.iloc[0]["source"] == "sector_graph_quote"
    assert any("local sector graph" in warning for warning in result.warnings)


def test_local_concept_snapshot_includes_theme_nodes(monkeypatch):
    from modules.crawler import local_sector
    from modules.crawler.models import CrawlResult

    class FakeProvider:
        def graph_nodes(self):
            return [
                {
                    "tag": "Concept Alpha",
                    "type": "concept",
                    "members": [{"code": "000001", "name": "One"}],
                },
                {
                    "tag": "Theme Beta",
                    "type": "theme",
                    "members": [{"code": "000002", "name": "Two"}],
                },
                {
                    "tag": "Sector Gamma",
                    "type": "sector",
                    "members": [{"code": "000003", "name": "Three"}],
                },
            ]

    class FakeStockData:
        def get_spot(self, use_cache=True):
            return pd.DataFrame(
                [
                    {"code": "000001", "name": "One", "price": 10, "pct_change": 1, "turnover": 3},
                    {"code": "000002", "name": "Two", "price": 20, "pct_change": 2, "turnover": 4},
                ]
            )

    monkeypatch.setattr(local_sector, "SectorTagProvider", FakeProvider)
    monkeypatch.setattr(local_sector, "StockData", FakeStockData)
    monkeypatch.setattr(local_sector.cache, "read_json_cache", lambda *args, **kwargs: CrawlResult(ok=False))
    monkeypatch.setattr(local_sector.cache, "write_json_cache", lambda *args, **kwargs: None)

    result = local_sector.fetch_concept_boards(use_cache=False)

    assert result.ok
    assert set(result.data["sector"]) == {"Concept Alpha", "Theme Beta"}


def test_local_snapshot_fails_when_spot_quotes_unavailable(monkeypatch):
    from modules.crawler import local_sector
    from modules.crawler.models import CrawlResult, SOURCE_UNAVAILABLE

    class FakeProvider:
        def graph_nodes(self):
            return [{"tag": "Industry Alpha", "type": "sector", "members": [{"code": "000001", "name": "One"}]}]

    class EmptyStockData:
        def get_spot(self, use_cache=True):
            return pd.DataFrame()

    monkeypatch.setattr(local_sector, "SectorTagProvider", FakeProvider)
    monkeypatch.setattr(local_sector, "StockData", EmptyStockData)
    monkeypatch.setattr(local_sector.cache, "read_json_cache", lambda *args, **kwargs: CrawlResult(ok=False))

    result = local_sector.fetch_industry_boards(use_cache=False)

    assert not result.ok
    assert result.error == SOURCE_UNAVAILABLE
    assert "spot quote snapshot unavailable" in result.error_detail
