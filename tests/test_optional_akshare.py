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

    monkeypatch.setattr(
        fundflow,
        "_fetch_eastmoney_individual_fund_flow",
        lambda code, market: pd.DataFrame([{"date": "2026-05-22", "main_net_inflow": 10}]),
    )
    monkeypatch.setattr(fundflow.cache, "write_df_cache", lambda *args, **kwargs: None)

    result = fundflow.get_individual_fund_flow("000001", use_cache=False, days=1)

    assert result.ok
    assert result.source == "eastmoney_direct"
    assert result.data.iloc[0]["main_net_inflow"] == 10


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
