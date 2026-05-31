import pandas as pd

from modules.sector_tags import SectorTagProvider


class FakeAkShare:
    def __init__(self, fail: bool = False):
        self.fail = fail

    def stock_board_industry_name_ths(self):
        if self.fail:
            raise RuntimeError("industry names failed")
        return pd.DataFrame({"板块名称": ["石油行业"]})

    def stock_board_concept_name_ths(self):
        if self.fail:
            raise RuntimeError("concept names failed")
        return pd.DataFrame({"板块名称": ["黄金概念"]})

    def stock_board_industry_cons_ths(self, symbol):
        if self.fail:
            raise RuntimeError("industry cons failed")
        if symbol == "石油行业":
            return pd.DataFrame({"代码": ["600028"], "名称": ["中国石化"]})
        return pd.DataFrame()

    def stock_board_concept_cons_ths(self, symbol):
        if self.fail:
            raise RuntimeError("concept cons failed")
        if symbol == "黄金概念":
            return pd.DataFrame({"股票代码": ["600988.SH"], "股票简称": ["赤峰黄金"]})
        return pd.DataFrame()


def test_build_code_to_tags_from_industry_and_concept():
    provider = SectorTagProvider(ak=FakeAkShare(), max_workers=2)

    tags = provider.build_code_to_tags()

    assert tags["600028"] == {"石油行业"}
    assert tags["600988"] == {"黄金概念"}
    assert provider.get_tags("600988.SH") == {"黄金概念"}


def test_get_members_normalizes_common_columns(monkeypatch):
    provider = SectorTagProvider(ak=FakeAkShare())
    monkeypatch.setattr(provider, "_exposure_members", lambda target: pd.DataFrame())

    industry_members = provider.get_members("石油行业", "industry")
    concept_members = provider.get_members("黄金概念", "concept")

    assert industry_members[["code", "name"]].to_dict(orient="records") == [
        {"code": "600028", "name": "中国石化"}
    ]
    assert concept_members[["code", "name"]].to_dict(orient="records") == [
        {"code": "600988", "name": "赤峰黄金"}
    ]


def test_tag_matches_alias_suffix_and_contains_forms():
    assert SectorTagProvider.tag_matches("黄金概念", "黄金")
    assert SectorTagProvider.tag_matches("半导体及元件", "半导体")
    assert SectorTagProvider.tag_matches("光伏设备", "光伏")
    assert SectorTagProvider.tag_matches("石油行业", "石油")
    assert SectorTagProvider.tag_matches("贵金属概念", "贵金属")
    assert SectorTagProvider.tag_matches("国防军工", "军工")


def test_failures_degrade_to_empty_members_and_market_fallback(monkeypatch):
    provider = SectorTagProvider(ak=FakeAkShare(fail=True))
    monkeypatch.setattr(provider, "_apply_exposure_overlay", lambda mapping: None)
    monkeypatch.setattr(provider, "_exposure_members", lambda target: pd.DataFrame())

    assert provider.build_code_to_tags() == {}
    assert provider.get_members("石油行业", "industry").empty
    assert provider.get_tags("600001") == {"上海主板"}
    assert provider.get_tags("000001") == {"深圳主板"}


def test_default_bulk_maps_stay_graph_first(monkeypatch):
    provider = SectorTagProvider()

    monkeypatch.setattr(
        provider,
        "_board_names",
        lambda board_type: (_ for _ in ()).throw(RuntimeError("live bulk should not run by default")),
    )

    assert provider.build_code_to_tags()
    assert provider.build_code_to_industry()


def test_live_members_use_eastmoney_direct_board_constituents(monkeypatch):
    provider = SectorTagProvider(ak=FakeAkShare())
    monkeypatch.setattr(provider, "_graph_members", lambda target: pd.DataFrame())
    monkeypatch.setattr(provider, "_exposure_members", lambda target: pd.DataFrame())

    def fake_clist(*, fs, fields, fid, page_size=100, max_pages=20):
        if fs == "m:90 t:3 f:!50":
            return [{"f14": "PCB", "f12": "BK0877"}]
        if fs == "b:BK0877 f:!50":
            return [
                {"f12": "301362", "f14": "民爆光电", "f2": 238.73, "f3": 20.0, "f8": 18.45, "f6": 1.2e9}
            ]
        return []

    monkeypatch.setattr(provider, "_fetch_eastmoney_clist", fake_clist)

    members = provider.get_members("PCB概念", "concept")

    assert members[["code", "name", "source", "tag"]].to_dict(orient="records") == [
        {"code": "301362", "name": "民爆光电", "source": "eastmoney_board", "tag": "PCB概念"}
    ]


def test_default_provider_uses_sector_graph_members():
    provider = SectorTagProvider()

    members = provider.get_members("石油", "sector")

    assert not members.empty
    assert {"code", "name", "source", "tag"} <= set(members.columns)
    assert "600028" in set(members["code"])
    assert set(members["source"]) == {"sector_graph"}


def test_sector_graph_covers_news_intelligence_v2_themes():
    provider = SectorTagProvider()

    ai_members = provider.get_members("AI大模型", "theme")
    compute_members = provider.get_members("算力基础设施", "theme")
    robot_members = provider.get_members("人形机器人", "theme")
    low_altitude_members = provider.get_members("eVTOL", "concept")
    drug_members = provider.get_members("ADC", "concept")

    assert "002230" in set(ai_members["code"])
    assert "601138" in set(compute_members["code"])
    assert "002472" in set(robot_members["code"])
    assert "002085" in set(low_altitude_members["code"])
    assert "600276" in set(drug_members["code"])
