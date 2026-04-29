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


def test_get_members_normalizes_common_columns():
    provider = SectorTagProvider(ak=FakeAkShare())

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


def test_failures_degrade_to_empty_members_and_market_fallback():
    provider = SectorTagProvider(ak=FakeAkShare(fail=True))

    assert provider.build_code_to_tags() == {}
    assert provider.get_members("石油行业", "industry").empty
    assert provider.get_tags("600001") == {"上海主板"}
    assert provider.get_tags("000001") == {"深圳主板"}


def test_default_provider_uses_sector_graph_members():
    provider = SectorTagProvider()

    members = provider.get_members("石油", "sector")

    assert not members.empty
    assert {"code", "name", "source", "tag"} <= set(members.columns)
    assert "600028" in set(members["code"])
    assert set(members["source"]) == {"sector_graph"}
