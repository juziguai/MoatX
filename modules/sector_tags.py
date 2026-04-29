"""Shared A-share sector/concept tag provider.

This module centralizes board lookups, member normalization, and fuzzy
tag matching so event scoring, event intelligence, and portfolio concentration
logic use the same sector semantics.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import pandas as pd

from modules.utils import _clear_all_proxy

_logger = logging.getLogger("moatx.sector_tags")
_PROJECT_ROOT = Path(__file__).resolve().parent.parent

TAG_SUFFIXES = ("概念", "行业", "板块", "设备", "及元件")

TAG_ALIASES = {
    "黄金": {"黄金", "黄金概念", "贵金属", "贵金属概念"},
    "贵金属": {"黄金", "黄金概念", "贵金属", "贵金属概念"},
    "半导体": {"半导体", "半导体及元件", "芯片", "集成电路"},
    "芯片": {"半导体", "半导体及元件", "芯片", "集成电路"},
    "光伏": {"光伏", "光伏概念", "光伏设备"},
    "石油": {"石油", "石油行业", "油气", "油服工程", "天然气"},
    "石油行业": {"石油", "石油行业", "油气", "油服工程", "天然气"},
    "国防军工": {"国防军工", "军工", "军工电子", "航空装备", "航天", "船舶制造"},
    "军工": {"国防军工", "军工", "军工电子", "航空装备", "航天", "船舶制造"},
}

FALLBACK_MEMBERS = {
    "石油行业": [
        ("600028", "中国石化"),
        ("601857", "中国石油"),
        ("600938", "中国海油"),
        ("600339", "中油工程"),
        ("600583", "海油工程"),
        ("002353", "杰瑞股份"),
    ],
    "油服工程": [
        ("600339", "中油工程"),
        ("600583", "海油工程"),
        ("002353", "杰瑞股份"),
        ("002554", "惠博普"),
    ],
    "天然气": [
        ("600803", "新奥股份"),
        ("002267", "陕天然气"),
        ("600917", "重庆燃气"),
        ("600642", "申能股份"),
    ],
    "黄金": [
        ("600547", "山东黄金"),
        ("601899", "紫金矿业"),
        ("600489", "中金黄金"),
        ("000975", "山金国际"),
        ("002155", "湖南黄金"),
        ("600988", "赤峰黄金"),
    ],
    "贵金属": [
        ("600547", "山东黄金"),
        ("601899", "紫金矿业"),
        ("600489", "中金黄金"),
        ("000975", "山金国际"),
        ("002155", "湖南黄金"),
        ("600988", "赤峰黄金"),
    ],
    "国防军工": [
        ("600760", "中航沈飞"),
        ("000768", "中航西飞"),
        ("600893", "航发动力"),
        ("600150", "中国船舶"),
        ("600372", "中航机载"),
    ],
    "半导体": [
        ("688981", "中芯国际"),
        ("603501", "韦尔股份"),
        ("600584", "长电科技"),
        ("002371", "北方华创"),
        ("688012", "中微公司"),
        ("688256", "寒武纪"),
    ],
    "芯片": [
        ("688981", "中芯国际"),
        ("603501", "韦尔股份"),
        ("600584", "长电科技"),
        ("002371", "北方华创"),
        ("688012", "中微公司"),
        ("688256", "寒武纪"),
    ],
    "信创": [
        ("600536", "中国软件"),
        ("000066", "中国长城"),
        ("002368", "太极股份"),
        ("600588", "用友网络"),
        ("688111", "金山办公"),
    ],
}


class SectorTagProvider:
    """Provider for industry/concept tags and board constituents."""

    _graph_cache: dict[str, Any] | None = None

    def __init__(
        self,
        ak: Any | None = None,
        max_workers: int = 8,
        graph_path: str | Path | None = None,
    ):
        self._ak = ak
        self._max_workers = max_workers
        self._graph_path = Path(graph_path) if graph_path else _PROJECT_ROOT / "data" / "sector_graph.toml"
        self._code_to_tags: dict[str, set[str]] | None = None
        self._code_to_industry: dict[str, str] | None = None

    def get_tags(self, symbol: str) -> set[str]:
        """Return all known industry/concept tags for one stock code."""
        code = self.normalize_code(symbol)
        code_to_tags = self.build_code_to_tags()
        if code in code_to_tags:
            return set(code_to_tags[code])
        graph_tags = self._graph_tags_for_code(code)
        if graph_tags:
            return graph_tags
        return {self.market_fallback_tag(code)}

    def get_members(self, target: str, target_type: str) -> pd.DataFrame:
        """Fetch board constituents and normalize to at least code/name columns."""
        if self._ak is not None:
            live = self._live_members(target, target_type)
            if not live.empty:
                return self._attach_member_meta(live, source="live", tag=target)
            return pd.DataFrame()

        graph = self._graph_members(target)
        if not graph.empty:
            return graph

        live = self._live_members(target, target_type)
        if not live.empty:
            return self._attach_member_meta(live, source="live", tag=target)

        return self._fallback_members(target)

    def build_code_to_tags(self, force: bool = False) -> dict[str, set[str]]:
        """Build and cache {stock_code: {industry, concept, ...}}."""
        if self._code_to_tags is not None and not force:
            return self._code_to_tags

        industry_names = self._board_names("industry")
        concept_names = self._board_names("concept")
        code_to_tags: dict[str, set[str]] = {}

        tasks = [(name, "industry") for name in industry_names] + [
            (name, "concept") for name in concept_names
        ]
        if not tasks:
            if self._ak is None:
                code_to_tags.update(self._graph_code_to_tags())
            self._code_to_tags = code_to_tags
            return self._code_to_tags

        with ThreadPoolExecutor(max_workers=min(len(tasks), self._max_workers)) as executor:
            futures = {executor.submit(self.get_members, name, kind): name for name, kind in tasks}
            for future in as_completed(futures):
                board_name = futures[future]
                try:
                    members = future.result()
                except Exception as exc:
                    _logger.debug("board [%s] mapping failed: %s", board_name, exc)
                    continue
                if members.empty or "code" not in members.columns:
                    continue
                for code in members["code"].astype(str):
                    code_to_tags.setdefault(self.normalize_code(code), set()).add(board_name)

        if self._ak is None:
            for code, tags in self._graph_code_to_tags().items():
                code_to_tags.setdefault(code, set()).update(tags)

        self._code_to_tags = code_to_tags
        _logger.info("sector tag map built: %d stocks", len(code_to_tags))
        return code_to_tags

    def build_code_to_industry(self, force: bool = False) -> dict[str, str]:
        """Build and cache {stock_code: industry_name} for concentration checks."""
        if self._code_to_industry is not None and not force:
            return self._code_to_industry

        code_to_industry: dict[str, str] = {}
        industry_names = self._board_names("industry")
        if not industry_names:
            self._code_to_industry = self._graph_code_to_industry() if self._ak is None else {}
            return self._code_to_industry

        with ThreadPoolExecutor(max_workers=min(len(industry_names), self._max_workers)) as executor:
            futures = {executor.submit(self.get_members, name, "industry"): name for name in industry_names}
            for future in as_completed(futures):
                industry_name = futures[future]
                try:
                    members = future.result()
                except Exception as exc:
                    _logger.debug("industry [%s] mapping failed: %s", industry_name, exc)
                    continue
                if members.empty or "code" not in members.columns:
                    continue
                for code in members["code"].astype(str):
                    code_to_industry[self.normalize_code(code)] = industry_name

        if self._ak is None:
            for code, industry in self._graph_code_to_industry().items():
                code_to_industry.setdefault(code, industry)

        self._code_to_industry = code_to_industry
        _logger.info(
            "industry map built: %d stocks -> %d industries",
            len(code_to_industry),
            len(set(code_to_industry.values())),
        )
        return code_to_industry

    @staticmethod
    def normalize_code(symbol: str) -> str:
        """Normalize stock symbol to 6-digit code without suffix."""
        code = str(symbol or "").split(".")[0].strip()
        return code.zfill(6) if code else ""

    @staticmethod
    def market_fallback_tag(code: str) -> str:
        """Fallback tag when no board membership is available."""
        return "上海主板" if str(code).startswith(("6", "5", "9")) else "深圳主板"

    @staticmethod
    def canonical_tag(tag: str) -> str:
        """Normalize board labels from different data sources."""
        value = str(tag or "").strip()
        for suffix in TAG_SUFFIXES:
            if value.endswith(suffix) and len(value) > len(suffix):
                value = value[: -len(suffix)]
        return value

    @classmethod
    def tag_matches(cls, stock_tag: str, event_tag: str) -> bool:
        """Return True if two sector/concept labels should be considered equivalent."""
        stock = str(stock_tag or "").strip()
        event = str(event_tag or "").strip()
        if not stock or not event:
            return False
        if stock == event:
            return True

        stock_norm = cls.canonical_tag(stock)
        event_norm = cls.canonical_tag(event)
        if stock_norm == event_norm:
            return True

        stock_aliases = TAG_ALIASES.get(stock_norm, {stock_norm})
        event_aliases = TAG_ALIASES.get(event_norm, {event_norm})
        stock_aliases = stock_aliases | cls._graph_aliases(stock_norm)
        event_aliases = event_aliases | cls._graph_aliases(event_norm)
        if stock_aliases & event_aliases:
            return True

        return (
            len(event_norm) >= 2
            and len(stock_norm) >= 2
            and (event_norm in stock_norm or stock_norm in event_norm)
        )

    @staticmethod
    def normalize_members(df: pd.DataFrame | None) -> pd.DataFrame:
        """Normalize board constituent DataFrame to standard code/name columns."""
        if df is None or df.empty:
            return pd.DataFrame()

        rename = {
            "代码": "code",
            "股票代码": "code",
            "证券代码": "code",
            "名称": "name",
            "股票简称": "name",
            "证券简称": "name",
        }
        out = df.rename(columns={k: v for k, v in rename.items() if k in df.columns}).copy()
        if "code" not in out.columns:
            return pd.DataFrame()
        out["code"] = out["code"].astype(str).str.split(".").str[0].str.zfill(6)
        if "name" not in out.columns:
            out["name"] = ""
        if "source" not in out.columns:
            out["source"] = ""
        if "tag" not in out.columns:
            out["tag"] = ""
        return out

    def _live_members(self, target: str, target_type: str) -> pd.DataFrame:
        ak = self._akshare()
        if target_type == "concept":
            func_names = [
                "stock_board_concept_cons_ths",
                "stock_board_concept_cons_em",
                "stock_board_industry_cons_ths",
                "stock_board_industry_cons_em",
            ]
        else:
            func_names = [
                "stock_board_industry_cons_ths",
                "stock_board_industry_cons_em",
                "stock_board_concept_cons_ths",
                "stock_board_concept_cons_em",
            ]

        for func_name in func_names:
            func = getattr(ak, func_name, None)
            if func is None:
                continue
            try:
                df = func(symbol=target)
                normalized = self.normalize_members(df)
                if not normalized.empty:
                    return normalized
            except Exception as exc:
                _logger.debug("board members fetch failed [%s/%s]: %s", target_type, target, exc)
        return pd.DataFrame()

    def _board_names(self, board_type: str) -> list[str]:
        ak = self._akshare()
        if board_type == "concept":
            func_names = ["stock_board_concept_name_ths", "stock_board_concept_name_em"]
        else:
            func_names = ["stock_board_industry_name_ths", "stock_board_industry_name_em"]

        df = pd.DataFrame()
        for func_name in func_names:
            func = getattr(ak, func_name, None)
            if func is None:
                continue
            try:
                df = func()
                if df is not None and not df.empty:
                    break
            except Exception as exc:
                _logger.debug("%s %s board names fetch failed: %s", func_name, board_type, exc)

        if df is None or df.empty:
            return []
        for column in ("板块名称", "概念名称", "行业名称", "name", "板块名"):
            if column in df.columns:
                return [str(x) for x in df[column].dropna().tolist()]
        return []

    @classmethod
    def _fallback_members(cls, target: str) -> pd.DataFrame:
        """Return a small curated fallback universe for critical event sectors."""
        rows: list[dict[str, str]] = []
        for fallback_tag, members in FALLBACK_MEMBERS.items():
            if cls.tag_matches(fallback_tag, target):
                rows.extend(
                    {"code": code, "name": name, "source": "fallback", "tag": fallback_tag}
                    for code, name in members
                )
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows).drop_duplicates(subset=["code"]).reset_index(drop=True)

    def _graph(self) -> dict[str, Any]:
        if self._graph_cache is not None:
            return self._graph_cache
        if not self._graph_path.exists():
            self._graph_cache = {"version": "", "nodes": []}
            return self._graph_cache
        import tomllib

        self._graph_cache = tomllib.loads(self._graph_path.read_text(encoding="utf-8"))
        return self._graph_cache

    @classmethod
    def _default_graph(cls) -> dict[str, Any]:
        if cls._graph_cache is not None:
            return cls._graph_cache
        path = _PROJECT_ROOT / "data" / "sector_graph.toml"
        if not path.exists():
            cls._graph_cache = {"version": "", "nodes": []}
            return cls._graph_cache
        import tomllib

        cls._graph_cache = tomllib.loads(path.read_text(encoding="utf-8"))
        return cls._graph_cache

    def graph_version(self) -> str:
        """Return sector graph config version."""
        return str(self._graph().get("version", ""))

    def _graph_members(self, target: str) -> pd.DataFrame:
        rows: list[dict[str, str]] = []
        for node in self._graph().get("nodes", []):
            tag = str(node.get("tag", ""))
            aliases = [str(x) for x in node.get("aliases", [])]
            if not any(self.tag_matches(candidate, target) for candidate in [tag] + aliases):
                continue
            for member in node.get("members", []):
                rows.append(
                    {
                        "code": self.normalize_code(str(member.get("code", ""))),
                        "name": str(member.get("name", "")),
                        "source": "sector_graph",
                        "tag": tag,
                    }
                )
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows).drop_duplicates(subset=["code"]).reset_index(drop=True)

    def _graph_tags_for_code(self, code: str) -> set[str]:
        tags: set[str] = set()
        for node in self._graph().get("nodes", []):
            tag = str(node.get("tag", ""))
            for member in node.get("members", []):
                if self.normalize_code(str(member.get("code", ""))) == code and tag:
                    tags.add(tag)
        return tags

    def _graph_code_to_tags(self) -> dict[str, set[str]]:
        mapping: dict[str, set[str]] = {}
        for node in self._graph().get("nodes", []):
            tag = str(node.get("tag", ""))
            for member in node.get("members", []):
                code = self.normalize_code(str(member.get("code", "")))
                if code and tag:
                    mapping.setdefault(code, set()).add(tag)
        return mapping

    def _graph_code_to_industry(self) -> dict[str, str]:
        mapping: dict[str, str] = {}
        for node in self._graph().get("nodes", []):
            if str(node.get("type", "")) != "sector":
                continue
            tag = str(node.get("tag", ""))
            for member in node.get("members", []):
                code = self.normalize_code(str(member.get("code", "")))
                if code and tag:
                    mapping.setdefault(code, tag)
        return mapping

    @classmethod
    def _graph_aliases(cls, tag: str) -> set[str]:
        aliases: set[str] = {tag}
        for node in cls._default_graph().get("nodes", []):
            node_tag = str(node.get("tag", ""))
            node_aliases = {str(x) for x in node.get("aliases", [])}
            if tag == node_tag or tag in node_aliases:
                aliases.add(node_tag)
                aliases.update(node_aliases)
        return aliases

    @staticmethod
    def _attach_member_meta(df: pd.DataFrame, *, source: str, tag: str) -> pd.DataFrame:
        out = df.copy()
        if "source" not in out.columns or not out["source"].astype(str).str.len().any():
            out["source"] = source
        if "tag" not in out.columns or not out["tag"].astype(str).str.len().any():
            out["tag"] = tag
        return out

    def _akshare(self):
        _clear_all_proxy()
        if self._ak is not None:
            return self._ak
        import akshare as ak

        self._ak = ak
        return ak
