"""Sina crawler helpers.

Industry/concept board data via Market_Center API (vIndustryRank HTML page is dead).
Uses getHQNodes for node tree + getHQNodeData for per-board stock data.
"""

from __future__ import annotations

import json
import time
import pandas as pd
import requests

from . import cache
from .models import CrawlResult, SOURCE_UNAVAILABLE


SOURCE = "sina"
API_BASE = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php"
NODE_TREE_CACHE_KEY = "sina_node_tree"
STANDARD_COLUMNS = [
    "sector_type",
    "sector",
    "sector_code",
    "pct_change",
    "price",
    "turnover",
    "rise_count",
    "fall_count",
    "top_stock",
    "top_stock_pct",
    "source",
    "trade_date",
]


def _api_session():
    s = requests.Session()
    s.trust_env = False
    s.proxies = {"http": None, "https": None}
    s.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://vip.stock.finance.sina.com.cn/mkt/",
    })
    return s


def _call_api(method: str, params: dict | None = None) -> dict | list | str:
    s = _api_session()
    r = s.get(f"{API_BASE}/Market_Center.{method}", params=params or {}, timeout=10)
    text = r.text.strip()
    if text.startswith("[") or text.startswith("{"):
        return json.loads(text)
    return text


def _get_node_tree() -> list[dict]:
    """Get all industry/concept nodes from Sina's node tree. Cached daily."""
    cached = cache.read_json_cache(NODE_TREE_CACHE_KEY, max_age_seconds=86400)
    if cached.ok and cached.data:
        return cached.data

    raw = _call_api("getHQNodes")
    if not raw:
        return []

    nodes = _parse_node_tree(raw)
    cache.write_json_cache(NODE_TREE_CACHE_KEY, nodes, SOURCE)
    return nodes


def _parse_node_tree(node) -> list[dict]:
    """Recursively parse Sina node tree, returning leaf nodes with codes."""
    results = []
    children = node[1] if isinstance(node, list) and len(node) > 1 else []

    if not isinstance(children, list):
        return results

    for child in children:
        if not isinstance(child, list):
            continue
        # Leaf: [display_name, url_param, node_code]
        if len(child) == 3 and isinstance(child[2], str) and child[2]:
            results.append({"name": child[0], "node": child[2]})
        elif len(child) >= 2 and isinstance(child[1], list):
            results.extend(_parse_node_tree(child))

    return results


def _fetch_board_data(node_code: str, num: int = 100) -> dict:
    """Fetch board-level aggregate data for a single node. Retries on failure."""
    for attempt in range(3):
        try:
            data = _call_api("getHQNodeData", {
                "page": 1, "num": num, "sort": "changepercent", "asc": 0, "node": node_code,
            })
            if not isinstance(data, list) or not data:
                if attempt < 2:
                    time.sleep(1)
                    continue
                return {}
            break
        except Exception:
            if attempt < 2:
                time.sleep(1)
                continue
            return {}

        changes = [float(s.get("changepercent", 0) or 0) for s in data]
        avg_pct = sum(changes) / len(changes)
        up_count = sum(1 for c in changes if c > 0)
        down_count = sum(1 for c in changes if c < 0)
        top = data[0]

        return {
            "pct_change": round(avg_pct, 3),
            "rise_count": up_count,
            "fall_count": down_count,
            "top_stock": top.get("name", ""),
            "top_stock_pct": float(top.get("changepercent", 0) or 0),
            "trade": float(top.get("trade", 0) or 0),
        }


def fetch_industry_boards(use_cache: bool = True) -> CrawlResult:
    trade_date = cache.beijing_now().date().isoformat()
    cache_key = cache.build_cache_key("sector_industry_sina_v2", trade_date)
    if use_cache:
        cached = _read_cache_as_df(cache_key)
        if cached.ok:
            return cached

    try:
        all_nodes = _get_node_tree()
        industry_nodes = [n for n in all_nodes if n["node"].startswith("new_")]
    except Exception as exc:
        stale = _read_cache_as_df(cache_key, allow_stale=True)
        if stale.data is not None:
            stale.ok = True
            stale.user_message = "Sina 节点树获取失败，返回缓存"
            return stale
        return CrawlResult(
            ok=False, source=SOURCE, error=SOURCE_UNAVAILABLE,
            error_detail=str(exc), user_message="Sina 节点树获取失败",
        )

    if not industry_nodes:
        return CrawlResult(
            ok=False, source=SOURCE, error=SOURCE_UNAVAILABLE,
            user_message="Sina 节点树中无行业板块",
        )

    rows = []
    for node_info in industry_nodes:
        board = _fetch_board_data(node_info["node"])
        if board:
            rows.append({
                "sector_type": "行业",
                "sector": node_info["name"],
                "sector_code": node_info["node"],
                "pct_change": board.get("pct_change"),
                "price": board.get("trade"),
                "turnover": pd.NA,
                "rise_count": board.get("rise_count"),
                "fall_count": board.get("fall_count"),
                "top_stock": board.get("top_stock"),
                "top_stock_pct": board.get("top_stock_pct"),
                "source": SOURCE,
                "trade_date": trade_date,
            })
        time.sleep(0.3)  # rate limiting to avoid IP ban

    if not rows:
        return CrawlResult(
            ok=False, source=SOURCE, error=SOURCE_UNAVAILABLE,
            user_message="Sina 行业板块数据为空",
        )

    df = _normalize_board_df(pd.DataFrame(rows))
    cache.write_json_cache(cache_key, df.to_dict(orient="records"), SOURCE, trade_date=trade_date)

    return CrawlResult(
        ok=True, data=df, source=SOURCE, trade_date=trade_date,
        warnings=["Sina fallback: 通过 Market_Center API 采集，板块涨跌幅为成分股均值近似"],
    )


def fetch_concept_boards(use_cache: bool = True) -> CrawlResult:
    trade_date = cache.beijing_now().date().isoformat()
    cache_key = cache.build_cache_key("sector_concept_sina_v2", trade_date)
    if use_cache:
        cached = _read_cache_as_df(cache_key)
        if cached.ok:
            return cached

    try:
        all_nodes = _get_node_tree()
        concept_nodes = [n for n in all_nodes if n["node"].startswith("gn_")]
    except Exception as exc:
        stale = _read_cache_as_df(cache_key, allow_stale=True)
        if stale.data is not None:
            stale.ok = True
            stale.user_message = "Sina 节点树获取失败，返回缓存"
            return stale
        return CrawlResult(
            ok=False, source=SOURCE, error=SOURCE_UNAVAILABLE,
            error_detail=str(exc), user_message="Sina 节点树获取失败",
        )

    if not concept_nodes:
        return CrawlResult(
            ok=False, source=SOURCE, error=SOURCE_UNAVAILABLE,
            user_message="Sina 节点树中无概念板块",
        )

    rows = []
    for node_info in concept_nodes:
        board = _fetch_board_data(node_info["node"])
        if board:
            rows.append({
                "sector_type": "概念",
                "sector": node_info["name"],
                "sector_code": node_info["node"],
                "pct_change": board.get("pct_change"),
                "price": board.get("trade"),
                "turnover": pd.NA,
                "rise_count": board.get("rise_count"),
                "fall_count": board.get("fall_count"),
                "top_stock": board.get("top_stock"),
                "top_stock_pct": board.get("top_stock_pct"),
                "source": SOURCE,
                "trade_date": trade_date,
            })
        time.sleep(0.05)

    if not rows:
        return CrawlResult(
            ok=False, source=SOURCE, error=SOURCE_UNAVAILABLE,
            user_message="Sina 概念板块数据为空",
        )

    df = _normalize_board_df(pd.DataFrame(rows))
    cache.write_json_cache(cache_key, df.to_dict(orient="records"), SOURCE, trade_date=trade_date)

    return CrawlResult(
        ok=True, data=df, source=SOURCE, trade_date=trade_date,
        warnings=["Sina fallback: 通过 Market_Center API 采集，板块涨跌幅为成分股均值近似"],
    )


def _normalize_board_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for column in STANDARD_COLUMNS:
        if column not in out.columns:
            out[column] = pd.NA
    for column in ["pct_change", "price", "turnover", "rise_count", "fall_count", "top_stock_pct"]:
        out[column] = pd.to_numeric(out[column], errors="coerce")
    return out[STANDARD_COLUMNS]


def _read_cache_as_df(key: str, allow_stale: bool = False) -> CrawlResult:
    result = cache.read_json_cache(key, max_age_seconds=None)
    if result.ok or (allow_stale and result.data is not None):
        result.data = _normalize_board_df(pd.DataFrame(result.data or []))
        result.source = result.source or SOURCE
        if allow_stale and not result.ok and result.error:
            result.warnings.append(f"返回非新鲜缓存: {result.error}")
    return result
