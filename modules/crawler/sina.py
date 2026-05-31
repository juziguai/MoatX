"""Sina crawler helpers.

Industry/concept board data via Market_Center API (vIndustryRank HTML page is dead).
Uses getHQNodes for node tree + getHQNodeData for per-board stock data.
"""

from __future__ import annotations

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests

from . import cache
from .models import CrawlResult, SOURCE_UNAVAILABLE

_logger = logging.getLogger("moatx.crawler.sina")

SOURCE = "sina"
BAN_HTTP_CODES = {429, 456, 503}
API_BASE = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php"
NODE_TREE_CACHE_KEY = "sina_node_tree"
MAX_WORKERS = 3
STANDARD_COLUMNS = [
    "sector_type", "sector", "sector_code", "pct_change", "price",
    "turnover", "rise_count", "fall_count", "top_stock", "top_stock_pct",
    "source", "trade_date",
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
    """Call Sina Market_Center API with HTTP status code check and retry for bans."""
    s = _api_session()
    for attempt in range(3):
        r = s.get(f"{API_BASE}/Market_Center.{method}", params=params or {}, timeout=10)
        if r.status_code in BAN_HTTP_CODES:
            if attempt < 2:
                wait = (2 ** attempt) * 3
                _logger.warning("Sina API HTTP %d (ban), retrying in %ds (attempt %d/3)",
                                r.status_code, wait, attempt + 1)
                time.sleep(wait)
                s = _api_session()
                continue
            raise RuntimeError(f"Sina API blocked (HTTP {r.status_code}) after 3 retries")
        if r.status_code != 200:
            if attempt < 2:
                _logger.warning("Sina API HTTP %d, retrying (attempt %d/3)", r.status_code, attempt + 1)
                time.sleep(1)
                continue
            raise RuntimeError(f"Sina API returned HTTP {r.status_code}")
        break
    try:
        text = r.text.strip()
    except Exception:
        return ""
    if text and (text.startswith("[") or text.startswith("{")):
        return json.loads(text)
    return text


def _get_node_tree() -> list[dict]:
    """Get all industry/concept nodes from Sina node tree. Cached daily."""
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


def _build_board_row(node_info: dict, board: dict, sector_type: str, trade_date: str) -> dict:
    return {
        "sector_type": sector_type,
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
    }


def _fetch_boards_concurrent(node_list: list[dict], sector_type: str, trade_date: str) -> list[dict]:
    """Fetch board data concurrently with staggered starts to avoid burst."""
    import threading
    rows: list[dict] = []
    lock = threading.Lock()

    def _fetch_one(idx: int, node_info: dict):
        time.sleep(0.1 * (idx % MAX_WORKERS))
        board = _fetch_board_data(node_info["node"])
        if board:
            with lock:
                rows.append(_build_board_row(node_info, board, sector_type, trade_date))

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(_fetch_one, i, n): n for i, n in enumerate(node_list)}
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                _logger.debug("board fetch failed: %s", exc)
    return rows


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
            stale.user_message = "Sina \u8282\u70b9\u6811\u83b7\u53d6\u5931\u8d25\uff0c\u8fd4\u56de\u7f13\u5b58"
            return stale
        return CrawlResult(
            ok=False, source=SOURCE, error=SOURCE_UNAVAILABLE,
            error_detail=str(exc), user_message="Sina \u8282\u70b9\u6811\u83b7\u53d6\u5931\u8d25",
        )
    if not industry_nodes:
        return CrawlResult(
            ok=False, source=SOURCE, error=SOURCE_UNAVAILABLE,
            user_message="Sina \u8282\u70b9\u6811\u4e2d\u65e0\u884c\u4e1a\u677f\u5757",
        )
    rows = _fetch_boards_concurrent(industry_nodes, "\u884c\u4e1a", trade_date)
    if not rows:
        return CrawlResult(
            ok=False, source=SOURCE, error=SOURCE_UNAVAILABLE,
            user_message="Sina \u884c\u4e1a\u677f\u5757\u6570\u636e\u4e3a\u7a7a",
        )
    df = _normalize_board_df(pd.DataFrame(rows))
    cache.write_json_cache(cache_key, df.to_dict(orient="records"), SOURCE, trade_date=trade_date)
    return CrawlResult(
        ok=True, data=df, source=SOURCE, trade_date=trade_date,
        warnings=["Sina fallback: \u901a\u8fc7 Market_Center API \u91c7\u96c6\uff0c\u677f\u5757\u6da8\u8dcc\u5e45\u4e3a\u6210\u5206\u80a1\u5747\u503c\u8fd1\u4f3c"],
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
            stale.user_message = "Sina \u8282\u70b9\u6811\u83b7\u53d6\u5931\u8d25\uff0c\u8fd4\u56de\u7f13\u5b58"
            return stale
        return CrawlResult(
            ok=False, source=SOURCE, error=SOURCE_UNAVAILABLE,
            error_detail=str(exc), user_message="Sina \u8282\u70b9\u6811\u83b7\u53d6\u5931\u8d25",
        )
    if not concept_nodes:
        return CrawlResult(
            ok=False, source=SOURCE, error=SOURCE_UNAVAILABLE,
            user_message="Sina \u8282\u70b9\u6811\u4e2d\u65e0\u6982\u5ff5\u677f\u5757",
        )
    rows = _fetch_boards_concurrent(concept_nodes, "\u6982\u5ff5", trade_date)
    if not rows:
        return CrawlResult(
            ok=False, source=SOURCE, error=SOURCE_UNAVAILABLE,
            user_message="Sina \u6982\u5ff5\u677f\u5757\u6570\u636e\u4e3a\u7a7a",
        )
    df = _normalize_board_df(pd.DataFrame(rows))
    cache.write_json_cache(cache_key, df.to_dict(orient="records"), SOURCE, trade_date=trade_date)
    return CrawlResult(
        ok=True, data=df, source=SOURCE, trade_date=trade_date,
        warnings=["Sina fallback: \u901a\u8fc7 Market_Center API \u91c7\u96c6\uff0c\u677f\u5757\u6da8\u8dcc\u5e45\u4e3a\u6210\u5206\u80a1\u5747\u503c\u8fd1\u4f3c"],
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
            result.warnings.append(f"\u8fd4\u56de\u975e\u65b0\u9c9c\u7f13\u5b58: {result.error}")
    return result
