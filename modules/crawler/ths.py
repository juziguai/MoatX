"""TongHuaShun crawler helpers.

MVP 阶段用于行业板块 fallback。该接口能提供行业板块涨跌幅、上涨/下跌家数和领涨股。
"""

from __future__ import annotations

import pandas as pd
import akshare as ak
import requests
from bs4 import BeautifulSoup
from io import StringIO
from .ths_fund_flow import get_hexin_v_header
from modules.config import cfg

from . import cache
from .models import CrawlResult, PARSE_ERROR, SOURCE_UNAVAILABLE


SOURCE = "ths"
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


def fetch_industry_boards(use_cache: bool = True) -> CrawlResult:
    trade_date = cache.beijing_now().date().isoformat()
    cache_key = cache.build_cache_key("sector_industry_ths", trade_date)
    if use_cache:
        cached = _read_cache_as_df(cache_key, max_age_seconds=cfg().cache.board_seconds)
        if cached.ok:
            return cached

    try:
        df = ak.stock_board_industry_summary_ths()
        normalized = _normalize_ths_industry(df, trade_date=trade_date)
        cache.write_df_cache(cache_key, normalized, SOURCE, trade_date=trade_date)
        return CrawlResult(ok=True, data=normalized, source=SOURCE, trade_date=trade_date)
    except Exception as exc:
        stale = _read_cache_as_df(cache_key, allow_stale=True, max_age_seconds=cfg().cache.board_seconds)
        if stale.data is not None:
            stale.ok = True
            stale.warnings.append("THS 不可用，返回缓存快照")
            stale.user_message = "THS 不可用，已返回缓存快照"
            return stale
        return CrawlResult(
            ok=False,
            source=SOURCE,
            error=PARSE_ERROR if "columns" in str(exc).lower() else SOURCE_UNAVAILABLE,
            error_detail=str(exc),
            user_message="同花顺行业板块不可用",
        )


def fetch_concept_boards(use_cache: bool = True) -> CrawlResult:
    trade_date = cache.beijing_now().date().isoformat()
    cache_key = cache.build_cache_key("sector_concept_ths", trade_date)
    if use_cache:
        cached = _read_cache_as_df(cache_key, max_age_seconds=cfg().cache.board_seconds)
        if cached.ok:
            return cached

    try:
        df = _fetch_ths_concept_fund_flow()
        normalized = _normalize_ths_concept(df, trade_date=trade_date)
        cache.write_df_cache(cache_key, normalized, SOURCE, trade_date=trade_date)
        return CrawlResult(ok=True, data=normalized, source=SOURCE, trade_date=trade_date)
    except Exception as exc:
        stale = _read_cache_as_df(cache_key, allow_stale=True, max_age_seconds=cfg().cache.board_seconds)
        if stale.data is not None:
            stale.ok = True
            stale.warnings.append("THS 概念资金流不可用，返回缓存快照")
            stale.user_message = "THS 概念资金流不可用，已返回缓存快照"
            return stale
        return CrawlResult(
            ok=False,
            source=SOURCE,
            error=PARSE_ERROR if "columns" in str(exc).lower() else SOURCE_UNAVAILABLE,
            error_detail=str(exc),
            user_message="同花顺概念板块不可用",
        )


def _normalize_ths_industry(df: pd.DataFrame, trade_date: str) -> pd.DataFrame:
    rename_map = {
        "板块": "sector",
        "涨跌幅": "pct_change",
        "上涨家数": "rise_count",
        "下跌家数": "fall_count",
        "领涨股": "top_stock",
        "领涨股-涨跌幅": "top_stock_pct",
        "均价": "price",
    }
    out = df.rename(columns=rename_map).copy()
    out["sector_type"] = "行业"
    out["source"] = SOURCE
    out["trade_date"] = trade_date
    return _normalize_board_df(out)


def _normalize_ths_concept(df: pd.DataFrame, trade_date: str) -> pd.DataFrame:
    rename_map = {
        "行业": "sector",
        "行业指数": "price",
        "涨跌幅": "pct_change",
        "公司家数": "rise_count",
        "领涨股": "top_stock",
        "涨跌幅.1": "top_stock_pct",
        "当前价(元)": "top_stock_price",
    }
    out = df.rename(columns=rename_map).copy()
    out["sector_type"] = "概念"
    out["source"] = SOURCE
    out["trade_date"] = trade_date
    return _normalize_board_df(out)


def _fetch_ths_concept_fund_flow() -> pd.DataFrame:
    session = requests.Session()
    session.trust_env = False
    headers = _ths_headers()
    first_url = "http://data.10jqka.com.cn/funds/gnzjl/field/tradezdf/order/desc/ajax/1/free/1/"
    first = session.get(first_url, headers=headers, timeout=cfg().crawler.timeout)
    first.raise_for_status()
    soup = BeautifulSoup(first.text, features="lxml")
    page_info = soup.find(name="span", attrs={"class": "page_info"})
    page_num = int(page_info.text.split("/")[1]) if page_info else 1

    frames = []
    page_url = "http://data.10jqka.com.cn/funds/gnzjl/field/tradezdf/order/desc/page/{}/ajax/1/free/1/"
    for page in range(1, page_num + 1):
        response = session.get(page_url.format(page), headers=headers, timeout=10)
        response.raise_for_status()
        frames.append(pd.read_html(StringIO(response.text))[0])
    return pd.concat(frames, ignore_index=True)


def _ths_headers() -> dict[str, str]:
    v_code = get_hexin_v_header()
    return {
        "Accept": "text/html, */*; q=0.01",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "hexin-v": v_code,
        "Host": "data.10jqka.com.cn",
        "Pragma": "no-cache",
        "Referer": "http://data.10jqka.com.cn/funds/gnzjl/",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/90.0.4430.85 Safari/537.36"
        ),
        "X-Requested-With": "XMLHttpRequest",
    }


def _normalize_board_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for column in STANDARD_COLUMNS:
        if column not in out.columns:
            out[column] = pd.NA
    for column in ["pct_change", "price", "turnover", "rise_count", "fall_count", "top_stock_pct"]:
        if column in ["pct_change", "top_stock_pct"]:
            out[column] = out[column].astype(str).str.replace("%", "", regex=False).str.replace("+", "", regex=False)
        out[column] = pd.to_numeric(out[column], errors="coerce")
    return out[STANDARD_COLUMNS]


def _read_cache_as_df(key: str, allow_stale: bool = False, max_age_seconds: int | None = None) -> CrawlResult:
    result = cache.read_df_cache(key, max_age_seconds=max_age_seconds)
    if result.ok or (allow_stale and result.data is not None):
        if result.data is not None and not result.data.empty:
            result.data = _normalize_board_df(result.data)
        result.source = result.source or SOURCE
        if allow_stale and not result.ok and result.error:
            result.warnings.append(f"返回非新鲜缓存: {result.error}")
    return result
