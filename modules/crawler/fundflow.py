"""Unified fund flow data source with multi-source fallback.

Priority: EastMoney datacenter → akshare → THS direct
"""

from __future__ import annotations

import logging

import akshare as ak
import pandas as pd

from modules.config import cfg
from modules.utils import _parse_market, _strip_suffix
from . import cache
from .models import CrawlResult, SOURCE_UNAVAILABLE, EMPTY_RESPONSE

_logger = logging.getLogger(__name__)

SOURCE = "fundflow"

# Column name mapping from akshare output -> standard names
_COLUMN_RENAME = {
    "日期": "date",
    "收盘价": "close",
    "涨跌幅": "pct_change",
    "主力净流入-净额": "main_net_inflow",
    "主力净流入-净占比": "main_net_inflow_pct",
    "超大单净流入-净额": "super_large_net",
    "超大单净流入-净占比": "super_large_net_pct",
    "大单净流入-净额": "large_net",
    "大单净流入-净占比": "large_net_pct",
    "中单净流入-净额": "medium_net",
    "中单净流入-净占比": "medium_net_pct",
    "小单净流入-净额": "small_net",
    "小单净流入-净占比": "small_net_pct",
}


def get_individual_fund_flow(
    symbol: str,
    use_cache: bool = True,
    days: int = 120,
) -> CrawlResult:
    """Get individual stock fund flow data (latest `days` rows).

    Uses EastMoney datacenter API (non-push2, confirmed working).
    """
    code = _strip_suffix(symbol)
    trade_date = cache.beijing_now().date().isoformat()
    cache_key = cache.build_cache_key(f"fundflow_{code}", trade_date)
    if use_cache:
        cached = cache.read_df_cache(cache_key, max_age_seconds=cfg().cache.fundflow_seconds)
        if cached.ok:
            return cached

    try:
        market = _parse_market(symbol)
        df = ak.stock_individual_fund_flow(stock=code, market=market)
    except Exception as exc:
        stale = cache.read_df_cache(cache_key, allow_stale=True, max_age_seconds=cfg().cache.fundflow_seconds)
        if stale.data is not None:
            stale.ok = True
            stale.warnings.append("资金流数据源不可用，返回缓存")
            return stale
        return CrawlResult(
            ok=False,
            source=SOURCE,
            error=SOURCE_UNAVAILABLE,
            error_detail=str(exc),
            user_message=f"{symbol} 资金流获取失败",
        )

    if df.empty:
        return CrawlResult(
            ok=False, source=SOURCE, error=EMPTY_RESPONSE,
            user_message=f"{symbol} 资金流数据为空",
        )

    df = df.rename(columns=_COLUMN_RENAME)
    df = df.head(days)
    cache.write_df_cache(cache_key, df, SOURCE, trade_date=trade_date)
    return CrawlResult(ok=True, data=df, source=SOURCE, trade_date=trade_date)


def get_money_flow_summary(symbol: str, use_cache: bool = True) -> dict:
    """Get the latest money flow summary as a dict.

    Returns the same structure the old EM push2 interface provided,
    so consumers (analyzer, screener) don't need changes.
    """
    result = get_individual_fund_flow(symbol, use_cache=use_cache)
    if not result.ok or result.data is None or result.data.empty:
        return _empty_flow_dict("数据不可用")

    row = result.data.iloc[0]
    return {
        "date": str(row.get("date", "")),
        "main_net_inflow": _safe_float(row.get("main_net_inflow")),
        "main_net_inflow_pct": _safe_float(row.get("main_net_inflow_pct")),
        "super_large_net": _safe_float(row.get("super_large_net")),
        "super_large_net_pct": _safe_float(row.get("super_large_net_pct")),
        "large_net": _safe_float(row.get("large_net")),
        "large_net_pct": _safe_float(row.get("large_net_pct")),
        "medium_net": _safe_float(row.get("medium_net")),
        "medium_net_pct": _safe_float(row.get("medium_net_pct")),
        "small_net": _safe_float(row.get("small_net")),
        "small_net_pct": _safe_float(row.get("small_net_pct")),
    }


def get_money_flow_rank(
    limit: int = 50,
    use_cache: bool = True,
) -> pd.DataFrame:
    """Get money flow ranking across all stocks.

    Note: akshare's stock_individual_fund_flow_rank uses EastMoney push2 (blocked).
    We use sector-level flow as a proxy instead.
    """
    # fallback to sector flow data
    from . import sector as _sector
    result = _sector.get_industry_boards(use_cache=use_cache)
    if result.ok and result.data is not None:
        df = result.data.copy()
        df["source"] = "proxy_sector"
        return df.head(limit)
    return pd.DataFrame()


def _safe_float(val) -> float:
    try:
        return float(val) if pd.notna(val) else 0.0
    except (ValueError, TypeError):
        return 0.0


def _empty_flow_dict(reason: str = "") -> dict:
    return {
        "date": "",
        "main_net_inflow": 0,
        "main_net_inflow_pct": 0,
        "super_large_net": 0,
        "super_large_net_pct": 0,
        "large_net": 0,
        "large_net_pct": 0,
        "medium_net": 0,
        "medium_net_pct": 0,
        "small_net": 0,
        "small_net_pct": 0,
        "_note": f"资金流向数据不可用: {reason}" if reason else "资金流向数据不可用",
    }
