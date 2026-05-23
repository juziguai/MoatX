"""Unified fund flow data source with multi-source fallback.

Priority: EastMoney direct -> akshare optional fallback -> stale cache.
"""

from __future__ import annotations

import logging
import time

import pandas as pd
import requests

from modules.akshare_compat import import_akshare
from modules.config import cfg
from modules.utils import _parse_market, _strip_suffix
from . import cache
from .models import CrawlResult, SOURCE_UNAVAILABLE, EMPTY_RESPONSE

_logger = logging.getLogger(__name__)
ak = import_akshare()

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


def _fetch_eastmoney_individual_fund_flow(code: str, market: str) -> pd.DataFrame:
    """Fetch individual fund flow directly from EastMoney without akshare."""
    market_map = {"sh": 1, "sz": 0, "bj": 0}
    if market not in market_map:
        raise ValueError(f"unsupported market: {market}")

    session = requests.Session()
    session.trust_env = False
    session.proxies = {"http": None, "https": None}
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://quote.eastmoney.com/",
    }
    params = {
        "lmt": "0",
        "klt": "101",
        "secid": f"{market_map[market]}.{code}",
        "fields1": "f1,f2,f3,f7",
        "fields2": "f51,f52,f53,f54,f55,f56",
        "ut": "b2884a393a59ad64002292a3e90d46a5",
        "_": int(time.time() * 1000),
    }
    last_error: Exception | None = None
    payload = {}
    for url in (
        "https://push2his.eastmoney.com/api/qt/stock/fflow/kline/get",
        "https://push2.eastmoney.com/api/qt/stock/fflow/kline/get",
    ):
        try:
            resp = session.get(url, params=params, headers=headers, timeout=cfg().crawler.timeout)
            resp.raise_for_status()
            payload = resp.json()
            break
        except Exception as exc:
            last_error = exc
    else:
        raise RuntimeError(f"eastmoney fundflow endpoint unavailable: {last_error}") from last_error

    klines = (payload.get("data") or {}).get("klines") or []
    if not klines:
        return pd.DataFrame()

    rows = []
    for item in klines:
        parts = str(item).split(",")
        if len(parts) < 6:
            continue
        rows.append(
            {
                "date": parts[0],
                "main_net_inflow": _safe_float(parts[1]),
                "small_net": _safe_float(parts[2]),
                "medium_net": _safe_float(parts[3]),
                "large_net": _safe_float(parts[4]),
                "super_large_net": _safe_float(parts[5]),
            }
        )
    df = pd.DataFrame(rows)
    if df.empty:
        return df

    latest_pct = _fetch_eastmoney_latest_main_pct(session, code, market, headers)
    if latest_pct is not None:
        df.loc[df.index[-1], "main_net_inflow_pct"] = latest_pct

    return _normalize_fund_flow_frame(df)


def _fetch_eastmoney_latest_main_pct(
    session: requests.Session,
    code: str,
    market: str,
    headers: dict[str, str],
) -> float | None:
    """Fetch today's main-fund percentage from EastMoney quote fields."""
    market_map = {"sh": 1, "sz": 0, "bj": 0}
    params = {
        "secid": f"{market_map[market]}.{code}",
        "fields": "f57,f58,f184",
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
        "_": int(time.time() * 1000),
    }
    try:
        resp = session.get(
            "https://push2.eastmoney.com/api/qt/stock/get",
            params=params,
            headers=headers,
            timeout=cfg().crawler.timeout,
        )
        resp.raise_for_status()
        value = (resp.json().get("data") or {}).get("f184")
        return _safe_float(value)
    except Exception as exc:
        _logger.debug("eastmoney latest fund pct failed [%s]: %s", code, exc)
        return None


def _normalize_fund_flow_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Return fund-flow rows in newest-first order with stable columns."""
    out = df.rename(columns=_COLUMN_RENAME).copy()
    if "date" in out.columns:
        out["date"] = pd.to_datetime(out["date"], errors="coerce")
        out = out.dropna(subset=["date"]).sort_values("date", ascending=False)
        out["date"] = out["date"].dt.date.astype(str)

    columns = [
        "date",
        "close",
        "pct_change",
        "main_net_inflow",
        "main_net_inflow_pct",
        "super_large_net",
        "super_large_net_pct",
        "large_net",
        "large_net_pct",
        "medium_net",
        "medium_net_pct",
        "small_net",
        "small_net_pct",
    ]
    for column in columns:
        if column not in out.columns:
            out[column] = pd.NA
    for column in columns:
        if column != "date":
            out[column] = pd.to_numeric(out[column], errors="coerce")
    return out[columns].reset_index(drop=True)


def _fetch_fund_flow_with_optional_akshare(code: str, market: str) -> tuple[pd.DataFrame, str]:
    try:
        return _fetch_eastmoney_individual_fund_flow(code, market), "eastmoney_direct"
    except Exception as direct_exc:
        try:
            df = ak.stock_individual_fund_flow(stock=code, market=market)
            return _normalize_fund_flow_frame(df), "akshare"
        except Exception as ak_exc:
            raise RuntimeError(f"eastmoney_direct={direct_exc}; akshare={ak_exc}") from ak_exc


def get_individual_fund_flow(
    symbol: str,
    use_cache: bool = True,
    days: int = 120,
) -> CrawlResult:
    """Get individual stock fund flow data (latest `days` rows).

    Uses EastMoney direct first. Akshare is only an optional enhancement fallback.
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
        df, source = _fetch_fund_flow_with_optional_akshare(code, market)
    except Exception as exc:
        stale = cache.read_df_cache(cache_key, max_age_seconds=cfg().cache.fundflow_seconds)
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

    df = _normalize_fund_flow_frame(df)
    df = df.head(days)
    cache.write_df_cache(cache_key, df, source, trade_date=trade_date)
    return CrawlResult(ok=True, data=df, source=source, trade_date=trade_date)


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
