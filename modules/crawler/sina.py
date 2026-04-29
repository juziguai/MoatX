"""Sina crawler helpers.

MVP 阶段只实现行业板块 fallback，不承诺概念板块覆盖。
"""

from __future__ import annotations

import pandas as pd

from . import cache
from .base import CrawlerClient
from .models import CrawlResult, PARSE_ERROR


SOURCE = "sina"
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
    cache_key = cache.build_cache_key("sector_industry_sina", trade_date)
    if use_cache:
        cached = _read_cache_as_df(cache_key)
        if cached.ok:
            return cached

    client = CrawlerClient()
    result = client.get_text(
        "https://vip.stock.finance.sina.com.cn/q/go.php/vIndustryRank/kind/industry/index.phtml",
        params={"p": 1},
        source=SOURCE,
        host_key=f"{SOURCE}:industry",
    )
    if not result.ok:
        stale = _read_cache_as_df(cache_key, allow_stale=True)
        if stale.data is not None:
            stale.ok = True
            stale.warnings.append("Sina 不可用，返回过期缓存")
            stale.user_message = "Sina 不可用，已返回缓存快照"
            return stale
        return result

    parsed = _parse_html(result.data, trade_date=trade_date)
    if parsed.ok:
        cache.write_json_cache(cache_key, parsed.data.to_dict(orient="records"), SOURCE, trade_date=trade_date)
    return parsed


def _parse_html(html: str, trade_date: str) -> CrawlResult:
    try:
        if "__ERROR" in html or "Invalid view" in html:
            raise ValueError(html[:200])
        tables = pd.read_html(html)
        if not tables:
            raise ValueError("no html table found")
        df = _pick_table(tables)
        if df.empty:
            raise ValueError("industry table is empty")
        normalized = _normalize_sina_table(df, trade_date=trade_date)
        return CrawlResult(
            ok=True,
            data=normalized,
            source=SOURCE,
            trade_date=trade_date,
            warnings=["Sina fallback 字段覆盖不完整"],
        )
    except Exception as exc:
        return CrawlResult(
            ok=False,
            source=SOURCE,
            error=PARSE_ERROR,
            error_detail=str(exc),
            user_message="Sina 行业板块响应解析失败",
        )


def _pick_table(tables: list[pd.DataFrame]) -> pd.DataFrame:
    for table in tables:
        columns = {str(col) for col in table.columns}
        if any("涨跌幅" in col or "涨幅" in col for col in columns):
            return table
    return tables[0]


def _normalize_sina_table(df: pd.DataFrame, trade_date: str) -> pd.DataFrame:
    out = pd.DataFrame()
    columns = {str(col): col for col in df.columns}
    sector_col = _first_matching(columns, ["行业", "板块", "名称"])
    pct_col = _first_matching(columns, ["涨跌幅", "涨幅"])
    if sector_col is None or pct_col is None:
        raise ValueError(f"missing sector or pct column: {list(df.columns)}")
    out["sector"] = df[sector_col].astype(str).str.strip()
    out["pct_change"] = df[pct_col].map(_parse_pct)
    out["sector_type"] = "行业"
    out["source"] = SOURCE
    out["trade_date"] = trade_date
    return _normalize_board_df(out)


def _first_matching(columns: dict[str, object], patterns: list[str]) -> str | None:
    for name, original in columns.items():
        if any(pattern in name for pattern in patterns):
            return str(original)
    return None


def _parse_pct(value) -> float:
    if pd.isna(value):
        return float("nan")
    text = str(value).replace("%", "").replace("+", "").strip()
    return pd.to_numeric(text, errors="coerce")


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
