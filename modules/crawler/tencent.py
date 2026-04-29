"""
tencent.py - 腾讯财经数据源
实时行情：qt.gtimg.cn（稳定、免代理、支持批量）
"""

from __future__ import annotations

import re

import requests

from modules.config import cfg
from modules.utils import to_tencent_code
from . import cache
from .models import CrawlResult, SOURCE_UNAVAILABLE, PARSE_ERROR

SOURCE = "tencent"
BASE_URL = "http://qt.gtimg.cn/q"

# Tencent 行情字段索引（0-indexed），已验证正确
# 参考: https://qt.gtimg.cn/q=sh600519
FIELDS = {
    "market": 0,        # 0=深交所 1=上交所
    "name": 1,          # 股票名称
    "code": 2,          # 股票代码
    "price": 3,         # 当前价
    "prev_close": 4,    # 昨收
    "open": 5,          # 今开
    "volume": 6,        # 成交量（手）
    "datetime": 30,     # 日期时间 YYYYMMDDHHMMSS
    "change": 31,       # 涨跌额
    "pct_change": 32,   # 涨跌幅
    "high": 33,         # 最高
    "low": 34,          # 最低
    "amount": 37,       # 成交额
    "turnover": 38,     # 换手率(%)
    "pe": 39,           # 动态市盈率
    "amplitude": 43,    # 振幅
    "limit_up": 47,     # 涨停价
    "limit_down": 48,   # 跌停价
}


def _build_query(symbols: str | list[str]) -> str:
    if isinstance(symbols, str):
        return to_tencent_code(symbols)
    return ",".join(to_tencent_code(s) for s in symbols)


def _parse_tencent_response(text: str) -> list[dict]:
    """解析腾讯行情响应文本"""
    results = []
    # 格式: v_sh600519="1~name~code~...";\n 或 v_sz000001="...";\n
    pattern = re.compile(r'v_[a-z]{2}\d+="([^"]+)"')
    for match in pattern.finditer(text):
        parts = match.group(1).split("~")
        if len(parts) < 48:
            continue

        row = {}
        for field, idx in FIELDS.items():
            val = parts[idx] if idx < len(parts) else ""
            row[field] = val

        # 类型转换
        for num_field in ["price", "prev_close", "open", "high", "low",
                          "change", "pct_change", "turnover", "pe",
                          "amplitude", "limit_up", "limit_down",
                          "amount"]:
            val = row.get(num_field, "")
            try:
                row[num_field] = float(val) if val not in ("", "-", "0.0000") else None
            except (ValueError, TypeError):
                row[num_field] = None

        # 成交量（手 → 股）
        try:
            row["volume"] = int(float(row["volume"]) * 100) if row["volume"] not in ("", "-") else 0
        except (ValueError, TypeError):
            row["volume"] = 0

        row["source"] = SOURCE
        results.append(row)

    return results


def fetch_quote(symbols: str | list[str], use_cache: bool = False) -> CrawlResult:
    """
    获取实时行情（腾讯财经）

    Args:
        symbols: 股票代码或代码列表，如 "600519" 或 ["600519", "000001"]
        use_cache: 使用缓存（默认关闭，实时行情不缓存）

    Returns:
        CrawlResult with data = list[dict] | dict（单只返回 dict）
    """
    single = isinstance(symbols, str)
    codes = [symbols] if single else symbols

    query = _build_query(codes)
    cache_key = f"tencent_quote_{query}"
    if use_cache:
        cached = cache.read_json_cache(cache_key, max_age_seconds=cfg().cache.tencent_quote_seconds)
        if cached.ok:
            return cached

    try:
        session = requests.Session()
        session.trust_env = False
        session.proxies = {"http": None, "https": None}
        # Tencent 接口不用特殊 UA
        r = session.get(BASE_URL, params={"q": query}, timeout=cfg().crawler.timeout)
        # Tencent 接口返回 GBK 编码，设为 gbk 正确解码中文名称
        r.encoding = "gbk"

        if r.status_code != 200:
            return CrawlResult(
                ok=False, source=SOURCE, error=SOURCE_UNAVAILABLE,
                error_detail=f"HTTP {r.status_code}",
                user_message="腾讯行情接口不可用",
            )

        rows = _parse_tencent_response(r.text)
        if not rows:
            return CrawlResult(
                ok=False, source=SOURCE, error=PARSE_ERROR,
                error_detail="empty result",
                user_message="腾讯行情未返回数据",
            )

        result = rows[0] if single else rows
        cache.write_json_cache(cache_key, result, SOURCE)
        return CrawlResult(ok=True, data=result, source=SOURCE)

    except requests.RequestException as exc:
        return CrawlResult(
            ok=False, source=SOURCE, error=SOURCE_UNAVAILABLE,
            error_detail=str(exc),
            user_message="腾讯行情接口请求失败",
        )


def fetch_quotes_batch(symbols: list[str], use_cache: bool = True) -> CrawlResult:
    """批量获取实时行情，返回 DataFrame"""
    return fetch_quote(symbols, use_cache=use_cache)
