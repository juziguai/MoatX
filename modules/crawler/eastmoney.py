"""
eastmoney.py - 东方财富数据源（仅保留非 push2 通道）

可用接口：
  datacenter-web.eastmoney.com  → 财务/F10 数据
  emweb.securities.eastmoney.com → F10 页面

不可用（push2 全线被封）：
  push2.eastmoney.com           → 实时行情/板块列表
  push2his.eastmoney.com        → 历史 K 线
"""

from __future__ import annotations

import requests

from modules.config import cfg
from modules.utils import _strip_suffix
from . import cache
from .models import CrawlResult, SOURCE_UNAVAILABLE, EMPTY_RESPONSE

SOURCE = "eastmoney"
DATACENTER_HOST = "datacenter-web.eastmoney.com"


def fetch_stock_info(symbol: str, use_cache: bool = True) -> CrawlResult:
    """获取个股基本信息（东方财富 F10，走 datacenter API，不走 push2）"""
    code = _strip_suffix(symbol)
    cache_key = f"stock_info_{code}"
    if use_cache:
        cached = cache.read_json_cache(cache_key, max_age_seconds=cfg().cache.f10_seconds)
        if cached.ok:
            return cached

    try:
        session = requests.Session()
        session.trust_env = False
        session.proxies = {"http": None, "https": None}
        session.headers.update({"User-Agent": "Mozilla/5.0"})

        r = session.get(
            f"https://{DATACENTER_HOST}/api/data/v1/get",
            params={
                "reportName": "RPT_F10_FINANCE_MAINFINADATA",
                "columns": "ALL",
                "filter": f'(SECURITY_CODE="{code}")',
                "pageSize": 1,
            },
            timeout=cfg().crawler.timeout,
        )
        if r.status_code != 200:
            return CrawlResult(ok=False, source=SOURCE, error=SOURCE_UNAVAILABLE,
                               error_detail=f"HTTP {r.status_code}",
                               user_message="东方财富数据中心不可用")

        payload = r.json()
        rows = payload.get("result", {}).get("data", [])
        if not rows:
            return CrawlResult(ok=False, source=SOURCE, error=EMPTY_RESPONSE,
                               user_message=f"{symbol} 无数据中心数据")

        info = rows[0]
        cache.write_json_cache(cache_key, info, SOURCE)
        return CrawlResult(ok=True, data=info, source=SOURCE)

    except Exception as exc:
        return CrawlResult(ok=False, source=SOURCE, error=SOURCE_UNAVAILABLE,
                           error_detail=str(exc),
                           user_message=f"{symbol} 个股信息获取失败")
