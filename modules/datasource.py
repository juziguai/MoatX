"""
datasource.py - 数据源抽象层

定义统一查询接口，业务层通过 QuoteManager 查询实时行情。
当前支持的实时行情数据源：Tencent / EastMoney / Sina。
默认行为为多源查询、交叉校验、聚合输出；单源失败时自动降级为可用源。
"""

from abc import ABC, abstractmethod
import logging
from typing import Literal

import requests

from modules.config import cfg
from modules.utils import to_eastmoney_secid, to_full_code, to_sina_code

logger = logging.getLogger("moatx.datasource")
logger.setLevel(logging.WARNING)

DEFAULT_QUOTE_TOLERANCE_PCT = 0.15


class QuoteSource(ABC):
    """实时行情数据源基类"""

    @property
    @abstractmethod
    def name(self) -> str:
        """数据源标识"""

    @abstractmethod
    def fetch_quotes(self, symbols: list[str]) -> dict[str, dict]:
        """
        批量查询实时行情。

        Args:
            symbols: 裸股票代码列表，如 ["600519", "000858"]

        Returns:
            {full_code: {code, name, price, change_pct, volume, prev_close, high, low, open, amount, turnover, pe}}
            失败或空返回 {}
        """

    def __repr__(self) -> str:
        return f"<{type(self).__name__}[{self.name}]>"


class TencentSource(QuoteSource):
    """腾讯财经 —— qt.gtimg.cn"""

    @property
    def name(self) -> str:
        return "tencent"

    def fetch_quotes(self, symbols: list[str]) -> dict[str, dict]:
        if not symbols:
            return {}
        try:
            from modules.crawler.tencent import fetch_quotes_batch
            result = fetch_quotes_batch(symbols, use_cache=False)
            if not result.ok or not result.data:
                return {}
            rows = result.data if isinstance(result.data, list) else [result.data]
            out = {}
            for q in rows:
                code = str(q.get("code", ""))
                if not code:
                    continue
                full_code = to_full_code(code)
                out[full_code] = {
                    "code": full_code,
                    "name": q.get("name", ""),
                    "price": q.get("price", 0) or 0,
                    "change_pct": q.get("pct_change", 0) or 0,
                    "volume": int(q.get("volume", 0) or 0),
                    "amount": float(q.get("amount", 0) or 0),
                    "turnover": q.get("turnover", 0) or 0,
                    "pe": q.get("pe"),
                    "high": q.get("high", 0) or 0,
                    "low": q.get("low", 0) or 0,
                    "open": q.get("open", 0) or 0,
                    "prev_close": q.get("prev_close", 0) or 0,
                }
            return out
        except Exception as e:
            logger.warning("TencentSource: %s", e)
            return {}


class EastMoneySource(QuoteSource):
    """东方财富 —— push2.eastmoney.com"""

    @property
    def name(self) -> str:
        return "eastmoney"

    def fetch_quotes(self, symbols: list[str]) -> dict[str, dict]:
        if not symbols:
            return {}
        secids = [to_eastmoney_secid(s) for s in symbols]
        try:
            session = requests.Session()
            session.trust_env = False
            session.proxies = {"http": None, "https": None}
            r = session.get(
                "http://push2.eastmoney.com/api/qt/ulist.np/get",
                params={
                    "fltt": 2,
                    "secids": ",".join(secids),
                    "fields": "f2,f3,f4,f12,f14,f15,f16,f17,f18",
                },
                timeout=cfg().crawler.timeout,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            if r.status_code != 200:
                return {}
            items = (r.json().get("data") or {}).get("diff")
            if not items:
                return {}
            out = {}
            for item in items:
                code = str(item.get("f12", ""))
                if not code:
                    continue
                full_code = to_full_code(code)
                price = item.get("f2") or 0
                pct = item.get("f3") or 0
                prev_close = round(price / (1 + pct / 100), 2) if abs(pct) > 0.001 else price
                out[full_code] = {
                    "code": full_code,
                    "name": item.get("f14", ""),
                    "price": price,
                    "change_pct": pct,
                    "volume": int(item.get("f17") or 0) * 100 if item.get("f17") else 0,
                    "prev_close": prev_close,
                    "high": item.get("f15") or 0,
                    "low": item.get("f16") or 0,
                    "open": item.get("f18") or 0,
                    "amount": 0,
                    "turnover": 0,
                    "pe": None,
                }
            return out
        except Exception as e:
            logger.warning("EastMoneySource: %s", e)
            return {}


class SinaSource(QuoteSource):
    """新浪财经 —— hq.sinajs.cn"""

    @property
    def name(self) -> str:
        return "sina"

    def fetch_quotes(self, symbols: list[str]) -> dict[str, dict]:
        if not symbols:
            return {}
        sina_codes = [to_sina_code(s) for s in symbols]
        try:
            session = requests.Session()
            session.trust_env = False
            session.proxies = {"http": None, "https": None}
            r = session.get(
                f"http://hq.sinajs.cn/list={','.join(sina_codes)}",
                timeout=cfg().crawler.timeout,
                headers={"Referer": "https://finance.sina.com.cn"},
            )
            if r.status_code != 200 or not r.text:
                return {}
            out = {}
            for line in r.text.strip().split("\n"):
                if '="' not in line:
                    continue
                parts = line.split('="')[1].strip('";\r\n').split(",")
                if len(parts) < 32:
                    continue
                raw = line.split("hq_str_", 1)[1].split("=", 1)[0]
                code = raw[2:]
                full_code = to_full_code(code)
                name = parts[0]
                price = float(parts[3]) if parts[3] else 0
                prev_close = float(parts[2]) if parts[2] else price
                pct = round((price - prev_close) / prev_close * 100, 2) if prev_close else 0
                out[full_code] = {
                    "code": full_code,
                    "name": name,
                    "price": price,
                    "change_pct": pct,
                    "volume": int(parts[8]) if len(parts) > 8 and parts[8] else 0,
                    "prev_close": prev_close,
                    "high": float(parts[4]) if len(parts) > 4 and parts[4] else 0,
                    "low": float(parts[5]) if len(parts) > 5 and parts[5] else 0,
                    "open": float(parts[1]) if len(parts) > 1 and parts[1] else 0,
                    "amount": float(parts[9]) if len(parts) > 9 and parts[9] else 0,
                    "turnover": 0,
                    "pe": None,
                }
            return out
        except Exception as e:
            logger.warning("SinaSource: %s", e)
            return {}


# ─── 数据源管理器 ───────────────────────────────────────────────


class QuoteManager:
    """
    数据源管理器 — 多源查询、交叉校验、聚合输出。

    用法:
        mgr = QuoteManager()
        quotes = mgr.fetch_quotes(["600519", "000858", "515230"])
    """

    def __init__(
        self,
        sources: list[QuoteSource] | None = None,
        source_names: list[str] | None = None,
        mode: Literal["single", "validate"] | None = None,
        tolerance_pct: float = DEFAULT_QUOTE_TOLERANCE_PCT,
    ):
        self._sources = sources or _build_sources_from_config(source_names=source_names, mode=mode)
        self._tolerance_pct = tolerance_pct

    @property
    def sources(self) -> list[QuoteSource]:
        return list(self._sources)

    def fetch_quotes(self, symbols: list[str]) -> dict[str, dict]:
        """
        查询所有可用数据源，按股票聚合并生成校验状态。

        返回字段兼容旧协议，并额外附加：
        - source: 主采用数据源
        - sources: 命中的数据源列表
        - validation_status: verified / diverged / single_source
        - source_quotes: 各源原始归一化结果
        - max_price_diff / max_pct_diff / warning
        """
        if not symbols:
            return {}

        requested_codes = [to_full_code(symbol) for symbol in symbols]
        source_results: dict[str, dict[str, dict]] = {}
        for source in self._sources:
            try:
                data = source.fetch_quotes(symbols)
                if data:
                    source_results[source.name] = data
            except Exception as e:
                logger.warning("QuoteManager: %s 异常 %s", source.name, e)

        results: dict[str, dict] = {}
        for full_code in requested_codes:
            quotes = []
            for source in self._sources:
                source_data = source_results.get(source.name, {})
                quote = source_data.get(full_code)
                if quote:
                    q = dict(quote)
                    q["source"] = source.name
                    quotes.append(q)
            if quotes:
                results[full_code] = self._aggregate_quote(full_code, quotes)
        return results

    def _aggregate_quote(self, full_code: str, quotes: list[dict]) -> dict:
        """Aggregate one stock quote from multiple normalized source rows."""
        quotes = [q for q in quotes if float(q.get("price") or 0) > 0]
        if not quotes:
            return {}

        primary = quotes[0]
        max_price_diff = 0.0
        max_pct_diff = 0.0
        for quote in quotes[1:]:
            max_price_diff = max(max_price_diff, abs(float(primary.get("price") or 0) - float(quote.get("price") or 0)))
            max_pct_diff = max(max_pct_diff, abs(float(primary.get("change_pct") or 0) - float(quote.get("change_pct") or 0)))

        status = "single_source" if len(quotes) == 1 else "verified"
        warning = ""
        if len(quotes) >= 2 and max_pct_diff > self._tolerance_pct:
            status = "diverged"
            warning = f"多源涨跌幅差异 {max_pct_diff:.2f}%，超过阈值 {self._tolerance_pct:.2f}%"

        result = dict(primary)
        result.update({
            "code": full_code,
            "source": primary.get("source", ""),
            "sources": [q.get("source", "") for q in quotes if q.get("source")],
            "validation_status": status,
            "source_quotes": quotes,
            "max_price_diff": round(max_price_diff, 4),
            "max_pct_diff": round(max_pct_diff, 4),
            "warning": warning,
        })
        return result


def _build_sources_from_config(
    source_names: list[str] | None = None,
    mode: Literal["single", "validate"] | None = None,
) -> list[QuoteSource]:
    """Build quote sources from cfg().datasource, preserving configured priority."""
    registry: dict[str, type[QuoteSource]] = {
        "sina": SinaSource,
        "tencent": TencentSource,
        "eastmoney": EastMoneySource,
    }
    sources: list[QuoteSource] = []
    names = (
        [str(item).strip().lower() for item in source_names if str(item).strip()]
        if source_names is not None
        else cfg().datasource.ordered_sources(mode=mode)
    )
    for name in names:
        source_cls = registry.get(name)
        if source_cls is not None:
            sources.append(source_cls())
    return sources or [SinaSource(), TencentSource(), EastMoneySource()]
