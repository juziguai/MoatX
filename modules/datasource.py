"""
datasource.py - Backward-compatibility layer (DEPRECATED)

QuoteManager / QuoteSource / SinaSource kept for backward compat.
New code: use DataSourceManager from modules.data_source_manager.
Real providers: modules.data_sources/ (TencentProvider, SinaProvider, etc.)
"""

from abc import ABC, abstractmethod
import logging
import warnings
import time
from dataclasses import dataclass
from typing import Literal

import requests

from modules.config import cfg
from modules.utils import to_full_code, to_sina_code

logger = logging.getLogger("moatx.datasource")
logger.setLevel(logging.WARNING)

DEFAULT_QUOTE_TOLERANCE_PCT = 0.15

@dataclass
class SourceHealth:
    source: str
    healthy: bool
    latency_ms: float = 0.0
    error: str = ""
    sample_count: int = 0



# DEPRECATED: use DataSource ABC from modules.data_source instead
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

    def health_check(self):
        t0 = time.time()
        try:
            result = self.fetch_quotes(["600519"])
            latency = (time.time() - t0) * 1000
            if result:
                return SourceHealth(
                    source=self.name, healthy=True,
                    latency_ms=round(latency, 1),
                    sample_count=len(result),
                )
            return SourceHealth(
                source=self.name, healthy=False,
                latency_ms=round(latency, 1),
                error="empty response",
            )
        except Exception as e:
            latency = (time.time() - t0) * 1000
            return SourceHealth(
                source=self.name, healthy=False,
                latency_ms=round(latency, 1),
                error=f"{type(e).__name__}: {e}",
            )

    def __repr__(self) -> str:
        return f"<{type(self).__name__}[{self.name}]>"


# DEPRECATED: use SinaProvider from modules.data_sources.sina instead
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
        warnings.warn(
            "QuoteManager is deprecated; use DataSourceManager instead.",
            DeprecationWarning, stacklevel=2,
        )
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

    def health_check_all(self):
        results = []
        for source in self._sources:
            try:
                h = source.health_check()
            except Exception as e:
                h = SourceHealth(
                    source=source.name, healthy=False,
                    error=f"{type(e).__name__}: {e}",
                )
            results.append(h)
            logger.info(
                "health_check %%s: healthy=%%s latency=%%.1fms error=%%s",
                h.source, h.healthy, h.latency_ms, h.error,
            )
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
) -> list:
    """Build quote sources from config, delegating to data_sources registry."""
    from modules.data_sources import get_provider

    names = (
        [str(item).strip().lower() for item in source_names if str(item).strip()]
        if source_names is not None
        else cfg().datasource.ordered_sources(mode=mode)
    )
    sources = []
    for name in names:
        p = get_provider(name)
        if p is not None:
            sources.append(p)
    return sources
