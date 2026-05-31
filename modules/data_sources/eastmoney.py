"""EastMoney data provider — quotes + fund flow + valuation.

API: push2.eastmoney.com
Capabilities: QUOTE, FUND_FLOW, STOCK_INFO
"""

from __future__ import annotations

from modules.config import cfg
from modules.data_source import Capability, DataSource
from modules.result import Result
from modules.utils import to_eastmoney_secid


class EastMoneyProvider(DataSource):
    """EastMoney — quote + fund flow provider."""

    @property
    def name(self) -> str:
        return "eastmoney"

    def capabilities(self) -> set[Capability]:
        return {Capability.QUOTE, Capability.FUND_FLOW, Capability.STOCK_INFO}

    def fetch(self, capability: Capability, **params):
        import time
        t0 = time.time()

        try:
            if capability == Capability.QUOTE:
                return self._fetch_quotes(params.get("symbols", []), t0)
            elif capability == Capability.FUND_FLOW:
                return self._fetch_fund_flow(params.get("symbol", ""), params.get("days", 5), t0)
            elif capability == Capability.STOCK_INFO:
                return self._fetch_stock_info(params.get("symbol", ""), t0)
            else:
                return Result.fail(f"unsupported: {capability}", source=self.name)
        except Exception as exc:
            return Result.fail(str(exc), source=self.name, elapsed_ms=(time.time() - t0) * 1000)

    def _fetch_quotes(self, symbols: list[str], t0: float) -> Result[dict]:
        if not symbols:
            return Result.ok({}, source=self.name)

        import requests
        secids = [to_eastmoney_secid(s) for s in symbols]
        session = requests.Session()
        session.trust_env = False
        session.proxies = {"http": None, "https": None}
        r = session.get(
            "http://push2.eastmoney.com/api/qt/ulist.np/get",
            params={"fltt": 2, "secids": ",".join(secids), "fields": "f2,f3,f4,f12,f14,f15,f16,f17,f18"},
            timeout=cfg().crawler.timeout,
        )
        if r.status_code != 200:
            return Result.fail(f"HTTP {r.status_code}", source=self.name)

        data = r.json()
        items = data.get("data", {}).get("diff", [])
        out = {}
        for item in items:
            code = str(item.get("f12", ""))
            market = {0: "SZ", 1: "SH"}.get(item.get("f13", 1), "SZ")
            full_code = f"{code}.{market}"
            out[full_code] = {
                "code": full_code, "name": item.get("f14", ""),
                "price": item.get("f2"), "change_pct": item.get("f3"),
                "volume": item.get("f5"), "prev_close": item.get("f18"),
                "high": item.get("f15"), "low": item.get("f16"),
                "open": item.get("f17"), "amount": item.get("f6"),
            }
        return Result.ok(out, source=self.name, elapsed_ms=(time.time() - t0) * 1000)

    def _fetch_fund_flow(self, symbol: str, days: int, t0: float) -> Result:
        from modules.crawler import fundflow
        result = fundflow.get_individual_fund_flow(symbol, use_cache=False, days=days)
        if result.ok:
            return Result.ok(result.data, source="eastmoney", elapsed_ms=(time.time() - t0) * 1000)
        return Result.fail(result.error, source="eastmoney")

    def _fetch_stock_info(self, symbol: str, t0: float) -> Result[dict]:
        from modules.crawler.eastmoney import fetch_stock_info
        result = fetch_stock_info(symbol, use_cache=True)
        if result.ok and result.data:
            return Result.ok(result.data, source="eastmoney", elapsed_ms=(time.time() - t0) * 1000)
        return Result.fail("no data", source="eastmoney")
