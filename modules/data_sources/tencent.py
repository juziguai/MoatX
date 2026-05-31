"""Tencent data provider — real-time quotes.

API: qt.gtimg.cn
Capabilities: QUOTE
"""

from __future__ import annotations

import time

from modules.data_source import Capability, DataSource
from modules.result import Result
from modules.utils import to_full_code


class TencentProvider(DataSource):
    """Tencent Finance — primary quote source."""

    @property
    def name(self) -> str:
        return "tencent"

    def capabilities(self) -> set[Capability]:
        return {Capability.QUOTE, Capability.INDEX_QUOTE}

    def fetch(self, capability: Capability, **params):
        t0 = time.time()
        if capability == Capability.INDEX_QUOTE:
            return self._fetch_indices(params.get("codes", []), t0)
        if capability == Capability.QUOTE:
            return self._fetch_quotes(params.get("symbols", []))
        return Result.fail(f"unsupported: {capability}", source=self.name)

    def _fetch_quotes(self, symbols: list[str]) -> Result[dict]:
        import time
        t0 = time.time()

        if not symbols:
            return Result.ok({}, source=self.name)

        try:
            from modules.crawler.tencent import fetch_quotes_batch
            result = fetch_quotes_batch(symbols, use_cache=False)
            if not result.ok or not result.data:
                return Result.fail("no data", source=self.name)

            rows = result.data if isinstance(result.data, list) else [result.data]
            out = {}
            for q in rows:
                code = str(q.get("code", ""))
                if not code:
                    continue
                full_code = to_full_code(code)
                out[full_code] = {
                    "code": full_code, "name": q.get("name", ""),
                    "price": q.get("price"), "change_pct": q.get("change_pct"),
                    "volume": q.get("volume"), "prev_close": q.get("prev_close"),
                    "high": q.get("high"), "low": q.get("low"),
                    "open": q.get("open"), "amount": q.get("amount"),
                    "turnover": q.get("turnover"), "pe": q.get("pe"),
                }

            return Result.ok(out, source=self.name, elapsed_ms=(time.time() - t0) * 1000)
        except Exception as exc:
            return Result.fail(str(exc), source=self.name, elapsed_ms=(time.time() - t0) * 1000)
    def _fetch_indices(self, codes, t0):
        """Fetch index quotes from Tencent."""
        from modules.market_index import fetch_tencent_indices
        try:
            data = fetch_tencent_indices(codes)
            return Result.ok(data, source=self.name, elapsed_ms=(time.time() - t0) * 1000)
        except Exception as exc:
            return Result.fail(str(exc), source=self.name, elapsed_ms=(time.time() - t0) * 1000)


