"""THS (TongHuaShun) data provider — board data via akshare.

Capabilities: BOARD_INDUSTRY, BOARD_CONCEPT, PROFIT_FORECAST, CASH_FLOW
"""

from __future__ import annotations

from modules.data_source import Capability, DataSource
from modules.result import Result


class THSProvider(DataSource):
    """TongHuaShun — board + financial data."""

    @property
    def name(self) -> str:
        return "ths"

    def capabilities(self) -> set[Capability]:
        return {Capability.BOARD_INDUSTRY, Capability.BOARD_CONCEPT,
                Capability.PROFIT_FORECAST, Capability.CASH_FLOW}

    def fetch(self, capability: Capability, **params):
        import time
        t0 = time.time()
        try:
            if capability in (Capability.BOARD_INDUSTRY, Capability.BOARD_CONCEPT):
                from modules.crawler import ths
                fetch = ths.fetch_industry_boards if capability == Capability.BOARD_INDUSTRY else ths.fetch_concept_boards
                result = fetch(use_cache=params.get("use_cache", True))
                if result.ok:
                    return Result.ok(result.data, source=self.name, elapsed_ms=(time.time()-t0)*1000,
                                     warnings=result.warnings)
                return Result.fail(result.error or "no data", source=self.name)
            else:
                return Result.fail(f"unsupported: {capability}", source=self.name)
        except Exception as exc:
            return Result.fail(str(exc), source=self.name, elapsed_ms=(time.time()-t0)*1000)
