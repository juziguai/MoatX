"""Unified DataSourceManager — single entry point for all data operations.

Replaces QuoteManager + BoardManager with one config-driven manager.
"""

from __future__ import annotations

from typing import Any

from modules.data_source import Capability
from modules.fallback_policy import FallbackPolicy
from modules.result import Result


class DataSourceManager:
    """Unified data source manager with fallback chains and cross-validation.

    Usage:
        mgr = DataSourceManager()
        quotes = mgr.fetch_quotes(["600519", "000001"])  # auto chain + validate
        boards = mgr.fetch_boards("industry")             # config-driven fallback
        fin = mgr.fetch(Capability.DIVIDEND, symbol="600519")
        health = mgr.health_all()
    """

    def __init__(self, policy: FallbackPolicy | None = None):
        self._policy = policy or FallbackPolicy.from_config()
        self._providers: dict[str, Any] = {}

    def _get(self, name: str):
        if name not in self._providers:
            from modules.data_sources import get_provider
            p = get_provider(name)
            if p is None and name == "local":
                from modules.crawler.board_sources import LocalSectorBoardSource
                p = LocalSectorBoardSource()
            self._providers[name] = p
        return self._providers[name]

    # ─── Quotes ──────────────────────────────────────

    def fetch_quotes(self, symbols: list[str], mode: str = "validate",
                     tolerance_pct: float = 2.0,
                     source_names: list[str] | None = None) -> dict[str, dict]:
        """Fetch quotes with fallback and cross-validation.

        Args:
            symbols: bare codes like ["600519", "000001"]
            mode: "validate" (cross-check) or "single" (first success)
            tolerance_pct: max price diff % for "verified" status
        """
        if not symbols:
            return {}

        if source_names:
            chain = list(source_names)
        else:
            chain = self._policy.chain_for("quote")
        source_results: dict[str, dict] = {}
        unresolved = set(symbols)

        single_mode = (mode == "single")

        for name in chain:
            if single_mode and not unresolved:
                break
            p = self._get(name)
            if p is None:
                continue
            try:
                r = p.fetch(Capability.QUOTE, symbols=list(unresolved))
                if r.ok and r.data:
                    source_results[name] = r.data
                    resolved = set(r.data.keys())
                    # Normalize: strip market suffix for matching
                    resolved_bare = {k.split(".")[0] if "." in k else k for k in resolved}
                    unresolved -= resolved_bare
            except Exception:
                continue

        if single_mode or len(source_results) <= 1:
            # Return first source's results
            for name in chain:
                if name in source_results:
                    return self._annotate(source_results[name], name, [], "single_source")

        # Cross-validate
        return self._cross_validate(source_results, chain, tolerance_pct)

    def _cross_validate(self, source_results: dict, chain: list, tol: float) -> dict:

        merged: dict[str, dict] = {}
        all_codes = set()
        for data in source_results.values():
            all_codes.update(data.keys())

        primary_name = chain[0]

        for full_code in all_codes:
            quotes = {}
            for name in chain:
                if name in source_results and full_code in source_results[name]:
                    quotes[name] = source_results[name][full_code]

            if not quotes:
                continue

            primary_q = quotes.get(primary_name)
            if primary_q is None:
                primary_q = next(iter(quotes.values()))

            entry = dict(primary_q)
            entry["source"] = primary_name
            entry["sources"] = list(quotes.keys())

            if len(quotes) >= 2:
                prices = [q.get("price") for q in quotes.values() if q.get("price") is not None]
                if prices and max(prices) > 0:
                    max_diff = (max(prices) - min(prices)) / min(prices) * 100
                    pcts = [q.get("change_pct") for q in quotes.values() if q.get("change_pct") is not None]
                    if pcts:
                        max_pct_diff = abs(max(pcts) - min(pcts))
                        entry["max_pct_diff"] = round(max_pct_diff, 4)
                    if max_diff <= tol:
                        entry["validation_status"] = "verified"
                        entry["max_price_diff"] = round(max_diff, 2)
                    else:
                        entry["validation_status"] = "diverged"
                        entry["max_price_diff"] = round(max_diff, 2)
                        entry["warning"] = "多源涨跌幅差异超过阈值"
                else:
                    entry["validation_status"] = "verified"
            else:
                entry["validation_status"] = "single_source"

            merged[full_code] = entry

        return merged

    def _annotate(self, data: dict, source: str, warnings: list, status: str) -> dict:
        for v in data.values():
            v["source"] = source
            v["sources"] = [source]
            v["validation_status"] = status
        return data

    # ─── Boards ──────────────────────────────────────

    def fetch_boards(self, board_type: str = "industry", use_cache: bool = True) -> Result:
        """Fetch boards with fallback chain from config.

        Args:
            board_type: "industry" or "concept"
            use_cache: use cached data if available
        """
        cap = Capability.BOARD_INDUSTRY if board_type == "industry" else Capability.BOARD_CONCEPT
        chain = self._policy.chain_for("board")
        warnings: list[str] = []

        for name in chain:
            p = self._get(name)
            if p is None:
                continue
            try:
                if name == "local":
                    r = p.fetch_industry_boards(use_cache=use_cache) if board_type == "industry" else p.fetch_concept_boards(use_cache=use_cache)
                else:
                    r = p.fetch(cap, use_cache=use_cache)
            except Exception as exc:
                warnings.append(f"{name}: {exc}")
                continue

            if r.ok and r.data is not None:
                if name == "local" and warnings:
                    r.warnings.append("Realtime boards unavailable; using local snapshot")
                r.warnings.extend(warnings)
                return r
            warnings.append(f"{name}: {getattr(r, 'error', 'no data')}")

        return Result.fail("all board sources failed", source="manager", warnings=warnings)

    # ─── Generic Fetch ───────────────────────────────

    def fetch(self, capability: Capability, **params) -> Result:
        """Generic fetch with fallback for any capability."""
        chain = self._resolve_chain(capability)
        warnings: list[str] = []

        for name in chain:
            p = self._get(name)
            if p is None:
                continue
            try:
                r = p.fetch(capability, **params)
                if r.ok and r.data is not None:
                    r.warnings.extend(warnings)
                    return r
                warnings.append(f"{name}: {getattr(r, 'error', 'no data')}")
            except Exception as exc:
                warnings.append(f"{name}: {exc}")

        return Result.fail("all sources failed", source="manager", warnings=warnings)

    def _resolve_chain(self, cap: Capability) -> list[str]:
        """Map capability to fallback chain."""
        quote_caps = {Capability.QUOTE, Capability.INDEX_QUOTE}
        board_caps = {Capability.BOARD_INDUSTRY, Capability.BOARD_CONCEPT}
        financial_caps = {Capability.DIVIDEND, Capability.PROFIT_FORECAST,
                          Capability.MAJOR_SHAREHOLDERS, Capability.SHAREHOLDER_CHANGES,
                          Capability.PROFIT_SHEET, Capability.CASH_FLOW}
        fund_caps = {Capability.FUND_FLOW}

        if cap in quote_caps:
            return self._policy.chain_for("quote")
        if cap in board_caps:
            return self._policy.chain_for("board")
        if cap in financial_caps:
            return self._policy.chain_for("financial")
        if cap in fund_caps:
            return self._policy.chain_for("fund_flow")
        return []

    # ─── Health ───────────────────────────────────────


    # --- Indices ---

    def fetch_indices(self, codes=None, sources=None):
        """Fetch market index quotes with optional cross-validation."""
        from modules.market_index import DEFAULT_INDEX_CODES, AggregatedIndexQuote

        codes = codes or DEFAULT_INDEX_CODES
        chain = sources or self._policy.chain_for("quote")
        source_results = {}

        for name in chain:
            p = self._get(name)
            if p is None:
                continue
            if Capability.INDEX_QUOTE not in p.capabilities():
                continue
            try:
                r = p.fetch(Capability.INDEX_QUOTE, codes=codes)
                if r.ok and r.data:
                    source_results[name] = r.data
            except Exception:
                continue

        if not source_results:
            return []

        rows = []
        for code in codes:
            quotes = []
            for name in chain:
                if name in source_results and code in source_results[name]:
                    q = source_results[name][code]
                    quotes.append((name, q))
            if not quotes:
                continue

            primary_name, primary = quotes[0]
            sources_list = [n for n, _ in quotes]
            max_diff = 0.0
            pct_diff = 0.0

            if len(quotes) >= 2:
                prices = [q.price for _, q in quotes]
                max_diff = abs(max(prices) - min(prices))
                pct_diff = max_diff / min(prices) * 100 if min(prices) > 0 else 0
                status = "verified" if pct_diff <= 2.0 else "diverged"
            else:
                status = "single_source"

            rows.append(AggregatedIndexQuote(
                code=code, name=primary.name, price=primary.price,
                prev_close=primary.prev_close, change=primary.change,
                pct_change=primary.pct_change, datetime=primary.datetime,
                status=status, sources=sources_list,
                max_price_diff=round(max_diff, 4),
                max_pct_diff=round(pct_diff, 4),
            ))

        return rows

    def health_all(self) -> dict[str, dict]:
        """Health check all registered providers."""
        from modules.data_sources import provider_names
        result = {}
        for name in sorted(provider_names()):
            p = self._get(name)
            if p is None:
                continue
            try:
                h = p.health()
                result[name] = {
                    "healthy": h.healthy,
                    "latency_ms": h.latency_ms,
                    "error": h.error,
                    "checked_at": h.checked_at,
                }
            except Exception as exc:
                result[name] = {"healthy": False, "latency_ms": 0, "error": str(exc)}
        return result
