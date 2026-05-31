"""Unified A-share sector board crawler interface.

数据源优先级（EastMoney push2 已废弃）：
  行业板块: THS → Sina
  概念板块: THS → EastMoney cache
"""

from __future__ import annotations

import pandas as pd

# Backward-compat module refs (used by tests via monkeypatch)
from . import local_sector  # noqa: F401
from . import sina  # noqa: F401
from . import ths  # noqa: F401

from .models import CrawlResult, SOURCE_UNAVAILABLE


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


class BoardManager:
    """Config-driven board data manager with fallback chain.

    Usage:
        mgr = BoardManager()                     # use config order
        mgr = BoardManager(sources=["ths","sina","local"])  # explicit order
        result = mgr.get_industry_boards()
    """

    _REGISTRY: dict[str, type] = {}

    def __init__(self, sources: list[str] | None = None):
        if not self._REGISTRY:
            self._register_defaults()
        names = sources or _board_source_order()
        self._sources: list = [self._REGISTRY[n]() for n in names if n in self._REGISTRY]

    @classmethod
    def _register_defaults(cls):
        from modules.data_sources import get_provider
        cls._REGISTRY.update({
            "ths": type(get_provider("ths")),
            "sina": type(get_provider("sina")),
            "local": type(get_provider("local")) if get_provider("local") else None,
        })
        # Remove None entries
        cls._REGISTRY = {k: v for k, v in cls._REGISTRY.items() if v is not None}
        # Add local from board_sources
        from modules.crawler.board_sources import LocalSectorBoardSource
        cls._REGISTRY["local"] = LocalSectorBoardSource

    @classmethod
    def register(cls, name: str, source_cls: type):
        """Register a new board source. Call before creating BoardManager."""
        cls._REGISTRY[name] = source_cls

    @property
    def sources(self):
        return list(self._sources)

    def get_industry_boards(self, use_cache: bool = True) -> "CrawlResult":
        return self._try_chain("industry", use_cache)

    def get_concept_boards(self, use_cache: bool = True) -> "CrawlResult":
        return self._try_chain("concept", use_cache)

    def _try_chain(self, board_type: str, use_cache: bool) -> "CrawlResult":
        from .models import CrawlResult

        warnings: list[str] = []
        for src in self._sources:
            try:
                fetch = src.fetch_industry_boards if board_type == "industry" else src.fetch_concept_boards
                result = fetch(use_cache=use_cache)
            except Exception as exc:
                warnings.append(f"{src.name}: {exc}")
                continue

            if result.ok:
                if src.name == "local" and warnings:
                    result.warnings.append("Realtime boards unavailable; using local sector graph quote snapshot")
                result.warnings.extend(warnings)
                return result
            warnings.append(f"{src.name}: {result.error}")

        return CrawlResult(
            ok=False, source="board_manager", error=SOURCE_UNAVAILABLE,
            warnings=warnings, user_message=f"All sources failed for {board_type} boards",
        )


def _board_source_order() -> list[str]:
    """Read board source order from config, fall back to defaults."""
    try:
        from modules.config import cfg
        order = getattr(cfg(), "boards", None)
        if order and hasattr(order, "sources"):
            return list(order.sources)
    except Exception:
        pass
    return ["ths", "sina", "local"]


# Backward-compatible module-level API
_default_manager: BoardManager | None = None


def _get_manager() -> BoardManager:
    global _default_manager
    if _default_manager is None:
        _default_manager = BoardManager()
    return _default_manager


def get_industry_boards(use_cache: bool = True):
    return _get_manager().get_industry_boards(use_cache=use_cache)


def get_concept_boards(use_cache: bool = True):
    return _get_manager().get_concept_boards(use_cache=use_cache)


def get_all_boards(use_cache: bool = True, board_types=("行业", "概念")):
    """Backward-compatible get_all_boards."""
    from .models import CrawlResult

    results = []
    warnings_list = []
    errors = []

    if "行业" in board_types:
        industry = get_industry_boards(use_cache=use_cache)
        if industry.ok:
            results.append(industry.data)
        else:
            errors.append(f"行业: {industry.error} {industry.error_detail}".strip())
        warnings_list.extend(industry.warnings)

    if "概念" in board_types:
        concept = get_concept_boards(use_cache=use_cache)
        if concept.ok:
            results.append(concept.data)
        else:
            errors.append(f"概念: {concept.error} {concept.error_detail}".strip())
        warnings_list.extend(concept.warnings)

    if results:
        df = pd.concat(results, ignore_index=True)
        return CrawlResult(ok=True, data=df, source="board_manager", warnings=warnings_list)
    return CrawlResult(
        ok=False, source="board_manager", error="; ".join(errors) if errors else "no data",
        warnings=warnings_list,
    )
