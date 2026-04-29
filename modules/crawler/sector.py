"""Unified A-share sector board crawler interface.

数据源优先级（EastMoney push2 已废弃）：
  行业板块: THS → Sina
  概念板块: THS → EastMoney cache
"""

from __future__ import annotations

import pandas as pd

from . import sina
from . import ths
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


def get_industry_boards(use_cache: bool = True) -> CrawlResult:
    result = ths.fetch_industry_boards(use_cache=use_cache)
    if result.ok:
        return _with_normalized_data(result)

    fallback = sina.fetch_industry_boards(use_cache=use_cache)
    if fallback.ok:
        fallback.warnings.append("THS 行业板块不可用，已切换 Sina fallback")
        fallback.user_message = "THS 行业板块不可用，已切换 Sina fallback"
        return _with_normalized_data(fallback)

    return CrawlResult(
        ok=False,
        source="sector",
        error=SOURCE_UNAVAILABLE,
        error_detail=f"ths={result.error}: {result.error_detail}; sina={fallback.error}: {fallback.error_detail}",
        user_message="行业板块数据不可用：THS 和 Sina 均失败",
        warnings=result.warnings + fallback.warnings,
    )


def get_concept_boards(use_cache: bool = True) -> CrawlResult:
    result = ths.fetch_concept_boards(use_cache=use_cache)
    if result.ok:
        return _with_normalized_data(result)

    return CrawlResult(
        ok=False,
        source="sector",
        error=SOURCE_UNAVAILABLE,
        error_detail=f"ths={result.error}: {result.error_detail}",
        user_message="概念板块数据不可用：THS 失败",
        warnings=result.warnings,
    )


def get_all_boards(
    use_cache: bool = True,
    board_types: tuple[str, ...] = ("行业", "概念"),
) -> CrawlResult:
    results = []
    warnings = []
    errors = []

    if "行业" in board_types:
        industry = get_industry_boards(use_cache=use_cache)
        if industry.ok:
            results.append(industry.data)
        else:
            errors.append(f"行业: {industry.error} {industry.error_detail}".strip())
        warnings.extend(industry.warnings)

    if "概念" in board_types:
        concept = get_concept_boards(use_cache=use_cache)
        if concept.ok:
            results.append(concept.data)
        else:
            errors.append(f"概念: {concept.error} {concept.error_detail}".strip())
        warnings.extend(concept.warnings)

    if not results:
        return CrawlResult(
            ok=False,
            source="sector",
            error=SOURCE_UNAVAILABLE,
            error_detail="; ".join(errors),
            user_message="行业/概念板块数据均不可用",
            warnings=warnings,
        )

    df = pd.concat(results, ignore_index=True)
    return CrawlResult(ok=True, data=_normalize_board_df(df), source="sector", warnings=warnings)


def filter_boards_by_pct_change(
    min_pct: float,
    board_types: tuple[str, ...] = ("行业", "概念"),
    use_cache: bool = True,
) -> CrawlResult:
    result = get_all_boards(use_cache=use_cache, board_types=board_types)
    if not result.ok:
        return result
    df = result.data.copy()
    result.data = df[df["pct_change"] >= min_pct].sort_values("pct_change", ascending=False)
    return result


def _with_normalized_data(result: CrawlResult) -> CrawlResult:
    if result.ok:
        result.data = _normalize_board_df(result.data)
    return result


def _normalize_board_df(df: pd.DataFrame | None) -> pd.DataFrame:
    out = pd.DataFrame() if df is None else df.copy()
    for column in STANDARD_COLUMNS:
        if column not in out.columns:
            out[column] = pd.NA
    for column in ["pct_change", "price", "turnover", "rise_count", "fall_count", "top_stock_pct"]:
        out[column] = pd.to_numeric(out[column], errors="coerce")
    return out[STANDARD_COLUMNS]
