"""Local sector board snapshots from sector graph plus live spot quotes."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from modules.config import cfg
from modules.sector_tags import SectorTagProvider
from modules.stock_data import StockData

from . import cache
from .models import CrawlResult, EMPTY_RESPONSE, SOURCE_UNAVAILABLE

SOURCE = "sector_graph_quote"

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

_logger = logging.getLogger(__name__)


def fetch_industry_boards(use_cache: bool = True) -> CrawlResult:
    """Return industry board snapshots from the curated local sector graph."""
    return _fetch_graph_boards("sector", "industry", use_cache=use_cache)


def fetch_concept_boards(use_cache: bool = True) -> CrawlResult:
    """Return concept/theme board snapshots from the curated local sector graph."""
    return _fetch_graph_boards(("concept", "theme"), "concept", use_cache=use_cache)


def _fetch_graph_boards(graph_type: str | tuple[str, ...], sector_type: str, use_cache: bool) -> CrawlResult:
    graph_types = (graph_type,) if isinstance(graph_type, str) else graph_type
    graph_label = "_".join(graph_types)
    trade_date = cache.beijing_now().date().isoformat()
    cache_key = cache.build_cache_key(f"sector_{graph_label}_graph_quote", trade_date)
    if use_cache:
        cached = _read_cache_as_df(cache_key, max_age_seconds=cfg().cache.board_seconds)
        if cached.ok:
            return cached

    try:
        provider = SectorTagProvider()
        nodes = [node for node in provider.graph_nodes() if str(node.get("type", "")) in graph_types]
        if not nodes:
            return CrawlResult(
                ok=False,
                source=SOURCE,
                error=EMPTY_RESPONSE,
                user_message=f"local {graph_label} graph has no nodes",
            )

        spot = _spot_by_code()
        if spot.empty:
            raise RuntimeError("spot quote snapshot unavailable")

        rows = [_node_snapshot(node, sector_type, spot, trade_date) for node in nodes]
        df = _normalize_board_df(pd.DataFrame(rows))
        if df.empty:
            return CrawlResult(
                ok=False,
                source=SOURCE,
                error=EMPTY_RESPONSE,
                user_message=f"local {graph_label} graph snapshot is empty",
            )
        cache.write_json_cache(cache_key, df.to_dict(orient="records"), SOURCE, trade_date=trade_date)
        return CrawlResult(ok=True, data=df, source=SOURCE, trade_date=trade_date)
    except Exception as exc:
        _logger.warning("local sector snapshot failed [%s]: %s", graph_label, exc)
        stale = _read_cache_as_df(cache_key, allow_stale=True)
        if stale.data is not None:
            stale.ok = True
            stale.warnings.append("local sector snapshot unavailable; returned stale cache")
            return stale
        return CrawlResult(
            ok=False,
            source=SOURCE,
            error=SOURCE_UNAVAILABLE,
            error_detail=str(exc),
            user_message=f"local {graph_label} graph snapshot unavailable",
        )


def _spot_by_code() -> pd.DataFrame:
    spot = StockData().get_spot(use_cache=True)
    if spot is None or spot.empty or "code" not in spot.columns:
        return pd.DataFrame()
    out = spot.copy()
    out["code"] = out["code"].astype(str).str.split(".").str[0].str.zfill(6)
    for column in ["price", "pct_change", "turnover", "amount"]:
        if column in out.columns:
            out[column] = pd.to_numeric(out[column], errors="coerce")
    return out.drop_duplicates(subset=["code"]).set_index("code", drop=False)


def _node_snapshot(node: dict[str, Any], sector_type: str, spot: pd.DataFrame, trade_date: str) -> dict[str, Any]:
    tag = str(node.get("tag", ""))
    member_codes = [
        str(member.get("code", "")).split(".")[0].zfill(6)
        for member in node.get("members", [])
        if member.get("code")
    ]
    member_rows = spot.loc[spot.index.intersection(member_codes)] if not spot.empty and member_codes else pd.DataFrame()

    if member_rows.empty:
        return {
            "sector_type": sector_type,
            "sector": tag,
            "sector_code": "",
            "pct_change": 0.0,
            "price": pd.NA,
            "turnover": pd.NA,
            "rise_count": 0,
            "fall_count": 0,
            "top_stock": "",
            "top_stock_pct": pd.NA,
            "source": SOURCE,
            "trade_date": trade_date,
        }

    pct = _numeric_series(member_rows, "pct_change")
    top_idx = pct.idxmax() if pct.notna().any() else member_rows.index[0]
    top_row = member_rows.loc[top_idx]
    return {
        "sector_type": sector_type,
        "sector": tag,
        "sector_code": "",
        "pct_change": round(float(pct.mean(skipna=True) if pct.notna().any() else 0.0), 2),
        "price": _mean_numeric(member_rows, "price"),
        "turnover": _mean_numeric(member_rows, "turnover"),
        "rise_count": int((pct > 0).sum()),
        "fall_count": int((pct < 0).sum()),
        "top_stock": str(top_row.get("name", "")),
        "top_stock_pct": round(float(top_row.get("pct_change") or 0.0), 2),
        "source": SOURCE,
        "trade_date": trade_date,
    }


def _numeric_series(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series(dtype="float64")
    return pd.to_numeric(df[column], errors="coerce")


def _mean_numeric(df: pd.DataFrame, column: str) -> float:
    values = _numeric_series(df, column)
    if values.empty or not values.notna().any():
        return 0.0
    return round(float(values.mean(skipna=True)), 2)


def _normalize_board_df(df: pd.DataFrame | None) -> pd.DataFrame:
    out = pd.DataFrame() if df is None else df.copy()
    for column in STANDARD_COLUMNS:
        if column not in out.columns:
            out[column] = pd.NA
    for column in ["pct_change", "price", "turnover", "rise_count", "fall_count", "top_stock_pct"]:
        out[column] = pd.to_numeric(out[column], errors="coerce")
    return out[STANDARD_COLUMNS]


def _read_cache_as_df(
    key: str,
    *,
    allow_stale: bool = False,
    max_age_seconds: int | None = None,
) -> CrawlResult:
    result = cache.read_json_cache(key, max_age_seconds=max_age_seconds)
    if result.ok or (allow_stale and result.data is not None):
        result.data = _normalize_board_df(pd.DataFrame(result.data or []))
        result.source = result.source or SOURCE
        if allow_stale and not result.ok and result.error:
            result.warnings.append(f"returned stale cache: {result.error}")
    return result
