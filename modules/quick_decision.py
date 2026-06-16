"""Fast intraday buy/no-buy decision helper.

This module is intentionally lightweight. It avoids the slow single-stock
report chain and uses only live quotes plus local daily cache/topic tags.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import time
import tomllib
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from modules.utils import normalize_symbol, to_sina_code, to_tencent_code

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_WAREHOUSE_DB = _PROJECT_ROOT / "data" / "warehouse.db"
_SECTOR_GRAPH = _PROJECT_ROOT / "data" / "sector_graph.toml"
_DEFAULT_WATCHLIST = _PROJECT_ROOT / "data" / "swing_watchlist_latest.json"
_TAG_CACHE: dict[str, list[str]] | None = None


def build_quick_decision(
    symbols: list[str],
    *,
    source: str = "auto",
    timeout: float = 1.2,
    include_tags: bool = True,
    include_event_factors: bool = True,
    db_path: str | Path | None = None,
) -> dict[str, Any]:
    """Build fast intraday decisions for stock symbols."""
    started = time.perf_counter()
    settings = _quick_settings()
    codes = _unique_codes(symbols)
    quote_started = time.perf_counter()
    quotes, quote_source, quote_warning = _fetch_quotes(codes, source=source, timeout=timeout)
    quote_elapsed = time.perf_counter() - quote_started
    daily_map = _load_daily_cache(codes, db_path=db_path)
    tags_by_code = _code_tags(codes) if include_tags else {}
    event_factors = (
        _event_factors_by_code(
            codes,
            tags_by_code,
            db_path=db_path,
            max_age_days=settings.max_event_factor_age_days,
        )
        if include_tags and include_event_factors
        else {}
    )

    rows = []
    for code in codes:
        quote = quotes.get(code, {"code": code})
        daily = daily_map.get(code, [])
        rows.append(
            _decision_for(
                code,
                quote,
                daily,
                tags_by_code.get(code, []),
                event_factors.get(code),
                settings,
            )
        )

    elapsed = time.perf_counter() - started
    return {
        "engine": "quick_decision_v1",
        "generated_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "summary": {
            "count": len(rows),
            "source": quote_source,
            "quote_elapsed_seconds": round(quote_elapsed, 3),
            "elapsed_seconds": round(elapsed, 3),
            "warning": quote_warning,
            "event_factor_count": sum(1 for row in event_factors.values() if row.get("status") == "active"),
        },
        "decisions": rows,
    }


def save_quick_decision(payload: dict[str, Any], *, db_path: str | Path | None = None) -> int:
    """Persist a quick decision payload and return the run id."""
    from modules.db import DatabaseManager

    db = DatabaseManager(str(_resolve_db_path(db_path)))
    try:
        return db.quick_decision().record_run(payload)
    finally:
        db.close()


def review_quick_decisions(
    symbols: list[str] | None = None,
    *,
    limit: int = 20,
    horizon_days: int = 3,
    action: str | None = None,
    db_path: str | Path | None = None,
) -> dict[str, Any]:
    """Evaluate stored quick decisions against later daily closes."""
    from modules.db import DatabaseManager

    codes = _unique_codes(symbols or [])
    db = DatabaseManager(str(_resolve_db_path(db_path)))
    try:
        payload = db.quick_decision().evaluate(
            limit=max(1, int(limit)),
            horizon_days=max(1, int(horizon_days)),
            symbols=codes or None,
            action=action,
        )
    finally:
        db.close()
    payload["symbols"] = codes
    return payload


def evaluate_quick_decisions(
    symbols: list[str] | None = None,
    *,
    horizons: list[int] | None = None,
    limit: int = 200,
    action: str | None = None,
    save: bool = True,
    db_path: str | Path | None = None,
) -> dict[str, Any]:
    """Evaluate stored quick decisions for several horizons."""
    from modules.db import DatabaseManager

    codes = _unique_codes(symbols or [])
    db = DatabaseManager(str(_resolve_db_path(db_path)))
    try:
        payload = db.quick_decision().evaluate_many(
            horizons=horizons or [1, 3, 5],
            limit=max(1, int(limit)),
            symbols=codes or None,
            action=action,
            save=save,
        )
    finally:
        db.close()
    payload["symbols"] = codes
    payload["saved_to_db"] = bool(save)
    return payload


def summarize_quick_decision_evaluations(
    *,
    horizon_days: int = 3,
    limit: int = 500,
    min_samples: int = 1,
    db_path: str | Path | None = None,
) -> dict[str, Any]:
    """Summarize persisted quick-decision evaluations."""
    from modules.db import DatabaseManager

    db = DatabaseManager(str(_resolve_db_path(db_path)))
    try:
        return db.quick_decision().evaluation_report(
            horizon_days=max(1, int(horizon_days)),
            limit=max(1, int(limit)),
            min_samples=max(1, int(min_samples)),
        )
    finally:
        db.close()


def collect_sample_symbols(
    *,
    sources: list[str] | None = None,
    limit: int | None = None,
    watchlist_file: str | Path | None = None,
    min_event_score: float | None = None,
    fusion_limit: int | None = None,
    fusion_pool_limit: int | None = None,
    fusion_deadline_seconds: float | None = None,
    db_path: str | Path | None = None,
) -> dict[str, Any]:
    """Collect quick-decision sample symbols from configured sources."""
    settings = _quick_settings()
    clean_sources = _normalize_sources(sources or list(settings.sample_sources))
    max_symbols = int(limit or settings.sample_limit)
    source_map: dict[str, list[str]] = {}
    warnings: list[str] = []

    for source in clean_sources:
        try:
            if source == "watchlist":
                rows = load_watchlist_symbols(watchlist_file)
            elif source == "event":
                rows = _load_event_opportunity_symbols(
                    db_path=db_path,
                    min_score=float(min_event_score if min_event_score is not None else settings.sample_min_event_score),
                    limit=max_symbols * 3,
                )
            elif source == "fusion":
                rows = _load_fusion_candidate_symbols(
                    limit=int(fusion_limit or settings.sample_fusion_limit),
                    pool_limit=int(fusion_pool_limit or settings.sample_fusion_pool_limit),
                    deadline_seconds=float(
                        fusion_deadline_seconds
                        if fusion_deadline_seconds is not None
                        else settings.sample_fusion_deadline_seconds
                    ),
                )
            else:
                rows = []
        except Exception as exc:
            rows = []
            warnings.append(f"{source}: {exc}")
        source_map[source] = _unique_codes(rows)

    symbols: list[str] = []
    for source in clean_sources:
        for code in source_map.get(source, []):
            if code not in symbols:
                symbols.append(code)
            if len(symbols) >= max_symbols:
                break
        if len(symbols) >= max_symbols:
            break

    return {
        "sources": clean_sources,
        "symbols": symbols,
        "source_counts": {key: len(value) for key, value in source_map.items()},
        "source_symbols": source_map,
        "warnings": warnings,
    }


def sample_quick_decisions(
    *,
    sources: list[str] | None = None,
    limit: int | None = None,
    max_per_symbol_per_day: int | None = None,
    source: str = "auto",
    timeout: float = 1.2,
    include_tags: bool = True,
    include_event_factors: bool = True,
    watchlist_file: str | Path | None = None,
    min_event_score: float | None = None,
    fusion_limit: int | None = None,
    fusion_pool_limit: int | None = None,
    fusion_deadline_seconds: float | None = None,
    save: bool = True,
    db_path: str | Path | None = None,
) -> dict[str, Any]:
    """Collect and persist quick-decision samples with same-day frequency limits."""
    settings = _quick_settings()
    collected = collect_sample_symbols(
        sources=sources,
        limit=limit or settings.sample_limit,
        watchlist_file=watchlist_file,
        min_event_score=min_event_score,
        fusion_limit=fusion_limit,
        fusion_pool_limit=fusion_pool_limit,
        fusion_deadline_seconds=fusion_deadline_seconds,
        db_path=db_path,
    )
    max_count = int(max_per_symbol_per_day or settings.sample_max_per_symbol_per_day)
    due_symbols, skipped_symbols = _filter_sample_due_symbols(
        collected["symbols"],
        max_per_symbol_per_day=max_count,
        db_path=db_path,
    )
    if due_symbols:
        payload = build_quick_decision(
            due_symbols,
            source=source,
            timeout=timeout,
            include_tags=include_tags,
            include_event_factors=include_event_factors,
            db_path=db_path,
        )
        if save:
            payload["summary"]["run_id"] = save_quick_decision(payload, db_path=db_path)
    else:
        payload = {
            "engine": "quick_decision_sample_v1",
            "generated_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "summary": {
                "count": 0,
                "source": "none",
                "quote_elapsed_seconds": 0.0,
                "elapsed_seconds": 0.0,
                "warning": "",
                "event_factor_count": 0,
            },
            "decisions": [],
        }
    payload["summary"]["sample"] = {
        "requested": len(collected["symbols"]),
        "sampled": len(due_symbols),
        "skipped_same_day_limit": len(skipped_symbols),
        "max_per_symbol_per_day": max_count,
        "sources": collected["sources"],
        "source_counts": collected["source_counts"],
        "warnings": collected["warnings"],
    }
    return payload


def learn_quick_decision(
    *,
    horizon_days: int = 3,
    limit: int = 500,
    min_samples: int = 3,
    suggest_config: bool = False,
    db_path: str | Path | None = None,
) -> dict[str, Any]:
    """Diagnose weak spots from persisted quick-decision evaluation results."""
    report = summarize_quick_decision_evaluations(
        horizon_days=horizon_days,
        limit=limit,
        min_samples=min_samples,
        db_path=db_path,
    )
    diagnostics = _learn_diagnostics(report, min_samples=max(1, int(min_samples)))
    payload = {
        "horizon_days": horizon_days,
        "limit": limit,
        "min_samples": min_samples,
        "summary": report.get("summary") or {},
        "diagnostics": diagnostics,
        "report": report,
    }
    if suggest_config:
        payload["suggested_config"] = _suggest_config_from_diagnostics(diagnostics)
    return payload


def backfill_quick_decision_samples(
    symbols: list[str] | None = None,
    *,
    start_date: str,
    end_date: str | None = None,
    source: str = "event",
    limit: int | None = None,
    watchlist_file: str | Path | None = None,
    min_event_score: float | None = None,
    include_tags: bool = True,
    include_event_factors: bool = True,
    event_factor_max_age_days: int | None = None,
    save: bool = True,
    evaluate_horizons: list[int] | None = None,
    db_path: str | Path | None = None,
) -> dict[str, Any]:
    """Build historical quick-decision samples from warehouse daily bars."""
    settings = _quick_settings()
    clean_start = _normalize_date_text(start_date)
    clean_end = _normalize_date_text(end_date or start_date)
    if not clean_start or not clean_end:
        raise ValueError("start_date and end_date must be YYYY-MM-DD or YYYYMMDD")
    if clean_end < clean_start:
        raise ValueError("end_date must be >= start_date")

    requested_symbols = _unique_codes(symbols or [])
    effective_source = "manual" if requested_symbols else source
    if not requested_symbols:
        requested_symbols = _historical_replay_symbols(
            source=source,
            limit=limit or settings.sample_limit,
            watchlist_file=watchlist_file,
            min_event_score=(
                float(min_event_score)
                if min_event_score is not None
                else float(settings.sample_min_event_score)
            ),
            db_path=db_path,
        )
    max_symbols = max(1, int(limit or settings.sample_limit))
    replay_symbols = requested_symbols[:max_symbols]

    dates = _available_trade_dates(
        replay_symbols,
        start_date=clean_start,
        end_date=clean_end,
        db_path=db_path,
    )
    run_ids: list[int] = []
    row_count = 0
    skipped_missing = 0
    skipped_duplicate = 0
    source_counts = {"requested_symbols": len(requested_symbols), "replay_symbols": len(replay_symbols)}
    for trade_date in dates:
        payload = _build_historical_replay_payload(
            replay_symbols,
            trade_date=trade_date,
            source=effective_source,
            include_tags=include_tags,
            include_event_factors=include_event_factors,
            event_factor_max_age_days=event_factor_max_age_days,
            db_path=db_path,
        )
        row_count += len(payload.get("decisions") or [])
        skipped_missing += int((payload.get("summary") or {}).get("skipped_missing_daily") or 0)
        skipped_duplicate += int((payload.get("summary") or {}).get("skipped_duplicate") or 0)
        if save and payload.get("decisions"):
            run_ids.append(save_quick_decision(payload, db_path=db_path))

    result: dict[str, Any] = {
        "engine": "quick_decision_historical_replay_v1",
        "generated_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "summary": {
            "source": effective_source,
            "start_date": clean_start,
            "end_date": clean_end,
            "trade_dates": len(dates),
            "requested_symbols": len(requested_symbols),
            "replay_symbols": len(replay_symbols),
            "rows": row_count,
            "runs": len(run_ids),
            "run_ids": run_ids,
            "skipped_missing_daily": skipped_missing,
            "skipped_duplicate": skipped_duplicate,
            "saved": bool(save),
            "source_counts": source_counts,
        },
    }
    if evaluate_horizons:
        result["evaluation"] = evaluate_quick_decisions(
            horizons=evaluate_horizons,
            limit=max(row_count + 50, 200),
            save=True,
            db_path=db_path,
        )
    return result


def _quick_settings() -> Any:
    try:
        from modules.config import cfg

        return cfg().quick_decision
    except Exception:
        from modules.config import QuickDecisionSettings

        return QuickDecisionSettings()


def _normalize_sources(sources: list[str] | tuple[str, ...] | str) -> list[str]:
    allowed = {"watchlist", "fusion", "event"}
    if isinstance(sources, str):
        raw_items = sources.split(",")
    else:
        raw_items = list(sources)
    out: list[str] = []
    for item in raw_items:
        source = str(item or "").strip().lower()
        if source in allowed and source not in out:
            out.append(source)
    return out or ["watchlist", "fusion", "event"]


def _load_event_opportunity_symbols(
    *,
    db_path: str | Path | None = None,
    min_score: float,
    limit: int,
) -> list[str]:
    from modules.db import DatabaseManager

    db = DatabaseManager(str(_resolve_db_path(db_path)))
    try:
        rows = db.event().list_opportunities(limit=max(1, int(limit)))
    finally:
        db.close()
    symbols: list[str] = []
    for _, row in rows.iterrows():
        if _num(row.get("opportunity_score")) < float(min_score):
            continue
        code = normalize_symbol(str(row.get("symbol") or ""))
        if code:
            symbols.append(code)
    return _unique_codes(symbols)


def _load_fusion_candidate_symbols(
    *,
    limit: int,
    pool_limit: int,
    deadline_seconds: float,
) -> list[str]:
    from modules.strategy_fusion import StrategyFusionEngine

    payload = StrategyFusionEngine().scan(
        limit=max(1, int(limit)),
        pool_limit=max(1, int(pool_limit)),
        score_pool_limit=max(1, int(pool_limit)),
        deadline_seconds=max(1.0, float(deadline_seconds)),
        mode="fast",
    )
    return _unique_codes([str(row.get("symbol") or "") for row in payload.get("candidates") or []])


def _historical_replay_symbols(
    *,
    source: str,
    limit: int,
    watchlist_file: str | Path | None,
    min_event_score: float,
    db_path: str | Path | None = None,
) -> list[str]:
    sources = [item.strip().lower() for item in str(source or "event").split(",") if item.strip()]
    out: list[str] = []
    for item in sources or ["event"]:
        if item == "watchlist":
            out.extend(load_watchlist_symbols(watchlist_file))
        elif item == "event":
            out.extend(
                _load_event_opportunity_symbols(
                    db_path=db_path,
                    min_score=float(min_event_score),
                    limit=max(1, int(limit)) * 3,
                )
            )
    return _unique_codes(out)[: max(1, int(limit))]


def _available_trade_dates(
    symbols: list[str],
    *,
    start_date: str,
    end_date: str,
    db_path: str | Path | None = None,
) -> list[str]:
    codes = _unique_codes(symbols)
    if not codes:
        return []
    warehouse_db = _resolve_db_path(db_path)
    if not warehouse_db.exists():
        return []
    placeholders = ",".join("?" for _ in codes)
    try:
        with sqlite3.connect(warehouse_db) as conn:
            rows = conn.execute(
                f"""SELECT DISTINCT trade_date
                    FROM price_daily
                    WHERE symbol IN ({placeholders})
                      AND adjust = 'qfq'
                      AND trade_date >= ?
                      AND trade_date <= ?
                    ORDER BY trade_date""",
                [*codes, start_date, end_date],
            ).fetchall()
    except sqlite3.Error:
        return []
    return [str(row[0]) for row in rows]


def _build_historical_replay_payload(
    symbols: list[str],
    *,
    trade_date: str,
    source: str,
    include_tags: bool,
    include_event_factors: bool,
    event_factor_max_age_days: int | None,
    db_path: str | Path | None = None,
) -> dict[str, Any]:
    started = time.perf_counter()
    settings = _quick_settings()
    codes = _unique_codes(symbols)
    tags_by_code = _code_tags(codes) if include_tags else {}
    factor_max_age = (
        max(1, int(event_factor_max_age_days))
        if event_factor_max_age_days is not None
        else int(settings.max_event_factor_age_days)
    )
    event_factors = (
        _historical_event_factors_by_code(
            codes,
            tags_by_code,
            trade_date=trade_date,
            max_age_days=factor_max_age,
            db_path=db_path,
        )
        if include_tags and include_event_factors
        else {}
    )
    existing = _existing_historical_replay_symbols(trade_date=trade_date, db_path=db_path)
    rows: list[dict[str, Any]] = []
    skipped_missing = 0
    skipped_duplicate = 0
    for code in codes:
        if code in existing:
            skipped_duplicate += 1
            continue
        quote = _historical_quote(code, trade_date=trade_date, db_path=db_path)
        if quote is None:
            skipped_missing += 1
            continue
        daily = _historical_daily_rows(code, before_date=trade_date, db_path=db_path)
        rows.append(
            _decision_for(
                code,
                quote,
                daily,
                tags_by_code.get(code, []),
                event_factors.get(code),
                settings,
            )
        )
    elapsed = time.perf_counter() - started
    warning_parts = []
    if include_event_factors and tags_by_code and not event_factors:
        warning_parts.append("no historical event factor snapshot matched")
    if skipped_duplicate:
        warning_parts.append(f"skipped duplicate historical replay rows: {skipped_duplicate}")
    return {
        "engine": "quick_decision_historical_replay_v1",
        "generated_at": f"{trade_date}T14:30:00",
        "summary": {
            "count": len(rows),
            "source": f"historical_replay:{source}",
            "quote_elapsed_seconds": 0.0,
            "elapsed_seconds": round(elapsed, 3),
            "warning": "; ".join(warning_parts),
            "event_factor_count": sum(1 for row in event_factors.values() if row.get("status") == "active"),
            "historical_replay": {
                "trade_date": trade_date,
                "requested_symbols": len(codes),
                "skipped_missing_daily": skipped_missing,
                "skipped_duplicate": skipped_duplicate,
            },
            "skipped_missing_daily": skipped_missing,
            "skipped_duplicate": skipped_duplicate,
        },
        "decisions": rows,
    }


def _historical_quote(
    symbol: str,
    *,
    trade_date: str,
    db_path: str | Path | None = None,
) -> dict[str, Any] | None:
    warehouse_db = _resolve_db_path(db_path)
    if not warehouse_db.exists():
        return None
    try:
        with sqlite3.connect(warehouse_db) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                """SELECT open, high, low, close, volume, amount, pct_change
                   FROM price_daily
                   WHERE symbol = ? AND adjust = 'qfq' AND trade_date = ?""",
                (symbol, trade_date),
            ).fetchone()
            if row is None:
                return None
            prev = conn.execute(
                """SELECT close
                   FROM price_daily
                   WHERE symbol = ? AND adjust = 'qfq' AND trade_date < ?
                   ORDER BY trade_date DESC LIMIT 1""",
                (symbol, trade_date),
            ).fetchone()
    except sqlite3.Error:
        return None
    price = _num(row["close"])
    prev_close = _num(prev["close"]) if prev else 0.0
    pct = _num(row["pct_change"])
    if not pct and price and prev_close:
        pct = (price / prev_close - 1.0) * 100
    return {
        "code": symbol,
        "name": symbol,
        "open": _num(row["open"]),
        "high": _num(row["high"]),
        "low": _num(row["low"]),
        "price": price,
        "prev_close": prev_close,
        "volume": _num(row["volume"]),
        "amount": _num(row["amount"]),
        "change_pct": round(pct, 2),
        "quote_time": f"{trade_date} 14:30:00",
    }


def _historical_daily_rows(
    symbol: str,
    *,
    before_date: str,
    days: int = 80,
    db_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    warehouse_db = _resolve_db_path(db_path)
    if not warehouse_db.exists():
        return []
    try:
        with sqlite3.connect(warehouse_db) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """SELECT trade_date, open, high, low, close, volume, amount, turn, pct_change
                   FROM price_daily
                   WHERE symbol = ? AND adjust = 'qfq' AND trade_date < ?
                   ORDER BY trade_date DESC
                   LIMIT ?""",
                (symbol, before_date, max(1, int(days))),
            ).fetchall()
    except sqlite3.Error:
        return []
    return [
        {
            "date": row["trade_date"],
            "open": _num(row["open"]),
            "high": _num(row["high"]),
            "low": _num(row["low"]),
            "close": _num(row["close"]),
            "volume": _num(row["volume"]),
            "amount": _num(row["amount"]),
            "turn": _num(row["turn"]),
            "pct_change": _num(row["pct_change"]),
        }
        for row in reversed(rows)
    ]


def _historical_event_factors_by_code(
    symbols: list[str],
    tags_by_code: dict[str, list[str]],
    *,
    trade_date: str,
    max_age_days: int,
    db_path: str | Path | None = None,
) -> dict[str, dict[str, Any]]:
    sectors = sorted({tag for code in symbols for tag in tags_by_code.get(code, [])})
    warehouse_db = _resolve_db_path(db_path)
    if not sectors or not warehouse_db.exists():
        return {}
    placeholders = ",".join("?" for _ in sectors)
    try:
        with sqlite3.connect(warehouse_db) as conn:
            conn.row_factory = sqlite3.Row
            snapshot_row = conn.execute(
                "SELECT MAX(snapshot_date) FROM event_news_factor_snapshots WHERE snapshot_date <= ?",
                (trade_date,),
            ).fetchone()
            snapshot_date = str(snapshot_row[0] or "") if snapshot_row else ""
            if not snapshot_date:
                return {}
            query = (
                "SELECT sector, factor_score, direction, insight_count, avg_value_score, "
                f"top_topic, top_titles_json, avg_time_decay, updated_at FROM event_news_factor_snapshots "
                f"WHERE snapshot_date = ? AND sector IN ({placeholders})"
            )
            factors = {}
            for row in conn.execute(query, [snapshot_date, *sectors]):
                factor = dict(row)
                factor["snapshot_date"] = snapshot_date
                factor["status"] = _event_factor_status_at(
                    str(factor.get("updated_at") or snapshot_date),
                    as_of_date=trade_date,
                    max_age_days=max_age_days,
                )
                factors[str(factor.get("sector") or "")] = factor
    except sqlite3.Error:
        return {}
    out: dict[str, dict[str, Any]] = {}
    for code in symbols:
        candidates = [factors[tag] for tag in tags_by_code.get(code, []) if tag in factors]
        if candidates:
            out[code] = max(candidates, key=lambda item: _num(item.get("factor_score")))
    return out


def _existing_historical_replay_symbols(
    *,
    trade_date: str,
    db_path: str | Path | None = None,
) -> set[str]:
    warehouse_db = _resolve_db_path(db_path)
    if not warehouse_db.exists():
        return set()
    try:
        with sqlite3.connect(warehouse_db) as conn:
            rows = conn.execute(
                """SELECT r.symbol
                   FROM quick_decision_rows r
                   JOIN quick_decision_runs q ON q.id = r.run_id
                   WHERE q.engine = 'quick_decision_historical_replay_v1'
                     AND substr(r.created_at, 1, 10) = ?""",
                (trade_date,),
            ).fetchall()
    except sqlite3.Error:
        return set()
    return {normalize_symbol(str(row[0] or "")) for row in rows if row and row[0]}


def _filter_sample_due_symbols(
    symbols: list[str],
    *,
    max_per_symbol_per_day: int,
    db_path: str | Path | None = None,
) -> tuple[list[str], list[str]]:
    codes = _unique_codes(symbols)
    if not codes:
        return [], []
    max_count = max(1, int(max_per_symbol_per_day))
    warehouse_db = _resolve_db_path(db_path)
    if not warehouse_db.exists():
        return codes, []

    placeholders = ",".join("?" for _ in codes)
    today = date.today().isoformat()
    counts: dict[str, int] = {}
    try:
        with sqlite3.connect(warehouse_db) as conn:
            rows = conn.execute(
                f"""SELECT symbol, COUNT(*)
                    FROM quick_decision_rows
                    WHERE symbol IN ({placeholders}) AND substr(created_at, 1, 10) = ?
                    GROUP BY symbol""",
                [*codes, today],
            ).fetchall()
    except sqlite3.Error:
        return codes, []
    for symbol, count in rows:
        counts[normalize_symbol(str(symbol))] = int(count or 0)

    due = [code for code in codes if counts.get(code, 0) < max_count]
    skipped = [code for code in codes if counts.get(code, 0) >= max_count]
    return due, skipped


def _learn_diagnostics(report: dict[str, Any], *, min_samples: int) -> dict[str, Any]:
    summary = report.get("summary") or {}
    diagnostics = {
        "low_win_rate_actions": _weak_rows(report.get("by_action") or [], min_samples=min_samples),
        "losing_score_buckets": _weak_rows(
            report.get("by_score_bucket") or [],
            min_samples=min_samples,
            losing_only=True,
        ),
        "low_efficiency_tags": _weak_rows(
            report.get("by_tag") or [],
            min_samples=min_samples,
            skip_prefixes=("无标签",),
        ),
        "weak_event_factors": _weak_rows(
            report.get("by_event_sector") or [],
            min_samples=min_samples,
            skip_prefixes=("无事件因子",),
        ),
    }
    diagnostics["recommendations"] = _learn_recommendations(diagnostics, summary)
    return diagnostics


def _weak_rows(
    rows: list[dict[str, Any]],
    *,
    min_samples: int,
    losing_only: bool = False,
    skip_prefixes: tuple[str, ...] = (),
) -> list[dict[str, Any]]:
    weak: list[dict[str, Any]] = []
    for row in rows:
        key = str(row.get("key") or "")
        if any(key.startswith(prefix) for prefix in skip_prefixes):
            continue
        count = int(row.get("count") or 0)
        success_rate = _num(row.get("success_rate"))
        avg_return = _num(row.get("avg_forward_return_pct"))
        if count < max(1, int(min_samples)):
            continue
        is_weak = avg_return < 0 if losing_only else (success_rate < 50.0 or avg_return < 0)
        if not is_weak:
            continue
        weak.append(
            {
                "key": key,
                "count": count,
                "success_rate": success_rate,
                "avg_forward_return_pct": avg_return,
                "avg_max_drawdown_pct": _num(row.get("avg_max_drawdown_pct")),
                "symbols": row.get("symbols") or [],
            }
        )
    return sorted(weak, key=lambda item: (float(item["avg_forward_return_pct"]), float(item["success_rate"])))


def _learn_recommendations(diagnostics: dict[str, Any], summary: dict[str, Any]) -> list[str]:
    if int(summary.get("count") or 0) <= 0:
        return ["暂无已评价样本，先让 collect-samples 跑满几个交易日再判断权重。"]

    recommendations: list[str] = []
    for row in diagnostics.get("low_win_rate_actions") or []:
        recommendations.append(
            f"动作“{row['key']}”样本 {row['count']}，成功率 {row['success_rate']:.1f}%，"
            f"均值 {row['avg_forward_return_pct']:+.2f}%，先复核触发条件。"
        )
    for row in diagnostics.get("losing_score_buckets") or []:
        recommendations.append(
            f"分数段 {row['key']} 均值 {row['avg_forward_return_pct']:+.2f}%，"
            "优先观察对应阈值和追高惩罚是否需要收紧。"
        )
    for row in diagnostics.get("low_efficiency_tags") or []:
        recommendations.append(
            f"主题“{row['key']}”样本 {row['count']} 表现偏弱，先观察是否是阶段性题材退潮。"
        )
    for row in diagnostics.get("weak_event_factors") or []:
        recommendations.append(
            f"事件因子“{row['key']}”样本 {row['count']} 表现偏弱，考虑降低事件加分或缩短有效期。"
        )
    if not recommendations:
        recommendations.append("当前样本没有显著弱项，建议继续积累样本，暂不调整权重。")
    return recommendations[:10]


def _suggest_config_from_diagnostics(diagnostics: dict[str, Any]) -> dict[str, Any]:
    settings = _quick_settings()
    changes: dict[str, dict[str, Any]] = {}

    def add_change(key: str, proposed: float | int, reason: str, rows: list[dict[str, Any]], risk: str) -> None:
        current = getattr(settings, key)
        if proposed == current:
            return
        evidence_samples = sum(int(row.get("count") or 0) for row in rows)
        item = changes.setdefault(
            key,
            {
                "key": key,
                "current": current,
                "proposed": proposed,
                "evidence_samples": 0,
                "reasons": [],
                "risk": risk,
                "expected_impact": "",
            },
        )
        item["proposed"] = proposed
        item["evidence_samples"] += evidence_samples
        if reason not in item["reasons"]:
            item["reasons"].append(reason)

    weak_actions = diagnostics.get("low_win_rate_actions") or []
    losing_buckets = diagnostics.get("losing_score_buckets") or []
    weak_events = diagnostics.get("weak_event_factors") or []

    buy_rows = [row for row in weak_actions if "可轻仓" in str(row.get("key") or "") or "买" in str(row.get("key") or "")]
    if buy_rows or any(str(row.get("key")) == "70+" for row in losing_buckets):
        add_change(
            "buy_score_threshold",
            round(float(settings.buy_score_threshold) + 2.0, 1),
            "买入类动作或高分段后验偏弱，需要提高进入“可轻仓观察”的门槛。",
            [*buy_rows, *[row for row in losing_buckets if str(row.get("key")) == "70+"]],
            "可能减少样本量并错过一部分强势修复标的。",
        )

    watch_rows = [row for row in weak_actions if str(row.get("key") or "") == "观察"]
    if watch_rows or any(str(row.get("key")) == "60-69" for row in losing_buckets):
        add_change(
            "watch_score_threshold",
            round(float(settings.watch_score_threshold) + 2.0, 1),
            "观察动作或 60-69 分段后验偏弱，需要提高进入观察池的门槛。",
            [*watch_rows, *[row for row in losing_buckets if str(row.get("key")) == "60-69"]],
            "可能让边缘机会更早被归为不买。",
        )

    if weak_events:
        add_change(
            "event_strong_bonus",
            round(float(settings.event_strong_bonus) - 1.0, 1),
            "事件因子分组后验偏弱，先小幅降低强事件加分。",
            weak_events,
            "可能低估真正强催化的短期弹性。",
        )
        add_change(
            "event_support_bonus",
            round(float(settings.event_support_bonus) - 1.0, 1),
            "事件因子分组后验偏弱，先小幅降低普通事件支持加分。",
            weak_events,
            "可能降低事件机会进入观察池的概率。",
        )
        if any("[stale]" in str(row.get("key") or "") for row in weak_events):
            add_change(
                "max_event_factor_age_days",
                max(1, int(settings.max_event_factor_age_days) - 1),
                "过期事件因子表现偏弱，缩短事件因子有效期。",
                weak_events,
                "可能让低频但持续性的宏观事件过早失效。",
            )

    rows = list(changes.values())
    for item in rows:
        item["expected_impact"] = _expected_config_impact(str(item["key"]), item["current"], item["proposed"])

    return {
        "changes": rows,
        "toml": _suggested_toml(rows),
        "diff": _suggested_diff(rows),
        "notes": ["仅输出建议，不自动覆盖 data/moatx.toml。"] if rows else ["证据不足或未发现需要调权的方向。"],
    }


def _expected_config_impact(key: str, current: Any, proposed: Any) -> str:
    if key.endswith("_threshold"):
        return f"门槛从 {current} 调到 {proposed}，会减少低置信样本进入更积极动作。"
    if key.startswith("event_"):
        return f"事件加分从 {current} 调到 {proposed}，会降低事件因子对总分的拉动。"
    if key == "max_event_factor_age_days":
        return f"有效期从 {current} 天调到 {proposed} 天，旧事件更快失效。"
    return f"{key} 从 {current} 调到 {proposed}。"


def _suggested_toml(changes: list[dict[str, Any]]) -> str:
    if not changes:
        return ""
    lines = ["[quick_decision]"]
    for item in changes:
        lines.append(f"{item['key']} = {_toml_value(item['proposed'])}")
    return "\n".join(lines)


def _suggested_diff(changes: list[dict[str, Any]]) -> str:
    if not changes:
        return ""
    lines = ["[quick_decision]"]
    for item in changes:
        lines.append(f"-{item['key']} = {_toml_value(item['current'])}")
        lines.append(f"+{item['key']} = {_toml_value(item['proposed'])}")
    return "\n".join(lines)


def _toml_value(value: Any) -> str:
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _unique_codes(symbols: list[str]) -> list[str]:
    out: list[str] = []
    for raw in symbols:
        code = normalize_symbol(str(raw or ""))
        if code and code not in out:
            out.append(code)
    return out


def _fetch_quotes(
    symbols: list[str],
    *,
    source: str,
    timeout: float,
) -> tuple[dict[str, dict[str, Any]], str, str]:
    sources = [source] if source in {"sina", "tencent"} else ["sina", "tencent"]
    warnings: list[str] = []
    for item in sources:
        try:
            if item == "sina":
                quotes = _fetch_sina_quotes(symbols, timeout=timeout)
            else:
                quotes = _fetch_tencent_quotes(symbols, timeout=timeout)
            if quotes:
                return quotes, item, "; ".join(warnings)
            warnings.append(f"{item}: empty")
        except Exception as exc:
            warnings.append(f"{item}: {exc}")
    return {}, "none", "; ".join(warnings)


def _fetch_sina_quotes(symbols: list[str], *, timeout: float) -> dict[str, dict[str, Any]]:
    import re

    import requests

    if not symbols:
        return {}
    session = requests.Session()
    session.trust_env = False
    session.proxies = {"http": None, "https": None}
    session.headers.update({"User-Agent": "Mozilla/5.0", "Referer": "https://finance.sina.com.cn"})
    url = "http://hq.sinajs.cn/list=" + ",".join(to_sina_code(symbol) for symbol in symbols)
    resp = session.get(url, timeout=max(0.2, float(timeout)))
    resp.encoding = "gbk"
    if resp.status_code != 200:
        raise RuntimeError(f"HTTP {resp.status_code}")
    out: dict[str, dict[str, Any]] = {}
    pattern = re.compile(r'var hq_str_([a-z]{2}\d+)="(.*)";')
    for line in resp.text.strip().splitlines():
        match = pattern.match(line.strip())
        if not match:
            continue
        full_code, body = match.groups()
        parts = body.split(",")
        if len(parts) < 32 or not parts[0]:
            continue
        prev_close = _num(parts[2])
        price = _num(parts[3])
        change_pct = ((price - prev_close) / prev_close * 100) if price and prev_close else 0.0
        code = normalize_symbol(full_code)
        out[code] = {
            "code": code,
            "source_code": full_code,
            "name": parts[0],
            "open": _num(parts[1]),
            "prev_close": prev_close,
            "price": price,
            "high": _num(parts[4]),
            "low": _num(parts[5]),
            "volume": int(_num(parts[8]) or 0),
            "amount": _num(parts[9]),
            "change_pct": round(change_pct, 2),
            "quote_time": f"{parts[30]} {parts[31]}" if len(parts) > 31 else "",
        }
    return out


def _fetch_tencent_quotes(symbols: list[str], *, timeout: float) -> dict[str, dict[str, Any]]:
    import requests

    from modules.crawler.tencent import _parse_tencent_response

    if not symbols:
        return {}
    query = ",".join(to_tencent_code(symbol) for symbol in symbols)
    session = requests.Session()
    session.trust_env = False
    session.proxies = {"http": None, "https": None}
    resp = session.get("http://qt.gtimg.cn/q", params={"q": query}, timeout=max(0.2, float(timeout)))
    resp.encoding = "gbk"
    if resp.status_code != 200:
        raise RuntimeError(f"HTTP {resp.status_code}")
    out: dict[str, dict[str, Any]] = {}
    for row in _parse_tencent_response(resp.text):
        code = normalize_symbol(str(row.get("code") or ""))
        if not code:
            continue
        out[code] = {
            "code": code,
            "name": row.get("name", ""),
            "open": row.get("open"),
            "prev_close": row.get("prev_close"),
            "price": row.get("price"),
            "high": row.get("high"),
            "low": row.get("low"),
            "volume": row.get("volume"),
            "amount": row.get("amount"),
            "turnover": row.get("turnover"),
            "change_pct": row.get("pct_change"),
            "quote_time": row.get("datetime", ""),
        }
    return out


def _load_daily_cache(
    symbols: list[str],
    *,
    days: int = 80,
    db_path: str | Path | None = None,
) -> dict[str, list[dict[str, Any]]]:
    warehouse_db = _resolve_db_path(db_path)
    if not warehouse_db.exists() or not symbols:
        return {}
    start = (date.today() - timedelta(days=days * 2)).isoformat()
    placeholders = ",".join("?" for _ in symbols)
    query = (
        "SELECT symbol, trade_date, open, high, low, close, volume, amount, turn, pct_change "
        f"FROM price_daily WHERE symbol IN ({placeholders}) AND adjust=? AND trade_date>=? "
        "ORDER BY symbol, trade_date"
    )
    params = [*symbols, "qfq", start]
    out: dict[str, list[dict[str, Any]]] = {}
    try:
        with sqlite3.connect(warehouse_db) as conn:
            conn.row_factory = sqlite3.Row
            for row in conn.execute(query, params):
                code = normalize_symbol(str(row["symbol"]))
                out.setdefault(code, []).append(
                    {
                        "date": row["trade_date"],
                        "open": _num(row["open"]),
                        "high": _num(row["high"]),
                        "low": _num(row["low"]),
                        "close": _num(row["close"]),
                        "volume": _num(row["volume"]),
                        "amount": _num(row["amount"]),
                        "turn": _num(row["turn"]),
                        "pct_change": _num(row["pct_change"]),
                    }
                )
    except Exception:
        return {}
    return out


def _resolve_db_path(db_path: str | Path | None = None) -> Path:
    if db_path:
        return Path(db_path)
    try:
        from modules.config import cfg

        return Path(cfg().data.warehouse_path)
    except Exception:
        return _WAREHOUSE_DB


def load_watchlist_symbols(path: str | Path | None = None) -> list[str]:
    """Load symbols from the latest swing watchlist."""
    watchlist_path = Path(path) if path else _DEFAULT_WATCHLIST
    if not watchlist_path.exists():
        return []
    try:
        payload = json.loads(watchlist_path.read_text(encoding="utf-8"))
    except Exception:
        return []

    symbols: list[str] = []
    for bucket in ("positions", "candidates", "raw_candidates"):
        rows = payload.get(bucket) or []
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            symbol = row.get("symbol") or row.get("code") or row.get("stock_code")
            code = normalize_symbol(str(symbol or ""))
            if code:
                symbols.append(code)
    return _unique_codes(symbols)


def _decision_for(
    code: str,
    quote: dict[str, Any],
    daily: list[dict[str, Any]],
    tags: list[str],
    event_factor: dict[str, Any] | None = None,
    settings: Any | None = None,
) -> dict[str, Any]:
    settings = settings or _quick_settings()
    price = _num(quote.get("price"))
    prev_close = _num(quote.get("prev_close"))
    pct = _num(quote.get("change_pct"))
    if not pct and price and prev_close:
        pct = (price / prev_close - 1.0) * 100

    metrics = _technical_metrics(daily, quote)
    warnings: list[str] = []
    reasons: list[str] = []
    action = "观察"
    recommendation = "先观察，不直接追"
    score = float(settings.base_score)

    if not price:
        action = "不买"
        recommendation = "行情缺失，不能下单"
        warnings.append("未取到有效实时价格")
        score = 0.0
    else:
        score += _pct_score(pct, settings)
        ma5_gap = metrics.get("ma5_gap_pct")
        high20_gap = metrics.get("high20_gap_pct")
        above_ma5 = metrics.get("above_ma5")
        above_ma20 = metrics.get("above_ma20")

        if pct >= 8.5:
            warnings.append("涨幅接近涨停，追高回撤风险很大")
            score += float(settings.near_limit_up_penalty)
        elif pct >= 5.5:
            warnings.append("早盘涨幅过大，优先等回踩")
            score += float(settings.early_surge_penalty)
        elif 2.0 <= pct < 5.5:
            reasons.append("盘中强度在线")
            score += float(settings.intraday_strength_bonus)
        elif -1.0 <= pct < 2.0:
            reasons.append("涨幅不极端，仍有观察空间")
            score += float(settings.calm_bonus)
        elif pct <= -3.0:
            warnings.append("当日跌幅偏大，先避开")
            score += float(settings.daily_drop_penalty)

        if ma5_gap is not None:
            if ma5_gap > 6.0:
                warnings.append(f"现价高于5日线 {ma5_gap:.1f}%，位置偏急")
                score += float(settings.ma5_extended_penalty)
            elif -1.5 <= ma5_gap <= 3.5:
                reasons.append("距离5日线不远")
                score += float(settings.ma5_near_bonus)
        if high20_gap is not None:
            if high20_gap > -1.0 and pct >= 3.0:
                warnings.append("贴近20日高点且已拉升，适合等确认")
                score += float(settings.high20_risk_penalty)
            elif high20_gap <= -5.0 and pct >= 0:
                reasons.append("距离20日高点仍有空间")
                score += float(settings.high20_room_bonus)
        if above_ma5:
            reasons.append("站上5日线")
            score += float(settings.above_ma5_bonus)
        if above_ma20 is False:
            warnings.append("仍在20日线下方，中期趋势未完全修复")
            score += float(settings.below_ma20_penalty)

        factor_delta, factor_reason, factor_warning = _event_factor_score(event_factor, settings)
        score += factor_delta
        if factor_reason:
            reasons.append(factor_reason)
        if factor_warning:
            warnings.append(factor_warning)

        if score >= float(settings.buy_score_threshold) and pct < 5.5:
            action = "可轻仓观察"
            recommendation = "只适合回踩承接，不建议市价猛追"
        elif score >= float(settings.watch_score_threshold):
            action = "观察"
            recommendation = "等回踩或尾盘确认"
        else:
            action = "不买"
            recommendation = "当前位置不适合新开仓"

    buy_zone = _buy_zone(price, prev_close, metrics, action)
    return {
        "symbol": code,
        "name": quote.get("name") or code,
        "action": action,
        "score": round(max(0.0, min(100.0, score)), 1),
        "recommendation": recommendation,
        "buy_zone": buy_zone,
        "quote": {
            "price": round(price, 3) if price else None,
            "prev_close": round(prev_close, 3) if prev_close else None,
            "change_pct": round(pct, 2),
            "open": _round(quote.get("open")),
            "high": _round(quote.get("high")),
            "low": _round(quote.get("low")),
            "amount": _round(quote.get("amount")),
            "quote_time": quote.get("quote_time", ""),
        },
        "metrics": metrics,
        "tags": tags[:8],
        "event_factor": event_factor or {},
        "reasons": reasons[:5],
        "warnings": warnings[:5],
    }


def _event_factor_score(event_factor: dict[str, Any] | None, settings: Any | None = None) -> tuple[float, str, str]:
    settings = settings or _quick_settings()
    if not event_factor:
        return 0.0, "", ""
    sector = str(event_factor.get("sector") or "")
    topic = str(event_factor.get("top_topic") or "")
    score = _num(event_factor.get("factor_score"))
    direction = str(event_factor.get("direction") or "neutral")
    if event_factor.get("status") in {"stale", "future"}:
        updated_at = str(event_factor.get("updated_at") or "")
        status_text = "晚于回放日" if event_factor.get("status") == "future" else "已过期"
        return 0.0, "", f"事件因子{status_text}（{updated_at}），不参与加分"
    label = f"{sector}/{topic}" if topic else sector
    if direction == "bearish" and score >= 8:
        return float(settings.event_bearish_penalty), "", f"事件因子偏空：{label} {score:.1f}"
    if direction == "bullish":
        if score >= 18:
            return float(settings.event_strong_bonus), f"事件因子强势：{label} {score:.1f}", ""
        if score >= 10:
            return float(settings.event_support_bonus), f"事件因子支持：{label} {score:.1f}", ""
    return 0.0, "", ""


def _technical_metrics(daily: list[dict[str, Any]], quote: dict[str, Any]) -> dict[str, Any]:
    rows = [row for row in daily if _num(row.get("close"))]
    price = _num(quote.get("price"))
    if price:
        today = date.today().isoformat()
        current = {
            "date": today,
            "open": _num(quote.get("open")) or price,
            "high": _num(quote.get("high")) or price,
            "low": _num(quote.get("low")) or price,
            "close": price,
            "volume": _num(quote.get("volume")),
            "amount": _num(quote.get("amount")),
        }
        if rows and str(rows[-1].get("date")) == today:
            rows[-1] = current
        else:
            rows.append(current)
    closes = [_num(row.get("close")) for row in rows if _num(row.get("close"))]
    highs = [_num(row.get("high")) or _num(row.get("close")) for row in rows if _num(row.get("close"))]
    if len(closes) < 5:
        return {"daily_bars": len(closes)}
    last = closes[-1]
    ma5 = sum(closes[-5:]) / 5
    ma10 = sum(closes[-10:]) / 10 if len(closes) >= 10 else None
    ma20 = sum(closes[-20:]) / 20 if len(closes) >= 20 else None
    high20 = max(highs[-20:]) if len(highs) >= 20 else max(highs)
    return {
        "daily_bars": len(closes),
        "ma5": round(ma5, 3),
        "ma10": round(ma10, 3) if ma10 else None,
        "ma20": round(ma20, 3) if ma20 else None,
        "high20": round(high20, 3) if high20 else None,
        "ma5_gap_pct": round((last / ma5 - 1.0) * 100, 2) if ma5 else None,
        "ma20_gap_pct": round((last / ma20 - 1.0) * 100, 2) if ma20 else None,
        "high20_gap_pct": round((last / high20 - 1.0) * 100, 2) if high20 else None,
        "above_ma5": last >= ma5 if ma5 else None,
        "above_ma20": last >= ma20 if ma20 else None,
    }


def _pct_score(pct: float, settings: Any | None = None) -> float:
    settings = settings or _quick_settings()
    if -0.5 <= pct <= 2.5:
        return float(settings.pct_neutral_bonus)
    if 2.5 < pct <= 5.5:
        return float(settings.pct_strong_bonus)
    if -3.0 <= pct < -0.5:
        return float(settings.pct_weak_bonus)
    if 5.5 < pct <= 8.5:
        return float(settings.pct_high_penalty)
    return float(settings.pct_extreme_penalty)


def _buy_zone(price: float, prev_close: float, metrics: dict[str, Any], action: str) -> str:
    if not price or action == "不买":
        return "无"
    anchors = []
    ma5 = metrics.get("ma5")
    if ma5:
        anchors.append(float(ma5))
    if prev_close:
        anchors.append(float(prev_close))
    if not anchors:
        return f"{price:.2f} 下方回踩承接"
    low = min(anchors)
    high = max(anchors)
    if high < price:
        return f"{low:.2f}-{high:.2f} 回踩不破再看"
    return f"{low:.2f}-{min(price, high):.2f} 分批观察"


def _code_tags(symbols: list[str]) -> dict[str, list[str]]:
    global _TAG_CACHE
    if _TAG_CACHE is None:
        _TAG_CACHE = _load_sector_graph_tags()
    return {code: _TAG_CACHE.get(code, []) for code in symbols}


def _load_sector_graph_tags() -> dict[str, list[str]]:
    if not _SECTOR_GRAPH.exists():
        return {}
    try:
        raw = tomllib.loads(_SECTOR_GRAPH.read_text(encoding="utf-8"))
    except Exception:
        return {}
    out: dict[str, list[str]] = {}
    for node in raw.get("nodes", []):
        tag = str(node.get("tag") or "").strip()
        if not tag:
            continue
        for member in node.get("members", []):
            code = normalize_symbol(str(member.get("code") or ""))
            if code:
                out.setdefault(code, [])
                if tag not in out[code]:
                    out[code].append(tag)
    return out


def _event_factors_by_code(
    symbols: list[str],
    tags_by_code: dict[str, list[str]],
    *,
    db_path: str | Path | None = None,
    max_age_days: int = 5,
) -> dict[str, dict[str, Any]]:
    sectors = sorted({tag for code in symbols for tag in tags_by_code.get(code, [])})
    warehouse_db = _resolve_db_path(db_path)
    if not sectors or not warehouse_db.exists():
        return {}
    placeholders = ",".join("?" for _ in sectors)
    query = (
        "SELECT sector, factor_score, direction, insight_count, avg_value_score, "
        f"top_topic, top_titles_json, avg_time_decay, updated_at FROM event_news_factors "
        f"WHERE sector IN ({placeholders})"
    )
    factors: dict[str, dict[str, Any]] = {}
    try:
        with sqlite3.connect(warehouse_db) as conn:
            conn.row_factory = sqlite3.Row
            for row in conn.execute(query, sectors):
                factor = dict(row)
                factor["status"] = _event_factor_status(str(factor.get("updated_at") or ""), max_age_days)
                factors[str(factor.get("sector") or "")] = factor
    except Exception:
        return {}
    out: dict[str, dict[str, Any]] = {}
    for code in symbols:
        candidates = [factors[tag] for tag in tags_by_code.get(code, []) if tag in factors]
        if candidates:
            out[code] = max(candidates, key=lambda item: _num(item.get("factor_score")))
    return out


def _event_factor_status(updated_at: str, max_age_days: int) -> str:
    try:
        value = datetime.fromisoformat(str(updated_at).replace(" ", "T"))
    except Exception:
        return "unknown"
    age = datetime.now() - value
    return "stale" if age.days > max_age_days else "active"


def _event_factor_status_at(updated_at: str, *, as_of_date: str, max_age_days: int) -> str:
    try:
        value = datetime.fromisoformat(str(updated_at).replace(" ", "T"))
        as_of = datetime.fromisoformat(str(as_of_date)[:10] + "T23:59:59")
    except Exception:
        return "unknown"
    age = as_of - value
    if age.days < 0:
        return "future"
    return "stale" if age.days > max_age_days else "active"


def _normalize_date_text(value: str | None) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(text[:10] if fmt == "%Y-%m-%d" else text[:8], fmt).date().isoformat()
        except Exception:
            continue
    return ""


def _num(value: Any) -> float:
    try:
        if value in (None, "", "-", "None"):
            return 0.0
        return float(value)
    except Exception:
        return 0.0


def _round(value: Any, digits: int = 3) -> float | None:
    number = _num(value)
    return round(number, digits) if number else None


def print_quick_decision(payload: dict[str, Any]) -> None:
    summary = payload.get("summary") or {}
    print(
        "MoatX 极速盘中判断 | "
        f"行情源 {summary.get('source')} | "
        f"行情 {float(summary.get('quote_elapsed_seconds') or 0):.2f}s | "
        f"总耗时 {float(summary.get('elapsed_seconds') or 0):.2f}s"
    )
    if summary.get("warning"):
        print(f"数据提示: {summary.get('warning')}")
    if summary.get("run_id"):
        print(f"已记录: quick_decision_run_id={summary.get('run_id')}")
    for idx, row in enumerate(payload.get("decisions") or [], 1):
        quote = row.get("quote") or {}
        print(
            f"{idx}. {row.get('symbol')} {row.get('name')} | "
            f"{row.get('action')} {float(row.get('score') or 0):.1f} | "
            f"现价 {quote.get('price')} | 涨跌 {float(quote.get('change_pct') or 0):+.2f}% | "
            f"{row.get('recommendation')}"
        )
        buy_zone = row.get("buy_zone")
        if buy_zone and buy_zone != "无":
            print(f"   参考: {buy_zone}")
        tags = row.get("tags") or []
        if tags:
            print("   主题: " + "、".join(str(item) for item in tags[:5]))
        factor = row.get("event_factor") or {}
        if factor:
            status = str(factor.get("status") or "unknown")
            sector = str(factor.get("sector") or "")
            topic = str(factor.get("top_topic") or "")
            score = float(factor.get("factor_score") or 0)
            print(f"   事件: {sector}/{topic or '-'} {score:.1f} [{status}]")
        reasons = row.get("reasons") or []
        if reasons:
            print("   支持: " + "；".join(str(item) for item in reasons[:4]))
        warnings = row.get("warnings") or []
        if warnings:
            print("   风险: " + "；".join(str(item) for item in warnings[:4]))


def print_quick_decision_review(payload: dict[str, Any]) -> None:
    summary = payload.get("summary") or {}
    print(
        "MoatX 极速盘中判断复盘 | "
        f"T+{payload.get('horizon_days')} | "
        f"样本 {summary.get('evaluated')}/{summary.get('total')} | "
        f"待评估 {summary.get('pending')}"
    )
    print(
        f"平均收益 {float(summary.get('avg_forward_return_pct') or 0):+.2f}% | "
        f"买入胜率 {float(summary.get('buy_win_rate') or 0):.1f}% | "
        f"避开命中 {float(summary.get('avoid_hit_rate') or 0):.1f}%"
    )
    for item in payload.get("items") or []:
        forward = item.get("forward_return_pct")
        forward_text = "pending" if forward is None else f"{float(forward):+.2f}%"
        drawdown = item.get("max_drawdown_pct")
        drawdown_text = "-" if drawdown is None else f"{float(drawdown):+.2f}%"
        print(
            f"{item.get('id')}. {item.get('symbol')} {item.get('name')} | "
            f"{item.get('action')} {float(item.get('score') or 0):.1f} | "
            f"{item.get('outcome')} | 收益 {forward_text} | 最大回撤 {drawdown_text} | "
            f"{item.get('decision_at')} -> {item.get('exit_date') or '-'}"
        )


def print_quick_decision_evaluation(payload: dict[str, Any]) -> None:
    summary = payload.get("summary") or {}
    print(
        "MoatX 极速盘中判断自动评价 | "
        f"horizons={payload.get('horizons')} | "
        f"样本 {summary.get('evaluated')}/{summary.get('total')} | "
        f"待评估 {summary.get('pending')} | "
        f"保存 {payload.get('saved')}"
    )
    for result in payload.get("results") or []:
        item = result.get("summary") or {}
        print(
            f"T+{result.get('horizon_days')}: "
            f"样本 {item.get('evaluated')}/{item.get('total')} | "
            f"待评估 {item.get('pending')} | "
            f"保存 {result.get('saved')}"
        )


def print_quick_decision_summary(payload: dict[str, Any]) -> None:
    summary = payload.get("summary") or {}
    print(
        "MoatX 极速盘中判断评价面板 | "
        f"T+{payload.get('horizon_days')} | "
        f"样本 {summary.get('count')} | "
        f"成功率 {float(summary.get('success_rate') or 0):.1f}% | "
        f"均值 {float(summary.get('avg_forward_return_pct') or 0):+.2f}% | "
        f"回撤 {float(summary.get('avg_max_drawdown_pct') or 0):+.2f}%"
    )
    sections = [
        ("动作", payload.get("by_action") or []),
        ("分数段", payload.get("by_score_bucket") or []),
        ("主题", payload.get("by_tag") or []),
        ("事件板块", payload.get("by_event_sector") or []),
    ]
    for title, rows in sections:
        print(f"{title}:")
        for row in rows[:8]:
            print(
                f"  {row.get('key')}: n={row.get('count')} "
                f"成功率={float(row.get('success_rate') or 0):.1f}% "
                f"均值={float(row.get('avg_forward_return_pct') or 0):+.2f}% "
                f"回撤={float(row.get('avg_max_drawdown_pct') or 0):+.2f}%"
            )


def print_quick_decision_sample(payload: dict[str, Any]) -> None:
    summary = payload.get("summary") or {}
    sample = summary.get("sample") or {}
    print(
        "MoatX 极速决策样本采集 | "
        f"入库 {sample.get('sampled', 0)} | "
        f"候选 {sample.get('requested', 0)} | "
        f"同日限频跳过 {sample.get('skipped_same_day_limit', 0)}"
    )
    if summary.get("run_id"):
        print(f"已记录: quick_decision_run_id={summary.get('run_id')}")
    counts = sample.get("source_counts") or {}
    if counts:
        print("来源: " + "；".join(f"{key}={value}" for key, value in counts.items()))
    warnings = sample.get("warnings") or []
    if warnings:
        print("采样提示: " + "；".join(str(item) for item in warnings[:5]))


def print_quick_decision_backfill(payload: dict[str, Any]) -> None:
    summary = payload.get("summary") or {}
    print(
        "MoatX 极速决策历史回放采样 | "
        f"{summary.get('start_date')} -> {summary.get('end_date')} | "
        f"交易日 {summary.get('trade_dates', 0)} | "
        f"入库 {summary.get('rows', 0)} | "
        f"runs {summary.get('runs', 0)}"
    )
    print(
        f"标的 {summary.get('replay_symbols', 0)}/{summary.get('requested_symbols', 0)} | "
        f"缺日线跳过 {summary.get('skipped_missing_daily', 0)} | "
        f"重复跳过 {summary.get('skipped_duplicate', 0)}"
    )
    run_ids = summary.get("run_ids") or []
    if run_ids:
        print("已记录 run_id: " + "，".join(str(item) for item in run_ids[:10]))
    evaluation = payload.get("evaluation") or {}
    if evaluation:
        item = evaluation.get("summary") or {}
        print(
            "自动评价: "
            f"样本 {item.get('evaluated', 0)}/{item.get('total', 0)} | "
            f"待评估 {item.get('pending', 0)} | "
            f"保存 {evaluation.get('saved', 0)}"
        )


def print_quick_decision_learn(payload: dict[str, Any]) -> None:
    summary = payload.get("summary") or {}
    diagnostics = payload.get("diagnostics") or {}
    print(
        "MoatX 极速决策学习诊断 | "
        f"T+{payload.get('horizon_days')} | "
        f"样本 {summary.get('count', 0)} | "
        f"成功率 {float(summary.get('success_rate') or 0):.1f}% | "
        f"均值 {float(summary.get('avg_forward_return_pct') or 0):+.2f}%"
    )
    sections = [
        ("低胜率动作", diagnostics.get("low_win_rate_actions") or []),
        ("亏损分数段", diagnostics.get("losing_score_buckets") or []),
        ("低效主题", diagnostics.get("low_efficiency_tags") or []),
        ("失效事件因子", diagnostics.get("weak_event_factors") or []),
    ]
    for title, rows in sections:
        print(f"{title}:")
        if not rows:
            print("  暂无显著弱项")
            continue
        for row in rows[:6]:
            print(
                f"  {row.get('key')}: n={row.get('count')} "
                f"成功率={float(row.get('success_rate') or 0):.1f}% "
                f"均值={float(row.get('avg_forward_return_pct') or 0):+.2f}% "
                f"回撤={float(row.get('avg_max_drawdown_pct') or 0):+.2f}%"
            )
    print("建议观察/调权方向:")
    for item in diagnostics.get("recommendations") or []:
        print(f"  - {item}")

    suggested = payload.get("suggested_config") or {}
    changes = suggested.get("changes") or []
    if changes:
        print("建议配置片段:")
        print(suggested.get("toml") or "")
        print("证据与风险:")
        for item in changes:
            print(
                f"  - {item.get('key')}: {item.get('current')} -> {item.get('proposed')} | "
                f"证据样本 {item.get('evidence_samples')} | {item.get('expected_impact')} "
                f"风险: {item.get('risk')}"
            )
    elif suggested:
        for note in suggested.get("notes") or []:
            print(f"配置建议: {note}")


def _parse_horizons(value: str | None) -> list[int]:
    if not value:
        return [1, 3, 5]
    out: list[int] = []
    for part in str(value).split(","):
        try:
            number = int(part.strip())
        except Exception:
            continue
        if number > 0 and number not in out:
            out.append(number)
    return out or [1, 3, 5]


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="python -m modules.quick_decision")
    parser.add_argument("symbols", nargs="*", help="股票代码，如 601899 000063；或 review/evaluate/summary")
    parser.add_argument("--source", choices=["auto", "sina", "tencent"], default="auto", help="行情源")
    parser.add_argument("--timeout", type=float, default=1.2, help="单源请求超时秒数")
    parser.add_argument("--no-tags", action="store_true", help="不读取本地主题图谱")
    parser.add_argument("--no-event-factors", action="store_true", help="不读取新闻事件因子")
    parser.add_argument("--watchlist", action="store_true", help="追加读取短线观察名单")
    parser.add_argument("--watchlist-file", help="观察名单 JSON 路径，默认 data/swing_watchlist_latest.json")
    parser.add_argument("--sources", default="", help="采样来源，逗号分隔: watchlist,fusion,event")
    parser.add_argument("--replay-source", default="event", help="历史回放标的来源: event/watchlist，或逗号组合")
    parser.add_argument("--start", dest="start_date", default="", help="历史回放开始日期 YYYY-MM-DD")
    parser.add_argument("--end", dest="end_date", default="", help="历史回放结束日期 YYYY-MM-DD")
    parser.add_argument("--event-factor-max-age-days", type=int, default=None, help="历史回放事件因子有效期天数")
    parser.add_argument("--no-save", action="store_true", help="不写入决策日志")
    parser.add_argument("--review", action="store_true", help="复盘历史 quick-decision 结果")
    parser.add_argument("--limit", type=int, default=20, help="复盘条数")
    parser.add_argument("--horizon", type=int, default=3, help="复盘未来交易日数量")
    parser.add_argument("--horizons", default="1,3,5", help="自动评价 horizon 列表，如 1,3,5")
    parser.add_argument("--save-evaluation", action="store_true", help="复盘时保存评价结果")
    parser.add_argument("--min-samples", type=int, default=1, help="汇总面板最小样本数")
    parser.add_argument("--max-per-symbol-per-day", type=int, default=None, help="同一标的同日最多采样次数")
    parser.add_argument("--min-event-score", type=float, default=None, help="事件机会入样最低分")
    parser.add_argument("--fusion-limit", type=int, default=None, help="融合候选入样数量")
    parser.add_argument("--fusion-pool-limit", type=int, default=None, help="融合候选扫描池大小")
    parser.add_argument("--fusion-deadline-seconds", type=float, default=None, help="融合候选扫描时间预算")
    parser.add_argument("--suggest-config", action="store_true", help="学习诊断时输出建议配置片段")
    parser.add_argument("--action", help="按决策动作过滤复盘，如 不买")
    parser.add_argument("--json", dest="as_json", action="store_true", help="JSON 格式输出")
    args = parser.parse_args(argv)

    mode = (
        args.symbols[0]
        if args.symbols
        and args.symbols[0]
        in {
            "review",
            "evaluate",
            "summary",
            "dashboard",
            "sample",
            "collect-samples",
            "backfill-samples",
            "replay-samples",
            "learn",
        }
        else ""
    )
    review_mode = args.review or mode == "review"
    evaluate_mode = mode == "evaluate"
    summary_mode = mode in {"summary", "dashboard"}
    review_symbols = args.symbols[1:] if mode else args.symbols
    if mode in {"backfill-samples", "replay-samples"}:
        if not args.start_date:
            parser.error("历史回放需要 --start YYYY-MM-DD")
        payload = backfill_quick_decision_samples(
            review_symbols,
            start_date=args.start_date,
            end_date=args.end_date or args.start_date,
            source=args.replay_source,
            limit=args.limit,
            watchlist_file=args.watchlist_file,
            min_event_score=args.min_event_score,
            include_tags=not args.no_tags,
            include_event_factors=not args.no_event_factors,
            event_factor_max_age_days=args.event_factor_max_age_days,
            save=not args.no_save,
            evaluate_horizons=_parse_horizons(args.horizons) if args.save_evaluation else None,
        )
        if args.as_json:
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            print_quick_decision_backfill(payload)
        return
    if mode in {"sample", "collect-samples"}:
        payload = sample_quick_decisions(
            sources=_normalize_sources(args.sources) if args.sources else None,
            limit=args.limit,
            max_per_symbol_per_day=args.max_per_symbol_per_day,
            source=args.source,
            timeout=args.timeout,
            include_tags=not args.no_tags,
            include_event_factors=not args.no_event_factors,
            watchlist_file=args.watchlist_file,
            min_event_score=args.min_event_score,
            fusion_limit=args.fusion_limit,
            fusion_pool_limit=args.fusion_pool_limit,
            fusion_deadline_seconds=args.fusion_deadline_seconds,
            save=not args.no_save,
        )
        if args.as_json:
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            print_quick_decision_sample(payload)
        return
    if mode == "learn":
        payload = learn_quick_decision(
            horizon_days=args.horizon,
            limit=args.limit,
            min_samples=max(1, int(args.min_samples or 1)),
            suggest_config=args.suggest_config,
        )
        if args.as_json:
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            print_quick_decision_learn(payload)
        return
    if summary_mode:
        payload = summarize_quick_decision_evaluations(
            horizon_days=args.horizon,
            limit=args.limit,
            min_samples=args.min_samples,
        )
        if args.as_json:
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            print_quick_decision_summary(payload)
        return
    if evaluate_mode or args.save_evaluation:
        payload = evaluate_quick_decisions(
            review_symbols,
            horizons=_parse_horizons(args.horizons),
            limit=args.limit,
            action=args.action,
            save=True,
        )
        if args.as_json:
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            print_quick_decision_evaluation(payload)
        return
    if review_mode:
        payload = review_quick_decisions(
            review_symbols,
            limit=args.limit,
            horizon_days=args.horizon,
            action=args.action,
        )
        if args.as_json:
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        else:
            print_quick_decision_review(payload)
        return
    if not args.symbols:
        if not args.watchlist:
            parser.error("请提供股票代码，或使用 --watchlist / --review")
    symbols = list(args.symbols)
    if args.watchlist:
        symbols = _unique_codes([*symbols, *load_watchlist_symbols(args.watchlist_file)])
    payload = build_quick_decision(
        symbols,
        source=args.source,
        timeout=args.timeout,
        include_tags=not args.no_tags,
        include_event_factors=not args.no_event_factors,
    )
    if args.watchlist:
        payload["summary"]["watchlist_path"] = str(Path(args.watchlist_file) if args.watchlist_file else _DEFAULT_WATCHLIST)
    if not args.no_save:
        payload["summary"]["run_id"] = save_quick_decision(payload)
    if args.as_json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print_quick_decision(payload)


if __name__ == "__main__":
    main()
