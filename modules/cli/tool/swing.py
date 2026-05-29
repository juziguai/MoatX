"""CLI for low-absorb next-day swing scans."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any


def cmd_swing(args) -> None:
    if not getattr(args, "verbose", False):
        logging.disable(logging.WARNING)

    from modules.swing_low_absorb import LowAbsorbSwingEngine

    engine = LowAbsorbSwingEngine()
    action = args.swing_action
    if action == "analyze":
        market_context = None
        sector_context = None
        quote = None
        if not args.no_context:
            spot = engine._spot_snapshot()
            market_context = engine._market_context(spot)
            sector_context = engine._sector_context([args.symbol], spot=spot)
            from modules.utils import normalize_symbol

            quote = engine._quote_snapshot([args.symbol]).get(normalize_symbol(args.symbol))
        payload: dict[str, Any] | list[dict[str, Any]] = engine.analyze(
            args.symbol,
            name=args.name or "",
            quote=quote,
            check_risk=not args.no_risk,
            market_context=market_context,
            sector_context=sector_context,
        )
    elif action == "candidates":
        payload = engine.candidates(
            limit=args.limit,
            pool_limit=args.pool_limit,
            check_risk=args.check_risk,
            workers=args.workers,
            deadline_seconds=args.deadline_seconds,
            network_daily_fallback=not args.no_network_daily,
            allow_breakout=not args.no_breakout,
        )
    elif action == "watchlist":
        payload = engine.generate_watchlist(
            limit=args.limit,
            pool_limit=args.pool_limit,
            workers=args.workers,
            min_score=args.min_score,
            cash_per_stock=args.cash_per_stock,
            lot_size=args.lot_size,
            check_risk=args.check_risk,
            include_watch=not args.candidate_only,
            deadline_seconds=args.deadline_seconds,
            network_daily_fallback=args.network_daily_fallback,
            score_gate=not args.no_score_gate,
            min_comprehensive_score=args.min_comprehensive_score,
            allow_breakout=not args.no_breakout,
            output=args.output,
        )
        if args.send:
            from modules.alerter import Alerter

            Alerter().send(_format_watchlist(payload), "MoatX 明日短线观察名单")
    elif action == "monitor":
        payload = engine.monitor_watchlist(
            watchlist_path=args.watchlist,
            market_hours_only=not args.ignore_market_hours,
        )
        if args.send and payload.get("alerts"):
            from modules.alerter import Alerter

            Alerter().send(_format_monitor(payload), "MoatX 短线目标/止损提醒")
    elif action == "backtest":
        symbols = _resolve_symbols(args)
        payload = engine.backtest(
            start_date=args.start,
            end_date=args.end,
            symbols=symbols or None,
            universe_limit=args.universe_limit,
            pool_limit=args.pool_limit,
            top_n=args.top_n,
            min_score=args.min_score,
            cash_per_trade=args.cash_per_trade,
            initial_capital=args.initial_capital,
            lot_size=args.lot_size,
            workers=args.workers,
            lookback_days=args.lookback_days,
            check_risk=args.check_risk,
            include_watch=not args.candidate_only,
            slippage_pct=args.slippage_pct,
            intraday_policy=args.intraday_policy,
            use_event_context=not args.no_event_context,
            allow_breakout=not args.no_breakout,
        )
        if args.output:
            Path(args.output).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    elif action == "validate":
        symbols = _resolve_symbols(args)
        payload = _run_validation_matrix(engine, args, symbols)
        if args.output:
            Path(args.output).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    elif action == "diagnose":
        symbols = _resolve_symbols(args)
        payload = _run_diagnosis(engine, args, symbols)
        if args.output:
            output = Path(args.output)
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    elif action == "freeze-universe":
        if not getattr(args, "output", ""):
            from datetime import datetime

            args.output = f"data/swing_universe_fixed_{datetime.now():%Y%m%d}.json"
        payload = _freeze_universe(engine, args)
        if args.output:
            output = Path(args.output)
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    elif action == "paper":
        rows = engine.candidates(
            limit=args.limit,
            pool_limit=args.pool_limit,
            check_risk=args.check_risk,
            workers=args.workers,
            allow_breakout=not args.no_breakout,
        )
        payload = engine.build_paper_account(
            rows,
            cash_per_stock=args.cash_per_stock,
            lot_size=args.lot_size,
        )
        if args.output:
            Path(args.output).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    else:
        raise SystemExit(f"Unknown swing action: {action}")

    if args.as_json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return

    if action == "freeze-universe":
        _print_frozen_universe(payload, getattr(args, "output", ""))
    elif action == "diagnose":
        _print_diagnosis(payload)
    elif action in {"paper", "backtest", "watchlist", "validate"} and getattr(args, "output", None):
        print(f"written: {args.output}")
    elif isinstance(payload, list):
        _print_candidates(payload)
    elif action == "watchlist":
        print(_format_watchlist(payload))
    elif action == "monitor":
        print(_format_monitor(payload))
    elif action == "backtest":
        _print_backtest(payload)
    elif action == "validate":
        _print_validation(payload)
    elif action == "paper":
        _print_paper_account(payload)
    else:
        _print_plan(payload)


def _parse_symbols_arg(value: str) -> list[str]:
    from modules.market_filters import is_excluded_selection_board
    from modules.utils import normalize_symbol

    seen: set[str] = set()
    symbols: list[str] = []
    for raw in str(value or "").replace("，", ",").replace("\n", ",").split(","):
        code = normalize_symbol(raw.strip())
        if not code or code in seen or is_excluded_selection_board(code):
            continue
        seen.add(code)
        symbols.append(code)
    return symbols


def _symbols_from_item(item: Any) -> list[str]:
    if isinstance(item, str):
        return _parse_symbols_arg(item)
    if isinstance(item, dict):
        for key in ("code", "symbol", "ts_code", "stock_code"):
            value = item.get(key)
            if value:
                return _parse_symbols_arg(str(value))
    return []


def _load_symbols_file(path: str | Path) -> list[str]:
    source = Path(path)
    if not source.exists():
        raise SystemExit(f"symbols file not found: {source}")
    text = source.read_text(encoding="utf-8-sig").strip()
    if not text:
        return []

    symbols: list[str] = []
    if source.suffix.lower() == ".json" or text[:1] in {"[", "{"}:
        data = json.loads(text)
        if isinstance(data, dict):
            for key in ("symbols", "universe", "stocks", "codes"):
                if key in data:
                    data = data[key]
                    break
        if isinstance(data, list):
            for item in data:
                symbols.extend(_symbols_from_item(item))
        else:
            symbols.extend(_symbols_from_item(data))
    else:
        for line in text.splitlines():
            line = line.split("#", 1)[0].strip()
            if line:
                symbols.extend(_parse_symbols_arg(line))

    return _parse_symbols_arg(",".join(symbols))


def _resolve_symbols(args: Any) -> list[str]:
    symbols = _parse_symbols_arg(getattr(args, "symbols", ""))
    symbols_file = str(getattr(args, "symbols_file", "") or "").strip()
    if symbols_file:
        symbols.extend(_load_symbols_file(symbols_file))
    return _parse_symbols_arg(",".join(symbols))


def _freeze_universe(engine: Any, args: Any) -> dict[str, Any]:
    from datetime import datetime

    target_limit = max(1, int(args.universe_limit or 1))
    source_limit = int(getattr(args, "source_limit", 0) or 0)
    if source_limit <= 0:
        source_limit = target_limit * 2
    source_limit = max(target_limit, source_limit)
    raw_universe, notes = engine._backtest_universe(symbols=None, universe_limit=source_limit)
    if getattr(args, "no_quality_filter", False):
        universe = raw_universe[:target_limit]
        rejected: list[dict[str, Any]] = []
        quality = {
            "enabled": False,
            "target_count": target_limit,
            "source_limit": source_limit,
            "raw_count": len(raw_universe),
        }
    else:
        universe, rejected, quality = _quality_filter_universe(engine, args, raw_universe, target_limit)
        notes = [*notes, "quality_filtered"]
    symbols = [str(row.get("code") or "") for row in universe if row.get("code")]
    return {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "source": notes[0] if notes else "unknown",
        "universe_limit": target_limit,
        "source_limit": source_limit,
        "raw_count": len(raw_universe),
        "count": len(symbols),
        "notes": notes,
        "quality": quality,
        "symbols": symbols,
        "universe": universe,
        "rejected": rejected,
    }


def _quality_filter_universe(
    engine: Any,
    args: Any,
    raw_universe: list[dict[str, Any]],
    target_limit: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    from collections import Counter
    from datetime import datetime, timedelta

    from modules.market_filters import market_board
    from modules.utils import normalize_symbol

    min_bars = max(36, int(getattr(args, "min_daily_bars", 60) or 60))
    lookback_days = max(min_bars * 2, int(getattr(args, "quality_lookback_days", 260) or 260))
    end_text = str(getattr(args, "quality_end", "") or "").strip()
    end_dt = datetime.strptime(end_text, "%Y-%m-%d") if end_text else datetime.now()
    load_start_dt = end_dt - timedelta(days=lookback_days)
    load_start = load_start_dt.strftime("%Y%m%d")
    load_end = end_dt.strftime("%Y%m%d")
    stale_cutoff = end_dt - timedelta(days=max(10, int(getattr(args, "stale_days", 20) or 20)))

    rejected: list[dict[str, Any]] = []
    candidates: list[dict[str, str]] = []
    seen: set[str] = set()
    for row in raw_universe:
        code = normalize_symbol(str(row.get("code") or row.get("symbol") or ""))
        name = str(row.get("name") or code)
        board = market_board(code)
        if not code or code in seen:
            continue
        seen.add(code)
        if board != "Main":
            rejected.append({"code": code, "name": name, "reason": "board_excluded", "detail": board})
            continue
        candidates.append({"code": code, "name": name})

    history = engine._load_backtest_daily_history(
        candidates,
        load_start=load_start,
        load_end=load_end,
        workers=getattr(args, "workers", 4),
    )
    qualified: list[dict[str, Any]] = []
    for row in candidates:
        code = normalize_symbol(str(row.get("code") or ""))
        daily = history.get(code)
        if daily is None or daily.empty:
            rejected.append({"code": code, "name": row.get("name", ""), "reason": "no_daily_data"})
            continue
        bars = int(len(daily))
        if bars < min_bars:
            rejected.append(
                {
                    "code": code,
                    "name": row.get("name", ""),
                    "reason": "too_short_history",
                    "daily_bars": bars,
                }
            )
            continue
        latest = daily.index.max()
        if hasattr(latest, "to_pydatetime"):
            latest_dt = latest.to_pydatetime()
        else:
            latest_dt = latest
        if latest_dt < stale_cutoff:
            rejected.append(
                {
                    "code": code,
                    "name": row.get("name", ""),
                    "reason": "stale_daily_data",
                    "daily_bars": bars,
                    "daily_end": _date_text(latest),
                }
            )
            continue
        first = daily.index.min()
        qualified.append(
            {
                "code": code,
                "name": row.get("name", ""),
                "daily_bars": bars,
                "daily_start": _date_text(first),
                "daily_end": _date_text(latest),
            }
        )

    selected = qualified[:target_limit]
    reason_counts = Counter(str(item.get("reason") or "unknown") for item in rejected)
    quality = {
        "enabled": True,
        "target_count": target_limit,
        "source_limit": max(target_limit, int(getattr(args, "source_limit", 0) or target_limit * 2)),
        "raw_count": len(raw_universe),
        "board_checked_count": len(candidates),
        "qualified_count": len(qualified),
        "overflow_qualified_count": max(0, len(qualified) - len(selected)),
        "rejected_count": len(rejected),
        "rejected_by_reason": dict(sorted(reason_counts.items())),
        "min_daily_bars": min_bars,
        "lookback_days": lookback_days,
        "load_start": load_start,
        "load_end": load_end,
        "stale_cutoff": stale_cutoff.strftime("%Y-%m-%d"),
    }
    return selected, rejected, quality


def _date_text(value: Any) -> str:
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d")
    return str(value or "")


def _print_frozen_universe(payload: dict[str, Any], output: str = "") -> None:
    path_note = f" | 文件 {output}" if output else ""
    print(f"固定股票池已生成: {payload.get('count', 0)} 只{path_note}")
    quality = payload.get("quality") or {}
    if quality.get("enabled"):
        print(
            f"质量过滤: 源池 {quality.get('raw_count', 0)} | "
            f"合格 {quality.get('qualified_count', 0)} | "
            f"剔除 {quality.get('rejected_count', 0)} | "
            f"最少日线 {quality.get('min_daily_bars', 0)}"
        )
        reasons = quality.get("rejected_by_reason") or {}
        if reasons:
            reason_text = "，".join(f"{key}:{value}" for key, value in reasons.items())
            print(f"剔除原因: {reason_text}")
    sample = payload.get("universe") or []
    if sample:
        preview = "，".join(f"{row.get('code')} {row.get('name')}" for row in sample[:10])
        print(f"样例: {preview}")
    if not payload.get("symbols"):
        print("提示: 当前行情快照不可用，未能冻结股票池。")


def _action_label(action: Any) -> str:
    labels = {
        "candidate": "强候选",
        "watch": "观察",
        "skip": "剔除",
    }
    value = str(action or "")
    return labels.get(value, value or "未知")


def _run_validation_matrix(engine: Any, args: Any, symbols: list[str]) -> dict[str, Any]:
    variants: list[dict[str, Any]] = [
        {"name": "满血默认", "include_watch": True, "use_event_context": True, "allow_breakout": True},
        {"name": "满血强候选", "include_watch": False, "use_event_context": True, "allow_breakout": True},
        {"name": "满血强候选禁突破", "include_watch": False, "use_event_context": True, "allow_breakout": False},
        {"name": "无新闻强候选禁突破", "include_watch": False, "use_event_context": False, "allow_breakout": False},
        {"name": "无新闻默认", "include_watch": True, "use_event_context": False, "allow_breakout": True},
    ]
    matrix = engine.backtest_variants(
        variants=variants,
        start_date=args.start,
        end_date=args.end,
        symbols=symbols or None,
        universe_limit=args.universe_limit,
        pool_limit=args.pool_limit,
        top_n=args.top_n,
        min_score=args.min_score,
        cash_per_trade=args.cash_per_trade,
        initial_capital=args.initial_capital,
        lot_size=args.lot_size,
        workers=args.workers,
        lookback_days=args.lookback_days,
        check_risk=args.check_risk,
        slippage_pct=args.slippage_pct,
        intraday_policy=args.intraday_policy,
    )
    rows: list[dict[str, Any]] = []
    for result in matrix.get("results") or []:
        payload = result.get("payload") or {}
        variant = result.get("variant") or {}
        summary = payload.get("summary") or {}
        assumptions = payload.get("assumptions") or {}
        row = {
            "name": str(result.get("name") or variant.get("name") or ""),
            "include_watch": bool(variant.get("include_watch", True)),
            "use_event_context": bool(variant.get("use_event_context", True)),
            "allow_breakout": bool(variant.get("allow_breakout", True)),
            "trade_count": int(summary.get("trade_count") or 0),
            "win_rate_pct": float(summary.get("win_rate_pct") or 0.0),
            "target_hit_rate_pct": float(summary.get("target_hit_rate_pct") or 0.0),
            "stop_hit_rate_pct": float(summary.get("stop_hit_rate_pct") or 0.0),
            "total_pnl": float(summary.get("total_pnl") or 0.0),
            "total_return_on_capital_pct": float(summary.get("total_return_on_capital_pct") or 0.0),
            "total_return_on_deployed_pct": float(summary.get("total_return_on_deployed_pct") or 0.0),
            "max_drawdown_pct": float(summary.get("max_drawdown_pct") or 0.0),
            "profit_factor": float(summary.get("profit_factor") or 0.0),
            "event_snapshot_days": int(assumptions.get("event_snapshot_days") or 0),
            "event_context_hits": int(assumptions.get("event_context_hits") or 0),
            "setup_stats": payload.get("setup_stats") or [],
        }
        row["validation_score"] = round(
            row["total_return_on_capital_pct"]
            - row["max_drawdown_pct"] * 0.8
            + max(0.0, row["win_rate_pct"] - 50.0) * 0.03
            - row["stop_hit_rate_pct"] * 0.015,
            3,
        )
        rows.append(row)

    baseline = next((item for item in rows if item["name"] == "满血默认"), None)
    event_strict = next((item for item in rows if item["name"] == "满血强候选禁突破"), None)
    no_event_strict = next((item for item in rows if item["name"] == "无新闻强候选禁突破"), None)
    rows.sort(key=lambda item: (item["validation_score"], item["total_pnl"]), reverse=True)
    best = rows[0] if rows else {}
    diagnosis = _build_validation_diagnosis(
        best=best,
        baseline=baseline,
        event_strict=event_strict,
        no_event_strict=no_event_strict,
    )
    return {
        "engine": "swing_validation_matrix_v1",
        "period": {"start": args.start, "end": args.end or ""},
        "universe": {
            "symbols": symbols,
            "universe_limit": args.universe_limit,
            "pool_limit": args.pool_limit,
            "top_n": args.top_n,
            "min_score": args.min_score,
            "stability_note": "显式 --symbols 可复现；动态股票池会受当前行情快照和日线缓存可用性影响",
        },
        "ranking": rows,
        "best": best,
        "diagnosis": diagnosis,
        "performance": matrix.get("context") or {},
    }


def _run_diagnosis(engine: Any, args: Any, symbols: list[str]) -> dict[str, Any]:
    variants = _diagnosis_variants(str(getattr(args, "variants", "") or ""))
    results: list[dict[str, Any]] = []
    for variant in variants:
        payload = engine.backtest(
            start_date=args.start,
            end_date=args.end,
            symbols=symbols or None,
            universe_limit=args.universe_limit,
            pool_limit=args.pool_limit,
            top_n=args.top_n,
            min_score=args.min_score,
            cash_per_trade=args.cash_per_trade,
            initial_capital=args.initial_capital,
            lot_size=args.lot_size,
            workers=args.workers,
            lookback_days=args.lookback_days,
            check_risk=args.check_risk,
            include_watch=variant["include_watch"],
            slippage_pct=args.slippage_pct,
            intraday_policy=args.intraday_policy,
            use_event_context=variant["use_event_context"],
            allow_breakout=variant["allow_breakout"],
        )
        results.append(
            {
                "name": variant["name"],
                "variant": variant,
                "summary": payload.get("summary") or {},
                "setup_stats": payload.get("setup_stats") or [],
                "daily": payload.get("daily") or [],
                "diagnosis": _build_trade_diagnosis(payload.get("trades") or []),
                "trades": payload.get("trades") or [],
            }
        )

    worst = sorted(
        results,
        key=lambda item: float((item.get("summary") or {}).get("total_pnl") or 0.0),
    )
    best = sorted(
        results,
        key=lambda item: float((item.get("summary") or {}).get("total_pnl") or 0.0),
        reverse=True,
    )
    return {
        "engine": "swing_trade_diagnosis_v1",
        "period": {"start": args.start, "end": args.end or ""},
        "universe": {
            "symbols": symbols,
            "universe_limit": args.universe_limit,
            "pool_limit": args.pool_limit,
            "top_n": args.top_n,
            "min_score": args.min_score,
        },
        "results": results,
        "best": best[0] if best else {},
        "worst": worst[0] if worst else {},
        "recommendations": _diagnosis_recommendations(results),
    }


def _diagnosis_variants(value: str) -> list[dict[str, Any]]:
    presets = {
        "candidate": {
            "name": "满血强候选",
            "include_watch": False,
            "use_event_context": True,
            "allow_breakout": True,
        },
        "candidate_no_breakout": {
            "name": "满血强候选禁突破",
            "include_watch": False,
            "use_event_context": True,
            "allow_breakout": False,
        },
        "default": {
            "name": "满血默认",
            "include_watch": True,
            "use_event_context": True,
            "allow_breakout": True,
        },
        "no_news_default": {
            "name": "无新闻默认",
            "include_watch": True,
            "use_event_context": False,
            "allow_breakout": True,
        },
    }
    names = [item.strip() for item in value.replace("，", ",").split(",") if item.strip()]
    if not names:
        names = ["candidate", "default"]
    variants: list[dict[str, Any]] = []
    seen: set[str] = set()
    for name in names:
        key = name.lower()
        if key not in presets:
            raise SystemExit(f"unknown diagnose variant: {name}")
        if key in seen:
            continue
        seen.add(key)
        variants.append(dict(presets[key]))
    return variants


def _build_trade_diagnosis(trades: list[dict[str, Any]]) -> dict[str, Any]:
    sorted_trades = sorted(trades, key=lambda row: float(row.get("pnl") or 0.0))
    return {
        "summary": _trade_group_stats(trades),
        "by_setup": _group_trades(trades, "setup"),
        "by_action": _group_trades(trades, "action"),
        "by_exit_reason": _group_trades(trades, "exit_reason"),
        "by_signal_date": _group_trades(trades, "signal_date"),
        "by_warning": _group_trades_by_token(trades, "warnings"),
        "by_reason": _group_trades_by_token(trades, "reasons"),
        "by_history_confidence": _group_by_history_confidence(trades),
        "worst_trades": [_trade_brief(row) for row in sorted_trades[:8]],
        "best_trades": [_trade_brief(row) for row in reversed(sorted_trades[-8:])],
    }


def _trade_group_stats(trades: list[dict[str, Any]]) -> dict[str, Any]:
    count = len(trades)
    pnl = sum(float(row.get("pnl") or 0.0) for row in trades)
    deployed = sum(float(row.get("cost") or 0.0) for row in trades)
    wins = sum(1 for row in trades if float(row.get("pnl") or 0.0) > 0)
    target_hits = sum(1 for row in trades if row.get("target_hit"))
    stop_hits = sum(1 for row in trades if row.get("stop_hit"))
    avg_return = sum(float(row.get("return_pct") or 0.0) for row in trades) / count if count else 0.0
    return {
        "trade_count": count,
        "pnl": round(pnl, 2),
        "deployed_cash": round(deployed, 2),
        "return_on_deployed_pct": round(pnl / deployed * 100, 3) if deployed > 0 else 0.0,
        "avg_return_pct": round(avg_return, 3),
        "win_rate_pct": round(wins / count * 100, 1) if count else 0.0,
        "target_hit_rate_pct": round(target_hits / count * 100, 1) if count else 0.0,
        "stop_hit_rate_pct": round(stop_hits / count * 100, 1) if count else 0.0,
    }


def _group_trades(trades: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    buckets: dict[str, list[dict[str, Any]]] = {}
    for row in trades:
        value = str(row.get(key) or "未知")
        buckets.setdefault(value, []).append(row)
    return _sorted_group_rows(buckets)


def _group_trades_by_token(trades: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    buckets: dict[str, list[dict[str, Any]]] = {}
    for row in trades:
        items = row.get(key) or []
        if not items:
            buckets.setdefault("无", []).append(row)
            continue
        for item in items:
            buckets.setdefault(str(item), []).append(row)
    return _sorted_group_rows(buckets)


def _group_by_history_confidence(trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
    buckets: dict[str, list[dict[str, Any]]] = {}
    for row in trades:
        hist = row.get("historical_reference") or {}
        value = str(hist.get("confidence") or "unknown")
        buckets.setdefault(value, []).append(row)
    return _sorted_group_rows(buckets)


def _sorted_group_rows(buckets: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    rows = []
    for name, items in buckets.items():
        stats = _trade_group_stats(items)
        stats["name"] = name
        rows.append(stats)
    rows.sort(key=lambda item: (float(item.get("pnl") or 0.0), -int(item.get("trade_count") or 0)))
    return rows


def _trade_brief(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "symbol": row.get("symbol"),
        "name": row.get("name"),
        "signal_date": row.get("signal_date"),
        "exit_date": row.get("exit_date"),
        "setup": row.get("setup"),
        "action": row.get("action"),
        "score": row.get("score"),
        "return_pct": row.get("return_pct"),
        "pnl": row.get("pnl"),
        "exit_reason": row.get("exit_reason"),
        "target_hit": bool(row.get("target_hit")),
        "stop_hit": bool(row.get("stop_hit")),
        "warnings": (row.get("warnings") or [])[:3],
        "reasons": (row.get("reasons") or [])[:3],
    }


def _diagnosis_recommendations(results: list[dict[str, Any]]) -> list[str]:
    recommendations: list[str] = []
    for result in results:
        name = str(result.get("name") or "")
        diag = result.get("diagnosis") or {}
        for row in (diag.get("by_setup") or [])[:3]:
            if float(row.get("pnl") or 0.0) < 0 and int(row.get("trade_count") or 0) >= 2:
                recommendations.append(
                    f"{name}: {row.get('name')} 亏损 {float(row.get('pnl') or 0):.2f}，"
                    f"止损率 {float(row.get('stop_hit_rate_pct') or 0):.1f}%，建议降权或加确认。"
                )
        for row in (diag.get("by_exit_reason") or [])[:3]:
            if str(row.get("name") or "").startswith(("stop_loss", "both_hit")) and float(row.get("pnl") or 0.0) < 0:
                recommendations.append(
                    f"{name}: {row.get('name')} 贡献亏损 {float(row.get('pnl') or 0):.2f}，"
                    "需要复核止损距离、买入位置和同日触发口径。"
                )
        for row in (diag.get("by_warning") or [])[:5]:
            label = str(row.get("name") or "")
            if float(row.get("pnl") or 0.0) < 0 and ("追高" in label or "振幅" in label or "样本" in label):
                recommendations.append(
                    f"{name}: 带有“{label}”的交易亏损 {float(row.get('pnl') or 0):.2f}，建议作为硬降权信号。"
                )
                break
    deduped: list[str] = []
    seen: set[str] = set()
    for item in recommendations:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped[:8]


def _build_validation_diagnosis(
    *,
    best: dict[str, Any],
    baseline: dict[str, Any] | None,
    event_strict: dict[str, Any] | None,
    no_event_strict: dict[str, Any] | None,
) -> dict[str, Any]:
    if not best:
        return {
            "sample_quality": "无交易样本",
            "verdict": "不可用",
            "recommendation": "本区间没有可验证交易，不能据此调参或实盘。",
        }

    trade_count = int(best.get("trade_count") or 0)
    total_pnl = float(best.get("total_pnl") or 0.0)
    max_drawdown = float(best.get("max_drawdown_pct") or 0.0)
    win_rate = float(best.get("win_rate_pct") or 0.0)
    if trade_count < 10:
        sample_quality = "样本偏少"
    elif trade_count < 30:
        sample_quality = "样本一般"
    else:
        sample_quality = "样本较足"

    if trade_count < 8:
        verdict = "仅观察"
        recommendation = "交易样本太少，只能作为线索，不建议直接用于实盘。"
    elif total_pnl > 0 and win_rate >= 55.0 and max_drawdown <= 2.0:
        verdict = "可纸面盘验证"
        recommendation = "收益、胜率和回撤同时过线，可以进入小额纸面盘继续观察。"
    elif total_pnl > 0:
        verdict = "谨慎观察"
        recommendation = "收益为正但胜率或回撤仍需观察，先扩大样本再定。"
    else:
        verdict = "暂不实盘"
        recommendation = "当前最优版本仍未转正，只适合继续调参和纸面验证。"

    diagnosis: dict[str, Any] = {
        "sample_quality": sample_quality,
        "verdict": verdict,
        "recommendation": recommendation,
    }
    if baseline:
        diagnosis["best_vs_baseline"] = {
            "pnl_delta": round(total_pnl - float(baseline.get("total_pnl") or 0.0), 2),
            "capital_return_delta_pct": round(
                float(best.get("total_return_on_capital_pct") or 0.0)
                - float(baseline.get("total_return_on_capital_pct") or 0.0),
                3,
            ),
            "drawdown_delta_pct": round(max_drawdown - float(baseline.get("max_drawdown_pct") or 0.0), 3),
            "trade_count_delta": trade_count - int(baseline.get("trade_count") or 0),
        }
    if event_strict and no_event_strict:
        diagnosis["news_alpha_strict"] = {
            "pnl_delta": round(
                float(event_strict.get("total_pnl") or 0.0) - float(no_event_strict.get("total_pnl") or 0.0),
                2,
            ),
            "capital_return_delta_pct": round(
                float(event_strict.get("total_return_on_capital_pct") or 0.0)
                - float(no_event_strict.get("total_return_on_capital_pct") or 0.0),
                3,
            ),
            "trade_count_delta": int(event_strict.get("trade_count") or 0)
            - int(no_event_strict.get("trade_count") or 0),
        }
    return diagnosis


def _print_validation(payload: dict[str, Any]) -> None:
    period = payload.get("period") or {}
    universe = payload.get("universe") or {}
    performance = payload.get("performance") or {}
    rows = payload.get("ranking") or []
    best = payload.get("best") or {}
    diagnosis = payload.get("diagnosis") or {}
    print("短线策略收益验证矩阵")
    print(
        f"区间 {period.get('start')} -> {period.get('end') or '今天'} | "
        f"股票池 {len(universe.get('symbols') or []) or universe.get('universe_limit', 0)} | "
        f"每日复核 {universe.get('pool_limit', 0)} | "
        f"每日最多 {universe.get('top_n', 0)} | "
        f"最低分 {float(universe.get('min_score') or 0):.1f}"
    )
    if not universe.get("symbols"):
        print("提示: 当前使用动态股票池，结果会受实时行情快照和日线缓存可用性影响；固定 --symbols 更适合复现实验。")
    if performance:
        print(
            f"性能: 数据加载 {float(performance.get('load_seconds') or 0):.1f}s | "
            f"多版本回放 {float(performance.get('replay_seconds') or 0):.1f}s | "
            f"已加载 {performance.get('loaded', 0)}/{performance.get('selected', 0)} | "
            f"交易日 {performance.get('trading_days', 0)}"
        )
    if not rows:
        print("没有生成可验证的策略版本。")
        return
    print("排名 | 策略 | 交易 | 胜率 | 目标 | 止损 | 总收益 | 本金收益率 | 回撤 | 评分")
    for idx, row in enumerate(rows, 1):
        print(
            f"{idx}. {row.get('name')} | "
            f"{row.get('trade_count', 0)} | "
            f"{row.get('win_rate_pct', 0):.1f}% | "
            f"{row.get('target_hit_rate_pct', 0):.1f}% | "
            f"{row.get('stop_hit_rate_pct', 0):.1f}% | "
            f"{row.get('total_pnl', 0):+.2f} | "
            f"{row.get('total_return_on_capital_pct', 0):+.2f}% | "
            f"{row.get('max_drawdown_pct', 0):.2f}% | "
            f"{row.get('validation_score', 0):+.3f}"
        )
    if best:
        print(
            "当前最优: "
            f"{best.get('name')}，交易 {best.get('trade_count', 0)} 笔，"
            f"胜率 {best.get('win_rate_pct', 0):.1f}%，"
            f"总收益 {best.get('total_pnl', 0):+.2f}，"
            f"最大回撤 {best.get('max_drawdown_pct', 0):.2f}%"
        )
    baseline_delta = diagnosis.get("best_vs_baseline") or {}
    if baseline_delta:
        print(
            "相对满血默认: "
            f"收益 {baseline_delta.get('pnl_delta', 0):+.2f} | "
            f"本金收益率 {baseline_delta.get('capital_return_delta_pct', 0):+.3f}pct | "
            f"回撤 {baseline_delta.get('drawdown_delta_pct', 0):+.3f}pct | "
            f"交易 {baseline_delta.get('trade_count_delta', 0):+d} 笔"
        )
    news_alpha = diagnosis.get("news_alpha_strict") or {}
    if news_alpha:
        print(
            "新闻增益(强候选禁突破): "
            f"收益 {news_alpha.get('pnl_delta', 0):+.2f} | "
            f"本金收益率 {news_alpha.get('capital_return_delta_pct', 0):+.3f}pct | "
            f"交易 {news_alpha.get('trade_count_delta', 0):+d} 笔"
        )
    if diagnosis:
        print(
            f"验收结论: {diagnosis.get('verdict', '未知')} | "
            f"{diagnosis.get('sample_quality', '未知样本')} | "
            f"{diagnosis.get('recommendation', '')}"
        )
    print("口径: 同一股票池/资金/滑点下横向比较，评分偏向收益、低回撤、胜率和低止损率。")


def _print_diagnosis(payload: dict[str, Any]) -> None:
    period = payload.get("period") or {}
    universe = payload.get("universe") or {}
    print("短线策略归因诊断")
    print(
        f"区间 {period.get('start')} -> {period.get('end') or '今天'} | "
        f"股票池 {len(universe.get('symbols') or []) or universe.get('universe_limit', 0)} | "
        f"每日复核 {universe.get('pool_limit', 0)} | "
        f"每日最多 {universe.get('top_n', 0)} | "
        f"最低分 {float(universe.get('min_score') or 0):.1f}"
    )
    for result in payload.get("results") or []:
        summary = result.get("summary") or {}
        diagnosis = result.get("diagnosis") or {}
        print("")
        print(
            f"[{result.get('name')}] "
            f"交易 {summary.get('trade_count', 0)} 笔 | "
            f"胜率 {float(summary.get('win_rate_pct') or 0):.1f}% | "
            f"总收益 {float(summary.get('total_pnl') or 0):+.2f} | "
            f"回撤 {float(summary.get('max_drawdown_pct') or 0):.2f}%"
        )
        _print_group_rows("形态归因", diagnosis.get("by_setup") or [], limit=6)
        _print_group_rows("退出归因", diagnosis.get("by_exit_reason") or [], limit=6)
        _print_group_rows("警告归因", diagnosis.get("by_warning") or [], limit=5)
        _print_trade_briefs("最大亏损", diagnosis.get("worst_trades") or [], limit=5)
        _print_trade_briefs("最大盈利", diagnosis.get("best_trades") or [], limit=3)

    recommendations = payload.get("recommendations") or []
    if recommendations:
        print("")
        print("下一步建议:")
        for item in recommendations:
            print(f"- {item}")


def _print_group_rows(title: str, rows: list[dict[str, Any]], *, limit: int) -> None:
    if not rows:
        return
    print(title + ":")
    for row in rows[:limit]:
        print(
            f"- {row.get('name')} | "
            f"{row.get('trade_count', 0)} 笔 | "
            f"收益 {float(row.get('pnl') or 0):+.2f} | "
            f"胜率 {float(row.get('win_rate_pct') or 0):.1f}% | "
            f"止损 {float(row.get('stop_hit_rate_pct') or 0):.1f}% | "
            f"均值 {float(row.get('avg_return_pct') or 0):+.2f}%"
        )


def _print_trade_briefs(title: str, rows: list[dict[str, Any]], *, limit: int) -> None:
    if not rows:
        return
    print(title + ":")
    for row in rows[:limit]:
        print(
            f"- {row.get('signal_date')} {row.get('symbol')} {row.get('name')} | "
            f"{row.get('setup')} | {row.get('exit_reason')} | "
            f"{float(row.get('return_pct') or 0):+.2f}% | "
            f"{float(row.get('pnl') or 0):+.2f}"
        )
        warnings = row.get("warnings") or []
        if warnings:
            print(f"  风险: {'；'.join(str(item) for item in warnings[:2])}")


def _print_candidates(rows: list[dict[str, Any]]) -> None:
    if not rows:
        print("未筛到符合低吸隔日冲高模型的候选。")
        return

    print("低吸隔日冲高候选")
    for idx, row in enumerate(rows, 1):
        plan = row.get("plan") or {}
        metrics = row.get("metrics") or {}
        print(
            f"{idx}. {row.get('symbol')} {row.get('name')} | "
            f"{row.get('score'):.1f} | {_action_label(row.get('action'))} | "
            f"收盘 {metrics.get('close', 0):.2f} | "
            f"买入区间 {plan.get('entry_low', 0):.2f}-{plan.get('entry_high', 0):.2f} | "
            f"止损 {plan.get('stop_loss', 0):.2f} | "
            f"目标 {plan.get('target_1', 0):.2f}/{plan.get('target_2', 0):.2f}"
        )
        _print_items("  理由", row.get("reasons") or [])
        _print_items("  警告", row.get("warnings") or [])


def _print_plan(plan: dict[str, Any]) -> None:
    trade_plan = plan.get("plan") or {}
    metrics = plan.get("metrics") or {}
    print(f"{plan.get('symbol')} {plan.get('name')} - {plan.get('setup')}")
    print(f"交易日: {plan.get('trade_date')} | 动作: {_action_label(plan.get('action'))} | 分数: {plan.get('score'):.1f}")
    if metrics:
        print(
            f"收盘: {metrics.get('close', 0):.2f} | "
            f"涨跌幅: {metrics.get('pct_change', 0):+.2f}% | "
            f"量能/峰值: {metrics.get('volume_to_peak', 0):.2f} | "
            f"均线间距: {metrics.get('ma_spread_pct', 0):.2f}%"
        )
    print(
        f"买入区间: {trade_plan.get('entry_low', 0):.2f}-{trade_plan.get('entry_high', 0):.2f} | "
        f"止损: {trade_plan.get('stop_loss', 0):.2f} | "
        f"目标: {trade_plan.get('target_1', 0):.2f}/{trade_plan.get('target_2', 0):.2f}"
    )
    if trade_plan.get("exit_rule"):
        print(f"退出: {trade_plan['exit_rule']}")
    _print_items("理由", plan.get("reasons") or [])
    _print_items("警告", plan.get("warnings") or [])


def _print_items(title: str, items: list[str]) -> None:
    if not items:
        return
    print(f"{title}:")
    for item in items:
        print(f"- {item}")


def _print_paper_account(payload: dict[str, Any]) -> None:
    positions = payload.get("positions") or []
    skipped = payload.get("skipped") or []
    print(f"{payload.get('account_name')} | 已投入 {payload.get('deployed_cash'):.2f}")
    for idx, row in enumerate(positions, 1):
        print(
            f"{idx}. {row.get('symbol')} {row.get('name')} | "
            f"{row.get('quantity')}股 @{row.get('buy_price'):.2f} | "
            f"成本 {row.get('cost'):.2f} | "
            f"目标 {row.get('target_sell_price'):.2f} | "
            f"止损 {row.get('stop_loss'):.2f}"
        )
    if skipped:
        print("跳过:")
        for row in skipped:
            print(f"- {row.get('symbol')} {row.get('name', '')}: {row.get('reason')}")


def _format_watchlist(payload: dict[str, Any]) -> str:
    summary = payload.get("summary") or {}
    positions = payload.get("positions") or []
    lines = [
        "MoatX 明日短线观察名单",
        f"生成时间: {payload.get('generated_at', '')}",
        f"候选数量: {summary.get('candidate_count', 0)} / 来源 {summary.get('source_count', 0)}",
    ]
    scan = summary.get("scan") or {}
    if scan:
        lines.append(
            "扫描: "
            f"复核 {scan.get('scanned_count', 0)}/{scan.get('review_count', 0)} | "
            f"缓存命中 {scan.get('daily_cache_hits', 0)} | "
            f"新闻映射 {scan.get('event_context_count', 0)} | "
            f"跳过未缓存 {scan.get('skipped_uncached', 0)} | "
            f"耗时 {float(scan.get('elapsed_seconds') or 0):.1f}s"
        )
        if scan.get("deadline_hit"):
            lines.append("提示: 已达到扫描时间预算，本次使用部分结果。")
    gate = summary.get("score_gate") or {}
    if gate:
        if gate.get("enabled") and gate.get("status") == "ok":
            lines.append(
                "综合门控: "
                f"复核 {gate.get('scored_count', 0)}/{gate.get('input_count', 0)} | "
                f"通过 {gate.get('passed_count', 0)} | "
                f"降级 {gate.get('downgraded_count', 0)} | "
                f"剔除 {int(gate.get('failed_count', 0) or 0) + int(gate.get('vetoed_count', 0) or 0)}"
            )
        elif gate.get("enabled"):
            lines.append(f"综合门控: {gate.get('status', 'unknown')}，本次保留短线模型原始结果")
    if not positions:
        lines.append("今天没有达到阈值的隔日冲高观察票。")
        return "\n".join(lines)
    for idx, row in enumerate(positions, 1):
        lines.append(
            f"{idx}. {row.get('symbol')} {row.get('name')} | "
            f"{_action_label(row.get('action'))} {float(row.get('score') or 0):.1f} | "
            f"买入参考 {float(row.get('buy_price') or 0):.2f} | "
            f"目标 {float(row.get('target_sell_price') or 0):.2f}/{float(row.get('target_2_price') or 0):.2f} | "
            f"止损 {float(row.get('stop_loss') or 0):.2f}"
        )
        reasons = row.get("reasons") or []
        warnings = row.get("warnings") or []
        if reasons:
            lines.append(f"   理由: {'；'.join(str(item) for item in reasons[:3])}")
        if warnings:
            lines.append(f"   风险: {'；'.join(str(item) for item in warnings[:2])}")
    lines.append("口径: 次日只做分时承接确认，冲目标分批兑现，跌破止损执行纪律。")
    return "\n".join(lines)


def _format_monitor(payload: dict[str, Any]) -> str:
    alerts = payload.get("alerts") or []
    if not alerts:
        return f"MoatX 短线观察名单监控: {payload.get('status')}，暂无触发。"
    lines = [f"MoatX 短线目标/止损提醒 | {payload.get('checked_at', '')}"]
    for alert in alerts:
        lines.append(
            f"- {alert.get('symbol')} {alert.get('name')} | {alert.get('message')} | "
            f"现价 {float(alert.get('price') or 0):.2f} | "
            f"触发 {float(alert.get('trigger_price') or 0):.2f} | "
            f"目标 {float(alert.get('target_1') or 0):.2f}/{float(alert.get('target_2') or 0):.2f} | "
            f"止损 {float(alert.get('stop_loss') or 0):.2f}"
        )
    return "\n".join(lines)


def _print_backtest(payload: dict[str, Any]) -> None:
    period = payload.get("period") or {}
    summary = payload.get("summary") or {}
    universe = payload.get("universe") or {}
    assumptions = payload.get("assumptions") or {}
    print("短线策略历史回放")
    print(
        f"区间 {period.get('start')} -> {period.get('end')} | "
        f"交易日 {period.get('trading_days', 0)} | 信号日 {period.get('signal_days', 0)} | "
        f"股票池 {universe.get('loaded', 0)}/{universe.get('selected', 0)}"
    )
    print(
        f"交易 {summary.get('trade_count', 0)} 笔 | "
        f"胜率 {summary.get('win_rate_pct', 0):.1f}% | "
        f"目标命中 {summary.get('target_hit_rate_pct', 0):.1f}% | "
        f"止损触发 {summary.get('stop_hit_rate_pct', 0):.1f}%"
    )
    print(
        f"总收益 {summary.get('total_pnl', 0):+.2f} | "
        f"本金收益率 {summary.get('total_return_on_capital_pct', 0):+.2f}% | "
        f"投入收益率 {summary.get('total_return_on_deployed_pct', 0):+.2f}% | "
        f"最大回撤 {summary.get('max_drawdown_pct', 0):.2f}%"
    )
    print(
        f"均值 {summary.get('avg_return_pct', 0):+.2f}% | "
        f"中位 {summary.get('median_return_pct', 0):+.2f}% | "
        f"盈亏比 {summary.get('profit_factor', 0):.2f} | "
        f"最长连续亏损 {summary.get('max_consecutive_losses', 0)}"
    )
    if assumptions.get("event_context"):
        print(
            f"新闻快照: {assumptions.get('event_context')} | "
            f"命中交易日 {assumptions.get('event_snapshot_days', 0)} | "
            f"映射股票 {assumptions.get('event_context_hits', 0)}"
        )
    best = summary.get("best_trade")
    worst = summary.get("worst_trade")
    if best:
        print(
            f"最佳: {best.get('symbol')} {best.get('name')} "
            f"{best.get('return_pct', 0):+.2f}% {best.get('exit_reason', '')}"
        )
    if worst:
        print(
            f"最差: {worst.get('symbol')} {worst.get('name')} "
            f"{worst.get('return_pct', 0):+.2f}% {worst.get('exit_reason', '')}"
        )
    setup_stats = payload.get("setup_stats") or []
    if setup_stats:
        print("形态分布:")
        for row in setup_stats[:6]:
            print(
                f"- {row.get('setup')}: {row.get('trade_count')} 笔 | "
                f"胜率 {row.get('win_rate_pct', 0):.1f}% | "
                f"均值 {row.get('avg_return_pct', 0):+.2f}% | "
                f"收益 {row.get('pnl', 0):+.2f}"
            )
    print(f"口径: {assumptions.get('entry')} -> {assumptions.get('exit')}，同日目标/止损={assumptions.get('intraday_policy')}")
