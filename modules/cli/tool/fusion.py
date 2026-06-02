"""CLI for unified strategy fusion scans."""

from __future__ import annotations

import json
import logging
from typing import Any


def cmd_fusion(args) -> None:
    if not getattr(args, "verbose", False):
        logging.disable(logging.WARNING)

    from modules.strategy_fusion import StrategyFusionEngine

    engine = StrategyFusionEngine()
    action = args.fusion_action
    if action == "scan":
        payload = engine.scan(
            limit=args.limit,
            pool_limit=args.pool_limit,
            score_pool_limit=args.score_pool_limit,
            min_score=args.min_score,
            workers=args.workers,
            include_intraday=args.intraday,
            use_event_context=args.full_context,
            deep_score=args.deep_score,
            deadline_seconds=args.deadline_seconds,
            allow_breakout=not args.no_breakout,
            mode=args.mode,
            intraday_limit=args.intraday_limit,
        )
    else:
        raise SystemExit(f"Unknown fusion action: {action}")

    if args.as_json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        _print_scan(payload)


def _print_scan(payload: dict[str, Any]) -> None:
    summary = payload.get("summary") or {}
    mode_label = {"fast": "快扫", "tail": "尾盘", "full": "满血"}.get(str(summary.get("mode") or "fast"), "快扫")
    print(f"MoatX 融合选股 | {mode_label}模式")
    print(
        f"候选 {summary.get('count', 0)} | "
        f"来源股票 {summary.get('source_symbols', 0)} | "
        f"短线来源 {summary.get('swing_source', 0)} | "
        f"事件来源 {summary.get('event_source', 0)} | "
        f"盘中复核 {summary.get('intraday_targets', 0)} | "
        f"耗时 {float(summary.get('elapsed_seconds') or 0):.1f}s"
    )
    scope = payload.get("strategy_scope") or {}
    total_units = int(scope.get("total_units") or 0)
    enabled_units = int(scope.get("enabled_units") or 0)
    disabled = scope.get("disabled") or []
    if total_units:
        print(f"策略融合: 已启用 {enabled_units}/{total_units}")
        if disabled:
            print("未启用: " + "、".join(str(item.get("name") or "") for item in disabled if item))
    groups = scope.get("fused_groups") or []
    if groups:
        print("融合链路: " + " / ".join(str(item) for item in groups))
    grouped = scope.get("groups") or {}
    if grouped:
        for group, units in grouped.items():
            print(f"- {group}: " + "、".join(str(item) for item in units))
    candidates = payload.get("candidates") or []
    if not candidates:
        print("当前没有达到融合阈值的候选。")
        return
    for idx, row in enumerate(candidates, 1):
        components = row.get("components") or {}
        print(
            f"{idx}. {row.get('symbol')} {row.get('name')} | "
            f"{row.get('action')} {float(row.get('score') or 0):.1f} | "
            f"建议仓位 {float(row.get('suggested_weight') or 0) * 100:.1f}% | "
            f"多因子 {float(components.get('multi_factor') or 0):.1f} "
            f"短线 {float(components.get('swing') or 0):.1f} "
            f"事件 {float(components.get('event') or 0):.1f} "
            f"技术 {float(components.get('technical') or 0):.1f} "
            f"盘中 {float(components.get('intraday') or 0):.1f}"
        )
        hits = row.get("strategy_hits") or []
        if hits:
            print("   命中: " + "；".join(str(item) for item in hits[:6]))
        reasons = row.get("reasons") or []
        if reasons:
            print("   理由: " + "；".join(str(item) for item in reasons[:3]))
        warnings = row.get("warnings") or []
        if warnings:
            print("   风险: " + "；".join(str(item) for item in warnings[:2]))
