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
        )
    elif action == "paper":
        rows = engine.candidates(
            limit=args.limit,
            pool_limit=args.pool_limit,
            check_risk=args.check_risk,
            workers=args.workers,
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

    if action == "paper" and args.output:
        print(f"written: {args.output}")
    elif isinstance(payload, list):
        _print_candidates(payload)
    elif action == "paper":
        _print_paper_account(payload)
    else:
        _print_plan(payload)


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
            f"{row.get('score'):.1f} | {row.get('action')} | "
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
    print(f"交易日: {plan.get('trade_date')} | 动作: {plan.get('action')} | 分数: {plan.get('score'):.1f}")
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
    print(f"{payload.get('account_name')} | deployed {payload.get('deployed_cash'):.2f}")
    for idx, row in enumerate(positions, 1):
        print(
            f"{idx}. {row.get('symbol')} {row.get('name')} | "
            f"{row.get('quantity')}股 @{row.get('buy_price'):.2f} | "
            f"cost {row.get('cost'):.2f} | "
            f"target {row.get('target_sell_price'):.2f} | "
            f"stop {row.get('stop_loss'):.2f}"
        )
    if skipped:
        print("skipped:")
        for row in skipped:
            print(f"- {row.get('symbol')} {row.get('name', '')}: {row.get('reason')}")
