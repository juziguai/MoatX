"""CLI for intraday anomaly radar."""

from __future__ import annotations

import json
import logging
from typing import Any


def cmd_intraday(args) -> None:
    if not getattr(args, "verbose", False):
        logging.disable(logging.WARNING)

    from modules.intraday_radar import IntradayRadarService
    from modules.intraday_radar.models import RadarConfig

    config = RadarConfig(
        min_score=args.min_score,
        min_pct=args.min_pct,
        max_entry_pct=args.max_entry_pct,
        min_ret_10m=args.min_ret_10m,
        min_amount_ratio=args.min_amount_ratio,
    )
    service = IntradayRadarService(config=config)
    action = args.intraday_action
    if action == "replay":
        payload = service.replay(symbol=args.symbol, trade_date=args.date)
    elif action == "radar":
        symbols = service.resolve_symbols(args.symbols, args.symbols_file)
        if not symbols:
            raise SystemExit("请通过 --symbols 或 --symbols-file 指定扫描股票池")
        payload = service.scan(
            symbols=symbols[: max(1, int(args.limit or 1))],
            trade_date=args.date or None,
            write_snapshot=args.write_snapshot,
        )
    else:
        raise SystemExit(f"Unknown intraday action: {action}")

    if args.as_json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return
    if action == "replay":
        _print_replay(payload)
    else:
        _print_radar(payload)


def _print_replay(payload: dict[str, Any]) -> None:
    summary = payload.get("summary") or {}
    print(f"盘中异动复盘: {payload.get('symbol')} {payload.get('name')} | {payload.get('trade_date')}")
    print(
        f"前收 {float(payload.get('prev_close') or 0):.2f} | "
        f"最新 {float(summary.get('latest_price') or 0):.2f} | "
        f"涨幅 {float(summary.get('latest_pct') or 0):+.2f}% | "
        f"上午高点 {float(summary.get('morning_high') or 0):.2f} | "
        f"首次触板 {summary.get('first_limit_time') or '-'}"
    )
    signals = payload.get("signals") or []
    if not signals:
        print("未识别到达到阈值的盘中启动信号。")
        return
    for signal in signals:
        _print_signal(signal)


def _print_radar(payload: dict[str, Any]) -> None:
    print(
        f"盘中异动雷达 | 扫描 {payload.get('scanned', 0)}/{payload.get('requested', 0)} | "
        f"信号 {payload.get('signal_count', 0)} | 错误 {len(payload.get('errors') or [])} | "
        f"耗时 {float(payload.get('elapsed_seconds') or 0):.1f}s"
    )
    if payload.get("snapshot_path"):
        print(f"快照: {payload.get('snapshot_path')}")
    signals = payload.get("signals") or []
    if not signals:
        print("暂无达到阈值的盘中异动。")
        _print_errors(payload.get("errors") or [])
        return
    _print_sector_resonance(payload.get("sector_resonance") or [])
    for idx, signal in enumerate(signals, 1):
        print(f"{idx}. ", end="")
        _print_signal(signal)
    _print_errors(payload.get("errors") or [])


def _print_signal(signal: dict[str, Any]) -> None:
    metrics = signal.get("metrics") or {}
    boost = float(metrics.get("sector_boost") or 0)
    score_text = f"分 {float(signal.get('score') or 0):.1f}"
    if boost > 0:
        score_text += f"(共振+{boost:.0f})"
    print(
        f"{signal.get('symbol')} {signal.get('name')} | {signal.get('signal_time')} | "
        f"{signal.get('level')} | {score_text} | "
        f"价 {float(signal.get('price') or 0):.2f} | "
        f"涨幅 {float(signal.get('pct_change') or 0):+.2f}% | "
        f"10分钟 {float(metrics.get('ret_10m') or 0):+.2f}% | "
        f"距涨停 {float(metrics.get('distance_to_limit_pct') or 0):.2f}%"
    )
    reasons = signal.get("reasons") or []
    warnings = signal.get("warnings") or []
    if reasons:
        print(f"  理由: {'；'.join(str(item) for item in reasons[:4])}")
    if warnings:
        print(f"  风险: {'；'.join(str(item) for item in warnings[:3])}")


def _print_sector_resonance(rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    print("板块共振:")
    for row in rows[:5]:
        members = row.get("members") or []
        member_text = "、".join(
            f"{item.get('symbol')} {item.get('name')}".strip()
            for item in members[:3]
            if item.get("symbol")
        )
        print(
            f"- {row.get('tag')}: {int(row.get('signal_count') or 0)}/"
            f"{int(row.get('scanned_count') or 0)} 异动 | "
            f"信号均涨幅 {float(row.get('avg_signal_pct') or 0):+.2f}% | "
            f"共振加分 +{float(row.get('boost') or 0):.0f}"
            + (f" | {member_text}" if member_text else "")
        )


def _print_errors(errors: list[dict[str, Any]]) -> None:
    if not errors:
        return
    print("错误:")
    for row in errors[:5]:
        print(f"- {row.get('symbol')}: {row.get('error')}")
