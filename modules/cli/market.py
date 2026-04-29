"""Market index CLI commands."""

from __future__ import annotations

import json
import time
from dataclasses import asdict
from datetime import datetime

from modules.config import cfg
from modules.market_index import DEFAULT_INDEX_CODES, MarketIndexQuoteManager


def cmd_market(args) -> None:
    """Show validated multi-source A-share market index quotes."""
    if getattr(args, "breadth", False):
        _emit_breadth(args)
        return

    symbols = list(args.symbols or DEFAULT_INDEX_CODES)
    configured_sources = ",".join(cfg().datasource.ordered_sources())
    sources = [item.strip() for item in str(args.sources or configured_sources).split(",") if item.strip()]
    manager = MarketIndexQuoteManager(tolerance_pct=float(args.tolerance_pct))

    started = time.perf_counter()
    rows = manager.fetch(symbols, sources=sources)
    elapsed = time.perf_counter() - started

    if args.as_json:
        print(json.dumps({
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "sources": sources,
            "count": len(rows),
            "items": [asdict(row) for row in rows],
            "elapsed": round(elapsed, 3),
        }, ensure_ascii=False, indent=2))
        return

    if not rows:
        print("未获取到大盘指数数据")
        return

    print(f"查询时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"数据源: {', '.join(sources)} | 校验阈值: {float(args.tolerance_pct):.2f}%")
    print()
    print("┌────────┬──────────┬──────────┬────────┬────────┬────────────┬────────────────┐")
    print("│  代码  │   名称   │   点位   │ 涨跌点 │ 涨跌幅 │ 校验状态   │ 数据源         │")
    print("├────────┼──────────┼──────────┼────────┼────────┼────────────┼────────────────┤")
    for row in rows:
        pct = f"{row.pct_change:+.2f}%"
        change = f"{row.change:+.2f}"
        status = _status_label(row.status)
        sources_text = "+".join(row.sources)
        print(
            f"│ {row.code[-6:]:>6s} │ {_pad_visual(row.name, 8)} │ {row.price:>8.2f} │ "
            f"{change:>6s} │ {pct:>6s} │ {_pad_visual(status, 10)} │ {_pad_visual(sources_text, 14)} │"
        )
        print("├────────┼──────────┼──────────┼────────┼────────┼────────────┼────────────────┤")
    print("└────────┴──────────┴──────────┴────────┴────────┴────────────┴────────────────┘")
    print(f"\n共 {len(rows)} 个指数 | 耗时 {elapsed:.2f} 秒")

    warnings = [row for row in rows if row.warning]
    if warnings:
        print("\n数据源分歧:")
        for row in warnings:
            print(f"- {row.name}: {row.warning}")


def _emit_breadth(args) -> None:
    manager = MarketIndexQuoteManager()
    breadth = manager.breadth(source=args.breadth_source)
    if args.as_json:
        print(json.dumps({
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "breadth": asdict(breadth),
        }, ensure_ascii=False, indent=2))
        return

    print(f"查询时间: {breadth.datetime}")
    print(f"数据源: {breadth.source} | 方法: {breadth.method} | 耗时: {breadth.elapsed:.2f}s")
    print()
    print("┌────────┬────────┬────────┬────────┬────────┐")
    print("│  总数  │  上涨  │  下跌  │  平盘  │ 涨跌比 │")
    print("├────────┼────────┼────────┼────────┼────────┤")
    ratio = breadth.up / breadth.down if breadth.down else 0
    print(f"│ {breadth.total:>6d} │ {breadth.up:>6d} │ {breadth.down:>6d} │ {breadth.flat:>6d} │ {ratio:>6.2f} │")
    print("└────────┴────────┴────────┴────────┴────────┘")
    if breadth.warning:
        print(f"\n提示: {breadth.warning}")


def _status_label(status: str) -> str:
    if status == "verified":
        return "✅ 已校验"
    if status == "diverged":
        return "⚠️ 有分歧"
    return "单源"


def _visual_len(s: str) -> int:
    return sum(2 if ord(c) > 127 else 1 for c in str(s))


def _pad_visual(s: str, width: int) -> str:
    text = str(s)
    pad = width - _visual_len(text)
    return text + " " * pad if pad > 0 else text
