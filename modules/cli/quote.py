"""
modules/cli/quote.py - 行情命令
"""

import logging
from modules.stock_data import StockData
from modules.portfolio import Portfolio
from modules.calendar import is_trading_time, is_trading_day

_logger = logging.getLogger("moatx.cli.quote")


def _visual_len(s: str) -> int:
    return sum(2 if ord(c) > 127 else 1 for c in s)


def _pad_visual(s: str, width: int) -> str:
    pad = width - _visual_len(s)
    return s + " " * pad if pad > 0 else s


def _fmt_volume(vol: int) -> str:
    if vol >= 100_000_000:
        return f"{vol / 100_000_000:.1f}亿"
    if vol >= 10_000:
        return f"{vol / 10_000:.1f}万"
    return str(vol)


def cmd_quote(args):
    """查看实时行情。"""
    import time as _time
    from datetime import datetime

    symbols = list(args.symbols)
    if not symbols:
        try:
            pf = Portfolio()
            df = pf.list_holdings()
            if not df.empty:
                symbols = df["symbol"].tolist()
                print(f"读取持仓 {len(symbols)} 只股票\n")
        except Exception:
            pass

    if not symbols:
        print("请指定股票代码，如: quote 600519 000858")
        return

    p = StockData()
    t0 = _time.perf_counter()
    result = p.get_realtime_quotes(
        symbols,
        source=getattr(args, "source", None),
        mode=getattr(args, "mode", None),
    )
    elapsed = _time.perf_counter() - t0
    if not result:
        print("未获取到数据，请检查股票代码是否正确")
        return

    items = sorted(result.items(), key=lambda x: float(x[1]["change_pct"]), reverse=True)

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"查询时间: {now}")

    # Data freshness indicator
    if is_trading_time():
        freshness = "🟢 实时（交易时段）"
    elif is_trading_day():
        freshness = "🟡 非交易时段（已收盘）"
    else:
        freshness = "⚪ 非交易日"
    print(f"数据状态: {freshness}")
    print()

    top = "┌────────┬──────────┬─────────┬─────────┬────────┬────────┬──────────┬────────────┬────────────────────┐"
    sep = "├────────┼──────────┼─────────┼─────────┼────────┼────────┼──────────┼────────────┼────────────────────┤"
    head = "│  代码  │   名称   │ 最新价  │  昨收   │涨跌额 │ 涨跌幅 │  成交量  │ 校验状态   │ 数据源             │"
    bot = "└────────┴──────────┴─────────┴─────────┴────────┴────────┴──────────┴────────────┴────────────────────┘"

    print(top)
    print(head)
    print(sep)

    for code, q in items:
        short_code = code.replace(".SH", "").replace(".SZ", "").replace(".BJ", "")
        name = q["name"]
        price = float(q["price"])
        pct = float(q["change_pct"])
        prev_close = round(price / (1 + pct / 100), 2) if abs(pct) > 0.001 else price
        change = price - prev_close
        vol = int(q.get("volume", 0))

        vol_str = _fmt_volume(vol)
        fmt_change = f"{change:>+7.2f}"
        fmt_pct = f"{pct:>+6.2f}%"
        status = _status_label(str(q.get("validation_status", "")))
        sources = "+".join(q.get("sources") or [str(q.get("source", ""))])

        print(
            f"│ {short_code:>6s} │ {_pad_visual(name, 8)} │ {price:>7.2f}  │ {prev_close:>7.2f}  │ "
            f"{fmt_change}  │ {fmt_pct} │ {vol_str:>7s} │ {_pad_visual(status, 10)} │ {_pad_visual(sources, 18)} │"
        )
        print(sep)

    print(bot)
    print(f"\n共 {len(result)} 只股票 | 耗时 {elapsed:.2f} 秒")

    warnings = [q for _, q in items if q.get("warning")]
    if warnings:
        print("\n数据源分歧:")
        for q in warnings:
            print(f"- {q.get('name', q.get('code', ''))}: {q.get('warning')}")


def _status_label(status: str) -> str:
    if status == "verified":
        return "✅ 已校验"
    if status == "diverged":
        return "⚠️ 有分歧"
    if status == "single_source":
        return "单源"
    return status or "未知"
