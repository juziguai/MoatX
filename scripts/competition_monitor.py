"""Paper-only monitor for the 2026-04-28 ChatGPT portfolio contest."""

from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, time as dt_time
from pathlib import Path
from typing import Any

from modules.datasource import QuoteManager
from modules.event_intelligence.summary import build_event_monitor_summary, format_event_monitor_summary
from modules.market_index import MarketIndexQuoteManager


DEFAULT_STATE = Path("data/chatgpt_competition_portfolio_20260428.json")


@dataclass
class PositionView:
    symbol: str
    name: str
    shares: float
    price: float
    prev_close: float
    pct_change: float
    market_value: float
    daily_pnl: float
    total_pnl: float
    weight: float = 0.0


def main() -> None:
    parser = argparse.ArgumentParser(description="Run paper-only competition portfolio monitor.")
    parser.add_argument("--state", default=str(DEFAULT_STATE), help="Competition state JSON path")
    parser.add_argument("--json", action="store_true", help="Print machine-readable output")
    parser.add_argument("--dry-run", action="store_true", help="Do not append snapshot/order records")
    parser.add_argument("--no-orders", action="store_true", help="Only produce a snapshot; do not propose paper orders")
    parser.add_argument("--no-events", action="store_true", help="Do not attach macro event intelligence summary")
    args = parser.parse_args()

    state_path = Path(args.state)
    state = json.loads(state_path.read_text(encoding="utf-8"))
    positions = _positions_with_quotes(state)
    total_value = sum(item.market_value for item in positions) + float(state.get("cash") or 0)
    baseline = float(state.get("baseline_total_value") or total_value)
    for item in positions:
        item.weight = item.market_value / total_value if total_value else 0.0

    breadth = _safe_breadth()
    orders = [] if args.no_orders else _decide_orders(state, positions, total_value, baseline, breadth)
    event_summary = {} if args.no_events else _safe_event_summary()
    snapshot = _snapshot(state, positions, total_value, baseline, breadth, orders, event_summary)

    if not args.dry_run:
        _apply_orders(state, orders, positions)
        state.setdefault("snapshots", []).append(snapshot)
        state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")

    if args.json:
        print(json.dumps({"snapshot": snapshot, "orders": orders}, ensure_ascii=False, indent=2))
    else:
        _print_report(snapshot, orders, positions)


def _positions_with_quotes(state: dict[str, Any]) -> list[PositionView]:
    raw_positions = [p for p in state.get("positions", []) if float(p.get("shares") or 0) > 0]
    quotes = QuoteManager().fetch_quotes([p["symbol"] for p in raw_positions])

    items: list[PositionView] = []
    for pos in raw_positions:
        quote = _quote_for(quotes, pos["symbol"])
        if quote:
            price = float(quote.get("price") or pos.get("baseline_price") or pos.get("cost_price") or 0)
            prev_close = float(quote.get("prev_close") or price)
            pct_change = float(quote.get("change_pct") or 0)
            name = str(quote.get("name") or pos.get("name") or pos["symbol"])
        else:
            price = float(pos.get("baseline_price") or pos.get("cost_price") or 0)
            prev_close = price
            pct_change = 0.0
            name = str(pos.get("name") or pos["symbol"])
        shares = float(pos.get("shares") or 0)
        cost_price = float(pos.get("cost_price") or pos.get("baseline_price") or price)
        items.append(
            PositionView(
                symbol=pos["symbol"],
                name=name,
                shares=shares,
                price=price,
                prev_close=prev_close,
                pct_change=pct_change,
                market_value=round(shares * price, 2),
                daily_pnl=round((price - prev_close) * shares, 2),
                total_pnl=round((price - cost_price) * shares, 2),
            )
        )
    return items


def _quote_for(quotes: dict[str, dict[str, Any]], symbol: str) -> dict[str, Any] | None:
    for key in (symbol, f"{symbol}.SH", f"{symbol}.SZ", f"{symbol}.BJ"):
        if key in quotes:
            return quotes[key]
    for key, quote in quotes.items():
        if key.startswith(symbol):
            return quote
    return None


def _safe_breadth() -> dict[str, Any]:
    try:
        breadth = MarketIndexQuoteManager(timeout=6).breadth()
        return {
            "total": breadth.total,
            "up": breadth.up,
            "down": breadth.down,
            "flat": breadth.flat,
            "ratio": round(breadth.up / breadth.down, 3) if breadth.down else 0,
            "method": breadth.method,
        }
    except Exception as exc:
        return {"error": str(exc)}


def _safe_event_summary() -> dict[str, Any]:
    try:
        return build_event_monitor_summary()
    except Exception as exc:
        return {
            "enabled": False,
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "top_events": [],
            "error": str(exc),
        }


def _decide_orders(
    state: dict[str, Any],
    positions: list[PositionView],
    total_value: float,
    baseline: float,
    breadth: dict[str, Any],
) -> list[dict[str, Any]]:
    now = datetime.now()
    if not _is_decision_time(now):
        return []
    if _already_ordered_this_slot(state, now):
        return []

    portfolio_return = (total_value - baseline) / baseline * 100 if baseline else 0.0
    market_weak = float(breadth.get("ratio") or 0) < 0.9 if "error" not in breadth else False
    orders: list[dict[str, Any]] = []

    weak_positions = sorted(
        [p for p in positions if p.daily_pnl < 0],
        key=lambda p: (p.daily_pnl, -p.weight),
    )
    for item in weak_positions[:2]:
        if item.pct_change <= -2.0 or (portfolio_return <= -1.0 and item.weight >= 0.1 and market_weak):
            sell_shares = min(100.0, _round_lot(item.shares))
            if sell_shares > 0:
                orders.append(_order("sell", item, sell_shares, "弱势拖累，纸面降仓"))

    cash = float(state.get("cash") or 0)
    cash_after_sells = cash + sum(float(order["net_amount"]) for order in orders if order["action"] == "sell")
    strongest = sorted(
        [p for p in positions if p.pct_change > 0 and p.weight < 0.18],
        key=lambda p: p.pct_change,
        reverse=True,
    )
    if cash_after_sells >= 500 and strongest and portfolio_return > -1.5 and not market_weak:
        target = strongest[0]
        buy_value = min(cash_after_sells * 0.5, total_value * 0.08)
        buy_shares = _round_lot(buy_value / target.price)
        if buy_shares > 0:
            orders.append(_order("buy", target, buy_shares, "强势仓补位，纸面加仓"))

    return orders


def _is_decision_time(now: datetime) -> bool:
    current = now.time()
    return dt_time(9, 30) <= current <= dt_time(11, 30) or dt_time(13, 0) <= current <= dt_time(15, 0)


def _already_ordered_this_slot(state: dict[str, Any], now: datetime) -> bool:
    slot = now.strftime("%Y-%m-%d %H")
    return any(
        str(order.get("created_at", "")).startswith(slot) and order.get("status") != "invalid"
        for order in state.get("orders", [])
    )


def _round_lot(shares: float) -> float:
    return float(int(shares // 100) * 100) if shares >= 100 else 0.0


def _order(action: str, item: PositionView, shares: float, reason: str) -> dict[str, Any]:
    gross_amount = round(shares * item.price, 2)
    fees = _estimate_fees(action, gross_amount, item.symbol)
    net_amount = gross_amount - fees if action == "sell" else gross_amount + fees
    order = {
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "action": action,
        "symbol": item.symbol,
        "name": item.name,
        "shares": shares,
        "price": item.price,
        "gross_amount": gross_amount,
        "fee": round(fees, 2),
        "net_amount": round(net_amount, 2),
        "amount": gross_amount,
        "reason": reason,
        "paper_only": True,
        "status": "valid",
        "effective": True,
    }
    order["order_hash"] = _hash_payload(order)
    return order


def _apply_orders(state: dict[str, Any], orders: list[dict[str, Any]], positions: list[PositionView]) -> None:
    if not orders:
        return
    state.setdefault("orders", [])
    position_map = {p["symbol"]: p for p in state.get("positions", [])}
    for order in orders:
        symbol = order["symbol"]
        shares = float(order["shares"])
        pos = position_map.get(symbol)
        if not pos:
            continue
        if order["action"] == "sell":
            pos["shares"] = max(0.0, float(pos.get("shares") or 0) - shares)
            state["cash"] = round(float(state.get("cash") or 0) + float(order["net_amount"]), 2)
        elif order["action"] == "buy":
            affordable = _affordable_shares(float(state.get("cash") or 0), float(order["price"]), symbol, shares)
            if affordable <= 0:
                continue
            pos["shares"] = float(pos.get("shares") or 0) + affordable
            order["shares"] = affordable
            order["gross_amount"] = round(affordable * float(order["price"]), 2)
            order["fee"] = round(_estimate_fees("buy", float(order["gross_amount"]), symbol), 2)
            order["net_amount"] = round(float(order["gross_amount"]) + float(order["fee"]), 2)
            order["amount"] = order["gross_amount"]
            order["order_hash"] = _hash_payload(order)
            state["cash"] = round(float(state.get("cash") or 0) - float(order["net_amount"]), 2)
        if order.get("shares", 0) > 0 and float(order["shares"]) % 100 == 0:
            state["orders"].append(order)


def _snapshot(
    state: dict[str, Any],
    positions: list[PositionView],
    total_value: float,
    baseline: float,
    breadth: dict[str, Any],
    orders: list[dict[str, Any]],
    event_summary: dict[str, Any],
) -> dict[str, Any]:
    top_drag = min(positions, key=lambda p: p.daily_pnl, default=None)
    top_gain = max(positions, key=lambda p: p.daily_pnl, default=None)
    snapshot = {
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "phase": _trading_phase(datetime.now()),
        "cash": round(float(state.get("cash") or 0), 2),
        "market_value": round(sum(p.market_value for p in positions), 2),
        "total_value": round(total_value, 2),
        "baseline_total_value": round(baseline, 2),
        "pnl": round(total_value - baseline, 2),
        "pnl_pct": round((total_value - baseline) / baseline * 100, 3) if baseline else 0.0,
        "breadth": breadth,
        "top_drag": top_drag.symbol if top_drag else "",
        "top_drag_pnl": top_drag.daily_pnl if top_drag else 0.0,
        "top_gain": top_gain.symbol if top_gain else "",
        "top_gain_pnl": top_gain.daily_pnl if top_gain else 0.0,
        "orders_count": len(orders),
        "event_intelligence": event_summary,
    }
    snapshot["snapshot_hash"] = _hash_payload(snapshot)
    return snapshot


def _print_report(snapshot: dict[str, Any], orders: list[dict[str, Any]], positions: list[PositionView]) -> None:
    print(f"时间: {snapshot['created_at']}")
    print(f"阶段: {snapshot['phase']}")
    print(f"总值: {snapshot['total_value']:.2f} | 盈亏: {snapshot['pnl']:+.2f} ({snapshot['pnl_pct']:+.2f}%)")
    print(f"最大拖累: {snapshot['top_drag']} {snapshot['top_drag_pnl']:+.2f}")
    print(f"最大贡献: {snapshot['top_gain']} {snapshot['top_gain_pnl']:+.2f}")
    for line in format_event_monitor_summary(snapshot.get("event_intelligence") or {}):
        print(line)
    print(f"快照哈希: {snapshot['snapshot_hash']}")
    print("操作:")
    if not orders:
        print("- HOLD：未触发纸面交易")
    for order in orders:
        action_text = "卖出" if order["action"] == "sell" else "买入"
        print(f"- 决策：{action_text} {order['name']}（{order['symbol']}）")
        print(f"  数量：{order['shares']:.0f} 股")
        print(f"  价格：{order['price']:.3f} 元")
        print(f"  成交金额：{order['gross_amount']:.2f} 元")
        print(f"  预估手续费：{order['fee']:.2f} 元")
        print(f"  现金影响：{order['net_amount']:.2f} 元")
        print(f"  理由：{order['reason']}")
        print(f"  订单哈希：{order['order_hash']}")
    print("持仓:")
    for item in sorted(positions, key=lambda p: p.daily_pnl):
        print(f"- {item.symbol} {item.name}: {item.shares:.0f}股 {item.price:.3f} {item.pct_change:+.2f}% 今日{item.daily_pnl:+.2f}")


def _trading_phase(now: datetime) -> str:
    current = now.time()
    if dt_time(9, 30) <= current <= dt_time(11, 30):
        return "morning_trading"
    if dt_time(11, 30) < current < dt_time(13, 0):
        return "lunch_break"
    if dt_time(13, 0) <= current <= dt_time(15, 0):
        return "afternoon_trading"
    if current < dt_time(9, 30):
        return "pre_open"
    return "after_close"


def _hash_payload(payload: dict[str, Any]) -> str:
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _estimate_fees(action: str, gross_amount: float, symbol: str = "") -> float:
    commission = max(gross_amount * 0.00025, 5.0) if gross_amount > 0 else 0.0
    transfer_fee = gross_amount * 0.00001 if not _is_fund(symbol) else 0.0
    stamp_duty = gross_amount * 0.0005 if action == "sell" and not _is_fund(symbol) else 0.0
    return commission + transfer_fee + stamp_duty


def _affordable_shares(cash: float, price: float, symbol: str, requested: float) -> float:
    shares = min(requested, cash // price if price > 0 else 0)
    shares = _round_lot(shares)
    while shares > 0:
        gross_amount = shares * price
        if gross_amount + _estimate_fees("buy", gross_amount, symbol) <= cash:
            return shares
        shares = _round_lot(shares - 100 if shares >= 100 else shares - 1)
    return 0.0


def _is_fund(symbol: str) -> bool:
    return str(symbol).startswith(("51", "15", "16", "18"))


if __name__ == "__main__":
    main()
