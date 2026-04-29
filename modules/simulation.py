"""Simulation trading task callbacks for the scheduler."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

import pandas as pd

from modules.config import cfg as _cfg
from modules.db import DatabaseManager
from modules.scoring_engine import ScoringEngine
from modules.screener import MoatXScreener
from modules.signal.paper_trader import PaperTrader
from modules.sell_signal import SellSignalEngine
from modules.stock_data import StockData

_logger = logging.getLogger("moatx.simulation")


def scan_and_buy() -> dict[str, Any]:
    """Scan market for candidates and simulate buying.

    Returns:
        dict with keys: scanned, bought, skipped, errors
    """
    sim_cfg = _cfg().simulation
    db = DatabaseManager(_cfg().data.warehouse_path)
    trader = PaperTrader(db=db)

    # Get existing paper holdings
    existing = db.signal().all_paper_holdings()
    existing_symbols = set(existing["symbol"].tolist()) if not existing.empty else set()

    # Scan candidates
    screener = MoatXScreener()
    candidates = screener.scan_all(
        pe_range=(0, sim_cfg.pe_max),
        limit=100,
    )
    if candidates.empty:
        _logger.warning("scan_and_buy: no candidates found")
        return {"scanned": 0, "bought": 0, "skipped": [], "errors": []}

    # Score with multi-factor engine (Layer 0-3 + P0 fixes)
    engine = ScoringEngine(sim_cfg)
    scored = engine.score_batch(candidates, list(existing_symbols))

    # Filter: only buy stocks with total >= 41 and not vetoed
    buyable = scored[(scored["total"] >= 41) & (scored["vetoed"] == False)]
    if buyable.empty:
        return {"scanned": len(candidates), "bought": 0, "skipped": [], "errors": []}

    # Take top N
    top_n = min(sim_cfg.max_buy_count, len(buyable))
    top = buyable.head(top_n)

    # Calculate available cash
    current_holdings = trader.holdings
    market_value = 0.0
    if not current_holdings.empty:
        prices = trader._current_prices(current_holdings["symbol"].tolist())
        for _, h in current_holdings.iterrows():
            sym = h["symbol"]
            market_value += h["shares"] * prices.get(sym, h["avg_cost"])

    available_cash = sim_cfg.initial_capital * sim_cfg.max_total_position_pct - market_value
    if available_cash <= 0:
        _logger.info("scan_and_buy: no available cash")
        return {"scanned": len(candidates), "bought": 0, "skipped": [], "errors": ["no cash"]}

    # P3: Account-level drawdown circuit breaker
    total_value = trader.total_value
    drawdown_pct = (total_value - sim_cfg.initial_capital) / sim_cfg.initial_capital
    if drawdown_pct < -0.15:
        _logger.warning("scan_and_buy: 回撤 %.1f%% > 15%%, 暂停新买入", drawdown_pct * 100)
        return {
            "scanned": len(candidates),
            "bought": 0,
            "skipped": [],
            "errors": [f"回撤熔断触发 {drawdown_pct*100:+.1f}%"],
        }

    # Weighted allocation by score (not rank)
    total_top_score = top["total"].sum()
    if total_top_score <= 0:
        return {"scanned": len(candidates), "bought": 0, "skipped": [], "errors": ["no scoring data"]}

    max_per_stock = sim_cfg.initial_capital * sim_cfg.max_single_position_pct
    bought = 0
    errors = []

    for _, row in top.iterrows():
        sym = row["code"]
        price = float(row.get("price") or 0)
        score = row.get("total", 0)
        quality = row.get("quality", 0)
        veto_reason = row.get("veto_reason", "")

        if price <= 0:
            errors.append(f"{sym}: no valid price")
            continue

        # Higher score = higher budget weight
        weight = score / total_top_score
        budget = min(max_per_stock, available_cash * weight)
        shares = (int(budget / price) // 100) * 100
        if shares <= 0:
            errors.append(f"{sym}: {price:.2f} 需 {price*100:.0f}/lot 预算 {budget:.0f} 不足")
            continue

        reason = f"评分={score:.1f} 质量={quality:.1f}"
        try:
            trader._buy(sym, price, reason, 0)
            engine.record_buy(sym, row.to_dict(), price)  # P2: record for feedback learning
            bought += 1
        except Exception as e:
            errors.append(f"{sym}: {e}")

    _logger.info("scan_and_buy: scanned=%d buyable=%d bought=%d",
                 len(candidates), len(buyable), bought)
    return {"scanned": len(candidates), "bought": bought, "skipped": [], "errors": errors}


def generate_sell_signals() -> dict[str, Any]:
    """Generate sell signals for current paper holdings.

    Returns:
        dict with keys: holdings_count, signals_generated, signals
    """
    db = DatabaseManager(_cfg().data.warehouse_path)
    engine = SellSignalEngine()

    holdings = db.signal().all_paper_holdings()
    if holdings.empty:
        return {"holdings_count": 0, "signals_generated": 0, "signals": []}

    holding_list = [
        {
            "symbol": row["symbol"],
            "avg_cost": row["avg_cost"],
            "shares": row["shares"],
            "entry_date": row.get("created_at", "")[:10],
        }
        for _, row in holdings.iterrows()
    ]

    signals = engine.evaluate_all(holding_list)

    # Record signals to journal
    for sig in signals:
        db.signal().record_signal(
            symbol=sig.symbol,
            strategy_name="sell_signal",
            signal_type=sig.signal_type,
            price=sig.price,
            reason=sig.reason,
            confidence=100.0,
        )

    _logger.info("generate_sell_signals: %d holdings, %d signals", len(holding_list), len(signals))
    return {
        "holdings_count": len(holding_list),
        "signals_generated": len(signals),
        "signals": [
            {"symbol": s.symbol, "type": s.signal_type, "reason": s.reason}
            for s in signals
        ],
    }


def execute_signals() -> dict[str, Any]:
    """Execute unexecuted sell signals from journal via PaperTrader.

    Returns:
        dict with keys: signals_found, executed, skipped, errors
    """
    db = DatabaseManager(_cfg().data.warehouse_path)
    trader = PaperTrader(db=db)
    engine = ScoringEngine(_cfg().simulation)

    # Get latest unexecuted signals
    signals_df = db.signal().list_signals(limit=100)
    if signals_df.empty:
        return {"signals_found": 0, "executed": 0, "skipped": 0, "errors": []}

    # Filter to sell signals not yet executed
    unexecuted = signals_df[
        (signals_df["executed"] == 0) &
        (signals_df["signal_type"].isin(["stop_profit", "stop_loss", "technical", "timeout"]))
    ]
    if unexecuted.empty:
        return {"signals_found": len(signals_df), "executed": 0, "skipped": 0, "errors": []}

    executed = 0
    skipped = 0
    errors = []

    for _, sig_row in unexecuted.iterrows():
        sym = sig_row["symbol"]
        price = float(sig_row["price"])
        if price <= 0:
            skipped += 1
            continue

        try:
            # Get current price
            q = trader._sd.get_realtime_quote(sym)
            current_price = float(q.get("price") or price)
        except Exception:
            current_price = price

        # Get holding to check shares
        holding = db.signal().paper_holding(sym)
        if not holding:
            skipped += 1
            continue

        from modules.signal.engine import Signal
        sig = Signal(
            symbol=sym,
            signal_type="sell",
            price=current_price,
            reason=sig_row["reason"],
            strategy_name=sig_row.get("strategy_name", "sell_signal"),
            confidence=100.0,
        )
        try:
            result = trader._sell(sym, current_price, sig_row["reason"], 0)
            if result:
                db.signal().mark_executed(sig_row["id"])
                engine.record_sell(sym, current_price)  # P2: close buy record, update factor stats
                executed += 1
            else:
                skipped += 1
        except Exception as e:
            errors.append(f"{sym}: {e}")

    _logger.info("execute_signals: executed=%d skipped=%d", executed, skipped)
    return {"signals_found": len(unexecuted), "executed": executed, "skipped": skipped, "errors": errors}


def daily_snapshot() -> dict[str, Any]:
    """Take daily snapshot of paper trading account.

    Returns:
        dict snapshot data
    """
    db = DatabaseManager(_cfg().data.warehouse_path)
    trader = PaperTrader(db=db)
    snap = trader.take_snapshot()
    _logger.info("daily_snapshot: total=%.2f positions=%d", snap["total_value"], len(snap["positions"]))
    return snap


def daily_report() -> str:
    """Generate daily simulation trading report.

    Returns:
        Markdown formatted report string
    """
    from modules.cli.tool.monitor import _check_data_sources
    db = DatabaseManager(_cfg().data.warehouse_path)
    trader = PaperTrader(db=db)

    today = datetime.now().strftime("%Y-%m-%d")
    lines = [
        f"## MoatX 模拟交易日报 — {today}",
        "",
    ]

    # Today's trades
    trades = db.signal().paper_trades(limit=100)
    today_trades = trades[trades["created_at"].str[:10] == today] if not trades.empty else pd.DataFrame()
    if not today_trades.empty:
        lines.append("## 今日操作")
        lines.append("| 时间 | 操作 | 代码 | 价格 | 数量 | 金额 | 原因 |")
        lines.append("|------|------|------|------|------|------|------|")
        for _, t in today_trades.iterrows():
            lines.append(
                f"| {t['created_at'][:16]} | {t['direction']} | {t['symbol']} | "
                f"{t['price']:.2f} | {t['shares']} | {t['value']:.0f} | {t.get('reason', '')} |"
            )
        lines.append("")
    else:
        lines.append("## 今日操作")
        lines.append("无交易")
        lines.append("")

    # Current holdings
    positions = trader.positions_detail()
    if positions:
        lines.append("## 当前持仓")
        lines.append("| 代码 | 名称 | 成本 | 现价 | 盈亏% | 持有天数 |")
        lines.append("|------|------|------|------|-------|----------|")
        for p in positions:
            entry = db.signal().paper_holding(p["symbol"])
            created = entry.get("created_at", "")[:10] if entry else ""
            try:
                days = (datetime.now().date() - datetime.strptime(created, "%Y-%m-%d").date()).days
            except Exception:
                days = 0
            lines.append(
                f"| {p['symbol']} | {p.get('name', '-')} | {p['avg_cost']:.3f} | "
                f"{p['current_price']:.3f} | {p['pnl_pct']:+.2f}% | {days} |"
            )
        lines.append("")
    else:
        lines.append("## 当前持仓")
        lines.append("空仓")
        lines.append("")

    # Account overview
    total = trader.total_value
    initial = _cfg().simulation.initial_capital
    ret_pct = (total - initial) / initial * 100
    lines.extend([
        "## 账户概览",
        f"- 初始资金: {initial:,.0f}",
        f"- 当前总资产: {total:,.2f}",
        f"- 累计收益率: {ret_pct:+.2f}%",
        f"- 可用现金: {trader.cash:,.2f}",
        "",
    ])

    return "\n".join(lines)
