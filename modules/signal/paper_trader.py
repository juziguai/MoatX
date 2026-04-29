"""模拟交易 — 基于信号的模拟持仓管理。"""

from __future__ import annotations


import pandas as pd

from modules.backtest.fees import calc_buy_cost, calc_sell_proceeds
from modules.config import cfg
from modules.db import DatabaseManager
from modules.stock_data import StockData
from .engine import Signal
from .journal import SignalJournal


class PaperTrader:
    """模拟交易系统。

    基于信号的模拟持仓管理：收到买入信号 → 建仓；卖出信号 → 平仓。
    使用 T+0 模式（收到信号立即以当前价格执行）。
    """

    def __init__(self, initial_capital: float = 100_000,
                 db: DatabaseManager | None = None):
        self.initial_capital = initial_capital
        self._db = db or DatabaseManager(cfg().data.warehouse_path)
        self._journal = SignalJournal(self._db)
        self._sd = StockData()

    @property
    def cash(self) -> float:
        """从交易历史计算可用现金（初始资金 - 买入总额 + 卖出收入）"""
        trades = self._db.signal().paper_trades(limit=10000)
        if trades.empty:
            return self.initial_capital
        spent = trades[trades["direction"] == "buy"]["value"].sum()
        received = trades[trades["direction"] == "sell"]["value"].sum()
        return self.initial_capital - spent + received

    @property
    def holdings(self) -> pd.DataFrame:
        return self._db.signal().all_paper_holdings()

    def _current_prices(self, symbols: list[str]) -> dict[str, float]:
        """Fetch real-time prices for symbols, fallback to avg_cost on failure."""
        prices = {}
        for sym in symbols:
            try:
                q = self._sd.get_realtime_quote(sym)
                prices[sym] = float(q.get("price") or 0)
            except Exception:
                h = self._db.signal().paper_holding(sym)
                prices[sym] = float(h["avg_cost"]) if h else 0.0
        return prices

    @property
    def total_value(self) -> float:
        """估算当前总资产（现金 + 持仓市值，按实时价格）。"""
        h = self.holdings
        if h.empty:
            return self.cash
        symbols = h["symbol"].tolist()
        prices = self._current_prices(symbols)
        market_value = sum(
            row["shares"] * prices.get(row["symbol"], row["avg_cost"])
            for _, row in h.iterrows()
        )
        return self.cash + market_value

    def positions_detail(self) -> list[dict]:
        """Return detailed positions with real-time P&L."""
        h = self.holdings
        if h.empty:
            return []
        symbols = h["symbol"].tolist()
        prices = self._current_prices(symbols)
        rows = []
        for _, row in h.iterrows():
            sym = row["symbol"]
            shares = row["shares"]
            avg_cost = row["avg_cost"]
            current_price = prices.get(sym, avg_cost)
            cost = shares * avg_cost
            market_val = shares * current_price
            pnl = market_val - cost
            pnl_pct = (current_price - avg_cost) / avg_cost * 100 if avg_cost > 0 else 0
            rows.append({
                "symbol": sym,
                "name": row.get("name", ""),
                "shares": shares,
                "avg_cost": round(avg_cost, 3),
                "current_price": round(current_price, 3),
                "cost": round(cost, 2),
                "market_value": round(market_val, 2),
                "pnl": round(pnl, 2),
                "pnl_pct": round(pnl_pct, 2),
            })
        return rows

    def take_snapshot(self, snapshot_date: str | None = None) -> dict:
        """Take a daily snapshot of the paper trading account."""
        from datetime import datetime
        date_str = snapshot_date or datetime.now().strftime("%Y-%m-%d")
        h = self.holdings
        positions = self.positions_detail()
        market_value = sum(p["market_value"] for p in positions)
        total = self.cash + market_value
        total_return_pct = (total - self.initial_capital) / self.initial_capital * 100

        self._db.signal().save_paper_snapshot(
            snapshot_date=date_str,
            total_value=round(total, 2),
            cash=round(self.cash, 2),
            market_value=round(market_value, 2),
            holdings_detail=[dict(symbol=r["symbol"], name=r["name"], shares=r["shares"],
                                  avg_cost=r["avg_cost"]) for r in positions],
            positions_detail=positions,
            total_return_pct=round(total_return_pct, 4),
        )
        return {
            "date": date_str,
            "total_value": round(total, 2),
            "cash": round(self.cash, 2),
            "market_value": round(market_value, 2),
            "total_return_pct": round(total_return_pct, 4),
            "positions": positions,
        }

    def execute_signal(self, signal: Signal, current_price: float | None = None) -> dict | None:
        """执行一条信号，创建模拟交易。

        Returns:
            {"order": "buy"/"sell", "symbol": ..., "shares": ..., "price": ..., "value": ...}
            或 None（无法执行）
        """
        price = current_price or signal.price
        if price <= 0:
            return None

        signal_id = self._journal.record(signal)

        if signal.signal_type == "buy":
            return self._buy(signal.symbol, price, signal.reason, signal_id)
        elif signal.signal_type == "sell":
            return self._sell(signal.symbol, price, signal.reason, signal_id)
        return None

    def apply_signal(self, signal: Signal, current_price: float | None = None) -> dict | None:
        """公开的交易信号执行接口（兼容 test_apply_signal_* 用法）。"""
        if signal.signal_type == "buy":
            price = current_price or signal.price
            if price <= 0:
                return None
            signal_id = self._journal.record(signal)
            return self._buy(signal.symbol, price, signal.reason, signal_id)
        elif signal.signal_type == "sell":
            price = current_price or signal.price
            if price <= 0:
                return None
            signal_id = self._journal.record(signal)
            return self._sell(signal.symbol, price, signal.reason, signal_id)
        return None

    def _buy(self, symbol: str, price: float, reason: str, signal_id: int) -> dict | None:
        max_shares = int(self.cash / (price * 1.001))  # rough max
        max_shares = (max_shares // 100) * 100  # round to lots
        if max_shares <= 0:
            return None

        target_value = min(max_shares * price, self.cash * 0.5)
        shares = (int(target_value / price) // 100) * 100
        if shares <= 0:
            return None

        actual_cost = calc_buy_cost(price, shares)
        if actual_cost > self.cash:
            return None

        existing = self._db.signal().paper_holding(symbol)
        name = ""
        try:
            q = self._sd.get_realtime_quote(symbol)
            name = q.get("name", "")
        except Exception:
            pass
        if existing:
            total_shares = existing["shares"] + shares
            total_cost = existing["avg_cost"] * existing["shares"] + price * shares
            avg_cost = total_cost / total_shares
            self._db.signal().upsert_paper_holding(symbol, name or existing.get("name", ""), total_shares, avg_cost)
        else:
            self._db.signal().upsert_paper_holding(symbol, name, shares, price)

        trade_value = price * shares
        self._db.signal().record_paper_trade(
            symbol, "buy", price, shares, trade_value, actual_cost - trade_value,
            reason, signal_id,
        )

        return {"order": "buy", "symbol": symbol, "shares": shares, "price": price, "value": trade_value}

    def _sell(self, symbol: str, price: float, reason: str, signal_id: int) -> dict | None:
        holding = self._db.signal().paper_holding(symbol)
        if holding is None:
            return None

        shares = holding["shares"]
        if shares <= 0:
            return None

        proceeds = calc_sell_proceeds(price, shares)
        self._db.signal().delete_paper_holding(symbol)
        trade_value = price * shares
        self._db.signal().record_paper_trade(
            symbol, "sell", price, shares, trade_value, trade_value - proceeds,
            reason, signal_id,
        )

        return {"order": "sell", "symbol": symbol, "shares": shares, "price": price, "value": trade_value}

    def sell_all(self, symbol: str, price: float, reason: str = "清仓") -> dict | None:
        """清仓指定标的。"""
        return self._sell(symbol, price, reason, 0)

    def daily_rebalance(self, signals: list[Signal], prices: dict[str, float]) -> list[dict]:
        """批量执行一组信号。"""
        results = []
        for sig in signals:
            price = prices.get(sig.symbol, sig.price)
            result = self.execute_signal(sig, current_price=price)
            if result:
                results.append(result)
        return results

    def pnl_report(self) -> pd.DataFrame:
        """Return paper trading P&L report (closed positions only)."""
        trades = self._db.signal().paper_trades(limit=1000)
        if trades.empty:
            return pd.DataFrame()

        buys = trades[trades["direction"] == "buy"].copy()
        sells = trades[trades["direction"] == "sell"].copy()

        report = []
        for _, sell in sells.iterrows():
            matching_buys = buys[buys["symbol"] == sell["symbol"]].sort_values("created_at")
            if matching_buys.empty:
                continue
            buy = matching_buys.iloc[-1]
            pnl = sell["value"] - buy["value"]
            pnl_pct = (sell["price"] - buy["price"]) / buy["price"] * 100 if buy["price"] > 0 else 0
            report.append({
                "symbol": sell["symbol"],
                "buy_price": round(buy["price"], 2),
                "sell_price": round(sell["price"], 2),
                "shares": sell["shares"],
                "pnl": round(pnl, 2),
                "pnl_pct": round(pnl_pct, 2),
                "buy_date": buy["created_at"][:10],
                "sell_date": sell["created_at"][:10],
            })

        return pd.DataFrame(report)
