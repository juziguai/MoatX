"""Sell signal engine for paper trading — stop profit/loss, technical, timeout."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

import pandas as pd

from modules.config import SimulationSettings
from modules.stock_data import StockData
from modules.indicators import IndicatorEngine


SignalType = Literal["stop_profit", "stop_loss", "technical", "timeout"]


@dataclass
class SellSignal:
    """Sell signal for a paper trading position."""
    symbol: str
    reason: str
    signal_type: SignalType
    price: float
    entry_price: float
    hold_days: int
    pnl_pct: float


class SellSignalEngine:
    """Evaluate sell conditions for paper trading holdings.

   四条规则:
    1. 止盈 — 当前价 / 买入均价 - 1 >= stop_profit_pct
    2. 止损 — 当前价 / 买入均价 - 1 <= -stop_loss_pct
    3. 技术卖出 — KDJ 超买 / RSI 超买 / MACD 死叉
    4. 超期持仓 — hold_days > max_hold_days
    """

    def __init__(self, sim_cfg: SimulationSettings | None = None):
        from modules.config import cfg as _cfg
        self._cfg = sim_cfg or _cfg().simulation
        self._sd = StockData()
        self._ind = IndicatorEngine()

    def evaluate(self, symbol: str, holding: dict) -> SellSignal | None:
        """Evaluate sell conditions for a single holding.

        Args:
            symbol: Stock code
            holding: dict with keys: avg_cost, shares, entry_date (YYYY-MM-DD)

        Returns:
            SellSignal if any condition triggers, None otherwise.
        """
        entry_price = float(holding.get("avg_cost", 0))
        shares = int(holding.get("shares", 0))
        if entry_price <= 0 or shares <= 0:
            return None

        # Current price
        try:
            q = self._sd.get_realtime_quote(symbol)
            current_price = float(q.get("price") or 0)
        except Exception:
            current_price = 0.0

        # Hold days
        entry_str = holding.get("entry_date") or holding.get("created_at", "")[:10]
        try:
            entry_date = datetime.strptime(entry_str[:10], "%Y-%m-%d").date()
            hold_days = (datetime.now().date() - entry_date).days
        except Exception:
            hold_days = 0

        pnl_pct = (current_price - entry_price) / entry_price if entry_price > 0 else 0.0

        # Rule 4: timeout (always checked, even without current price)
        if hold_days > self._cfg.max_hold_days:
            return SellSignal(
                symbol=symbol,
                reason=f"持仓超期 {hold_days}天，收益 {pnl_pct*100:+.1f}%",
                signal_type="timeout",
                price=current_price or entry_price,
                entry_price=entry_price,
                hold_days=hold_days,
                pnl_pct=pnl_pct,
            )

        # Skip rules 1-3 if no current price (non-trading hours)
        if current_price <= 0:
            return None

        # Rule 1: stop profit
        if pnl_pct >= self._cfg.stop_profit_pct:
            return SellSignal(
                symbol=symbol,
                reason=f"止盈 {pnl_pct*100:+.1f}%，持有 {hold_days}天",
                signal_type="stop_profit",
                price=current_price,
                entry_price=entry_price,
                hold_days=hold_days,
                pnl_pct=pnl_pct,
            )

        # Rule 2: stop loss
        if pnl_pct <= -self._cfg.stop_loss_pct:
            return SellSignal(
                symbol=symbol,
                reason=f"止损 {pnl_pct*100:+.1f}%，持有 {hold_days}天",
                signal_type="stop_loss",
                price=current_price,
                entry_price=entry_price,
                hold_days=hold_days,
                pnl_pct=pnl_pct,
            )

        # Rule 3: technical indicators
        reason = self._check_technical(symbol)
        if reason:
            return SellSignal(
                symbol=symbol,
                reason=f"{reason}，持有 {hold_days}天",
                signal_type="technical",
                price=current_price,
                entry_price=entry_price,
                hold_days=hold_days,
                pnl_pct=pnl_pct,
            )

        return None

    def _check_technical(self, symbol: str) -> str | None:
        """Check technical indicators, return reason string or None."""
        try:
            df = self._sd.get_daily(symbol)
            if df.empty or len(df) < 20:
                return None
            ind_df = self._ind.all_in_one(df)
            df = pd.concat([df, ind_df], axis=1)
            latest = df.iloc[-1]
        except Exception:
            return None

        reasons = []

        # KDJ overbought
        j = latest.get("j")
        if pd.notna(j) and j > self._cfg.kdj_overbought:
            reasons.append(f"KDJ 超买 J={j:.0f}")

        # RSI overbought
        rsi12 = latest.get("rsi12")
        if pd.notna(rsi12) and rsi12 > self._cfg.rsi_overbought:
            reasons.append(f"RSI 超买 {rsi12:.1f}")

        # MACD death cross
        macd = latest.get("macd")
        dif = latest.get("dif")
        if pd.notna(macd) and pd.notna(dif):
            if len(df) >= 2:
                prev_dif = df.iloc[-2].get("dif")
                prev_macd = df.iloc[-2].get("macd")
                if all(pd.notna(x) for x in (prev_dif, prev_macd)):
                    if prev_dif > prev_macd and dif <= macd:
                        reasons.append("MACD 死叉")

        return "；".join(reasons) if reasons else None

    def evaluate_all(self, holdings: list[dict]) -> list[SellSignal]:
        """Evaluate all holdings, return list of triggered sell signals."""
        signals = []
        for h in holdings:
            sym = h.get("symbol") or h.get("code")
            if not sym:
                continue
            sig = self.evaluate(sym, h)
            if sig is not None:
                signals.append(sig)
        return signals
