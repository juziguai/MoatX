"""信号生成引擎 — 对持仓/候选股运行策略，实时评估。"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

import pandas as pd

from modules.backtest.strategy import StrategyContext
from modules.indicators import IndicatorEngine
from modules.stock_data import StockData

_logger = logging.getLogger("moatx.signal")

SignalType = Literal["buy", "sell", "hold", "alert", "limit_up", "limit_down"]


@dataclass
class Signal:
    """交易信号数据类。"""
    symbol: str
    signal_type: SignalType
    price: float
    reason: str
    strategy_name: str = "moatx"
    confidence: float = 0.0     # 0-100
    indicators: dict = field(default_factory=dict)
    created_at: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


class SignalEngine:
    """信号引擎 — 用策略对单个标的做实时评估。

    与 BacktestEngine 不同（它遍历历史数据），SignalEngine 只评估最新一根 K 线。
    """

    def __init__(self, db=None):
        self.db = db
        self._sd = StockData()
        self._ind = IndicatorEngine()

    @staticmethod
    def load_params(strategy_name: str, params_file: str | None = None) -> dict | None:
        """从 JSON 文件加载指定策略的最优参数。

        如果 params_file 为 None，从默认路径 data/strategy_params.json 加载。
        返回 dict 参数，或 None（文件不存在/解析失败）。
        """
        from modules.strategy.optimizer import load_strategy_params
        from pathlib import Path
        path = Path(params_file) if params_file else None
        return load_strategy_params(strategy_name, path)

    def evaluate(self, symbol: str, strategy) -> Signal | None:
        """评估单个标的，返回信号（有动作）或 None（无动作）。"""
        df = self._fetch_data(symbol)
        if df.empty or len(df) < 60:
            return None

        ctx = StrategyContext()
        ctx.set_universe([symbol])
        # Inject a minimal portfolio-like interface
        ctx._current_date = df.index[-1].date() if hasattr(df.index[-1], "date") else df.index[-1]
        ctx._current_prices = {symbol: float(df["close"].iloc[-1])}

        # Patch get_position to return None (we don't track live positions here)
        # For paper trading positions, they're in the SignalStore
        ctx.get_position = lambda sym: None

        try:
            strategy.initialize(ctx)
            strategy.handle_bar(ctx, df.tail(60))
        except Exception as e:
            _logger.warning("策略 [%s] 初始化/运行失败（跳过信号）: %s", symbol, e)
            return None

        # Check if ctx has any pending orders (the strategy intentionally
        # accesses ctx.order_target_pct which does nothing without a portfolio)
        # We detect intent by evaluating the strategy's logic again with context
        return self._check_signal(strategy, ctx, symbol, df)

    def _fetch_data(self, symbol: str) -> pd.DataFrame:
        """Fetch daily data with indicators."""
        try:
            df = self._sd.get_daily(symbol)
            if df.empty:
                return df
            ind_df = self._ind.all_in_one(df)
            df = pd.concat([df, ind_df], axis=1)
            return df
        except Exception as e:
            _logger.warning("获取 [%s] 日线数据失败（跳过）: %s", symbol, e)
            return pd.DataFrame()

    def evaluate_all(self, symbols: list[str], strategy) -> list[Signal]:
        """批量评估多个标的。"""
        signals = []
        for sym in symbols:
            try:
                sig = self.evaluate(sym, strategy)
                if sig is not None:
                    signals.append(sig)
            except Exception as e:
                _logger.warning("评估 [%s] 时异常（跳过）: %s", sym, e)
                continue
        return signals

    def evaluate_holdings(self, holdings: list[str], strategy) -> list[Signal]:
        """对持仓列表运行策略评估。"""
        return self.evaluate_all(holdings, strategy)

    def generate_signals(self, holdings: list[str], strategy) -> list[Signal]:
        """对持仓列表生成交易信号（兼容接口）。"""
        return self.evaluate_all(holdings, strategy)

    def _check_signal(self, strategy, ctx, symbol, df) -> Signal | None:
        """Post-evaluation signal detection.

        For now, checks technical thresholds as a simple heuristic.
        Override in subclasses for strategy-specific signal logic.
        """
        if df.empty or len(df) < 2:
            return None

        latest = df.iloc[-1]
        prev = df.iloc[-2]
        close = float(latest.get("close", 0))
        _prev_close = float(prev.get("close", 0))

        # KDJ signal
        k, d, j = latest.get("k"), latest.get("d"), latest.get("j")
        prev_j = prev.get("j")

        signals = []
        confidence = 0.0

        # KDJ gold cross
        if all(pd.notna(x) for x in (k, d, j, prev_j)):
            if prev_j <= 20 and j > prev_j:
                signals.append("KDJ 超卖区回升")
                confidence = max(confidence, 60)

        # RSI oversold
        rsi6 = latest.get("rsi6")
        if pd.notna(rsi6):
            if rsi6 < 30:
                signals.append(f"RSI({rsi6:.1f}) 超卖")
                confidence = max(confidence, 65)
            elif rsi6 > 70:
                signals.append(f"RSI({rsi6:.1f}) 超买")
                confidence = max(confidence, 60)

        # Bollinger band touch
        boll_lower = latest.get("boll_lower")
        boll_upper = latest.get("boll_upper")
        if pd.notna(boll_lower) and close <= boll_lower:
            signals.append("触及布林带下轨")
            confidence = max(confidence, 55)
        elif pd.notna(boll_upper) and close >= boll_upper:
            signals.append("触及布林带上轨")
            confidence = max(confidence, 50)

        # MACD
        macd_val = latest.get("macd")
        prev_macd = prev.get("macd")
        if pd.notna(macd_val) and pd.notna(prev_macd):
            if prev_macd < 0 and macd_val > 0:
                signals.append("MACD 由负转正")
                confidence = max(confidence, 55)

        if not signals:
            return None

        sig_type: SignalType = "buy" if confidence >= 55 else "alert"
        reason = "; ".join(signals)

        indicators = {}
        for col in ("ma5", "ma10", "ma20", "macd", "k", "d", "j", "rsi6", "rsi12"):
            val = latest.get(col)
            if pd.notna(val):
                indicators[col] = round(float(val), 2)

        return Signal(
            symbol=symbol,
            signal_type=sig_type,
            price=close,
            reason=reason,
            strategy_name=getattr(strategy, "name", "moatx"),
            confidence=confidence,
            indicators=indicators,
        )
