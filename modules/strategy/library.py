"""内置策略模板库。"""

from __future__ import annotations

import pandas as pd

from modules.backtest.strategy import StrategyContext
from .base import ParametrizedStrategy, ParamSpec


class MovingAverageCross(ParametrizedStrategy):
    """经典均线金叉/死叉策略。

    快线上穿慢线 -> 买入；快线下穿慢线 -> 卖出。
    """
    fast_period: int = 5
    slow_period: int = 20
    stop_loss_pct: float = 0.05
    position_pct: float = 0.8

    @classmethod
    def param_specs(cls):
        return [
            ParamSpec("fast_period", "int", 5, (2, 30), "短期均线周期"),
            ParamSpec("slow_period", "int", 20, (10, 120), "长期均线周期"),
            ParamSpec("stop_loss_pct", "float", 0.05, (0.01, 0.15), "止损比例"),
            ParamSpec("position_pct", "float", 0.8, (0.1, 1.0), "买入仓位比例"),
        ]

    def initialize(self, ctx: StrategyContext):
        if not ctx.universe:
            ctx.set_universe(["600519"])

    def handle_bar(self, ctx: StrategyContext, data: pd.DataFrame):
        if len(data) < self.slow_period + 1:
            return
        close = data["close"].values
        ma_fast = pd.Series(close).rolling(self.fast_period).mean()
        ma_slow = pd.Series(close).rolling(self.slow_period).mean()

        for sym in ctx.universe:
            price = ctx.current_prices.get(sym, close[-1])
            pos = ctx.get_position(sym)
            shares = pos.shares if pos else 0

            # 止损检查
            if shares > 0 and pos:
                cost = pos.avg_cost
                if cost > 0 and (price - cost) / cost < -self.stop_loss_pct:
                    ctx.order_target_pct(sym, 0.0)
                    continue

            if len(ma_fast) >= 2 and len(ma_slow) >= 2:
                prev_fast, curr_fast = ma_fast.iloc[-2], ma_fast.iloc[-1]
                prev_slow, curr_slow = ma_slow.iloc[-2], ma_slow.iloc[-1]

                if pd.notna(curr_fast) and pd.notna(curr_slow):
                    if prev_fast <= prev_slow and curr_fast > curr_slow:
                        ctx.order_target_pct(sym, self.position_pct)
                    elif prev_fast >= prev_slow and curr_fast < curr_slow:
                        ctx.order_target_pct(sym, 0.0)


class MeanReversion(ParametrizedStrategy):
    """布林带均值回归 + RSI 过滤策略。

    价格触及下轨 + RSI 超卖 -> 买入；
    价格触及上轨 + RSI 超买 -> 卖出。
    """
    bb_period: int = 20
    bb_std: float = 2.0
    rsi_period: int = 14
    rsi_oversold: float = 30.0
    rsi_overbought: float = 70.0
    position_pct: float = 0.5

    @classmethod
    def param_specs(cls):
        return [
            ParamSpec("bb_period", "int", 20, (10, 50), "布林带周期"),
            ParamSpec("bb_std", "float", 2.0, (1.5, 3.0), "布林带标准差倍数"),
            ParamSpec("rsi_period", "int", 14, (6, 30), "RSI 周期"),
            ParamSpec("rsi_oversold", "float", 30.0, (20.0, 40.0), "RSI 超卖阈值"),
            ParamSpec("rsi_overbought", "float", 70.0, (60.0, 80.0), "RSI 超买阈值"),
            ParamSpec("position_pct", "float", 0.5, (0.1, 1.0), "仓位比例"),
        ]

    def initialize(self, ctx: StrategyContext):
        if not ctx.universe:
            ctx.set_universe(["600519"])

    def handle_bar(self, ctx: StrategyContext, data: pd.DataFrame):
        if len(data) < self.bb_period:
            return

        close = data["close"].values
        _high = data["high"].values
        _low = data["low"].values

        # 布林带
        sma = pd.Series(close).rolling(self.bb_period).mean()
        std = pd.Series(close).rolling(self.bb_period).std()
        upper = sma + self.bb_std * std
        lower = sma - self.bb_std * std

        # RSI
        delta = pd.Series(close).diff()
        gain = delta.clip(lower=0).rolling(self.rsi_period).mean()
        loss = (-delta.clip(upper=0)).rolling(self.rsi_period).mean()
        rsi = 100 - (100 / (1 + gain / loss.replace(0, 1e-10)))

        for sym in ctx.universe:
            price = ctx.current_prices.get(sym, close[-1])
            curr_rsi = rsi.iloc[-1] if len(rsi) > 0 else 50

            if len(lower) > 0 and len(upper) > 0:
                if price <= lower.iloc[-1] and curr_rsi < self.rsi_oversold:
                    ctx.order_target_pct(sym, self.position_pct)
                elif price >= upper.iloc[-1] and curr_rsi > self.rsi_overbought:
                    ctx.order_target_pct(sym, 0.0)


class TrendFollowing(ParametrizedStrategy):
    """ADX/EMA 趋势追踪策略。

    ADX > 阈值 + EMA 上升 -> 买入；
    ADX < 阈值 或 EMA 下降 -> 卖出。
    """
    ema_period: int = 20
    adx_period: int = 14
    adx_threshold: float = 25.0
    position_pct: float = 0.7

    @classmethod
    def param_specs(cls):
        return [
            ParamSpec("ema_period", "int", 20, (5, 60), "EMA 周期"),
            ParamSpec("adx_period", "int", 14, (7, 30), "ADX 周期"),
            ParamSpec("adx_threshold", "float", 25.0, (15.0, 40.0), "ADX 趋势阈值"),
            ParamSpec("position_pct", "float", 0.7, (0.1, 1.0), "仓位比例"),
        ]

    def initialize(self, ctx: StrategyContext):
        if not ctx.universe:
            ctx.set_universe(["600519"])

    def handle_bar(self, ctx: StrategyContext, data: pd.DataFrame):
        if len(data) < self.adx_period + 1:
            return

        close = data["close"].values
        _high = data["high"].values
        _low = data["low"].values

        ema = pd.Series(close).ewm(span=self.ema_period).mean()

        # 简易 ADX 计算
        tr = pd.DataFrame({
            "hl": _high - _low,
            "hc": abs(_high - pd.Series(close).shift(1)),
            "lc": abs(_low - pd.Series(close).shift(1)),
        }).max(axis=1)
        atr = tr.rolling(self.adx_period).mean()

        plus_dm = pd.Series(_high).diff()
        minus_dm = pd.Series(_low).diff()
        plus_di = 100 * (plus_dm.where(plus_dm > 0, 0).rolling(self.adx_period).mean() / atr.replace(0, 1e-10))
        minus_di = 100 * ((-minus_dm).where(minus_dm < 0, 0).rolling(self.adx_period).mean() / atr.replace(0, 1e-10))
        dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di).replace(0, 1e-10)
        adx = dx.rolling(self.adx_period).mean()

        for sym in ctx.universe:
            # price = ctx.current_prices.get(sym, close[-1])

            if len(ema) > 0 and len(adx) > 0:
                ema_rising = ema.iloc[-1] > ema.iloc[-2] if len(ema) >= 2 else False
                strong_trend = adx.iloc[-1] > self.adx_threshold if pd.notna(adx.iloc[-1]) else False

                if ema_rising and strong_trend:
                    ctx.order_target_pct(sym, self.position_pct)
                elif not ema_rising:
                    ctx.order_target_pct(sym, 0.0)


class BreakoutStrategy(ParametrizedStrategy):
    """N 日高低点突破策略。

    价格创 N 日新高 -> 买入；
    价格创 N 日新低 -> 卖出。
    """
    lookback: int = 20
    position_pct: float = 0.6
    stop_loss_pct: float = 0.07

    @classmethod
    def param_specs(cls):
        return [
            ParamSpec("lookback", "int", 20, (5, 60), "突破回看周期"),
            ParamSpec("position_pct", "float", 0.6, (0.1, 1.0), "买入仓位比例"),
            ParamSpec("stop_loss_pct", "float", 0.07, (0.02, 0.15), "止损比例"),
        ]

    def initialize(self, ctx: StrategyContext):
        if not ctx.universe:
            ctx.set_universe(["600519"])

    def handle_bar(self, ctx: StrategyContext, data: pd.DataFrame):
        if len(data) < self.lookback + 1:
            return

        close = data["close"].values
        _high = data["high"].values
        _low = data["low"].values

        rolling_high = pd.Series(_high).rolling(self.lookback).max()
        rolling_low = pd.Series(_low).rolling(self.lookback).min()

        for sym in ctx.universe:
            price = ctx.current_prices.get(sym, close[-1])
            pos = ctx.get_position(sym)
            shares = pos.shares if pos else 0

            # 止损
            if shares > 0 and pos and pos.avg_cost > 0:
                if (price - pos.avg_cost) / pos.avg_cost < -self.stop_loss_pct:
                    ctx.order_target_pct(sym, 0.0)
                    continue

            if pd.notna(rolling_high.iloc[-1]) and pd.notna(rolling_low.iloc[-1]):
                if close[-1] >= rolling_high.iloc[-1]:
                    ctx.order_target_pct(sym, self.position_pct)
                elif close[-1] <= rolling_low.iloc[-1]:
                    ctx.order_target_pct(sym, 0.0)


class MACrossWithVolume(ParametrizedStrategy):
    """均线交叉 + 成交量确认策略。

    金叉 + 成交量放大 -> 买入；
    死叉 -> 卖出。
    """
    fast_period: int = 5
    slow_period: int = 20
    volume_ratio: float = 1.5
    position_pct: float = 0.8

    @classmethod
    def param_specs(cls):
        return [
            ParamSpec("fast_period", "int", 5, (2, 30), "短期均线周期"),
            ParamSpec("slow_period", "int", 20, (10, 120), "长期均线周期"),
            ParamSpec("volume_ratio", "float", 1.5, (1.0, 3.0), "成交量放大倍数阈值"),
            ParamSpec("position_pct", "float", 0.8, (0.1, 1.0), "买入仓位比例"),
        ]

    def initialize(self, ctx: StrategyContext):
        if not ctx.universe:
            ctx.set_universe(["600519"])

    def handle_bar(self, ctx: StrategyContext, data: pd.DataFrame):
        if len(data) < self.slow_period + 1:
            return

        close = data["close"].values
        volume = data["volume"].values

        ma_fast = pd.Series(close).rolling(self.fast_period).mean()
        ma_slow = pd.Series(close).rolling(self.slow_period).mean()
        vol_ma = pd.Series(volume).rolling(self.slow_period).mean()

        for sym in ctx.universe:
            if len(ma_fast) >= 2 and len(ma_slow) >= 2 and len(vol_ma) > 0:
                prev_fast, curr_fast = ma_fast.iloc[-2], ma_fast.iloc[-1]
                prev_slow, curr_slow = ma_slow.iloc[-2], ma_slow.iloc[-1]
                vol_ratio = volume[-1] / vol_ma.iloc[-1] if vol_ma.iloc[-1] > 0 else 0

                if pd.notna(curr_fast) and pd.notna(curr_slow):
                    golden_cross = prev_fast <= prev_slow and curr_fast > curr_slow
                    dead_cross = prev_fast >= prev_slow and curr_fast < curr_slow

                    if golden_cross and vol_ratio >= self.volume_ratio:
                        ctx.order_target_pct(sym, self.position_pct)
                    elif dead_cross:
                        ctx.order_target_pct(sym, 0.0)
