"""内置策略模板库。

包含经典策略（均线、布林带、趋势、突破）和从 quant-trading-system
借鉴的增强策略（逆向、行业轮动、动量反转双模）。
"""

from __future__ import annotations

from typing import Any

import numpy as np
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


# ──────────────────────────────────────────────────────────────
# 以下策略借鉴自 JunHF/quant-trading-system
# ──────────────────────────────────────────────────────────────


class ContrarianStrategy(ParametrizedStrategy):
    """逆向策略 — 在资金流出但企稳的股票中寻找反弹机会。

    四因子非线性评分：
    - MFI (资金流量指数): 15-25 为超卖甜点区
    - CMF (蔡金资金流): -0.05~0 表示流出减速
    - OBV (能量潮): 下降中 = 逆向入场机会
    - VWAP 支撑: 价格低于 VWAP 得分更高
    """
    mfi_sweet_low: float = 15.0
    mfi_sweet_high: float = 25.0
    mfi_max: float = 35.0
    cmf_threshold: float = -0.05
    cmf_extreme: float = -0.30
    min_total_score: float = 55.0
    position_pct: float = 0.3

    @classmethod
    def param_specs(cls):
        return [
            ParamSpec("mfi_sweet_low", "float", 15.0, (10.0, 20.0), "MFI 甜点区下限"),
            ParamSpec("mfi_sweet_high", "float", 25.0, (20.0, 30.0), "MFI 甜点区上限"),
            ParamSpec("mfi_max", "float", 35.0, (30.0, 45.0), "MFI 过滤上限"),
            ParamSpec("cmf_threshold", "float", -0.05, (-0.10, 0.0), "CMF 减速阈值"),
            ParamSpec("min_total_score", "float", 55.0, (40.0, 70.0), "最低总分"),
            ParamSpec("position_pct", "float", 0.3, (0.1, 0.6), "仓位比例"),
        ]

    def initialize(self, ctx: StrategyContext):
        if not ctx.universe:
            ctx.set_universe(["600519"])

    def handle_bar(self, ctx: StrategyContext, data: pd.DataFrame):
        if len(data) < 30:
            return

        close = data["close"].values
        high = data["high"].values
        low = data["low"].values
        volume = data["volume"].values.astype(float)

        for sym in ctx.universe:
            score = self._score(close, high, low, volume)
            if score >= self.min_total_score:
                ctx.order_target_pct(sym, self.position_pct)

    def _score(self, close, high, low, volume) -> float:
        """四因子逆向评分，满分 100"""
        typical = (high + low + close) / 3

        # MFI (14-period)
        mfi = self._calc_mfi(typical, volume, 14)
        mfi_score = self._mfi_curve(mfi)

        # CMF (20-period)
        cmf = self._calc_cmf(high, low, close, volume, 20)
        cmf_score = self._cmf_curve(cmf)

        # OBV 趋势
        obv = self._calc_obv(close, volume)
        obv_score = 90 if len(obv) >= 2 and obv[-1] < obv[-2] else 70

        # VWAP 支撑
        vwap = np.sum(typical * volume) / np.sum(volume) if np.sum(volume) > 0 else close[-1]
        ratio = close[-1] / vwap if vwap > 0 else 1.0
        if ratio < 0.95:
            vwap_score = 100
        elif ratio < 1.0:
            vwap_score = 80
        else:
            vwap_score = 50

        return mfi_score * 0.35 + cmf_score * 0.25 + obv_score * 0.20 + vwap_score * 0.20

    def _mfi_curve(self, mfi: float) -> float:
        if mfi > self.mfi_max:
            return 0
        if self.mfi_sweet_low <= mfi <= self.mfi_sweet_high:
            return 100
        if mfi < self.mfi_sweet_low:
            return 60  # 极端超卖，可能还有下跌惯性
        return 40  # 甜点区上方

    def _cmf_curve(self, cmf: float) -> float:
        if cmf < self.cmf_extreme:
            return 0
        if self.cmf_threshold <= cmf <= 0:
            return 100  # 流出减速
        if cmf > 0:
            return 70  # 已经流入
        return 40

    @staticmethod
    def _calc_mfi(typical, volume, period):
        tp = pd.Series(typical)
        vol = pd.Series(volume)
        raw_mf = tp * vol
        pos_mf = raw_mf.where(tp.diff() > 0, 0).rolling(period).sum()
        neg_mf = raw_mf.where(tp.diff() < 0, 0).rolling(period).sum()
        ratio = pos_mf / neg_mf.replace(0, 1e-10)
        mfi = 100 - (100 / (1 + ratio))
        return float(mfi.iloc[-1]) if len(mfi) > 0 and pd.notna(mfi.iloc[-1]) else 50

    @staticmethod
    def _calc_cmf(high, low, close, volume, period):
        hl = pd.Series(high) - pd.Series(low)
        clv = ((pd.Series(close) - pd.Series(low)) - (pd.Series(high) - pd.Series(close))) / hl.replace(0, 1e-10)
        mfv = clv * pd.Series(volume)
        cmf = mfv.rolling(period).sum() / pd.Series(volume).rolling(period).sum().replace(0, 1e-10)
        return float(cmf.iloc[-1]) if len(cmf) > 0 and pd.notna(cmf.iloc[-1]) else 0

    @staticmethod
    def _calc_obv(close, volume):
        obv = [0.0]
        for i in range(1, len(close)):
            if close[i] > close[i - 1]:
                obv.append(obv[-1] + volume[i])
            elif close[i] < close[i - 1]:
                obv.append(obv[-1] - volume[i])
            else:
                obv.append(obv[-1])
        return obv


class SectorRotationStrategy(ParametrizedStrategy):
    """行业轮动策略 — 先选最强行业，再选行业内最强个股。

    通过 sector_map 参数定义行业分组（默认使用内置分组）。
    计算每个行业的 N 日平均涨幅，选 top K 行业，
    再在每个行业中选涨幅最大的个股。
    """
    lookback_period: int = 20
    top_sectors: int = 2
    stocks_per_sector: int = 2
    min_sector_return: float = 0.03
    position_pct: float = 0.5

    # 内置行业分组（可通过 set_params 覆盖 sector_map）
    _DEFAULT_SECTOR_MAP: dict = None

    @classmethod
    def param_specs(cls):
        return [
            ParamSpec("lookback_period", "int", 20, (10, 60), "行业动量回看周期"),
            ParamSpec("top_sectors", "int", 2, (1, 5), "选择前 N 个行业"),
            ParamSpec("stocks_per_sector", "int", 2, (1, 5), "每个行业选几只"),
            ParamSpec("min_sector_return", "float", 0.03, (0.01, 0.10), "行业最低涨幅阈值"),
            ParamSpec("position_pct", "float", 0.5, (0.1, 1.0), "单只股票仓位"),
        ]

    def initialize(self, ctx: StrategyContext):
        if not ctx.universe:
            ctx.set_universe(["600519"])

    def handle_bar(self, ctx: StrategyContext, data: pd.DataFrame):
        if len(data) < self.lookback_period + 1:
            return

        close = data["close"].values
        universe = ctx.universe or self.symbols

        # 按行业分组计算平均涨幅
        sector_map = self._get_sector_map(universe)
        sector_returns: dict[str, list[tuple[str, float]]] = {}

        for sector, symbols in sector_map.items():
            stock_returns = []
            for sym in symbols:
                if sym in universe:
                    # 使用 close 序列计算涨幅
                    ret = (close[-1] / close[-self.lookback_period] - 1) if close[-self.lookback_period] > 0 else 0
                    stock_returns.append((sym, ret))
            if stock_returns:
                avg_ret = np.mean([r for _, r in stock_returns])
                sector_returns[sector] = (avg_ret, stock_returns)

        # 选最强行业
        ranked = sorted(sector_returns.items(), key=lambda x: x[1][0], reverse=True)
        selected: list[str] = []
        for sector, (avg_ret, stocks) in ranked:
            if avg_ret < self.min_sector_return:
                break
            if len(selected) >= self.top_sectors * self.stocks_per_sector:
                break
            # 行业内按涨幅排序
            stocks_sorted = sorted(stocks, key=lambda x: x[1], reverse=True)
            for sym, _ in stocks_sorted[: self.stocks_per_sector]:
                selected.append(sym)

        # 执行：选中的买入，没选中的卖出
        for sym in universe:
            if sym in selected:
                ctx.order_target_pct(sym, self.position_pct / max(len(selected), 1))
            else:
                ctx.order_target_pct(sym, 0.0)

    def _get_sector_map(self, universe: list[str]) -> dict[str, list[str]]:
        """将 universe 按行业分组。优先用 sector_tags，回退到均分。"""
        try:
            from modules.sector_tags import SectorTagProvider
            provider = SectorTagProvider()
            groups: dict[str, list[str]] = {}
            for sym in universe:
                tags = provider.get_tags(sym)
                # 取第一个标签作为主行业
                sector = next(iter(tags), "其他") if tags else "其他"
                groups.setdefault(sector, []).append(sym)
            return groups
        except Exception:
            # 回退：所有股票归入同一组
            return {"全部": list(universe)}


class MomentumReversalStrategy(ParametrizedStrategy):
    """动量反转双模策略 — 自动切换趋势跟踪和逆向反转模式。

    动量模式：涨幅 >= 5%, RSI < 65, BB 位置 < 0.85
    反转模式：RSI <= 45, BB 位置 <= 0.40

    止盈：RSI >= 70 或 BB 位置 >= 0.85
    追踪止损：盈利 >= 8% 后回撤到 +5%
    """
    momentum_period: int = 10
    momentum_threshold: float = 0.05
    rsi_oversold: float = 45.0
    rsi_overbought: float = 70.0
    bb_upper_threshold: float = 0.85
    bb_lower_threshold: float = 0.40
    trailing_stop_trigger: float = 0.08
    trailing_stop_level: float = 0.05
    position_pct: float = 0.5

    @classmethod
    def param_specs(cls):
        return [
            ParamSpec("momentum_period", "int", 10, (5, 30), "动量回看周期"),
            ParamSpec("momentum_threshold", "float", 0.05, (0.03, 0.10), "动量模式涨幅阈值"),
            ParamSpec("rsi_oversold", "float", 45.0, (30.0, 50.0), "反转模式 RSI 上限"),
            ParamSpec("rsi_overbought", "float", 70.0, (65.0, 85.0), "止盈 RSI 阈值"),
            ParamSpec("trailing_stop_trigger", "float", 0.08, (0.05, 0.15), "追踪止损触发盈利"),
            ParamSpec("trailing_stop_level", "float", 0.05, (0.02, 0.10), "追踪止损回撤幅度"),
            ParamSpec("position_pct", "float", 0.5, (0.1, 1.0), "仓位比例"),
        ]

    def initialize(self, ctx: StrategyContext):
        if not ctx.universe:
            ctx.set_universe(["600519"])

    def handle_bar(self, ctx: StrategyContext, data: pd.DataFrame):
        if len(data) < max(self.momentum_period, 20) + 1:
            return

        close = data["close"].values

        # 指标计算
        momentum = close[-1] / close[-self.momentum_period] - 1 if close[-self.momentum_period] > 0 else 0
        rsi = self._rsi(close, 14)
        bb_pos = self._bb_position(close, 20, 2.0)

        for sym in ctx.universe:
            price = ctx.current_prices.get(sym, close[-1])
            pos = ctx.get_position(sym)
            shares = pos.shares if pos else 0

            # 持仓时：检查止盈和追踪止损
            if shares > 0 and pos and pos.avg_cost > 0:
                pnl_pct = (price - pos.avg_cost) / pos.avg_cost
                # 止盈
                if rsi >= self.rsi_overbought or bb_pos >= self.bb_upper_threshold:
                    ctx.order_target_pct(sym, 0.0)
                    continue
                # 追踪止损
                if pnl_pct >= self.trailing_stop_trigger:
                    if pnl_pct <= self.trailing_stop_level:
                        ctx.order_target_pct(sym, 0.0)
                        continue

            # 空仓时：判断模式并入场
            if shares == 0:
                is_momentum = (momentum >= self.momentum_threshold
                               and rsi < self.rsi_overbought
                               and bb_pos < self.bb_upper_threshold)
                is_reversal = (rsi <= self.rsi_oversold
                               and bb_pos <= self.bb_lower_threshold)

                if is_momentum:
                    ctx.order_target_pct(sym, self.position_pct)
                elif is_reversal:
                    ctx.order_target_pct(sym, self.position_pct * 0.6)  # 反转模式仓位更轻

    @staticmethod
    def _rsi(close, period=14) -> float:
        delta = pd.Series(close).diff()
        gain = delta.clip(lower=0).rolling(period).mean()
        loss = (-delta.clip(upper=0)).rolling(period).mean()
        rsi = 100 - (100 / (1 + gain / loss.replace(0, 1e-10)))
        return float(rsi.iloc[-1]) if len(rsi) > 0 and pd.notna(rsi.iloc[-1]) else 50

    @staticmethod
    def _bb_position(close, period=20, std_dev=2.0) -> float:
        s = pd.Series(close)
        sma = s.rolling(period).mean()
        std = s.rolling(period).std()
        upper = sma + std_dev * std
        lower = sma - std_dev * std
        if len(sma) == 0 or pd.isna(sma.iloc[-1]):
            return 0.5
        width = upper.iloc[-1] - lower.iloc[-1]
        if width <= 0:
            return 0.5
        return float((close[-1] - lower.iloc[-1]) / width)
