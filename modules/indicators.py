"""
indicators.py - 技术指标计算引擎
支持：MACD、KDJ、RSI、布林带、均线系、DMI、CCI、OBV、ATR等
"""

import pandas as pd
import numpy as np
from typing import Optional


class IndicatorEngine:
    """技术指标计算引擎"""

    @staticmethod
    def sma(series: pd.Series, window: int) -> pd.Series:
        """简单移动平均 SMA"""
        return series.rolling(window=window, min_periods=1).mean()

    @staticmethod
    def ema(series: pd.Series, span: int) -> pd.Series:
        """指数移动平均 EMA"""
        return series.ewm(span=span, adjust=False).mean()

    @staticmethod
    def macd(
        series: pd.Series,
        fast: int = 12,
        slow: int = 26,
        signal: int = 9
    ) -> pd.DataFrame:
        """
        MACD 指标
        返回: dif, dea, macd(柱状图 * 2)
        """
        ema_fast = series.ewm(span=fast, adjust=False).mean()
        ema_slow = series.ewm(span=slow, adjust=False).mean()
        dif = ema_fast - ema_slow
        dea = dif.ewm(span=signal, adjust=False).mean()
        macd = (dif - dea) * 2  # 柱状图放大2倍
        return pd.DataFrame({"dif": dif, "dea": dea, "macd": macd})

    @staticmethod
    def kdj(
        high: pd.Series,
        low: pd.Series,
        close: pd.Series,
        n: int = 9,
        m1: int = 3,
        m2: int = 3
    ) -> pd.DataFrame:
        """
        KDJ 随机指标
        """
        lowest_low = low.rolling(window=n, min_periods=1).min()
        highest_high = high.rolling(window=n, min_periods=1).max()

        rsv = (close - lowest_low) / (highest_high - lowest_low) * 100
        rsv = rsv.fillna(50)

        k = rsv.ewm(com=m1 - 1, adjust=False).mean()
        d = k.ewm(com=m2 - 1, adjust=False).mean()
        j = 3 * k - 2 * d

        return pd.DataFrame({"k": k, "d": d, "j": j})

    @staticmethod
    def rsi(series: pd.Series, period: int = 14) -> pd.Series:
        """
        RSI 相对强弱指标
        """
        delta = series.diff()
        gain = delta.where(delta > 0, 0)
        loss = (-delta).where(delta < 0, 0)

        avg_gain = gain.ewm(alpha=1/period, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1/period, adjust=False).mean()

        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return rsi

    @staticmethod
    def boll(
        series: pd.Series,
        window: int = 20,
        nb_std: float = 2.0
    ) -> pd.DataFrame:
        """
        布林带
        """
        mid = series.rolling(window=window).mean()
        std = series.rolling(window=window).std()
        upper = mid + nb_std * std
        lower = mid - nb_std * std

        return pd.DataFrame({"upper": upper, "mid": mid, "lower": lower})

    @staticmethod
    def obv(close: pd.Series, volume: pd.Series) -> pd.Series:
        """
        OBV 能量潮
        """
        direction = np.sign(close.diff())
        obv = (direction * volume).cumsum()
        return obv

    @staticmethod
    def atr(
        high: pd.Series,
        low: pd.Series,
        close: pd.Series,
        period: int = 14
    ) -> pd.Series:
        """
        ATR 平均真实波幅
        """
        tr1 = high - low
        tr2 = (high - close.shift()).abs()
        tr3 = (low - close.shift()).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.rolling(window=period).mean()
        return atr

    @staticmethod
    def cci(
        high: pd.Series,
        low: pd.Series,
        close: pd.Series,
        period: int = 14
    ) -> pd.Series:
        """
        CCI 顺势指标
        """
        tp = (high + low + close) / 3
        sma_tp = tp.rolling(window=period).mean()
        mad = tp.rolling(window=period).apply(
            lambda x: np.abs(x - x.mean()).mean(), raw=True
        )
        cci = (tp - sma_tp) / (0.015 * mad)
        return cci

    @staticmethod
    def dmi(
        high: pd.Series,
        low: pd.Series,
        close: pd.Series,
        period: int = 14
    ) -> pd.DataFrame:
        """
        DMI 趋向指标
        返回: +DI, -DI, ADX, ADXR
        """
        # 真实波幅
        tr1 = high - low
        tr2 = (high - close.shift()).abs()
        tr3 = (low - close.shift()).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

        # 方向性运动
        plus_dm = high.diff()
        minus_dm = -low.diff()
        plus_dm = plus_dm.where((plus_dm > minus_dm) & (plus_dm > 0), 0)
        minus_dm = minus_dm.where((minus_dm > plus_dm) & (minus_dm > 0), 0)

        # 平滑
        atr = tr.rolling(window=period).mean()
        plus_di = 100 * (plus_dm.rolling(window=period).mean() / atr)
        minus_di = 100 * (minus_dm.rolling(window=period).mean() / atr)

        dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di)
        adx = dx.rolling(window=period).mean()
        adxr = (adx + adx.shift(period)) / 2

        return pd.DataFrame({
            "plus_di": plus_di,
            "minus_di": minus_di,
            "adx": adx,
            "adxr": adxr
        })

    @staticmethod
    def ma_cross(close: pd.Series, fast: int = 5, slow: int = 20) -> pd.DataFrame:
        """
        均线金叉/死叉信号
        """
        ma_fast = close.rolling(window=fast).mean()
        ma_slow = close.rolling(window=slow).mean()

        cross_up = (ma_fast > ma_slow) & (ma_fast.shift(1) <= ma_slow.shift(1))
        cross_down = (ma_fast < ma_slow) & (ma_fast.shift(1) >= ma_slow.shift(1))

        return pd.DataFrame({
            "ma_fast": ma_fast,
            "ma_slow": ma_slow,
            "cross_up": cross_up,
            "cross_down": cross_down
        })

    @staticmethod
    def all_in_one(df: pd.DataFrame) -> pd.DataFrame:
        """
        计算所有常用指标，返回带标注的 DataFrame
        适用于有 OHLCV 列的日线数据
        """
        close = df["close"]
        high = df["high"]
        low = df["low"]
        open_ = df["open"]
        volume = df["volume"]

        result = pd.DataFrame(index=df.index)

        # 均线
        result["ma5"] = close.rolling(5).mean()
        result["ma10"] = close.rolling(10).mean()
        result["ma20"] = close.rolling(20).mean()
        result["ma60"] = close.rolling(60).mean()
        result["ma120"] = close.rolling(120).mean()
        result["ma250"] = close.rolling(250).mean()

        # MACD
        macd = IndicatorEngine.macd(close)
        result["dif"] = macd["dif"]
        result["dea"] = macd["dea"]
        result["macd"] = macd["macd"]

        # KDJ
        kdj = IndicatorEngine.kdj(high, low, close)
        result["k"] = kdj["k"]
        result["d"] = kdj["d"]
        result["j"] = kdj["j"]

        # RSI
        result["rsi6"] = IndicatorEngine.rsi(close, 6)
        result["rsi12"] = IndicatorEngine.rsi(close, 12)
        result["rsi24"] = IndicatorEngine.rsi(close, 24)

        # 布林带
        boll = IndicatorEngine.boll(close)
        result["boll_upper"] = boll["upper"]
        result["boll_mid"] = boll["mid"]
        result["boll_lower"] = boll["lower"]

        # ATR
        result["atr"] = IndicatorEngine.atr(high, low, close)

        # CCI
        result["cci"] = IndicatorEngine.cci(high, low, close)

        # DMI
        dmi = IndicatorEngine.dmi(high, low, close)
        result["plus_di"] = dmi["plus_di"]
        result["minus_di"] = dmi["minus_di"]
        result["adx"] = dmi["adx"]
        result["adxr"] = dmi["adxr"]

        # OBV
        result["obv"] = IndicatorEngine.obv(close, volume)

        # 偏离率
        result["bias5"] = (close - result["ma5"]) / result["ma5"] * 100
        result["bias10"] = (close - result["ma10"]) / result["ma10"] * 100
        result["bias20"] = (close - result["ma20"]) / result["ma20"] * 100

        # 成交量均线
        result["vol_ma5"] = volume.rolling(5).mean()
        result["vol_ma20"] = volume.rolling(20).mean()

        return result
