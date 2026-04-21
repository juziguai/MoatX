"""
tests/test_indicators.py - 技术指标引擎单元测试
"""

import pytest
import pandas as pd
import numpy as np
from modules.indicators import IndicatorEngine


def make_ohlcv(n=100, seed=42):
    """生成模拟 OHLCV 数据"""
    np.random.seed(seed)
    dates = pd.date_range("2024-01-01", periods=n, freq="B")
    close = 10 + np.cumsum(np.random.randn(n) * 0.5)
    open_ = close + np.random.randn(n) * 0.2
    high = np.maximum(open_, close) + np.abs(np.random.randn(n) * 0.3)
    low = np.minimum(open_, close) - np.abs(np.random.randn(n) * 0.3)
    volume = np.random.randint(1e6, 1e8, n)
    df = pd.DataFrame({
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
    }, index=dates)
    return df


class TestIndicatorEngine:

    def test_sma(self):
        sma5 = IndicatorEngine.sma(pd.Series([1, 2, 3, 4, 5]), 3)
        assert round(sma5.iloc[-1], 4) == 4.0

    def test_ema(self):
        ema5 = IndicatorEngine.ema(pd.Series([1, 2, 3, 4, 5]), 3)
        assert ema5.iloc[-1] > 0

    def test_macd(self):
        close = pd.Series(np.linspace(10, 20, 100))
        macd = IndicatorEngine.macd(close)
        assert "dif" in macd.columns
        assert "dea" in macd.columns
        assert "macd" in macd.columns
        assert len(macd) == 100

    def test_kdj(self):
        df = make_ohlcv(50)
        kdj = IndicatorEngine.kdj(df["high"], df["low"], df["close"])
        assert "k" in kdj.columns
        assert "d" in kdj.columns
        assert "j" in kdj.columns
        # KDJ 值应该在 0-100 之间（rsv=50初始化后）
        assert kdj["k"].notna().any()

    def test_rsi(self):
        close = pd.Series(np.linspace(10, 20, 50))
        rsi = IndicatorEngine.rsi(close)
        assert len(rsi) == 50
        valid = rsi.dropna()
        assert (valid >= 0).all() and (valid <= 100).all()

    def test_boll(self):
        close = pd.Series(np.linspace(10, 20, 50))
        boll = IndicatorEngine.boll(close)
        assert "upper" in boll.columns
        assert "mid" in boll.columns
        assert "lower" in boll.columns
        # 布林带，上轨 > 中轨 > 下轨（忽略 NaN 热身后）
        valid = boll.dropna()
        assert (valid["upper"] >= valid["mid"]).all()
        assert (valid["mid"] >= valid["lower"]).all()

    def test_obv(self):
        close = pd.Series([10, 11, 10.5, 12, 11.5])
        volume = pd.Series([100, 200, 150, 300, 250])
        obv = IndicatorEngine.obv(close, volume)
        assert len(obv) == 5
        assert obv.iloc[-1] > 0

    def test_atr(self):
        df = make_ohlcv(20)
        atr = IndicatorEngine.atr(df["high"], df["low"], df["close"])
        assert len(atr) == 20
        assert atr.notna().any()

    def test_ma_cross(self):
        close = pd.Series(np.linspace(10, 20, 50))
        cross = IndicatorEngine.ma_cross(close, fast=5, slow=20)
        assert "cross_up" in cross.columns
        assert "cross_down" in cross.columns

    def test_all_in_one(self):
        df = make_ohlcv(200)
        result = IndicatorEngine.all_in_one(df)
        # 均线
        assert "ma5" in result.columns
        assert "ma20" in result.columns
        assert "ma60" in result.columns
        # MACD
        assert "dif" in result.columns
        assert "dea" in result.columns
        assert "macd" in result.columns
        # KDJ
        assert "k" in result.columns
        assert "d" in result.columns
        assert "j" in result.columns
        # RSI
        assert "rsi6" in result.columns
        assert "rsi12" in result.columns
        assert "rsi24" in result.columns
        # 布林带
        assert "boll_upper" in result.columns
        assert "boll_mid" in result.columns
        assert "boll_lower" in result.columns
        # ATR / CCI / DMI / OBV
        assert "atr" in result.columns
        assert "cci" in result.columns
        assert "obv" in result.columns
        assert "plus_di" in result.columns
        assert "minus_di" in result.columns


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
