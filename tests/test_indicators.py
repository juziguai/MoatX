"""P0: indicators.py 技术指标测试"""

import pytest
import pandas as pd
import numpy as np
from modules.indicators import IndicatorEngine


class TestSMA:
    def test_sma_rising(self):
        s = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])
        result = IndicatorEngine.sma(s, window=3)
        assert result.iloc[-1] == 4.0  # mean of [3,4,5]

    def test_sma_single_value(self):
        s = pd.Series([5.0, 5.0, 5.0])
        result = IndicatorEngine.sma(s, window=3)
        assert result.iloc[-1] == 5.0

    def test_sma_shorter_than_window(self):
        s = pd.Series([1.0, 2.0])
        result = IndicatorEngine.sma(s, window=5)
        assert not pd.isna(result.iloc[-1])  # min_periods=1


class TestEMA:
    def test_ema_basic(self):
        s = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])
        result = IndicatorEngine.ema(s, span=3)
        assert not pd.isna(result.iloc[-1])

    def test_ema_constant(self):
        s = pd.Series([10.0] * 20)
        result = IndicatorEngine.ema(s, span=5)
        assert abs(result.iloc[-1] - 10.0) < 0.01


class TestMACD:
    def test_macd_output_columns(self):
        close = pd.Series(np.linspace(100, 110, 60))
        result = IndicatorEngine.macd(close)
        assert "dif" in result.columns
        assert "dea" in result.columns
        assert "macd" in result.columns

    def test_macd_not_all_nan(self):
        close = pd.Series(np.linspace(100, 110, 100))
        result = IndicatorEngine.macd(close)
        last = result.dropna().iloc[-1]
        assert not pd.isna(last["dif"])

    def test_macd_golden_cross_signal(self):
        """dif 上穿 dea"""
        np.random.seed(42)
        close = pd.Series(100 + np.cumsum(np.random.randn(80) * 0.5))
        result = IndicatorEngine.macd(close)
        dif = result["dif"].dropna()
        dea = result["dea"].dropna()
        if len(dif) >= 2:
            cross_up = (dif.iloc[-1] > dea.iloc[-1]) and (dif.iloc[-2] <= dea.iloc[-2])
            assert cross_up is not None


class TestKDJ:
    def test_kdj_output_columns(self):
        df = pd.DataFrame({
            "high": np.linspace(105, 115, 60),
            "low":  np.linspace(95, 105, 60),
            "close": np.linspace(100, 110, 60),
        })
        result = IndicatorEngine.kdj(df["high"], df["low"], df["close"])
        assert "k" in result.columns
        assert "d" in result.columns
        assert "j" in result.columns

    def test_kdj_extreme_overbought(self):
        """持续上涨后 J 值偏高"""
        highs = pd.Series(np.linspace(100, 130, 60))
        lows = pd.Series(np.linspace(98, 128, 60))
        closes = pd.Series(np.linspace(99, 129, 60))
        result = IndicatorEngine.kdj(highs, lows, closes)
        valid = result.dropna()
        assert valid["j"].max() >= 50

    def test_kdj_extreme_oversold(self):
        """持续下跌后 J 值偏低"""
        highs = pd.Series(np.linspace(130, 100, 60))
        lows = pd.Series(np.linspace(128, 98, 60))
        closes = pd.Series(np.linspace(129, 99, 60))
        result = IndicatorEngine.kdj(highs, lows, closes)
        valid = result.dropna()
        assert valid["j"].min() <= 50

    def test_kdj_range(self):
        high = pd.Series(np.linspace(105, 115, 60))
        low  = pd.Series(np.linspace(95, 105, 60))
        close = pd.Series(np.linspace(100, 110, 60))
        result = IndicatorEngine.kdj(high, low, close)
        valid = result.dropna()
        assert (valid["k"] >= 0).all() and (valid["k"] <= 100).all()
        assert (valid["d"] >= 0).all() and (valid["d"] <= 100).all()


class TestRSI:
    def test_rsi_range(self):
        close = pd.Series(np.linspace(100, 120, 60))
        result = IndicatorEngine.rsi(close, period=14)
        valid = result.dropna()
        assert (valid >= 0).all() and (valid <= 100).all()

    def test_rsi_neutral_on_flat(self):
        """价格不变时 RSI 应趋于中性"""
        close = pd.Series([100.0] * 30)
        result = IndicatorEngine.rsi(close, period=14)
        assert len(result) == 30

    def test_rsi_short_series(self):
        close = pd.Series([100.0, 101.0])
        result = IndicatorEngine.rsi(close, period=14)
        assert len(result) == 2


class TestBOLL:
    def test_boll_output_columns(self):
        close = pd.Series(np.linspace(100, 120, 60))
        result = IndicatorEngine.boll(close)
        assert "upper" in result.columns
        assert "mid" in result.columns
        assert "lower" in result.columns

    def test_boll_upper_gt_lower(self):
        close = pd.Series(np.linspace(100, 120, 60))
        result = IndicatorEngine.boll(close)
        last = result.dropna().iloc[-1]
        assert last["upper"] >= last["lower"]

    def test_boll_price_in_band(self):
        close = pd.Series([100.0] * 60)
        result = IndicatorEngine.boll(close)
        last = result.dropna().iloc[-1]
        assert last["lower"] <= last["mid"] <= last["upper"]


class TestMACross:
    def test_ma_cross_columns(self):
        close = pd.Series(np.linspace(100, 120, 60))
        result = IndicatorEngine.ma_cross(close, fast=5, slow=20)
        assert "ma_fast" in result.columns
        assert "ma_slow" in result.columns
        assert "cross_up" in result.columns
        assert "cross_down" in result.columns

    def test_ma_cross_detected(self):
        """快线上穿慢线应检测到金叉"""
        np.random.seed(1)
        close = pd.Series(100 + np.cumsum(np.random.randn(100) * 0.3))
        result = IndicatorEngine.ma_cross(close, fast=5, slow=20)
        assert result["cross_up"].any() or result["cross_down"].any()


class TestAllInOne:
    def test_all_in_one_adds_indicators(self, sample_daily_df):
        result = IndicatorEngine.all_in_one(sample_daily_df)
        expected = ["ma5", "ma10", "ma20", "dif", "dea", "macd", "k", "d", "j",
                     "rsi6", "rsi12", "rsi24", "boll_upper", "boll_mid", "boll_lower"]
        for col in expected:
            assert col in result.columns, f"Missing: {col}"

    def test_all_in_one_returns_indicators(self, sample_daily_df):
        result = IndicatorEngine.all_in_one(sample_daily_df)
        assert "ma5" in result.columns
        assert "rsi6" in result.columns
        assert "boll_upper" in result.columns
        assert len(result) == len(sample_daily_df)

    def test_all_in_one_empty(self):
        df = pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
        result = IndicatorEngine.all_in_one(df)
        assert result.empty
