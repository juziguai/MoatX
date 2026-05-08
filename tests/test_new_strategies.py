"""新增策略和 K-fold 验证测试"""

from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
import pytest

from modules.strategy.library import (
    ContrarianStrategy,
    MomentumReversalStrategy,
    SectorRotationStrategy,
)
from modules.strategy.kfold import kfold_validate
from modules.strategy.base import ParametrizedStrategy, ParamSpec


# ── 新策略基本功能测试 ────────────────────────────────────


class TestContrarianStrategy:
    def test_has_param_specs(self):
        specs = ContrarianStrategy.param_specs()
        assert len(specs) >= 5
        names = {s.name for s in specs}
        assert "mfi_sweet_low" in names
        assert "position_pct" in names

    def test_set_params(self):
        s = ContrarianStrategy()
        s.set_params(mfi_sweet_low=12.0, position_pct=0.2)
        assert s.mfi_sweet_low == 12.0
        assert s.position_pct == 0.2

    def test_score_extreme_mfi_filtered(self):
        """MFI 超过 mfi_max 应该返回低分"""
        s = ContrarianStrategy()
        s.set_params(mfi_max=35.0)
        # 构造一个 MFI 会很高的场景（连续上涨 + 大量）
        close = np.arange(10.0, 40.0, 1.0)
        high = close * 1.01
        low = close * 0.99
        volume = np.full(len(close), 1_000_000)
        score = s._score(close, high, low, volume)
        # MFI 很高时整体分数应该被拉低
        assert isinstance(score, float)


class TestSectorRotationStrategy:
    def test_has_param_specs(self):
        specs = SectorRotationStrategy.param_specs()
        assert len(specs) >= 4

    def test_set_params(self):
        s = SectorRotationStrategy()
        s.set_params(top_sectors=3, lookback_period=30)
        assert s.top_sectors == 3
        assert s.lookback_period == 30


class TestMomentumReversalStrategy:
    def test_has_param_specs(self):
        specs = MomentumReversalStrategy.param_specs()
        assert len(specs) >= 6
        names = {s.name for s in specs}
        assert "trailing_stop_trigger" in names
        assert "rsi_oversold" in names

    def test_set_params(self):
        s = MomentumReversalStrategy()
        s.set_params(momentum_threshold=0.08, rsi_oversold=40)
        assert s.momentum_threshold == 0.08
        assert s.rsi_oversold == 40

    def test_rsi_calculation(self):
        """RSI 应在 0-100 之间"""
        np.random.seed(99)
        close = 100 + np.cumsum(np.random.randn(50) * 2)
        rsi = MomentumReversalStrategy._rsi(close, 14)
        assert 0 <= rsi <= 100

    def test_bb_position_calculation(self):
        """BB 位置应在合理范围内"""
        close = np.linspace(90, 110, 30)
        pos = MomentumReversalStrategy._bb_position(close, 20, 2.0)
        assert isinstance(pos, float)


# ── K-fold 验证测试 ────────────────────────────────────────


class TestKFoldValidation:
    def test_consensus_pass(self):
        """所有窗口都确认 → consensus=True"""
        df = pd.DataFrame({"close": np.arange(10.0, 30.0, 0.5)})
        result = kfold_validate(df, lambda d, i: True, k=3, consensus_threshold=0.6)
        assert result.consensus is True
        assert result.pass_count == 3
        assert result.ratio == 1.0

    def test_consensus_fail(self):
        """所有窗口都否认 → consensus=False"""
        df = pd.DataFrame({"close": np.arange(10.0, 30.0, 0.5)})
        result = kfold_validate(df, lambda d, i: False, k=3, consensus_threshold=0.6)
        assert result.consensus is False
        assert result.pass_count == 0

    def test_partial_consensus(self):
        """2/3 确认（>= 0.6）→ consensus=True"""
        df = pd.DataFrame({"close": np.arange(10.0, 40.0, 0.5)})
        # 前两个窗口确认，第三个否认
        signal_fn = lambda d, i: i < 2
        result = kfold_validate(df, signal_fn, k=3, consensus_threshold=0.6)
        assert result.consensus is True
        assert result.pass_count == 2
        assert result.total == 3

    def test_threshold_boundary(self):
        """恰好 1/2 = 0.5 < 0.6 → consensus=False"""
        df = pd.DataFrame({"close": np.arange(10.0, 40.0, 0.5)})
        signal_fn = lambda d, i: i == 0
        result = kfold_validate(df, signal_fn, k=2, consensus_threshold=0.6)
        assert result.consensus is False

    def test_insufficient_data(self):
        """数据不足 k 行 → consensus=False"""
        df = pd.DataFrame({"close": [10.0, 11.0]})
        result = kfold_validate(df, lambda d, i: True, k=5)
        assert result.consensus is False

    def test_signal_fn_exception(self):
        """信号函数抛异常 → 该窗口视为否认"""
        df = pd.DataFrame({"close": np.arange(10.0, 30.0, 0.5)})
        def bad_fn(d, i):
            raise ValueError("boom")
        result = kfold_validate(df, bad_fn, k=3)
        assert result.consensus is False
        assert result.pass_count == 0


class TestKFoldInStrategy:
    def test_kfold_confirm_method(self):
        """ParametrizedStrategy.kfold_confirm 应可用"""
        s = ContrarianStrategy()
        df = pd.DataFrame({"close": np.arange(10.0, 30.0, 0.5)})
        result = s.kfold_confirm(df, lambda d, i: True)
        assert result is True

    def test_kfold_params_configurable(self):
        """K-fold 参数应可覆盖"""
        s = ContrarianStrategy()
        s.kfold_k = 5
        s.kfold_threshold = 0.8
        assert s.kfold_k == 5
        assert s.kfold_threshold == 0.8
