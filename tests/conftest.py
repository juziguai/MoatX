"""
tests/conftest.py - pytest 全局 fixtures
"""

from __future__ import annotations

import sys
from pathlib import Path

# 将项目根目录加入 path，确保 imports 正常
_root = Path(__file__).parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

import pytest
import sqlite3
import pandas as pd
import numpy as np


@pytest.fixture
def mem_db():
    """SQLite 内存数据库（每个测试独立）"""
    conn = sqlite3.connect(":memory:")
    yield conn
    conn.close()


@pytest.fixture
def sample_daily_df():
    """模拟 120 天日线数据（含 OHLCV）"""
    dates = pd.date_range("2026-01-01", periods=120, freq="B")
    np.random.seed(42)
    close = 10 + np.cumsum(np.random.randn(120) * 0.2)
    high = close * (1 + np.abs(np.random.randn(120) * 0.02))
    low = close * (1 - np.abs(np.random.randn(120) * 0.02))
    open_price = low + (high - low) * np.random.rand(120)
    volume = np.random.randint(1000000, 10000000, size=120)
    df = pd.DataFrame(
        {
            "open": open_price,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
        },
        index=pd.DatetimeIndex(dates, name="date"),
    )
    return df


@pytest.fixture
def sample_spot_df():
    """模拟全市场快照 DataFrame"""
    return pd.DataFrame(
        {
            "code": ["600519", "000858", "300750", "002261"],
            "name": ["贵州茅台", "五粮液", "宁德时代", "掌趣科技"],
            "price": [1850.0, 165.0, 520.0, 4.5],
            "pct_change": [1.2, -0.5, 3.1, -2.0],
            "volume": [2000000, 5000000, 8000000, 3000000],
            "amount": [3700000000, 825000000, 4160000000, 1350000],
            "pe": [30.5, 22.3, 45.0, None],
            "pb": [12.0, 5.5, 8.2, 1.8],
            "turnover": [0.5, 1.2, 2.0, 0.8],
        }
    )
