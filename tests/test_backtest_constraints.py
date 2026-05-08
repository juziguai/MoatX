"""回测引擎约束测试 — T+1、涨跌停"""

from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
import pytest

from modules.backtest.order import Portfolio, _Lot, Position


# ── T+1 测试 ──────────────────────────────────────────────


class TestTPlus1:
    """T+1 制度：当日买入的股票不能当日卖出"""

    def test_same_day_buy_cannot_sell(self):
        p = Portfolio(initial_capital=100_000)
        today = date(2026, 5, 8)
        # 买入 100 股
        p.buy("600519", 100.0, 100, today)
        assert "600519" in p.positions
        # 当日卖出应被拒绝（无可卖股数）
        result = p.sell("600519", 100.0, 0, today)
        assert result is None
        assert "600519" in p.positions  # 仓位仍在

    def test_next_day_can_sell(self):
        p = Portfolio(initial_capital=100_000)
        day1 = date(2026, 5, 7)
        day2 = date(2026, 5, 8)
        p.buy("600519", 100.0, 100, day1)
        # 次日可以卖出
        result = p.sell("600519", 105.0, 0, day2)
        assert result is not None
        assert result.shares == 100
        assert "600519" not in p.positions

    def test_partial_sell_respects_t1(self):
        p = Portfolio(initial_capital=200_000)
        day1 = date(2026, 5, 6)
        day2 = date(2026, 5, 7)
        day3 = date(2026, 5, 8)
        # Day1: 买 200 股
        p.buy("600519", 100.0, 200, day1)
        # Day2: 再买 100 股
        p.buy("600519", 102.0, 100, day2)
        assert p.positions["600519"].shares == 300
        # Day2: 只能卖 Day1 的 200 股
        result = p.sell("600519", 103.0, 0, day2)
        assert result is not None
        assert result.shares == 200
        assert p.positions["600519"].shares == 100  # 剩下 Day2 买的 100 股
        # Day3: 可以卖剩余 100 股
        result = p.sell("600519", 105.0, 0, day3)
        assert result is not None
        assert result.shares == 100
        assert "600519" not in p.positions

    def test_sellable_shares(self):
        pos = Position(symbol="600519", shares=200, avg_cost=100.0)
        pos._lots = deque([
            _Lot(shares=100, buy_date=date(2026, 5, 6)),
            _Lot(shares=100, buy_date=date(2026, 5, 8)),
        ])
        # 5月8日：只能卖5月6日的100股
        assert pos.sellable_shares(date(2026, 5, 8)) == 100
        # 5月9日：全部可卖
        assert pos.sellable_shares(date(2026, 5, 9)) == 200


# ── 涨跌停测试 ────────────────────────────────────────────


from collections import deque


class TestLimitUpDown:
    """涨停不能买入，跌停不能卖出"""

    def _make_portfolio_with_checker(self, prev_close: float):
        p = Portfolio(initial_capital=1_000_000)
        limit_pct = 0.095

        def checker(symbol, current_price, direction):
            if prev_close <= 0:
                return False
            pct = (current_price - prev_close) / prev_close
            if direction == "buy" and pct >= limit_pct:
                return True
            if direction == "sell" and pct <= -limit_pct:
                return True
            return False

        p._limit_checker = checker
        return p

    def test_limit_up_blocks_buy(self):
        p = self._make_portfolio_with_checker(prev_close=100.0)
        # 涨停价 = 100 * 1.095 = 109.5
        result = p.buy("600519", 109.5, 100, date(2026, 5, 8))
        assert result is None

    def test_below_limit_up_allows_buy(self):
        p = self._make_portfolio_with_checker(prev_close=100.0)
        # 未涨停 = 100 * 1.09 = 109
        result = p.buy("600519", 109.0, 100, date(2026, 5, 8))
        assert result is not None

    def test_limit_down_blocks_sell(self):
        p = self._make_portfolio_with_checker(prev_close=100.0)
        # 先买入
        p._limit_checker = None
        p.buy("600519", 100.0, 200, date(2026, 5, 7))
        # 恢复涨跌停检查
        p._limit_checker = self._make_limit_checker(100.0)
        # 跌停价 = 100 * 0.905 = 90.5
        result = p.sell("600519", 90.5, 0, date(2026, 5, 8))
        assert result is None

    def test_no_checker_allows_all(self):
        p = Portfolio(initial_capital=1_000_000)
        result = p.buy("600519", 200.0, 100, date(2026, 5, 8))
        assert result is not None

    @staticmethod
    def _make_limit_checker(prev_close: float):
        def checker(symbol, current_price, direction):
            if prev_close <= 0:
                return False
            pct = (current_price - prev_close) / prev_close
            if direction == "buy" and pct >= 0.095:
                return True
            if direction == "sell" and pct <= -0.095:
                return True
            return False
        return checker
