"""BaseStrategy — 回测策略基类。

用法:
    class MyStrategy(BaseStrategy):
        def initialize(self, ctx):
            ctx.set_universe(["600519", "000001"])

        def handle_bar(self, ctx, data):
            if data["close"] > data["ma20"]:
                ctx.order_target_pct("600519", 0.5)
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

import pandas as pd


class StrategyContext:
    """策略上下文 — 策略与引擎之间的接口。"""

    def __init__(self):
        self._universe: list[str] = []
        self._portfolio = None
        self._current_date = None
        self._indicators: dict[str, Any] = {}

    def set_universe(self, symbols: list[str]) -> None:
        self._universe = symbols

    @property
    def universe(self) -> list[str]:
        return self._universe

    @property
    def current_date(self):
        return self._current_date

    @property
    def current_prices(self) -> dict[str, float]:
        return getattr(self, "_current_prices", {})

    def get_position(self, symbol: str):
        """Get current position for a symbol, or None."""
        if self._portfolio is not None:
            return self._portfolio.positions.get(symbol)
        return None

    def order_target_pct(self, symbol: str, target_pct: float) -> None:
        """Place an order at next available price."""
        if self._portfolio is None:
            return
        price = self._current_prices.get(symbol)
        if price is not None and price > 0:
            self._portfolio.order_target_pct(symbol, price, target_pct, self._current_date)

    def buy(self, symbol: str, shares: int) -> None:
        if self._portfolio is None:
            return
        price = self._current_prices.get(symbol)
        if price is not None and price > 0:
            self._portfolio.buy(symbol, price, shares, self._current_date)

    def sell(self, symbol: str, shares: int = 0) -> None:
        if self._portfolio is None:
            return
        price = self._current_prices.get(symbol)
        if price is not None and price > 0:
            self._portfolio.sell(symbol, price, shares, self._current_date)


class BaseStrategy(ABC):
    """回测策略基类。"""

    def __init__(self):
        self.ctx = StrategyContext()
        self.name = self.__class__.__name__

    @abstractmethod
    def initialize(self, ctx: StrategyContext) -> None:
        """初始化策略：设置股票池、参数等。"""
        ...

    @abstractmethod
    def handle_bar(self, ctx: StrategyContext, data: pd.DataFrame) -> None:
        """每个交易日回调。

        Args:
            ctx: 策略上下文
            data: 当日及历史数据（含技术指标）
        """
        ...
