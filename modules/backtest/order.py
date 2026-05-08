"""订单与持仓模拟 — 支持 A 股 T+1 制度和涨跌停约束"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import date
from typing import Literal

from . import fees


@dataclass
class Order:
    symbol: str
    direction: Literal["buy", "sell"]
    price: float
    shares: int
    date: date
    filled: bool = True

    @property
    def value(self) -> float:
        return self.price * self.shares

    @property
    def cost(self) -> float:
        if self.direction == "buy":
            return fees.calc_buy_cost(self.price, self.shares)
        return 0.0  # sell cost deducted from proceeds

    @property
    def proceeds(self) -> float:
        if self.direction == "sell":
            return fees.calc_sell_proceeds(self.price, self.shares)
        return 0.0


@dataclass
class _Lot:
    """单笔买入记录 — 用于 T+1 可卖股数计算"""
    shares: int
    buy_date: date


@dataclass
class Position:
    symbol: str
    shares: int = 0
    avg_cost: float = 0.0
    _lots: deque[_Lot] = field(default_factory=deque)

    @property
    def buy_date(self) -> date | None:
        """最早一笔持仓的买入日期"""
        return self._lots[0].buy_date if self._lots else None

    def sellable_shares(self, current_date: date) -> int:
        """T+1：返回当前日期可卖出的股数（今日买入的不可卖）"""
        return sum(lot.shares for lot in self._lots if lot.buy_date < current_date)

    def consume_sell(self, shares: int, current_date: date) -> int:
        """按先进先出消耗可卖持仓，返回实际消耗股数"""
        remaining = shares
        new_lots: deque[_Lot] = deque()
        for lot in self._lots:
            if remaining <= 0:
                new_lots.append(lot)
                continue
            if lot.buy_date >= current_date:
                new_lots.append(lot)
                continue
            if lot.shares <= remaining:
                remaining -= lot.shares
            else:
                lot.shares -= remaining
                remaining = 0
                new_lots.append(lot)
        self._lots = new_lots
        return shares - remaining

    @property
    def market_value(self) -> float:
        return self.avg_cost * self.shares

    def market_value_at(self, current_price: float) -> float:
        return current_price * self.shares

    @property
    def pnl(self) -> float:
        return 0.0

    def pnl_at(self, current_price: float) -> float:
        return (current_price - self.avg_cost) * self.shares

    @property
    def pnl_pct(self) -> float:
        return 0.0

    def pnl_pct_at(self, current_price: float) -> float:
        if self.avg_cost == 0:
            return 0.0
        return (current_price - self.avg_cost) / self.avg_cost * 100


@dataclass
class Portfolio:
    """模拟账户 — 持仓与现金管理（A 股 T+1）。"""

    initial_capital: float = 100_000.0
    cash: float = field(default=100_000.0)
    positions: dict[str, Position] = field(default_factory=dict)
    orders: list[Order] = field(default_factory=list)
    _equity_curve: list[dict] = field(default_factory=list)
    slippage_pct: float = 0.001
    # 回调：(symbol, current_price, direction) -> True 表示被限制（涨停/跌停）
    _limit_checker: object = field(default=None, repr=False)

    def __post_init__(self):
        self.cash = self.initial_capital

    def buy(self, symbol: str, price: float, shares: int, date: date) -> Order | None:
        """Buy shares. Returns Order if filled, None if insufficient cash or limit-up."""
        shares = fees.round_lot(shares)
        if shares <= 0:
            return None
        adjusted_price = fees.apply_slippage(price, "buy", self.slippage_pct)
        # 涨跌停检查
        if self._limit_checker and self._limit_checker(symbol, price, "buy"):
            return None
        cost = fees.calc_buy_cost(adjusted_price, shares)
        if cost > self.cash:
            # buy max affordable
            shares = fees.round_lot(int(self.cash / (adjusted_price * 1.001)))
            if shares <= 0:
                return None
            cost = fees.calc_buy_cost(adjusted_price, shares)
        order = Order(symbol=symbol, direction="buy", price=adjusted_price, shares=shares, date=date)
        self.cash -= cost
        self.orders.append(order)

        if symbol in self.positions:
            pos = self.positions[symbol]
            total_cost = pos.avg_cost * pos.shares + adjusted_price * shares
            pos.shares += shares
            pos.avg_cost = total_cost / pos.shares
        else:
            self.positions[symbol] = Position(symbol=symbol, shares=shares, avg_cost=adjusted_price)
            pos = self.positions[symbol]
        pos._lots.append(_Lot(shares=shares, buy_date=date))
        return order

    def sell(self, symbol: str, price: float, shares: int = 0, date: date | None = None) -> Order | None:
        """Sell shares. shares=0 means sell all (T+1: only shares bought before *date*)."""
        if symbol not in self.positions:
            return None
        # 涨跌停检查
        if self._limit_checker and self._limit_checker(symbol, price, "sell"):
            return None
        pos = self.positions[symbol]
        if date is None:
            sellable = pos.shares
        else:
            sellable = pos.sellable_shares(date)
        shares = min(shares, sellable) if shares > 0 else sellable
        if shares <= 0:
            return None
        adjusted_price = fees.apply_slippage(price, "sell", self.slippage_pct)
        proceeds = fees.calc_sell_proceeds(adjusted_price, shares)
        order = Order(symbol=symbol, direction="sell", price=adjusted_price, shares=shares, date=date)
        self.cash += proceeds
        self.orders.append(order)

        if date is not None:
            pos.consume_sell(shares, date)
        pos.shares -= shares
        if pos.shares <= 0:
            del self.positions[symbol]
        return order

    def order_target_pct(self, symbol: str, price: float, target_pct: float, date: date) -> Order | None:
        """Adjust position to target percentage of portfolio value."""
        total_value = self.total_value(price) if symbol in self.positions else self.total_value()
        target_value = total_value * target_pct
        current_value = self.positions[symbol].market_value_at(price) if symbol in self.positions else 0.0
        diff = target_value - current_value

        if abs(diff) < 100:  # ignore tiny differences
            return None
        if diff > 0:
            shares = int(diff / price)
            return self.buy(symbol, price, shares, date)
        else:
            shares = int(abs(diff) / price)
            return self.sell(symbol, price, shares, date)

    def total_value(self, current_price: float | None = None) -> float:
        val = self.cash
        for pos in self.positions.values():
            val += pos.market_value_at(current_price)
        return val

    def position_ratio(self, current_prices: dict[str, float] | None = None) -> float:
        """计算当前仓位比例（持仓市值 / 总资产）。"""
        if not self.positions:
            return 0.0
        prices = current_prices or {}
        holdings = sum(
            pos.market_value_at(prices.get(sym, pos.avg_cost))
            for sym, pos in self.positions.items()
        )
        total = self.cash + holdings
        if total <= 0:
            return 0.0
        return holdings / total

    def snapshot(self, date: date, prices: dict[str, float]) -> dict:
        total = self.cash
        holdings_value = 0.0
        for sym, pos in self.positions.items():
            mv = pos.market_value_at(prices.get(sym, pos.avg_cost))
            holdings_value += mv
            total += mv
        snapshot = {
            "date": date,
            "cash": self.cash,
            "holdings_value": holdings_value,
            "total_value": total,
            "returns": (total / self.initial_capital - 1) * 100,
        }
        self._equity_curve.append(snapshot)
        return snapshot

    @property
    def equity_curve(self) -> list[dict]:
        return self._equity_curve
