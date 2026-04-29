"""费率模型 — 佣金、印花税、过户费"""

from __future__ import annotations

from modules.config import cfg


def calc_commission(trade_value: float) -> float:
    """计算佣金（最低 5 元）。"""
    rate = cfg().fees.commission_rate
    return max(trade_value * rate, cfg().fees.min_commission)


def calc_stamp_tax(trade_value: float) -> float:
    """计算印花税（仅卖出时收取）。"""
    return trade_value * cfg().fees.stamp_tax_rate


def calc_transfer_fee(trade_value: float) -> float:
    """计算过户费。"""
    return trade_value * cfg().fees.transfer_fee_rate


def calc_buy_cost(price: float, shares: int) -> float:
    """计算买入总成本（含佣金 + 过户费）。"""
    value = price * shares
    return value + calc_commission(value) + calc_transfer_fee(value)


def calc_sell_proceeds(price: float, shares: int) -> float:
    """计算卖出总收入（扣佣金 + 印花税 + 过户费）。"""
    value = price * shares
    return value - calc_commission(value) - calc_stamp_tax(value) - calc_transfer_fee(value)


def round_lot(shares: int) -> int:
    """将股数向下取整到整手（100 股）。"""
    return (shares // 100) * 100
