"""带参数规格声明的策略基类。"""

from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import Any, Literal

from modules.backtest.strategy import BaseStrategy


@dataclass
class ParamSpec:
    """参数规格说明书 — 声明策略的可优化参数。

    Attributes:
        name: 参数名（对应策略类的属性名）
        type: 参数类型 — int / float / categorical / bool
        default: 默认值
        range: (min, max) 用于 int/float；[values] 用于 categorical
        description: 中文描述
    """
    name: str
    type: Literal["int", "float", "categorical", "bool"]
    default: Any
    range: tuple | list | None = None
    description: str = ""


class ParametrizedStrategy(BaseStrategy, ABC):
    """可参数化的策略基类，声明可优化参数供 StrategyOptimizer 使用。

    子类需同时实现 initialize() 和 handle_bar()。
    """

    @classmethod
    def param_specs(cls) -> list[ParamSpec]:
        """返回该策略的参数规格列表。"""
        return []

    def set_params(self, **kwargs) -> None:
        """批量设置策略参数。"""
        specs = {s.name: s for s in self.param_specs()}
        for k, v in kwargs.items():
            if k in specs:
                spec = specs[k]
                if spec.type == "int":
                    v = int(v)
                elif spec.type == "float":
                    v = float(v)
                elif spec.type == "bool":
                    v = bool(v)
                setattr(self, k, v)
