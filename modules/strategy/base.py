"""带参数规格声明的策略基类。"""

from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import Any, Callable, Literal

import pandas as pd

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
        penalize_volatility: 优化时是否对高波动参数组合施加惩罚
    """
    name: str
    type: Literal["int", "float", "categorical", "bool"]
    default: Any
    range: tuple | list | None = None
    description: str = ""
    penalize_volatility: bool = False


class ParametrizedStrategy(BaseStrategy, ABC):
    """可参数化的策略基类，声明可优化参数供 StrategyOptimizer 使用。

    子类需同时实现 initialize() 和 handle_bar()。
    """

    # K-fold 验证配置（子类可覆盖）
    kfold_k: int = 3
    kfold_threshold: float = 0.6
    kfold_window: int = 20

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

    def kfold_confirm(
        self,
        df: pd.DataFrame,
        signal_fn: Callable[[pd.DataFrame, int], bool],
    ) -> bool:
        """使用 K-fold 时序验证确认信号有效性。

        Args:
            df: 截至当前的 OHLCV 数据
            signal_fn: 信号函数 (df_slice, fold_index) -> bool

        Returns:
            True 表示信号通过共识验证
        """
        from .kfold import kfold_validate
        result = kfold_validate(
            df, signal_fn,
            k=self.kfold_k,
            consensus_threshold=self.kfold_threshold,
            max_window=self.kfold_window,
        )
        return result.consensus
