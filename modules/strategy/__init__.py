"""策略研究平台 — 参数优化、多策略对比、Walk-Forward 分析。"""

from .base import ParametrizedStrategy as ParametrizedStrategy, ParamSpec as ParamSpec
from .library import (
    MovingAverageCross as MovingAverageCross,
    MeanReversion as MeanReversion,
    TrendFollowing as TrendFollowing,
    BreakoutStrategy as BreakoutStrategy,
    MACrossWithVolume as MACrossWithVolume,
)
from .optimizer import StrategyOptimizer as StrategyOptimizer
from .comparator import StrategyComparator as StrategyComparator
from .walkforward import WalkForwardAnalyzer as WalkForwardAnalyzer
