"""
MoatX - A-share Quantitative Analysis System
护城河量化分析系统
"""

__version__ = "0.1.0"
__author__ = "MoatX"

__all__ = [
    "StockData", "IndicatorEngine", "MoatXAnalyzer", "MoatXCharts",
    "MoatXScreener", "RankEngine", "Portfolio", "Alerter"
]


def __getattr__(name):
    """Lazy exports keep lightweight CLI commands from importing data providers."""
    if name == "StockData":
        from .stock_data import StockData
        return StockData
    if name == "IndicatorEngine":
        from .indicators import IndicatorEngine
        return IndicatorEngine
    if name == "MoatXAnalyzer":
        from .analyzer import MoatXAnalyzer
        return MoatXAnalyzer
    if name == "MoatXCharts":
        from .charts import MoatXCharts
        return MoatXCharts
    if name == "MoatXScreener":
        from .screener import MoatXScreener
        return MoatXScreener
    if name == "RankEngine":
        from .rank_engine import RankEngine
        return RankEngine
    if name == "Portfolio":
        from .portfolio import Portfolio
        return Portfolio
    if name == "Alerter":
        from .alerter import Alerter
        return Alerter
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
