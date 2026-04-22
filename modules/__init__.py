"""
MoatX - A-share Quantitative Analysis System
护城河量化分析系统
"""

__version__ = "0.1.0"
__author__ = "MoatX"

from .stock_data import StockData
from .indicators import IndicatorEngine
from .analyzer import MoatXAnalyzer
from .charts import MoatXCharts
from .screener import MoatXScreener
from .rank_engine import RankEngine
from .portfolio import Portfolio
from .alerter import Alerter

__all__ = [
    "StockData", "IndicatorEngine", "MoatXAnalyzer", "MoatXCharts",
    "MoatXScreener", "RankEngine", "Portfolio", "Alerter"
]
