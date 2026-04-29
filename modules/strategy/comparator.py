"""多策略对比器 — 同数据跑多个策略，指标并排对比。"""

from __future__ import annotations

from datetime import date

import pandas as pd

from modules.backtest.engine import BacktestEngine
from modules.backtest.strategy import BaseStrategy


class StrategyComparator:
    """Run N strategies on the same data, compare metrics side by side."""

    def compare(
        self,
        strategies: list[tuple[str, BaseStrategy]],
        symbols: list[str],
        start: date,
        end: date,
        initial_capital: float = 100_000,
    ) -> pd.DataFrame:
        """Run each strategy and return a DataFrame with one row per strategy.

        Args:
            strategies: list of (name, strategy_instance) tuples
            symbols: list of stock symbols
            start, end: backtest period
            initial_capital: starting capital

        Returns:
            DataFrame with columns: strategy_name, total_return_pct, annual_return_pct,
            sharpe_ratio, max_drawdown_pct, win_rate, profit_loss_ratio, calmar_ratio,
            trade_count, duration_ms
        """
        rows = []
        for name, strategy in strategies:
            engine = BacktestEngine(
                symbols=symbols,
                start=start,
                end=end,
                initial_capital=initial_capital,
            )
            try:
                import time
                t0 = time.time()
                result = engine.run(strategy)
                elapsed = int((time.time() - t0) * 1000)

                row = {
                    "strategy_name": name,
                    "总收益率%": result.get("total_return_pct"),
                    "年化收益率%": result.get("annual_return_pct"),
                    "夏普比率": result.get("sharpe_ratio"),
                    "最大回撤%": result.get("max_drawdown_pct"),
                    "胜率%": result.get("win_rate"),
                    "盈亏比": result.get("profit_loss_ratio"),
                    "Calmar比率": result.get("calmar_ratio"),
                    "交易次数": result.get("trade_count"),
                    "耗时ms": elapsed,
                }
                rows.append(row)
            except Exception as e:
                rows.append({"strategy_name": name, "error": str(e)})

        return pd.DataFrame(rows)
