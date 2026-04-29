"""Walk-Forward 分析 — 滚动窗口训练/测试验证。"""

from __future__ import annotations

from datetime import date


from modules.backtest.engine import BacktestEngine
from .base import ParametrizedStrategy
from .optimizer import StrategyOptimizer


class WalkForwardAnalyzer:
    """Walk-Forward 分析。

    在滚动时间窗口上重复：训练期优化参数 → 测试期验证。
    """

    def __init__(self, db=None):
        self.db = db

    def analyze(
        self,
        strategy_cls: type[ParametrizedStrategy],
        symbols: list[str],
        start: date,
        end: date,
        train_months: int = 12,
        test_months: int = 3,
        initial_capital: float = 100_000,
    ) -> dict:
        """执行 Walk-Forward 分析。

        Returns:
            {
                "windows": [{"train_start", "train_end", "test_start", "test_end",
                             "best_params", "test_return", "test_sharpe", ...}],
                "avg_test_return": 5.2,
                "avg_test_sharpe": 0.8,
                "total_windows": 4,
            }
        """
        windows = self._build_windows(start, end, train_months, test_months)
        results = []
        optimizer = StrategyOptimizer(db=self.db)

        for train_start, train_end, test_start, test_end in windows:
            window_result = self._run_window(
                strategy_cls, symbols, optimizer,
                train_start, train_end, test_start, test_end,
                initial_capital,
            )
            results.append(window_result)

        avg_return = (
            sum(r["test_return"] for r in results if r.get("test_return") is not None) / len(results)
            if results else 0
        )
        avg_sharpe = (
            sum(r["test_sharpe"] for r in results if r.get("test_sharpe") is not None) / len(results)
            if results else 0
        )

        return {
            "windows": results,
            "avg_test_return": round(avg_return, 2),
            "avg_test_sharpe": round(avg_sharpe, 2),
            "total_windows": len(windows),
            "train_months": train_months,
            "test_months": test_months,
        }

    def _build_windows(self, start: date, end: date, train_months: int, test_months: int) -> list:
        """Build rolling window pairs."""
        windows = []
        current_start = start
        while True:
            train_end = self._add_months(current_start, train_months)
            test_end = self._add_months(train_end, test_months)
            if test_end > end:
                break
            windows.append((current_start, train_end, train_end, test_end))
            current_start = self._add_months(current_start, test_months)
        return windows

    def _run_window(self, strategy_cls, symbols, optimizer,
                    train_start, train_end, test_start, test_end, capital):
        opt_result = optimizer.optimize(
            strategy_cls, symbols, train_start, train_end,
            metric="sharpe_ratio", max_workers=2,
        )
        best_params = opt_result["best_params"]

        strategy = strategy_cls()
        strategy.set_params(**best_params)
        engine = BacktestEngine(
            symbols=symbols,
            start=test_start,
            end=test_end,
            initial_capital=capital,
        )
        try:
            test_result = engine.run(strategy)
        except Exception as e:
            test_result = {"error": str(e)}

        return {
            "train_period": f"{train_start}~{train_end}",
            "test_period": f"{test_start}~{test_end}",
            "best_params": best_params,
            "train_sharpe": opt_result["best_result"].get("sharpe_ratio"),
            "test_return": test_result.get("total_return_pct"),
            "test_sharpe": test_result.get("sharpe_ratio"),
            "test_max_dd": test_result.get("max_drawdown_pct"),
        }

    @staticmethod
    def _add_months(d: date, months: int) -> date:
        month = d.month - 1 + months
        year = d.year + month // 12
        month = month % 12 + 1
        day = min(d.day, [31, 29 if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0) else 28,
                          31, 30, 31, 30, 31, 31, 30, 31, 30, 31][month - 1])
        return date(year, month, day)
