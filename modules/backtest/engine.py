"""BacktestEngine — 回测主引擎"""

from __future__ import annotations

from datetime import date

import pandas as pd

from modules.indicators import IndicatorEngine

from .calendar import trading_days_between
from .datafeed import DataFeed
from .order import Portfolio
from .strategy import BaseStrategy


class BacktestEngine:
    """回测引擎。

    用法:
        engine = BacktestEngine("600519", start=date(2024,1,1), end=date(2024,12,31))
        engine.run(MyStrategy())
        engine.report()
    """

    def __init__(
        self,
        symbol: str | list[str],
        start: date,
        end: date,
        initial_capital: float = 100_000.0,
    ):
        self.symbols = [symbol] if isinstance(symbol, str) else symbol
        self.start = start
        self.end = end
        self.initial_capital = initial_capital
        self._portfolio: Portfolio | None = None
        self._ind = IndicatorEngine()
        self._results: dict = {}

    def run(self, strategy: BaseStrategy) -> dict:
        """Run backtest for the given strategy."""
        # Initialize
        strategy.initialize(strategy.ctx)
        portfolio = Portfolio(initial_capital=self.initial_capital)
        strategy.ctx._portfolio = portfolio
        self._portfolio = portfolio

        # Load data for each symbol
        feeds: dict[str, DataFeed] = {}
        all_data: dict[str, pd.DataFrame] = {}
        for sym in strategy.ctx.universe or self.symbols:
            feed = DataFeed(sym, self.start, self.end)
            df = feed.preload()
            if df.empty:
                continue
            # Add indicators
            ind_df = self._ind.all_in_one(df)
            df = pd.concat([df, ind_df], axis=1)
            feeds[sym] = feed
            all_data[sym] = df

        # Get trading calendar
        trading_days = trading_days_between(self.start, self.end)

        # Main loop
        for bar_idx, day in enumerate(trading_days):
            current_prices: dict[str, float] = {}
            has_data = False

            for sym in all_data:
                df = all_data[sym]
                day_data = df[df["date"] == pd.Timestamp(day)]
                if day_data.empty:
                    continue
                has_data = True
                row = day_data.iloc[-1]
                if "close" in df.columns:
                    current_prices[sym] = float(row.get("close", 0))

            if not has_data:
                continue

            strategy.ctx._current_date = day
            strategy.ctx._current_prices = current_prices

            # Call strategy for each symbol
            for sym in all_data:
                feed = feeds.get(sym)
                if feed is None:
                    continue
                try:
                    strategy.handle_bar(strategy.ctx, feed.get_slice(bar_idx))
                except Exception:
                    continue

            # Snapshot
            portfolio.snapshot(day, current_prices)

        self._results = self._calc_results()
        return self._results

    def _calc_results(self) -> dict:
        """Calculate performance metrics."""
        from .metrics import calc_metrics
        eq = self._portfolio.equity_curve if self._portfolio else []
        if not eq:
            return {}
        df = pd.DataFrame(eq)
        _returns = df["returns"].values if "returns" in df.columns else []
        return calc_metrics(
            initial_capital=self.initial_capital,
            final_value=df["total_value"].iloc[-1] if len(df) > 0 else self.initial_capital,
            equity_curve=df,
            trade_count=len(self._portfolio.orders) if self._portfolio else 0,
        )

    @property
    def portfolio(self) -> Portfolio | None:
        return self._portfolio

    def report(self) -> str:
        """Generate a Markdown report."""
        if not self._results:
            return "回测未运行或无数据"

        r = self._results
        lines = [
            "## 回测报告",
            "",
            "| 指标 | 值 |",
            "|------|----|",
            f"| 初始资金 | {r.get('initial_capital', 0):,.2f} |",
            f"| 最终价值 | {r.get('final_value', 0):,.2f} |",
            f"| 总收益率 | {r.get('total_return_pct', 0):+.2f}% |",
            f"| 年化收益率 | {r.get('annual_return_pct', 0):+.2f}% |",
            f"| 夏普比率 | {r.get('sharpe_ratio', 0):.2f} |",
            f"| 最大回撤 | {r.get('max_drawdown_pct', 0):.2f}% |",
            f"| 最大回撤区间 | {r.get('max_drawdown_start', '-')} ~ {r.get('max_drawdown_end', '-')}（{r.get('max_drawdown_days', 0)} 天）|",
            f"| 回撤恢复 | {r.get('max_drawdown_recovery', '-')} |",
            f"| 交易次数 | {r.get('trade_count', 0)} |",
            f"| 胜率 | {r.get('win_rate', 0):.1f}% |",
            f"| 盈亏比 | {r.get('profit_loss_ratio', 0):.2f} |",
            f"| Calmar 比率 | {r.get('calmar_ratio', 0):.2f} |",
            "",
        ]

        # Annual returns
        annual = r.get("annual_returns")
        if annual is not None and len(annual) > 0:
            lines.append("## 年度收益")
            lines.append("")
            lines.append("| 年份 | 收益率 |")
            lines.append("|------|--------|")
            for _, row in annual.iterrows():
                ret = row["return_pct"]
                sign = "+" if ret >= 0 else ""
                lines.append(f"| {int(row['year'])} | {sign}{ret:.2f}% |")
            lines.append("")

        # Monthly returns
        monthly = r.get("monthly_returns")
        if monthly is not None and len(monthly) > 0:
            lines.append("## 月度收益分布")
            lines.append("")
            lines.append("| 月份 | 收益率 |")
            lines.append("|------|--------|")
            for row in monthly:
                ret = row["return_pct"]
                sign = "+" if ret >= 0 else ""
                lines.append(f"| {row['month']} | {sign}{ret:.2f}% |")
            lines.append("")

        return "\n".join(lines)
