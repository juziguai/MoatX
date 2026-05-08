"""BacktestEngine — 回测主引擎"""

from __future__ import annotations

from datetime import date

import pandas as pd

from modules.config import cfg
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
        slippage = cfg().backtest.slippage_pct if hasattr(cfg(), 'backtest') else 0.001
        portfolio = Portfolio(initial_capital=self.initial_capital, slippage_pct=slippage)
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

        # 风控配置
        risk_cfg = cfg().risk if hasattr(cfg(), 'risk') else None
        max_position_pct = getattr(risk_cfg, 'max_position_pct', 1.0) if risk_cfg else 1.0
        max_daily_drawdown_pct = getattr(risk_cfg, 'max_daily_drawdown_pct', 100.0) if risk_cfg else 100.0

        # 涨跌停配置
        backtest_cfg = cfg().backtest if hasattr(cfg(), 'backtest') else None
        limit_pct = getattr(backtest_cfg, 'limit_up_pct', 0.095) if backtest_cfg else 0.095
        prev_closes: dict[str, float] = {}

        def _limit_checker(symbol: str, current_price: float, direction: str) -> bool:
            """涨停不能买，跌停不能卖"""
            prev = prev_closes.get(symbol)
            if prev is None or prev <= 0:
                return False
            pct = (current_price - prev) / prev
            if direction == "buy" and pct >= limit_pct:
                return True  # 涨停，拒绝买入
            if direction == "sell" and pct <= -limit_pct:
                return True  # 跌停，拒绝卖出
            return False

        portfolio._limit_checker = _limit_checker

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

            # 风控检查：仓位超限则回滚最后的买入订单
            if portfolio.position_ratio(current_prices) > max_position_pct:
                # 找到最后一笔买入订单并撤销（简单处理：卖出多余部分）
                last_order = portfolio.orders[-1] if portfolio.orders else None
                if last_order and last_order.direction == "buy":
                    excess_ratio = portfolio.position_ratio(current_prices) - max_position_pct
                    total_val = portfolio.total_value()
                    excess_value = total_val * excess_ratio
                    sell_shares = int(excess_value / last_order.price)
                    sell_shares = (sell_shares // 100) * 100
                    if sell_shares > 0:
                        portfolio.sell(last_order.symbol, last_order.price, sell_shares, day)

            # 风控检查：单日回撤超限则全部平仓
            if portfolio.equity_curve:
                prev_value = portfolio.equity_curve[-1]["total_value"]
                # 计算当前总资产（现金 + 持仓市值）
                holdings_value = sum(
                    pos.market_value_at(current_prices.get(sym, pos.avg_cost))
                    for sym, pos in portfolio.positions.items()
                )
                current_value = portfolio.cash + holdings_value
                daily_dd = (prev_value - current_value) / prev_value * 100 if prev_value > 0 else 0
                if daily_dd > max_daily_drawdown_pct:
                    for sym in list(portfolio.positions.keys()):
                        price = current_prices.get(sym)
                        if price:
                            portfolio.sell(sym, price, 0, day)

            # Snapshot
            portfolio.snapshot(day, current_prices)

            # 更新前收盘价（用于下一日涨跌停判断）
            prev_closes.update(current_prices)

        self._results = self._calc_results()
        return self._results

    def _calc_results(self) -> dict:
        """Calculate performance metrics."""
        from .metrics import calc_metrics, calc_trade_metrics
        eq = self._portfolio.equity_curve if self._portfolio else []
        if not eq:
            return {}
        df = pd.DataFrame(eq)

        # 加载基准数据
        benchmark_curve = None
        try:
            benchmark_code = cfg().backtest.benchmark if hasattr(cfg(), 'backtest') else "000300"
            # 确保基准代码有正确的后缀
            if not benchmark_code.endswith((".SH", ".SZ")):
                benchmark_code = f"{benchmark_code}.SH"
            bm_feed = DataFeed(benchmark_code, self.start, self.end)
            bm_df = bm_feed.preload()
            if not bm_df.empty:
                bm_df = bm_df[["date", "close"]].copy()
                bm_df["total_value"] = bm_df["close"] / bm_df["close"].iloc[0] * self.initial_capital
                benchmark_curve = bm_df
        except Exception:
            pass

        result = calc_metrics(
            initial_capital=self.initial_capital,
            final_value=df["total_value"].iloc[-1] if len(df) > 0 else self.initial_capital,
            equity_curve=df,
            trade_count=len(self._portfolio.orders) if self._portfolio else 0,
            benchmark_curve=benchmark_curve,
        )

        # 交易级指标
        if self._portfolio and self._portfolio.orders:
            trade_metrics = calc_trade_metrics(self._portfolio.orders)
            result.update(trade_metrics)

        # 保存基准曲线引用（用于绘图）
        if benchmark_curve is not None:
            result["_benchmark_curve"] = benchmark_curve

        return result

    @property
    def portfolio(self) -> Portfolio | None:
        return self._portfolio

    def report(self, save_chart: str | None = None) -> str:
        """Generate a Markdown report.

        Args:
            save_chart: 可选，保存权益曲线图的路径（如 "backtest.png"）
        """
        if not self._results:
            return "回测未运行或无数据"

        # 生成图表
        if save_chart and self._portfolio:
            try:
                from modules.charts import plot_backtest
                benchmark_curve = self._results.get("_benchmark_curve")
                plot_backtest(
                    equity_curve=self._portfolio.equity_curve,
                    orders=self._portfolio.orders,
                    benchmark_curve=benchmark_curve,
                    title=f"回测结果 - {', '.join(self.symbols)}",
                    save_path=save_chart,
                )
            except Exception:
                pass

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
            f"| 日胜率 | {r.get('win_rate', 0):.1f}% |",
            f"| 盈亏比(日) | {r.get('profit_loss_ratio', 0):.2f} |",
            f"| Calmar 比率 | {r.get('calmar_ratio', 0):.2f} |",
            "",
        ]

        # 交易级指标
        if "trade_win_rate" in r:
            lines.extend([
                "## 交易统计",
                "",
                "| 指标 | 值 |",
                "|------|----|",
                f"| 交易级胜率 | {r.get('trade_win_rate', 0):.1f}% |",
                f"| 交易级盈亏比 | {r.get('trade_profit_loss_ratio', 0):.2f} |",
                f"| 平均持仓天数 | {r.get('avg_holding_days', 0):.1f} |",
                "",
            ])

        # 基准对比
        if "benchmark_return_pct" in r:
            lines.extend([
                "## 基准对比",
                "",
                "| 指标 | 策略 | 基准 |",
                "|------|------|------|",
                f"| 总收益率 | {r.get('total_return_pct', 0):+.2f}% | {r.get('benchmark_return_pct', 0):+.2f}% |",
                f"| 年化收益率 | {r.get('annual_return_pct', 0):+.2f}% | {r.get('benchmark_annual_return_pct', 0):+.2f}% |",
                f"| Alpha | {r.get('alpha', 0):+.2f}% | - |",
                f"| 信息比率 | {r.get('information_ratio', 0):.2f} | - |",
                "",
            ])

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
            if isinstance(monthly, pd.DataFrame):
                for _, row in monthly.iterrows():
                    ret = float(row["return_pct"])
                    # 处理 -0.0 的情况
                    if ret == 0:
                        ret = 0.0
                    sign = "+" if ret > 0 else ""
                    lines.append(f"| {row['month']} | {sign}{ret:.2f}% |")
            else:
                for row in monthly:
                    ret = float(row["return_pct"])
                    if ret == 0:
                        ret = 0.0
                    sign = "+" if ret > 0 else ""
                    lines.append(f"| {row['month']} | {sign}{ret:.2f}% |")
            lines.append("")

        return "\n".join(lines)
