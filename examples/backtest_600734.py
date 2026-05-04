"""实达集团(600734)回测示例"""

import sys
from pathlib import Path

# 添加项目根目录到 path
sys.path.insert(0, str(Path(__file__).parent.parent))

from datetime import date
import pandas as pd
from modules.backtest.engine import BacktestEngine
from modules.backtest.strategy import BaseStrategy, StrategyContext


class MACrossStrategy(BaseStrategy):
    """双均线交叉策略（简化版）

    - MA5 上穿 MA20 买入
    - MA5 下穿 MA20 卖出
    """

    def initialize(self, ctx: StrategyContext) -> None:
        ctx.set_universe(["600734"])
        self.ma_short = 5
        self.ma_long = 20

    def handle_bar(self, ctx: StrategyContext, data: pd.DataFrame) -> None:
        if len(data) < self.ma_long + 1:
            return

        # 计算均线
        close = data["close"].values
        ma_short = close[-self.ma_short:].mean()
        ma_long = close[-self.ma_long:].mean()
        prev_ma_short = close[-self.ma_short - 1:-1].mean()
        prev_ma_long = close[-self.ma_long - 1:-1].mean()

        # 获取当前持仓
        pos = ctx.get_position("600734")
        has_position = pos is not None and pos.shares > 0

        # 金叉买入
        if prev_ma_short <= prev_ma_long and ma_short > ma_long and not has_position:
            ctx.order_target_pct("600734", 0.95)  # 95%仓位

        # 死叉卖出
        elif prev_ma_short >= prev_ma_long and ma_short < ma_long and has_position:
            ctx.order_target_pct("600734", 0)


def main():
    # 回测参数
    symbol = "600734"
    start = date(2024, 1, 1)
    end = date(2026, 5, 1)
    initial_capital = 100_000

    print(f"=== 实达集团(600734)回测 ===")
    print(f"时间范围: {start} ~ {end}")
    print(f"初始资金: {initial_capital:,.2f}")
    print(f"策略: 双均线交叉 (MA5/MA20)")
    print()

    # 创建引擎并运行
    engine = BacktestEngine(
        symbol=symbol,
        start=start,
        end=end,
        initial_capital=initial_capital,
    )

    strategy = MACrossStrategy()
    results = engine.run(strategy)

    # 输出报告（同时保存图表）
    chart_path = "backtest_600734.png"
    print(engine.report(save_chart=chart_path))
    print(f"\n图表已保存: {chart_path}")

    # 输出交易记录
    portfolio = engine.portfolio
    if portfolio and portfolio.orders:
        print(f"\n## 交易记录（共 {len(portfolio.orders)} 笔）\n")
        print("| 日期 | 方向 | 股票 | 数量 | 价格 | 金额 |")
        print("|------|------|------|------|------|------|")
        for order in portfolio.orders:
            direction = "买入" if order.direction == "buy" else "卖出"
            print(f"| {order.date} | {direction} | {order.symbol} | {order.shares} | {order.price:.2f} | {order.value:,.2f} |")


if __name__ == "__main__":
    main()
