"""调试订单执行"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from datetime import date
import pandas as pd
from modules.backtest.engine import BacktestEngine
from modules.backtest.strategy import BaseStrategy, StrategyContext


class DebugOrderStrategy(BaseStrategy):
    """调试订单策略"""

    def initialize(self, ctx: StrategyContext) -> None:
        ctx.set_universe(["600734"])
        self.ma_short = 5
        self.ma_long = 20
        self.order_count = 0

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
            print(f"\n{ctx.current_date}: 触发金叉买入")
            print(f"  价格: {close[-1]:.2f}")
            print(f"  MA5: {ma_short:.2f}, MA20: {ma_long:.2f}")
            print(f"  持仓: {has_position}")
            print(f"  调用 order_target_pct(600734, 0.95)")
            ctx.order_target_pct("600734", 0.95)
            self.order_count += 1

            # 检查买入结果
            pos_after = ctx.get_position("600734")
            if pos_after:
                print(f"  买入成功: 持仓={pos_after.shares}股")
            else:
                print(f"  买入失败: 无持仓")

        # 死叉卖出
        elif prev_ma_short >= prev_ma_long and ma_short < ma_long and has_position:
            print(f"\n{ctx.current_date}: 触发死叉卖出")
            print(f"  价格: {close[-1]:.2f}")
            print(f"  MA5: {ma_short:.2f}, MA20: {ma_long:.2f}")
            print(f"  持仓: {has_position}")
            if pos:
                print(f"  当前持仓: {pos.shares}股")
            print(f"  调用 order_target_pct(600734, 0)")
            ctx.order_target_pct("600734", 0)
            self.order_count += 1

            # 检查卖出结果
            pos_after = ctx.get_position("600734")
            if pos_after:
                print(f"  卖出后持仓: {pos_after.shares}股")
            else:
                print(f"  卖出成功: 无持仓")


def main():
    symbol = "600734"
    start = date(2025, 1, 1)
    end = date(2026, 5, 1)
    initial_capital = 100_000

    print(f"=== 调试订单执行 {symbol} ===")
    print(f"时间范围: {start} ~ {end}")
    print(f"初始资金: {initial_capital:,.2f}")
    print()

    engine = BacktestEngine(
        symbol=symbol,
        start=start,
        end=end,
        initial_capital=initial_capital,
    )

    strategy = DebugOrderStrategy()
    results = engine.run(strategy)

    print(f"\n总订单数: {strategy.order_count}")

    # 打印最终持仓
    portfolio = engine.portfolio
    if portfolio:
        print(f"\n最终现金: {portfolio.cash:,.2f}")
        print(f"最终持仓:")
        for sym, pos in portfolio.positions.items():
            print(f"  {sym}: {pos.shares}股 @ {pos.avg_cost:.2f}")


if __name__ == "__main__":
    main()
