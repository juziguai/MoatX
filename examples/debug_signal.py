"""调试信号触发"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from datetime import date
import pandas as pd
from modules.backtest.engine import BacktestEngine
from modules.backtest.strategy import BaseStrategy, StrategyContext


class DebugSignalStrategy(BaseStrategy):
    """调试信号策略"""

    def initialize(self, ctx: StrategyContext) -> None:
        ctx.set_universe(["600734"])
        self.ma_short = 5
        self.ma_long = 20
        self.signals = []

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

        # 检查交叉
        golden_cross = prev_ma_short <= prev_ma_long and ma_short > ma_long
        death_cross = prev_ma_short >= prev_ma_long and ma_short < ma_long

        if golden_cross or death_cross:
            signal_type = "金叉" if golden_cross else "死叉"
            self.signals.append({
                "date": ctx.current_date,
                "type": signal_type,
                "price": close[-1],
                "ma5": ma_short,
                "ma20": ma_long,
                "has_position": has_position,
            })
            print(f"{ctx.current_date}: {signal_type} 价格={close[-1]:.2f} MA5={ma_short:.2f} MA20={ma_long:.2f} 持仓={has_position}")


def main():
    symbol = "600734"
    start = date(2025, 1, 1)
    end = date(2026, 5, 1)
    initial_capital = 100_000

    print(f"=== 调试信号 {symbol} ===")
    print(f"时间范围: {start} ~ {end}")
    print()

    engine = BacktestEngine(
        symbol=symbol,
        start=start,
        end=end,
        initial_capital=initial_capital,
    )

    strategy = DebugSignalStrategy()
    results = engine.run(strategy)

    print(f"\n总信号数: {len(strategy.signals)}")
    if strategy.signals:
        print("\n信号列表:")
        for sig in strategy.signals:
            print(f"  {sig['date']}: {sig['type']} 价格={sig['price']:.2f}")


if __name__ == "__main__":
    main()
