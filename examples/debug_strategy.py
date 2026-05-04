"""调试策略逻辑"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from datetime import date
import pandas as pd
from modules.backtest.engine import BacktestEngine
from modules.backtest.strategy import BaseStrategy, StrategyContext


class DebugStrategy(BaseStrategy):
    """调试策略"""

    def initialize(self, ctx: StrategyContext) -> None:
        ctx.set_universe(["600734"])
        self.ma_short = 5
        self.ma_long = 20
        self.call_count = 0

    def handle_bar(self, ctx: StrategyContext, data: pd.DataFrame) -> None:
        self.call_count += 1
        if self.call_count <= 5 or self.call_count % 50 == 0:
            print(f"\n=== 第 {self.call_count} 次调用 ===")
            print(f"日期: {ctx.current_date}")
            print(f"数据形状: {data.shape}")
            print(f"列名: {data.columns.tolist()}")
            if len(data) >= 20:
                close = data["close"].values
                ma_short = close[-self.ma_short:].mean()
                ma_long = close[-self.ma_long:].mean()
                print(f"当前价: {close[-1]:.2f}")
                print(f"MA5: {ma_short:.2f}")
                print(f"MA20: {ma_long:.2f}")
                print(f"MA5 > MA20: {ma_short > ma_long}")
            else:
                print(f"数据不足20行，跳过")


def main():
    symbol = "600734"
    start = date(2025, 1, 1)
    end = date(2026, 5, 1)
    initial_capital = 100_000

    print(f"=== 调试策略 {symbol} ===")
    print(f"时间范围: {start} ~ {end}")
    print()

    engine = BacktestEngine(
        symbol=symbol,
        start=start,
        end=end,
        initial_capital=initial_capital,
    )

    strategy = DebugStrategy()
    results = engine.run(strategy)

    print(f"\n总调用次数: {strategy.call_count}")


if __name__ == "__main__":
    main()
