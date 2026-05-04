"""调试实达集团(600734)数据加载"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from datetime import date
import pandas as pd
from modules.stock_data import StockData

def main():
    sd = StockData()

    # 测试数据加载
    start = "20250101"
    end = "20260501"

    print(f"=== 测试数据加载 {start} ~ {end} ===")

    try:
        df = sd.get_daily("600734", start_date=start, end_date=end, adjust="qfq")
        print(f"数据行数: {len(df)}")
        print(f"日期范围: {df.index.min()} ~ {df.index.max()}")
        print(f"\n最近10天数据:")
        print(df.tail(10))
    except Exception as e:
        print(f"错误: {e}")

if __name__ == "__main__":
    main()
