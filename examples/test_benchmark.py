"""测试基准数据加载"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from datetime import date
from modules.backtest.datafeed import DataFeed

# 测试加载沪深300
for code in ["000300", "sh000300", "000300.SH"]:
    try:
        feed = DataFeed(code, date(2024, 1, 1), date(2026, 5, 1))
        df = feed.preload()
        print(f"{code}: {len(df)} 行")
        if not df.empty:
            print(df.head(3))
            break
    except Exception as e:
        print(f"{code}: 失败 - {e}")
