"""
verify_candidates.py - 收盘后验证候选股涨跌

用法: python scripts/verify_candidates.py

流程：
  14:50  python -m modules.scheduler  (自动执行 set_pending --set)
  15:10  python scripts/verify_candidates.py   # 抓收盘价、写入结果、清 pending
  次日9:20 python -m modules.scheduler (自动执行 set_pending --reset)

全链路通过 Portfolio API 写入 candidate_results（INSERT，不覆盖历史）。
"""

from __future__ import annotations

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import warnings; warnings.filterwarnings('ignore')
import argparse
import requests
from datetime import date, datetime

from modules.config import cfg
from modules.portfolio import Portfolio
from modules.utils import normalize_symbol


def get_close_from_sina(symbol: str) -> float | None:
    """
    获取个股最近交易日收盘价。
    数据源：Sina money.finance 日K线接口，稳定返回上一交易日收盘价。
    """
    s = normalize_symbol(symbol)
    prefix = "sh" if s.startswith(("6", "9", "5")) else "sz"
    url = (
        f"https://money.finance.sina.com.cn/quotes_service/api/json_v2.php"
        f"/CN_MarketData.getKLineData?symbol={prefix}{s}&scale=240&ma=no&datalen=1"
    )
    try:
        r = requests.get(url, timeout=10)
        items = r.json()
        if items and len(items) > 0:
            return float(items[0]["close"])
    except Exception:
        pass
    return None


def main():
    parser = argparse.ArgumentParser(description="候选股收盘验证")
    parser.add_argument("--verbose", "-v", action="store_true", help="显示详细信息")
    args = parser.parse_args()

    pf = Portfolio()
    today = date.today().isoformat()

    # 获取所有待验证候选股
    pending = pf.get_pending_candidates()
    if not pending:
        print(f"[{today}] 没有待验证的候选股（请先运行 import_candidates.py + set_pending --set）")
        pf.close()
        return

    print(f"\n=== 候选股收盘验证 {today} ===\n")
    print(f"{'代码':<8} {'名称':<8} {'入场价':>8} {'收盘价':>8} {'涨跌%':>8}")
    print("-" * 50)

    wins = 0
    total = len(pending)
    verified = 0
    failed = 0

    for symbol, name, rec_rank, rec_date, entry_price in pending:
        sym = normalize_symbol(symbol)
        close = get_close_from_sina(sym)
        if close is None or close <= 0:
            print(f"  {sym:<6} {name:<8} {entry_price:>8.2f}  [获取收盘价失败]")
            failed += 1
            continue

        pct = (close - entry_price) / entry_price * 100

        # 通过 Portfolio API 写入 candidate_results + 更新 candidates 表
        pf.mark_candidate_verified(sym, close, pct)

        flag = "✅" if pct > 0 else "❌"
        print(f"{flag} {sym:<6} {name:<8} {entry_price:>8.2f} {close:>8.2f} {pct:>+7.1f}%")
        if pct > 0:
            wins += 1
        verified += 1

    pf.close()

    # 汇总
    if verified > 0:
        print(f"\n=== {today} 验证汇总 ===")
        print(f"  验证成功: {verified}/{total}")
        print(f"  涨跌比: {wins}/{verified} 赚钱 ({wins/verified*100:.0f}%)")
        print(f"  结果已写入 candidate_results 表（INSERT，不覆盖历史）")
    else:
        print(f"\n[{today}] 验证完成，{failed} 只获取收盘价失败")

    print("\n  → 候选股全流程验证完毕")


if __name__ == "__main__":
    main()
