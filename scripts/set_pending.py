"""
set_pending.py - 标记候选股待验证状态

用法:
    python scripts/set_pending.py --set    # 14:50 执行，标记候选股等待收盘验证
    python scripts/set_pending.py --reset  # 次日9:20 执行，清除前日残留 pending

全链路通过 Portfolio API 操作，不直接碰 SQL。
依赖：候选股须先用 import_candidates.py 导入到 candidates 表
"""

from __future__ import annotations

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
from datetime import date

from modules.portfolio import Portfolio


def main():
    parser = argparse.ArgumentParser(description="候选股 pending 状态管理")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--set", action="store_true", help="标记 pending_close=1（14:50 收盘前执行）")
    group.add_argument("--reset", action="store_true", help="清除 pending_close=1（次日9:20 执行）")
    args = parser.parse_args()

    today = date.today().isoformat()
    pf = Portfolio()

    if args.set:
        # 标记所有未验证的候选股为待验证
        from modules.candidate import CandidateManager
        cm = pf._get_candidate_manager()
        cur = pf.db.execute("""
            UPDATE candidates
            SET pending_close=1
            WHERE result_verified=0 AND pending_close=0
        """)
        pf.db.commit()
        changed = cur.rowcount
        print(f"[{today} 14:50] 标记 {changed} 只候选股为待验证（pending_close=1）")
        print("  → 今日 15:10 运行 verify_candidates.py 完成收盘验证")

    elif args.reset:
        cleared = pf.clear_candidate_pending()
        print(f"[{today} 09:20] 清除 {cleared} 只候选股的 pending_close=1 残留标记")

    pf.close()


if __name__ == "__main__":
    main()
