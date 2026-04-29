"""
import_candidates.py - 从筛选结果自动导入候选股
用法:
    python scripts/import_candidates.py                    # 默认：PE<50 低估前5只
    python scripts/import_candidates.py --limit 10        # 前10只
    python scripts/import_candidates.py --no-risk         # 跳过财务风险检查（快速）
    python scripts/import_candidates.py --pe 0 30         # 自定义PE区间
    python scripts/import_candidates.py --preview         # 仅预览，不写入
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import warnings; warnings.filterwarnings('ignore')
import argparse
from datetime import datetime

def main():
    parser = argparse.ArgumentParser(description="MoatX 候选股自动导入")
    parser.add_argument("--limit", type=int, default=5, help="导入候选股数量（默认5）")
    parser.add_argument("--no-risk", action="store_true", help="跳过财务风险检查（快速模式）")
    parser.add_argument("--preview", action="store_true", help="仅预览，不写入数据库")
    parser.add_argument("--pe", nargs=2, type=float, default=(0, 50), metavar=("MIN", "MAX"), help="PE 范围")
    parser.add_argument("--pb", nargs=2, type=float, default=(0, 10), metavar=("MIN", "MAX"), help="PB 范围")
    parser.add_argument("--cap-min", type=float, default=10, help="最小流通市值（亿元，默认10）")
    parser.add_argument("--pct-min", type=float, default=None, help="最低涨幅（%%，默认不限）")
    parser.add_argument("--risk-threshold", type=int, default=30, help="风险评分上限（默认30）")
    args = parser.parse_args()

    # 初始化
    from modules.stock_data import _clear_all_proxy
    _clear_all_proxy()
    from modules.screener import MoatXScreener
    from modules.portfolio import Portfolio
    from modules.analyzer import MoatXAnalyzer
    import time

    screener = MoatXScreener()
    pf = Portfolio()
    analyzer = MoatXAnalyzer()

    # 1. 扫描候选股
    print(f"\n{'='*50}")
    print(f"MoatX 候选股筛选")
    print(f"{'='*50}")
    t0 = time.time()
    cap_min = args.cap_min * 1e8

    df = screener.scan_all(
        pe_range=(args.pe[0], args.pe[1]),
        pb_range=(args.pb[0], args.pb[1]),
        cap_min=cap_min,
        pct_change_min=args.pct_min,
        sort_by="pe",
        ascending=True,
        limit=args.limit * 3,  # 多扫一些，过滤高风险后还够数
    )
    print(f"初筛: {len(df)} 只，耗时 {time.time()-t0:.1f}s")

    if df.empty:
        print("无符合条件股票")
        pf.close()
        return

    # 2. 预览
    print(f"\n{'代码':<8} {'名称':<8} {'价格':>6} {'涨幅':>6} {'PE':>6} {'PB':>5} {'风险分':>6} {'可买':>4}")
    print("-" * 60)

    candidates_to_add = []

    for i, row in df.iterrows():
        if len(candidates_to_add) >= args.limit:
            break
        sym = str(row.get("code", ""))
        name = str(row.get("name", ""))
        price = row.get("price", 0) or 0
        pct = row.get("pct_change", 0) or 0
        pe = row.get("pe", 0) or 0
        pb = row.get("pb", 0) or 0

        # 3. 财务风险检查
        risk_score = 0
        is_buyable = True
        if not args.no_risk:
            try:
                risk = analyzer.data.check_financial_risk(sym)
                risk_score = risk.get("risk_score", 0)
                is_buyable = risk.get("is_buyable", True)
            except Exception:
                pass

        if not is_buyable and args.no_risk:
            continue  # 快速模式下跳过已知高风险

        flag = "✅" if is_buyable else "❌"
        print(f"{sym:<8} {name:<8} {price:>6.2f} {pct:>+6.2f}% {pe:>6.1f} {pb:>5.2f} {risk_score:>6} {flag:>4}")

        candidates_to_add.append({
            "symbol": sym,
            "name": name,
            "entry_price": price,
            "rec_pct_change": pct,
            "pe_ratio": pe,
            "risk_score": risk_score,
        })

    print(f"\n候选池: {len(candidates_to_add)} 只")

    if args.preview:
        print("\n[预览模式，未写入数据库]")
        pf.close()
        return

    # 4. 写入数据库
    print(f"\n{'='*50}")
    print(f"写入候选股到 candidates 表")
    print(f"{'='*50}")
    now = datetime.now().strftime("%Y-%m-%d")
    added = 0
    skipped = 0

    for i, c in enumerate(candidates_to_add, 1):
        ok = pf.add_candidate(
            symbol=c["symbol"],
            name=c["name"],
            rec_rank=i,
            entry_price=c["entry_price"],
            rec_pct_change=c["rec_pct_change"],
            pe_ratio=c["pe_ratio"],
            risk_score=c["risk_score"],
        )
        if ok:
            added += 1
            print(f"  + {c['symbol']} {c['name']} 入场价={c['entry_price']:.2f} 风险分={c['risk_score']}")
        else:
            skipped += 1
            print(f"  ~ {c['symbol']} {c['name']} 已存在（跳过）")

    print(f"\n导入完成: {added} 只新候选股，{skipped} 只已存在（跳过）")
    print(f"明日 15:10 运行: python scripts/verify_candidates.py")
    pf.close()

if __name__ == "__main__":
    main()
