"""
modules/cli/tool/paper.py - 模拟交易
"""


def cmd_paper(args):
    from datetime import datetime
    from modules.config import cfg as _cfg
    from modules.db import DatabaseManager
    from modules.signal.paper_trader import PaperTrader

    db = DatabaseManager(_cfg().data.warehouse_path)
    trader = PaperTrader(db=db)

    if args.action == "status":
        now = datetime.now().strftime("%Y-%m-%d %H:%M")
        total = trader.total_value
        ret_pct = (total / trader.initial_capital - 1) * 100
        print(f"=== 模拟账户概览 {now} ===")
        print(f"初始资金: {trader.initial_capital:,.2f}")
        print(f"当前总资产: {total:,.2f}")
        print(f"累计收益率: {ret_pct:+.2f}%")
        print(f"现金: {trader.cash:,.2f}")
        print()

        positions = trader.positions_detail()
        if positions:
            print(f"--- 持仓明细 ({len(positions)} 只) ---")
            header = f"{'代码':<8} {'名称':<8} {'股数':>6} {'成本':>8} {'现价':>8} {'市值':>10} {'盈亏':>10} {'盈亏%':>8}"
            print(header)
            print("-" * 80)
            for p in positions:
                print(
                    f"{p['symbol']:<8} {(p['name'] or '-'):<8} {p['shares']:>6} "
                    f"{p['avg_cost']:>8.3f} {p['current_price']:>8.3f} "
                    f"{p['market_value']:>10.2f} {p['pnl']:>+10.2f} {p['pnl_pct']:>+8.2f}%"
                )
        else:
            print("--- 持仓明细: 空 ---")
        print()

        # Recent snapshots
        snapshots = db.signal().list_paper_snapshots(limit=5)
        if not snapshots.empty:
            print("--- 历史快照 ---")
            for _, row in snapshots.iterrows():
                print(
                    f"  {row['snapshot_date']}  总资产={row['total_value']:,.2f}  "
                    f"现金={row['cash']:,.2f}  市值={row['market_value']:,.2f}  "
                    f"收益率={row['total_return_pct']:+.2f}%"
                )
        return

    elif args.action == "holdings":
        h = trader.holdings
        if h.empty:
            print("模拟持仓为空")
            return
        print(h.to_string(index=False))

    elif args.action == "trades":
        trades = db.signal().paper_trades(limit=50)
        if trades.empty:
            print("无成交记录")
            return
        print(trades.to_string(index=False))

    elif args.action == "pnl":
        report = trader.pnl_report()
        if report.empty:
            print("无已平仓记录")
            return
        print(report.to_string(index=False))

    elif args.action == "snapshot":
        snap = trader.take_snapshot()
        print(f"快照已记录: {snap['date']}")
        print(f"  总资产: {snap['total_value']:,.2f}")
        print(f"  现金: {snap['cash']:,.2f}")
        print(f"  市值: {snap['market_value']:,.2f}")
        print(f"  收益率: {snap['total_return_pct']:+.2f}%")
        if snap["positions"]:
            print(f"  持仓数: {len(snap['positions'])}")
