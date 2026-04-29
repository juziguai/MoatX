"""
modules/cli/risk.py - 风控管理命令
"""

import logging
from .portfolio import _portfolio_cls

_logger = logging.getLogger("moatx.cli.risk")


def cmd_risk_check(args):
    """手动触发风控检查"""
    from modules.stock_data import StockData
    from modules.risk_controller import RiskController
    from modules.alerter import Alerter
    from modules.config import cfg

    pf = _portfolio_cls()()
    holdings = pf.list_holdings()
    if holdings.empty:
        print("持仓为空，无需风控检查")
        return

    risk_cfg = cfg().risk_control
    rc = RiskController(pf.db, alerter=Alerter(feishu_settings=cfg().feishu))

    # 获取实时行情
    symbols = holdings["symbol"].tolist()
    sd = StockData()
    print(f"正在获取 {len(symbols)} 只股票实时行情...")
    quotes = sd.get_realtime_quotes(symbols)

    # 执行风控检查并发送预警
    events = rc.check_and_alert(holdings, quotes, risk_cfg)

    if not events:
        print("风控检查通过，无预警")
        return

    # 打印到终端
    print(f"触发 {len(events)} 条风控预警：")
    for e in events:
        print(f"  [{e.level.upper()}] {e.message}")

    # 记录到数据库
    rc.log_events(events)
    print(f"\n已记录 {len(events)} 条风控事件")


def cmd_risk_status(args):
    """查看当前风控状态（各持仓的止损价和仓位占比）"""
    from modules.config import cfg

    pf = _portfolio_cls()()
    holdings = pf.list_holdings()
    if holdings.empty:
        print("持仓为空")
        return

    risk_cfg = cfg().risk_control
    print(f"全局风控阈值：止损 -{risk_cfg.stop_loss_pct:.1f}%，"
          f"单仓上限 {risk_cfg.max_single_position_pct:.1f}%，"
          f"当日回撤上限 {risk_cfg.max_daily_loss_pct:.1f}%")
    print()

    if "market_value" not in holdings.columns:
        print("(market_value 未刷新，请先运行 refresh)")
        return

    total_mv = holdings["market_value"].sum()
    print(f"{'代码':<8} {'名称':<10} {'持仓成本':>8} {'最新价':>8} {'持仓市值':>10} {'占比':>8} {'止损线':>8}")
    print("-" * 70)
    for _, row in holdings.iterrows():
        mv = float(row.get("market_value", 0))
        cost = float(row.get("cost_price", 0))
        # shares = float(row.get("shares", 0))
        stop_pct = float(row.get("stop_loss_pct", 0)) or risk_cfg.stop_loss_pct
        stop_price = cost * (1 - stop_pct / 100) if cost > 0 else 0
        pct = mv / total_mv * 100 if total_mv > 0 else 0
        current_price = float(row.get("current_price", 0))
        name = str(row.get("name", ""))[:10]
        print(
            f"  {row['symbol']:<8} {name:<10} "
            f"{cost:>8.2f} {current_price:>8.2f} {mv:>10.2f} "
            f"{pct:>7.1f}% {stop_price:>8.2f}"
        )
    print()


def cmd_risk_history(args):
    """查看风控事件历史"""
    pf = _portfolio_cls()()
    rc = pf._get_risk_controller() if hasattr(pf, '_get_risk_controller') else None
    if rc is None:
        from modules.risk_controller import RiskController
        rc = RiskController(pf.db)

    df = rc.get_event_history(limit=args.limit or 50)
    if df.empty:
        print("无风控事件记录")
        return
    print(f"共 {len(df)} 条风控事件：")
    print(df.to_string(index=False))
