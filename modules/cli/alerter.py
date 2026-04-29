"""
modules/cli/alerter.py - 预警相关命令
"""

import logging
import time
from datetime import datetime

from modules.config import cfg
from modules.portfolio import Portfolio
from modules.alerter import Alerter

_logger = logging.getLogger("moatx.cli.alerter")


def _build_alerter():
    return Alerter(feishu_settings=cfg().feishu)


def cmd_alert_check(args):
    pf = Portfolio()
    record_pnl = getattr(args, "record_pnl", False)

    # 刷新最新价格（仅在需要记录盈亏时）
    quotes = None
    if record_pnl:
        from modules.stock_data import StockData
        holdings = pf.list_holdings()
        if not holdings.empty:
            symbols = holdings["symbol"].tolist()
            print(f"正在获取 {len(symbols)} 只股票的实时行情...")
            sd = StockData()
            quotes = sd.get_realtime_quotes(symbols)
            if quotes:
                pf.refresh_holdings(quotes)
                print("价格已更新")

    alerter = _build_alerter()
    alerts = pf.check_alerts()
    report = Alerter.format_alert_report(alerts)
    alerter.send(report)

    if record_pnl:
        print("\n正在记录当日盈亏...")
        count = pf.record_daily_pnl(quotes=quotes)
        print(f"已记录 {count} 只股票的当日盈亏")


def cmd_alert_watch(args):
    pf = Portfolio()
    interval = args.interval or 300
    alerter = _build_alerter()
    feishu = cfg().feishu
    print(f"开始监控持仓，每 {interval} 秒检查一次（Ctrl+C 停止）")
    print(f"飞书 Webhook: {feishu.webhook or '未配置'}")

    while True:
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"\n[{ts}] 开始检查...")
        try:
            alerts = pf.check_alerts()
            report = Alerter.format_alert_report(alerts)
            if any(a.get("type") != "error" for a in alerts):
                alerter.send(report)
            else:
                print(report)
        except Exception as e:
            _logger.error("检查异常: %s", e)
        time.sleep(interval)


def cmd_alert_history(args):
    pf = Portfolio()
    df = pf.get_alert_history(limit=args.limit)
    if df.empty:
        print("暂无预警记录")
        return
    print(df.to_string(index=False))
