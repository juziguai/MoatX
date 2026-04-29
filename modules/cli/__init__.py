"""
modules/cli/__init__.py - CLI 模块包
用法：
    python -m modules.cli list
    python -m modules.cli alert check
    python -m modules.cli tool diagnose --fresh

（保留原入口 modules.cli_portfolio 不变，仅重构内部结构）
"""

import argparse
import logging
from datetime import datetime

from .portfolio import cmd_import, cmd_batch_import, cmd_list, cmd_remove, cmd_refresh, cmd_trade, cmd_summary
from .quote import cmd_quote
from .market import cmd_market
from .alerter import cmd_alert_check, cmd_alert_watch, cmd_alert_history
from .risk import cmd_risk_check, cmd_risk_status, cmd_risk_history
from .tool import cmd_diagnose, cmd_event, cmd_probe_api, cmd_signal, cmd_paper
from .tool.monitor import cmd_monitor

# CLI logging配置
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)

_logger = logging.getLogger("moatx.cli")


def _build_alerter():
    from modules.alerter import Alerter
    from modules.config import cfg
    return Alerter(feishu_settings=cfg().feishu)


def main():
    parser = argparse.ArgumentParser(prog="python -m modules.cli")
    sub = parser.add_subparsers(dest="cmd")

    # ── 持仓/交易 ────────────────────────────────────────
    p_import = sub.add_parser("import", help="从持仓截图导入")
    p_import.add_argument("image_path", help="截图文件路径")

    p_batch = sub.add_parser("batch-import", help="扫描目录批量导入截图")
    p_batch.add_argument("dir", nargs="?", help="截图目录路径（默认 data/image/）")

    sub.add_parser("list", help="查看持仓列表")

    p_remove = sub.add_parser("remove", help="删除持仓")
    p_remove.add_argument("symbol", help="股票代码")

    sub.add_parser("refresh", help="用实时行情刷新持仓价格")

    sub.add_parser("summary", help="持仓总览（总市值、总盈亏、仓位分布）")

    p_trade = sub.add_parser("trade", help="记录一笔交易（买/卖）")
    p_trade.add_argument("--date", default=datetime.now().strftime("%Y-%m-%d"), help="交易日期")
    p_trade.add_argument("action", choices=["buy", "sell"], help="买入或卖出")
    p_trade.add_argument("symbol", help="股票代码")
    p_trade.add_argument("shares", help="股数")
    p_trade.add_argument("price", help="成交价")
    p_trade.add_argument("amount", help="成交额（元）")
    p_trade.add_argument("--name", default="", help="股票名称（可选）")
    p_trade.add_argument("--fee", default="0", help="手续费")
    p_trade.add_argument("--stamp-duty", dest="stamp_duty", default="0", help="印花税")
    p_trade.add_argument("--transfer-fee", dest="transfer_fee", default="0", help="过户费")
    p_trade.add_argument("--trade-levy", dest="trade_levy", default="0", help="交易征费")

    # ── 行情 ────────────────────────────────────────────
    p_quote = sub.add_parser("quote", help="查看实时行情（不指定股票默认查询持仓）")
    p_quote.add_argument("symbols", nargs="*", help="股票代码，如 600519 000858")
    p_quote.add_argument("--source", choices=["sina", "tencent", "eastmoney"], help="只使用指定单一数据源")
    p_quote.add_argument("--mode", choices=["single", "validate"], help="查询模式；默认读取 [datasource].mode")

    p_market = sub.add_parser("market", help="查看大盘指数（多源校验）")
    p_market.add_argument("symbols", nargs="*", help="指数代码/名称，如 sh000001 399001 科创50")
    p_market.add_argument("--json", dest="as_json", action="store_true", help="JSON 格式输出")
    p_market.add_argument("--sources", default=None, help="数据源，逗号分隔；默认读取 [datasource] 配置")
    p_market.add_argument("--tolerance-pct", type=float, default=0.08, help="多源涨跌幅差异阈值")
    p_market.add_argument("--breadth", action="store_true", help="查询全市场上涨/下跌/平盘家数")
    p_market.add_argument("--breadth-source", default="sina", help="市场宽度数据源，目前支持 sina")

    # ── 预警 ────────────────────────────────────────────
    p_alert = sub.add_parser("alert", help="预警管理")
    p_alert_sub = p_alert.add_subparsers(dest="alert_action")

    p_alert_check = p_alert_sub.add_parser("check", help="立即运行预警检查")
    p_alert_check.add_argument("--record-pnl", action="store_true", help="检查后记录当日盈亏到 daily_pnl")
    p_alert_watch = p_alert_sub.add_parser("watch", help="定时监控持仓")
    p_alert_watch.add_argument("--interval", type=int, help="检查间隔秒数（默认300）")
    p_alert_history = p_alert_sub.add_parser("history", help="查看预警历史")
    p_alert_history.add_argument("--limit", type=int, default=50, help="显示条数")

    # ── 风控 ────────────────────────────────────────────
    p_risk = sub.add_parser("risk", help="风控管理")
    p_risk_sub = p_risk.add_subparsers(dest="risk_action")

    _p_risk_check = p_risk_sub.add_parser("check", help="手动触发风控检查（止损/仓位/回撤）")
    _p_risk_status = p_risk_sub.add_parser("status", help="查看当前持仓风控状态")
    p_risk_history = p_risk_sub.add_parser("history", help="查看风控事件历史")
    p_risk_history.add_argument("--limit", type=int, default=50, help="显示条数")

    # ── 配置 ────────────────────────────────────────────
    p_cfg = sub.add_parser("config", help="配置飞书推送")
    g = p_cfg.add_mutually_exclusive_group()
    g.add_argument("--feishu-webhook", help="飞书群机器人 Webhook URL")
    g.add_argument("--feishu-chat-id", dest="feishu_chat_id", help="飞书群 ID")
    g.add_argument("--feishu-openid", dest="feishu_open_id", help="飞书用户 Open ID")
    g.add_argument("--clear-feishu", action="store_true", help="清除飞书配置")

    # ── 监控 ────────────────────────────────────────────
    sub.add_parser("monitor", help="系统健康监控面板")

    # ── 工具 ────────────────────────────────────────────
    p_tool = sub.add_parser("tool", help="工具集")
    p_tool_sub = p_tool.add_subparsers(dest="tool_action")

    p_diag = p_tool_sub.add_parser("diagnose", help="数据源诊断")
    p_diag.add_argument("--source", default="all", help="数据源: all/sector/eastmoney/sina")
    p_diag.add_argument("--json", dest="as_json", action="store_true", help="JSON 格式输出")
    p_diag.add_argument("--fresh", action="store_true", help="跳过缓存，测试实时数据源")

    p_probe = p_tool_sub.add_parser("probe", help="通用网站/API 探测")
    _build_probe_parser(p_probe)

    p_sched = p_tool_sub.add_parser("schedule", help="自动化调度")
    p_sched.add_argument("--list", action="store_true", help="列出所有调度任务")
    p_sched.add_argument("--start", action="store_true", help="启动调度器")

    p_signal = p_tool_sub.add_parser("signal", help="交易信号管理")
    _build_signal_parser(p_signal)

    p_event = p_tool_sub.add_parser("event", help="macro event intelligence")
    _build_event_parser(p_event)

    p_paper = p_tool_sub.add_parser("paper", help="模拟交易管理")
    p_paper.add_argument("action", choices=["status", "pnl", "holdings", "trades", "snapshot"],
                         help="status=账户概要, pnl=盈亏报告, holdings=持仓, trades=成交记录, snapshot=记录当日快照")

    args = parser.parse_args()
    cmd = args.cmd

    # ── 持仓/交易 ────────────────────────────────────────
    if cmd == "import":
        cmd_import(args)
    elif cmd == "batch-import":
        cmd_batch_import(args)
    elif cmd == "list":
        cmd_list(args)
    elif cmd == "remove":
        cmd_remove(args)
    elif cmd == "refresh":
        cmd_refresh(args)
    elif cmd == "summary":
        cmd_summary(args)
    elif cmd == "trade":
        cmd_trade(args)

    # ── 行情 ────────────────────────────────────────────
    elif cmd == "quote":
        cmd_quote(args)
    elif cmd == "market":
        cmd_market(args)

    # ── 预警 ────────────────────────────────────────────
    elif cmd == "alert":
        if args.alert_action == "check":
            cmd_alert_check(args)
        elif args.alert_action == "watch":
            cmd_alert_watch(args)
        elif args.alert_action == "history":
            cmd_alert_history(args)

    # ── 风控 ────────────────────────────────────────────
    elif cmd == "risk":
        if args.risk_action == "check":
            cmd_risk_check(args)
        elif args.risk_action == "status":
            cmd_risk_status(args)
        elif args.risk_action == "history":
            cmd_risk_history(args)

    # ── 配置 ────────────────────────────────────────────
    elif cmd == "config":
        cmd_config(args)

    # ── 监控 ────────────────────────────────────────────
    elif cmd == "monitor":
        cmd_monitor(args)

    # ── 工具 ────────────────────────────────────────────
    elif cmd == "tool":
        if args.tool_action == "diagnose":
            cmd_diagnose(args)
        elif args.tool_action == "probe":
            cmd_probe_api(args)
        elif args.tool_action == "schedule":
            from modules.scheduler import list_tasks, build_scheduler
            if args.list:
                print(list_tasks())
            elif args.start:
                sched = build_scheduler()
                print("MoatX 调度器启动（Ctrl+C 停止）")
                try:
                    sched.start()
                except KeyboardInterrupt:
                    print("\n调度器已停止")
        elif args.tool_action == "signal":
            cmd_signal(args)
        elif args.tool_action == "event":
            cmd_event(args)
        elif args.tool_action == "paper":
            cmd_paper(args)

    else:
        parser.print_help()


# ── 内联的工具函数（避免循环导入）───────────────────────────────────────


def _build_probe_parser(p_probe):
    p_probe.add_argument("urls", nargs="*", help="待探测 URL，支持多个")
    p_probe.add_argument("--file", help="从文本文件读取 URL，每行一个")
    p_probe.add_argument("--har", action="append", default=[], help="导入浏览器 HAR 文件")
    p_probe.add_argument("--include-static", action="store_true", help="保留 JS/CSS/图片等静态资源")
    p_probe.add_argument("--analyze-har-body", action="store_true", help="离线分析 HAR 中已保存的 response body")
    p_probe.add_argument("--workers", type=int, default=8, help="并发数")
    p_probe.add_argument("--timeout", type=int, default=8, help="单请求超时秒数")
    p_probe.add_argument("--json", dest="as_json", action="store_true", help="JSON 格式输出")
    p_probe.add_argument("--discover", action="store_true", help="从网页中提取疑似 API/资源 URL")
    p_probe.add_argument("--probe-discovered", action="store_true", help="批量探测发现到的非静态接口")
    p_probe.add_argument("--probe-js-apis", action="store_true", help="抓取页面外部 JS 提取 API 候选并探测")
    p_probe.add_argument("--stock-code", help="股票代码，用于补全页面 API 模板")
    p_probe.add_argument("--market", help="市场代码，东方财富 secid 前缀，如 1=沪市,0=深/北")
    p_probe.add_argument("--method", default="GET", help="HTTP 方法")
    p_probe.add_argument("--header", action="append", default=[], help="请求头，格式 Key: Value")
    p_probe.add_argument("--headers-json", help="JSON 对象格式请求头")
    p_probe.add_argument("--cookie", action="append", default=[], help="Cookie，格式 name=value")
    p_probe.add_argument("--cookies-json", help="JSON 对象格式 Cookie")
    p_probe.add_argument("--cookie-file", help="Cookie 文件")
    p_probe.add_argument("--param", action="append", default=[], help="Query 参数，格式 key=value")
    p_probe.add_argument("--params-json", help="JSON 对象格式 query params")
    p_probe.add_argument("--body", help="原始请求体")
    p_probe.add_argument("--body-file", help="从文件读取原始请求体")
    p_probe.add_argument("--json-body", help="JSON 请求体")
    p_probe.add_argument("--json-body-file", help="从文件读取 JSON 请求体")
    p_probe.add_argument("--min-score", type=int, help="只输出评分不低于该值的接口")
    p_probe.add_argument("--sort-score", action="store_true", help="按接口评分降序输出")
    p_probe.add_argument("--output", help="导出结果文件，支持 .json/.jsonl/.csv")
    p_probe.add_argument("--output-format", choices=["json", "jsonl", "csv"], help="指定导出格式")
    p_probe.add_argument("--snapshot-dir", help="保存响应分析快照目录")
    p_probe.add_argument("--snapshot-challenges-only", action="store_true", help="仅保存验证码/风控类快照")
    p_probe.add_argument("--semantic-only", action="store_true", help="仅输出接口语义摘要")


def _build_signal_parser(p_signal):
    p_signal.add_argument("action", choices=["list", "run", "clear"],
                         help="list=查看信号, run=生成信号, clear=清除旧信号")
    p_signal.add_argument("--symbol", help="仅评估该股票（run 时使用）")
    p_signal.add_argument("--strategy", default="ma",
                         choices=["ma", "kdj"],
                         help="选择策略: ma=均线交叉(默认), kdj=KDJ 策略")
    p_signal.add_argument("--params-file",
                         help="策略参数 JSON 文件路径（默认从 data/strategy_params.json 自动加载）")
    p_signal.add_argument("--limit", type=int, default=50, help="显示条数")


def _build_event_parser(p_event):
    p_event.add_argument(
        "event_action",
        choices=[
            "collect",
            "ingest",
            "extract",
            "states",
            "opportunities",
            "report",
            "news",
            "news-report",
            "news-factors",
            "topics",
            "topic-snapshots",
            "sources",
            "notify",
            "context",
            "summary",
            "elasticity",
            "run",
        ],
        help="collect/ingest/extract/states/opportunities/report/news/news-report/news-factors/topics/topic-snapshots/sources/notify/context/summary/elasticity/run",
    )
    p_event.add_argument("--limit", type=int, default=200, help="news/report limit")
    p_event.add_argument("--topic", default="", help="news intelligence topic/category filter")
    p_event.add_argument("--min-score", type=float, default=45.0, help="minimum news value score")
    p_event.add_argument("--json", dest="as_json", action="store_true", help="output JSON")
    p_event.add_argument("--output", help="write output to UTF-8 file")
    p_event.add_argument("--title", help="manual ingest title")
    p_event.add_argument("--summary", default="", help="manual ingest summary")
    p_event.add_argument("--url", default="", help="manual ingest source URL")
    p_event.add_argument("--source", default="manual", help="manual ingest source id")
    p_event.add_argument("--published-at", dest="published_at", default="", help="manual ingest publish time")
    p_event.add_argument("--file", help="manual ingest UTF-8 text/JSON file")
    p_event.add_argument("--notify", action="store_true", help="run notification check after event cycle")
    p_event.add_argument("--send", action="store_true", help="actually send event notification; default is dry-run")
    p_event.add_argument("--probability-threshold", type=float, default=None, help="event notify probability threshold")
    p_event.add_argument("--opportunity-threshold", type=float, default=None, help="event notify opportunity-score threshold")
    p_event.add_argument("--top-events", type=int, default=None, help="number of event summary rows")
    p_event.add_argument("--event-id", default="", help="event id for elasticity backtest")
    p_event.add_argument("--windows", default="1,3,5,10", help="forward windows for elasticity, e.g. 1,3,5,10")
    p_event.add_argument(
        "--min-probability",
        type=float,
        default=0.35,
        help="minimum event probability for opportunity scan",
    )
    p_event.add_argument(
        "--per-effect-limit",
        type=int,
        default=20,
        help="maximum stocks per transmission effect",
    )


def cmd_config(args):
    from modules.config import cfg, set as config_set, save
    if args.feishu_webhook:
        config_set("feishu.webhook", args.feishu_webhook)
        save()
        print(f"已设置飞书 Webhook: {args.feishu_webhook}")
    elif args.feishu_chat_id:
        config_set("feishu.chat_id", args.feishu_chat_id)
        save()
        print(f"已设置飞书 Chat ID: {args.feishu_chat_id}")
    elif args.feishu_open_id:
        config_set("feishu.open_id", args.feishu_open_id)
        save()
        print(f"已设置飞书 Open ID: {args.feishu_open_id}")
    elif args.clear_feishu:
        config_set("feishu.webhook", "")
        config_set("feishu.chat_id", "")
        config_set("feishu.open_id", "")
        save()
        print("已清除飞书配置")
    else:
        feishu = cfg().feishu
        print(f"飞书 Webhook: {feishu.webhook or '(未配置)'}")
        print(f"飞书 Chat ID: {feishu.chat_id or '(未配置)'}")
        print(f"飞书 Open ID: {feishu.open_id or '(未配置)'}")


if __name__ == "__main__":
    main()
