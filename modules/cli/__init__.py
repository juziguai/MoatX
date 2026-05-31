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
from .market import cmd_market
from .alerter import cmd_alert_check, cmd_alert_watch, cmd_alert_history
from .risk import cmd_risk_check, cmd_risk_status, cmd_risk_history
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
    p_health = p_tool_sub.add_parser("health", help="数据源健康检查")
    p_health.add_argument("--json", dest="as_json", action="store_true", help="JSON 格式输出")

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

    p_stock_report = p_tool_sub.add_parser("stock-report", help="单股综合决策报告")
    p_stock_report.add_argument("symbol", help="股票代码，如 600519 或 002342")
    p_stock_report.add_argument("--json", dest="as_json", action="store_true", help="JSON 格式输出")
    p_stock_report.add_argument("--verbose", action="store_true", help="显示数据源降级日志")

    p_swing = p_tool_sub.add_parser("swing", help="低吸隔日冲高短线模型")
    _build_swing_parser(p_swing)

    p_intraday = p_tool_sub.add_parser("intraday", help="盘中异动雷达")
    _build_intraday_parser(p_intraday)

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
        from .quote import cmd_quote

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
            from .tool import cmd_diagnose

            cmd_diagnose(args)
        elif args.tool_action == "probe":
            from .tool import cmd_probe_api

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
            from .tool import cmd_signal

            cmd_signal(args)
        elif args.tool_action == "event":
            from .tool import cmd_event

            cmd_event(args)
        elif args.tool_action == "paper":
            from .tool import cmd_paper

            cmd_paper(args)
        elif args.tool_action == "stock-report":
            from .tool import cmd_stock_report

            cmd_stock_report(args)
        elif args.tool_action == "swing":
            from .tool import cmd_swing

            cmd_swing(args)
        elif args.tool_action == "health":
            import json
            from modules.source_health import run_health_check, get_source_status
            results = run_health_check()
            if hasattr(args, "as_json") and args.as_json:
                status = get_source_status()
                print(json.dumps(status, ensure_ascii=False, indent=2))
            else:
                for r in results:
                    status = "OK" if r.healthy else "FAIL"
                    print(f"[{status}] {r.source}: {r.latency_ms:.0f}ms samples={r.sample_count} {r.error}")
        elif args.tool_action == "intraday":
            from .tool import cmd_intraday

            cmd_intraday(args)

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
            "news-factor-backfill",
            "topics",
            "topic-snapshots",
            "llm-status",
            "llm-review",
            "llm-reviews",
            "sources",
            "notify",
            "context",
            "summary",
            "elasticity",
            "calibration",
            "run",
        ],
        help="collect/ingest/extract/states/opportunities/report/news/news-report/news-factors/news-factor-backfill/topics/topic-snapshots/llm-status/llm-review/llm-reviews/sources/notify/context/summary/elasticity/calibration/run",
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
    p_event.add_argument("--start-date", default="", help="snapshot/backfill start date, e.g. 2026-05-01")
    p_event.add_argument("--end-date", default="", help="snapshot/backfill end date, e.g. 2026-05-29")
    p_event.add_argument("--lookback-days", type=int, default=14, help="news factor snapshot lookback window")
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


def _build_swing_parser(p_swing):
    p_swing_sub = p_swing.add_subparsers(dest="swing_action")

    p_analyze = p_swing_sub.add_parser("analyze", help="分析单只股票的低吸隔日冲高形态")
    p_analyze.add_argument("symbol", help="股票代码，如 600519 或 002466")
    p_analyze.add_argument("--name", default="", help="股票名称")
    p_analyze.add_argument("--no-risk", action="store_true", help="跳过财务/公告风险检查")
    p_analyze.add_argument("--no-context", action="store_true", help="跳过大盘宽度和主题强弱上下文")
    p_analyze.add_argument("--json", dest="as_json", action="store_true", help="JSON 格式输出")
    p_analyze.add_argument("--verbose", action="store_true", help="显示数据源降级日志")

    p_candidates = p_swing_sub.add_parser("candidates", help="扫描低吸隔日冲高候选")
    p_candidates.add_argument("--limit", type=int, default=20, help="输出候选数量")
    p_candidates.add_argument("--pool-limit", type=int, default=80, help="从实时行情中取前 N 只做日线复核")
    p_candidates.add_argument("--workers", type=int, default=4, help="候选日线复核并发数")
    p_candidates.add_argument("--deadline-seconds", type=float, default=0.0, help="扫描时间预算秒数，0 表示不限制")
    p_candidates.add_argument("--no-network-daily", action="store_true", help="只用仓库日线缓存，不联网补日线")
    p_candidates.add_argument("--no-breakout", action="store_true", help="不启用放量突破首日追涨模式")
    p_candidates.add_argument("--check-risk", action="store_true", help="扫描候选时也执行财务/公告风险检查")
    p_candidates.add_argument("--json", dest="as_json", action="store_true", help="JSON 格式输出")
    p_candidates.add_argument("--verbose", action="store_true", help="显示数据源降级日志")

    p_watchlist = p_swing_sub.add_parser("watchlist", help="盘后生成明日短线观察名单")
    p_watchlist.add_argument("--limit", type=int, default=10, help="观察名单数量")
    p_watchlist.add_argument("--pool-limit", type=int, default=120, help="从实时行情中取前 N 只做日线复核")
    p_watchlist.add_argument("--workers", type=int, default=4, help="候选日线复核并发数")
    p_watchlist.add_argument("--deadline-seconds", type=float, default=180.0, help="扫描时间预算秒数，0 表示不限制")
    p_watchlist.add_argument("--network-daily-fallback", action="store_true", help="仓库日线缺失时联网补日线")
    p_watchlist.add_argument("--min-score", type=float, default=55.0, help="最低入选分数")
    p_watchlist.add_argument("--cash-per-stock", type=float, default=10_000.0, help="单票模拟投入金额")
    p_watchlist.add_argument("--lot-size", type=int, default=100, help="A股一手股数")
    p_watchlist.add_argument("--check-risk", action="store_true", help="执行财务/公告风险检查")
    p_watchlist.add_argument("--candidate-only", action="store_true", help="只输出强候选，忽略观察票")
    p_watchlist.add_argument("--no-breakout", action="store_true", help="不启用放量突破首日追涨模式")
    p_watchlist.add_argument("--no-score-gate", action="store_true", help="跳过综合打分/风控门控")
    p_watchlist.add_argument("--min-comprehensive-score", type=float, default=20.0, help="综合打分门控最低分")
    p_watchlist.add_argument("--output", help="额外写入 JSON 文件路径")
    p_watchlist.add_argument("--send", action="store_true", help="推送观察名单")
    p_watchlist.add_argument("--json", dest="as_json", action="store_true", help="JSON 格式输出")
    p_watchlist.add_argument("--verbose", action="store_true", help="显示数据源降级日志")

    p_monitor = p_swing_sub.add_parser("monitor", help="盘中监控短线观察名单目标/止损")
    p_monitor.add_argument("--watchlist", help="观察名单 JSON 路径，默认 data/swing_watchlist_latest.json")
    p_monitor.add_argument("--ignore-market-hours", action="store_true", help="非交易时间也执行检查")
    p_monitor.add_argument("--send", action="store_true", help="有触发时推送提醒")
    p_monitor.add_argument("--json", dest="as_json", action="store_true", help="JSON 格式输出")
    p_monitor.add_argument("--verbose", action="store_true", help="显示数据源降级日志")

    p_backtest = p_swing_sub.add_parser("backtest", help="回放短线策略历史表现")
    p_backtest.add_argument("--start", required=True, help="开始日期，如 2026-01-01")
    p_backtest.add_argument("--end", help="结束日期，默认今天")
    p_backtest.add_argument("--symbols", default="", help="逗号分隔股票代码；为空则使用当前流动性股票池")
    p_backtest.add_argument("--symbols-file", default="", help="固定股票池文件，支持 JSON/TXT/CSV")
    p_backtest.add_argument("--universe-limit", type=int, default=300, help="当前流动性股票池数量")
    p_backtest.add_argument("--pool-limit", type=int, default=80, help="每日预筛复核数量")
    p_backtest.add_argument("--top-n", type=int, default=5, help="每日最多买入数量")
    p_backtest.add_argument("--min-score", type=float, default=55.0, help="最低入选分数")
    p_backtest.add_argument("--cash-per-trade", type=float, default=10_000.0, help="单票等金额投入")
    p_backtest.add_argument("--initial-capital", type=float, default=100_000.0, help="收益率/回撤计算本金")
    p_backtest.add_argument("--lot-size", type=int, default=100, help="A股一手股数")
    p_backtest.add_argument("--workers", type=int, default=4, help="日线加载并发数")
    p_backtest.add_argument("--lookback-days", type=int, default=160, help="指标和历史参考预热天数")
    p_backtest.add_argument("--check-risk", action="store_true", help="使用当前财务/公告风险检查")
    p_backtest.add_argument("--candidate-only", action="store_true", help="只回放强候选，忽略观察票")
    p_backtest.add_argument("--no-breakout", action="store_true", help="回测时不启用放量突破首日追涨模式")
    p_backtest.add_argument("--no-event-context", action="store_true", help="回测时不使用历史新闻因子快照")
    p_backtest.add_argument("--slippage-pct", type=float, default=0.001, help="单边滑点比例")
    p_backtest.add_argument(
        "--intraday-policy",
        choices=["conservative", "target_first", "close"],
        default="conservative",
        help="次日同时触发目标和止损时的处理方式",
    )
    p_backtest.add_argument("--output", help="写入完整 JSON 文件路径")
    p_backtest.add_argument("--json", dest="as_json", action="store_true", help="JSON 格式输出")
    p_backtest.add_argument("--verbose", action="store_true", help="显示数据源降级日志")

    p_validate = p_swing_sub.add_parser("validate", help="横向验证短线策略不同开关的收益表现")
    p_validate.add_argument("--start", required=True, help="开始日期，如 2026-01-01")
    p_validate.add_argument("--end", help="结束日期，默认今天")
    p_validate.add_argument("--symbols", default="", help="逗号分隔股票代码；为空则使用当前流动性股票池")
    p_validate.add_argument("--symbols-file", default="", help="固定股票池文件，支持 JSON/TXT/CSV")
    p_validate.add_argument("--universe-limit", type=int, default=300, help="当前流动性股票池数量")
    p_validate.add_argument("--pool-limit", type=int, default=80, help="每日预筛复核数量")
    p_validate.add_argument("--top-n", type=int, default=5, help="每日最多买入数量")
    p_validate.add_argument("--min-score", type=float, default=55.0, help="最低入选分数")
    p_validate.add_argument("--cash-per-trade", type=float, default=10_000.0, help="单票等金额投入")
    p_validate.add_argument("--initial-capital", type=float, default=100_000.0, help="收益率/回撤计算本金")
    p_validate.add_argument("--lot-size", type=int, default=100, help="A股一手股数")
    p_validate.add_argument("--workers", type=int, default=4, help="日线加载并发数")
    p_validate.add_argument("--lookback-days", type=int, default=160, help="指标和历史参考预热天数")
    p_validate.add_argument("--check-risk", action="store_true", help="使用当前财务/公告风险检查")
    p_validate.add_argument("--slippage-pct", type=float, default=0.001, help="单边滑点比例")
    p_validate.add_argument(
        "--intraday-policy",
        choices=["conservative", "target_first", "close"],
        default="conservative",
        help="次日同时触发目标和止损时的处理方式",
    )
    p_validate.add_argument("--output", help="写入完整 JSON 文件路径")
    p_validate.add_argument("--json", dest="as_json", action="store_true", help="JSON 格式输出")
    p_validate.add_argument("--verbose", action="store_true", help="显示数据源降级日志")

    p_diagnose = p_swing_sub.add_parser("diagnose", help="归因诊断短线策略亏损/收益来源")
    p_diagnose.add_argument("--start", required=True, help="开始日期，如 2026-01-01")
    p_diagnose.add_argument("--end", help="结束日期，默认今天")
    p_diagnose.add_argument("--symbols", default="", help="逗号分隔股票代码；为空则使用当前流动性股票池")
    p_diagnose.add_argument("--symbols-file", default="", help="固定股票池文件，支持 JSON/TXT/CSV")
    p_diagnose.add_argument("--universe-limit", type=int, default=300, help="当前流动性股票池数量")
    p_diagnose.add_argument("--pool-limit", type=int, default=80, help="每日预筛复核数量")
    p_diagnose.add_argument("--top-n", type=int, default=5, help="每日最多买入数量")
    p_diagnose.add_argument("--min-score", type=float, default=55.0, help="最低入选分数")
    p_diagnose.add_argument("--cash-per-trade", type=float, default=10_000.0, help="单票等金额投入")
    p_diagnose.add_argument("--initial-capital", type=float, default=100_000.0, help="收益率/回撤计算本金")
    p_diagnose.add_argument("--lot-size", type=int, default=100, help="A股一手股数")
    p_diagnose.add_argument("--workers", type=int, default=4, help="日线加载并发数")
    p_diagnose.add_argument("--lookback-days", type=int, default=160, help="指标和历史参考预热天数")
    p_diagnose.add_argument("--check-risk", action="store_true", help="使用当前财务/公告风险检查")
    p_diagnose.add_argument("--slippage-pct", type=float, default=0.001, help="单边滑点比例")
    p_diagnose.add_argument(
        "--intraday-policy",
        choices=["conservative", "target_first", "close"],
        default="conservative",
        help="次日同时触发目标和止损时的处理方式",
    )
    p_diagnose.add_argument(
        "--variants",
        default="candidate,default",
        help="诊断版本: candidate, candidate_no_breakout, default, no_news_default",
    )
    p_diagnose.add_argument("--output", help="写入完整 JSON 文件路径")
    p_diagnose.add_argument("--json", dest="as_json", action="store_true", help="JSON 格式输出")
    p_diagnose.add_argument("--verbose", action="store_true", help="显示数据源降级日志")

    p_freeze = p_swing_sub.add_parser("freeze-universe", help="冻结当前动态股票池用于复现实验")
    p_freeze.add_argument("--universe-limit", type=int, default=120, help="冻结股票池数量")
    p_freeze.add_argument("--source-limit", type=int, default=0, help="质量过滤前的动态源池数量，默认目标数量的 2 倍")
    p_freeze.add_argument("--min-daily-bars", type=int, default=60, help="入池最少日线根数")
    p_freeze.add_argument("--quality-lookback-days", type=int, default=260, help="质量过滤读取日线窗口天数")
    p_freeze.add_argument("--quality-end", default="", help="质量过滤截止日期，默认今天，如 2026-05-29")
    p_freeze.add_argument("--stale-days", type=int, default=20, help="最新日线早于该天数视为过期")
    p_freeze.add_argument("--workers", type=int, default=4, help="日线质量检查并发数")
    p_freeze.add_argument("--no-quality-filter", action="store_true", help="跳过日线/板块质量过滤")
    p_freeze.add_argument("--output", default="", help="写入 JSON 文件路径，默认 data/swing_universe_fixed_YYYYMMDD.json")
    p_freeze.add_argument("--json", dest="as_json", action="store_true", help="JSON 格式输出")
    p_freeze.add_argument("--verbose", action="store_true", help="显示数据源降级日志")

    p_paper = p_swing_sub.add_parser("paper", help="按等金额生成低吸隔日冲高虚拟账号")
    p_paper.add_argument("--limit", type=int, default=5, help="建仓候选数量")
    p_paper.add_argument("--pool-limit", type=int, default=80, help="从实时行情中取前 N 只做日线复核")
    p_paper.add_argument("--workers", type=int, default=4, help="候选日线复核并发数")
    p_paper.add_argument("--cash-per-stock", type=float, default=10_000.0, help="每只股票计划投入金额")
    p_paper.add_argument("--lot-size", type=int, default=100, help="A股一手股数")
    p_paper.add_argument("--check-risk", action="store_true", help="生成账号时也执行财务/公告风险检查")
    p_paper.add_argument("--no-breakout", action="store_true", help="不启用放量突破首日追涨模式")
    p_paper.add_argument("--output", help="写入 JSON 文件路径")
    p_paper.add_argument("--json", dest="as_json", action="store_true", help="JSON 格式输出")
    p_paper.add_argument("--verbose", action="store_true", help="显示数据源降级日志")


def _build_intraday_parser(p_intraday):
    p_intraday_sub = p_intraday.add_subparsers(dest="intraday_action")

    def add_common(parser):
        parser.add_argument("--date", default="", help="交易日期，如 2026-05-29；为空则取接口最新交易日")
        parser.add_argument("--min-score", type=float, default=65.0, help="最低信号分")
        parser.add_argument("--min-pct", type=float, default=3.0, help="最低涨幅")
        parser.add_argument("--max-entry-pct", type=float, default=7.8, help="超过该涨幅视为高追风险")
        parser.add_argument("--min-ret-10m", type=float, default=2.0, help="10分钟最低拉升幅度")
        parser.add_argument("--min-amount-ratio", type=float, default=1.8, help="分钟成交额放大倍数")
        parser.add_argument("--json", dest="as_json", action="store_true", help="JSON 格式输出")
        parser.add_argument("--verbose", action="store_true", help="显示数据源降级日志")

    p_replay = p_intraday_sub.add_parser("replay", help="复盘单只股票盘中异动")
    p_replay.add_argument("symbol", help="股票代码，如 600011")
    add_common(p_replay)

    p_radar = p_intraday_sub.add_parser("radar", help="扫描股票池盘中异动")
    p_radar.add_argument("--symbols", default="", help="逗号分隔股票代码")
    p_radar.add_argument("--symbols-file", default="", help="股票池文件，支持 JSON/TXT/CSV")
    p_radar.add_argument("--limit", type=int, default=80, help="最多扫描股票数")
    p_radar.add_argument("--write-snapshot", action="store_true", help="写入 data/intraday_radar 快照")
    add_common(p_radar)


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
