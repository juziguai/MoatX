"""
cli_portfolio.py - 持仓管理 CLI 入口
用法:
    python -m modules.cli_portfolio import "截图路径"
    python -m modules.cli_portfolio list
    python -m modules.cli_portfolio remove 600519
    python -m modules.cli_portfolio check
    python -m modules.cli_portfolio watch
    python -m modules.cli_portfolio config --feishu-webhook "https://open.feishu.cn/..."
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime

from .portfolio import Portfolio
from .alerter import Alerter


def parse_screenshot(image_path: str) -> list:
    """
    调用 mmx vision describe 解析持仓截图
    返回 [(symbol, name, shares, cost_price), ...]
    """
    prompt = (
        "请仔细识别这张持仓截图中的所有股票信息。"
        "对于每只股票，请提供：股票代码（6位数字）、股票名称、持股数量。"
        "返回格式（每行一只股票，逗号分隔）："
        "股票代码,股票名称,持股数量"
        "例如：600519,贵州茅台,100"
        "如果无法识别某个字段，用?代替。只返回股票信息，不要其他说明文字。"
    )

    cmd = [
        "mmx", "vision", "describe",
        "--image", image_path,
        "--prompt", prompt,
        "--output", "json",
        "--quiet"
    ]

    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=60,
            encoding="utf-8", errors="replace"
        )
        output = result.stdout.strip()
        if result.returncode != 0:
            print(f"[WARN] mmx 返回码 {result.returncode}: {result.stderr}", file=sys.stderr)

        # 解析 JSON 输出
        if output:
            try:
                data = json.loads(output)
                # mmx vision describe 返回结构可能是 {"description": "..."} 或直接文本
                text = data.get("description", "") if isinstance(data, dict) else str(data)
            except json.JSONDecodeError:
                text = output
        else:
            text = ""

        # 从文本中提取股票代码
        results = []
        for line in text.split("\n"):
            line = line.strip()
            if not line:
                continue
            # 匹配 6位数字股票代码
            m = re.match(r"(\d{6})\s*[,，]?\s*(.{0,10}?)\s*[,，]?\s*([\d.]+)", line)
            if m:
                symbol = m.group(1)
                name = m.group(2).strip()
                try:
                    shares = float(m.group(3))
                except ValueError:
                    shares = 0
                results.append((symbol, name, shares, 0))
        return results

    except FileNotFoundError:
        print("[ERROR] mmx 命令未找到，请确保已安装 mmx CLI", file=sys.stderr)
        return []
    except subprocess.TimeoutExpired:
        print("[ERROR] mmx vision 解析超时", file=sys.stderr)
        return []
    except Exception as e:
        print(f"[ERROR] 截图解析异常: {e}", file=sys.stderr)
        return []


def cmd_import(args):
    pf = Portfolio()
    results = parse_screenshot(args.image_path)
    if not results:
        print("未能从截图识别到股票信息，请确认截图清晰且包含股票代码")
        return

    added = pf.import_parsed_results(results)
    print(f"成功导入 {len(added)} 只股票: {', '.join(added)}")
    print()
    print(pf.list_holdings().to_string(index=False))


def cmd_list(args):
    pf = Portfolio()
    df = pf.list_holdings()
    if df.empty:
        print("持仓为空，请先通过 import 命令导入")
        return
    print(f"共 {len(df)} 只持仓：")
    print(df.to_string(index=False))


def cmd_remove(args):
    pf = Portfolio()
    symbol = pf._normalize_symbol(args.symbol)
    existing = pf.get_holding(symbol)
    if not existing:
        print(f"持仓中未找到 {symbol}")
        return
    pf.remove_holding(symbol)
    print(f"已删除持仓: {symbol} {existing.get('name', '')}")


def cmd_check(args):
    pf = Portfolio()
    alerter = _build_alerter(pf)
    alerts = pf.check_alerts()
    report = Alerter.format_alert_report(alerts)
    alerter.send(report)
    # 同时打印到 stdout
    print(report)


def cmd_watch(args):
    pf = Portfolio()
    interval = args.interval or 300  # 默认5分钟
    print(f"开始监控持仓，每 {interval} 秒检查一次（Ctrl+C 停止）")
    print(f"飞书 Webhook: {pf.get_config('feishu_webhook') or '未配置'}")

    while True:
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"\n[{ts}] 开始检查...")
        try:
            alerter = _build_alerter(pf)
            alerts = pf.check_alerts()
            report = Alerter.format_alert_report(alerts)
            if any(a.get("type") != "error" for a in alerts):
                alerter.send(report)
            else:
                print(report)
        except Exception as e:
            print(f"[ERROR] 检查异常: {e}")
        time.sleep(interval)


def cmd_config(args):
    pf = Portfolio()
    if args.feishu_webhook:
        pf.set_config("feishu_webhook", args.feishu_webhook)
        print(f"已设置飞书 Webhook: {args.feishu_webhook}")
    elif args.feishu_chat_id:
        pf.set_config("feishu_chat_id", args.feishu_chat_id)
        print(f"已设置飞书 Chat ID: {args.feishu_chat_id}")
    elif args.feishu_open_id:
        pf.set_config("feishu_open_id", args.feishu_open_id)
        print(f"已设置飞书 Open ID: {args.feishu_open_id}")
    elif args.clear_feishu:
        pf.set_config("feishu_webhook", "")
        pf.set_config("feishu_chat_id", "")
        print("已清除飞书配置")
    else:
        # 打印当前配置
        webhook = pf.get_config("feishu_webhook")
        chat_id = pf.get_config("feishu_chat_id")
        open_id = pf.get_config("feishu_open_id")
        print(f"飞书 Webhook: {webhook or '(未配置)'}")
        print(f"飞书 Chat ID: {chat_id or '(未配置)'}")
        print(f"飞书 Open ID: {open_id or '(未配置)'}")


def cmd_alerts(args):
    pf = Portfolio()
    df = pf.get_alert_history(limit=args.limit)
    if df.empty:
        print("暂无预警记录")
        return
    print(df.to_string(index=False))


def _build_alerter(pf: Portfolio) -> Alerter:
    webhook = pf.get_config("feishu_webhook")
    chat_id = pf.get_config("feishu_chat_id")
    open_id = pf.get_config("feishu_open_id")
    return Alerter(
        feishu_webhook=webhook or None,
        feishu_chat_id=chat_id or None,
        feishu_open_id=open_id or None
    )


def main():
    parser = argparse.ArgumentParser(prog="python -m modules.cli_portfolio")
    sub = parser.add_subparsers(dest="cmd")

    # import
    p_import = sub.add_parser("import", help="从持仓截图导入")
    p_import.add_argument("image_path", help="截图文件路径")

    # list
    sub.add_parser("list", help="查看持仓列表")

    # remove
    p_remove = sub.add_parser("remove", help="删除持仓")
    p_remove.add_argument("symbol", help="股票代码")

    # check
    sub.add_parser("check", help="立即运行预警检查")

    # watch
    p_watch = sub.add_parser("watch", help="定时监控持仓")
    p_watch.add_argument("--interval", type=int, help="检查间隔秒数（默认300）")

    # config
    p_cfg = sub.add_parser("config", help="配置飞书推送")
    g = p_cfg.add_mutually_exclusive_group()
    g.add_argument("--feishu-webhook", help="飞书群机器人 Webhook URL")
    g.add_argument("--feishu-chat-id", dest="feishu_chat_id", help="飞书群 ID")
    g.add_argument("--feishu-openid", dest="feishu_open_id", help="飞书用户 Open ID")
    g.add_argument("--clear-feishu", action="store_true", help="清除飞书配置")

    # alerts
    p_alerts = sub.add_parser("alerts", help="查看预警历史")
    p_alerts.add_argument("--limit", type=int, default=50, help="显示条数")

    args = parser.parse_args()

    if args.cmd == "import":
        cmd_import(args)
    elif args.cmd == "list":
        cmd_list(args)
    elif args.cmd == "remove":
        cmd_remove(args)
    elif args.cmd == "check":
        cmd_check(args)
    elif args.cmd == "watch":
        cmd_watch(args)
    elif args.cmd == "config":
        cmd_config(args)
    elif args.cmd == "alerts":
        cmd_alerts(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
