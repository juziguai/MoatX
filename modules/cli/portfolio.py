"""
modules/cli/portfolio.py - 持仓/交易相关命令
"""

import json
import logging
import os
import re
import subprocess

from modules.portfolio import Portfolio

_logger = logging.getLogger("moatx.cli.portfolio")


def _portfolio_cls():
    return Portfolio


# ── 截图解析 ────────────────────────────────────────────────

def parse_screenshot(image_path: str) -> list:
    """
    调用 mmx vision describe 解析持仓截图
    返回 [(symbol, name, shares, cost_price), ...]
    """
    import shutil

    # 检查 mmx 是否可用
    # mmx_cmd = shutil.os.path.basename("mmx")
    if not shutil.which("mmx"):
        npm_mmx = os.path.join(os.environ.get("APPDATA", ""), "npm", "mmx.cmd")
        if not os.path.exists(npm_mmx):
            _logger.error("mmx 命令未找到")
            _logger.error("请先安装 mmx CLI: npm install -g @anthropic-ai/mmx")
            return []
        mmx_path = npm_mmx
    else:
        mmx_path = "mmx"

    prompt = (
        "请仔细识别这张持仓截图中的所有股票信息。"
        "对于每只股票，请提供：股票代码（6位数字）、股票名称、持股数量。"
        "返回格式（每行一只股票，逗号分隔）："
        "股票代码,股票名称,持股数量"
        "例如：600519,贵州茅台,100"
        "如果无法识别某个字段，用?代替。只返回股票信息，不要其他说明文字。"
    )

    abs_path = os.path.abspath(image_path)
    cmd = [mmx_path, "vision", "describe", "--image", abs_path, "--prompt", prompt,
           "--output", "json", "--quiet"]

    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=60,
            encoding="utf-8", errors="replace"
        )
        output = result.stdout.strip()
        if result.returncode != 0:
            _logger.warning("mmx 返回码 %d: %s", result.returncode, result.stderr)

        if output:
            try:
                data = json.loads(output)
                text = data.get("content", "") or data.get("description", "") if isinstance(data, dict) else str(data)
            except json.JSONDecodeError:
                text = output
        else:
            text = ""

        results = []
        for line in text.split("\n"):
            line = line.strip()
            if not line or line.startswith(("根据", "识别", "格式", "股票", "代码")):
                continue
            m = re.match(r"(\d{6})\s*[,，]?\s*(.{0,15}?)\s*[,，]?\s*([\d.]+)", line)
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
        _logger.error("mmx 命令未找到，请确保已安装 mmx CLI")
        return []
    except subprocess.TimeoutExpired:
        _logger.error("mmx vision 解析超时")
        return []
    except Exception as e:
        _logger.error("截图解析异常: %s", e)
        return []


# ── 命令实现 ────────────────────────────────────────────────

def cmd_import(args):
    pf = _portfolio_cls()()
    results = parse_screenshot(args.image_path)
    if not results:
        print("未能从截图识别到股票信息，请确认截图清晰且包含股票代码")
        return
    added = pf.import_parsed_results(results)
    print(f"成功导入 {len(added)} 只股票: {', '.join(added)}")

    # 导入后自动刷新行情
    if added:
        from modules.stock_data import StockData
        symbols = [pf._normalize_symbol(s) for s in added]
        sd = StockData()
        print(f"正在获取 {len(symbols)} 只股票实时行情...")
        quotes = sd.get_realtime_quotes(symbols)
        if quotes:
            pf.refresh_holdings(quotes)
            print("行情已更新")

    print()
    print(pf.list_holdings().to_string(index=False))


def cmd_batch_import(args):
    image_dir = args.dir or os.path.join(os.path.dirname(__file__), "..", "..", "data", "image")
    image_dir = os.path.abspath(image_dir)

    if not os.path.isdir(image_dir):
        _logger.error("目录不存在: %s", image_dir)
        return

    image_files = [
        os.path.join(image_dir, f)
        for f in os.listdir(image_dir)
        if f.lower().endswith((".png", ".jpg", ".jpeg", ".gif", ".webp"))
    ]

    if not image_files:
        _logger.error("目录中无图片文件: %s", image_dir)
        return

    print(f"找到 {len(image_files)} 张截图，开始解析...")
    pf = _portfolio_cls()()

    all_results = {}
    for path in image_files:
        print(f"  解析: {os.path.basename(path)}")
        results = parse_screenshot(path)
        filename = os.path.basename(path)
        all_results[filename] = results
        print(f"    -> 识别到 {len(results)} 只股票")

    merged = {}
    for filename, results in all_results.items():
        for symbol, name, shares, cost in results:
            if symbol not in merged:
                merged[symbol] = (symbol, name, shares, cost)
            elif shares > 0 and merged[symbol][2] == 0:
                merged[symbol] = (symbol, name, shares, cost)

    print(f"\n去重后共 {len(merged)} 只不同股票")
    added = pf.import_parsed_results(list(merged.values()))
    print(f"成功导入 {len(added)} 只股票: {', '.join(added)}")

    # 导入后自动刷新行情
    if added:
        from modules.stock_data import StockData
        symbols = [pf._normalize_symbol(s) for s in added]
        sd = StockData()
        print(f"正在获取 {len(symbols)} 只股票实时行情...")
        quotes = sd.get_realtime_quotes(symbols)
        if quotes:
            pf.refresh_holdings(quotes)
            print("行情已更新")

    print()
    print(pf.list_holdings().to_string(index=False))


def cmd_list(args):
    pf = _portfolio_cls()()
    df = pf.list_holdings()
    if df.empty:
        print("持仓为空，请先通过 import 命令导入")
        return
    print(f"共 {len(df)} 只持仓：")

    float_cols = ["cost_price", "current_price", "market_value", "total_pnl"]
    df_display = df.copy()
    for col in float_cols:
        if col in df_display.columns:
            df_display[col] = df_display[col].apply(lambda x: f"{x:.4f}" if x is not None else "")

    # daily_pnl_val / daily_pnl_ratio 已在 list_holdings() 时直接 JOIN 进来
    import pandas as pd
    if "daily_pnl_val" in df_display.columns:
        df_display["daily_pnl"] = df_display["daily_pnl_val"].apply(
            lambda x: f"{x:+.4f}" if not pd.isna(x) else "")
        df_display.drop(columns=["daily_pnl_val"], inplace=True)
    if "daily_pnl_ratio" in df_display.columns:
        # pnl_ratio 可能是字符串（如 "12.34%"）或数字
        def _fmt_pct(x):
            if pd.isna(x):
                return ""
            if isinstance(x, str):
                return x
            return f"{x:+.2f}%"
        df_display["daily_pct"] = df_display["daily_pnl_ratio"].apply(_fmt_pct)
        df_display.drop(columns=["daily_pnl_ratio"], inplace=True)

    print(df_display.to_string(index=False))


def cmd_remove(args):
    pf = _portfolio_cls()()
    symbol = pf._normalize_symbol(args.symbol)
    existing = pf.get_holding(symbol)
    if not existing:
        print(f"持仓中未找到 {symbol}")
        return
    pf.remove_holding(symbol)
    print(f"已删除持仓: {symbol} {existing.get('name', '')}")


def cmd_refresh(args):
    from modules.stock_data import StockData

    pf = _portfolio_cls()()
    df = pf.list_holdings()
    if df.empty:
        print("持仓为空，无需刷新")
        return

    symbols = df["symbol"].tolist()
    print(f"正在获取 {len(symbols)} 只股票的实时行情...")

    p = StockData()
    quotes = p.get_realtime_quotes(symbols)
    if not quotes:
        print("未获取到行情数据，更新失败")
        return

    updated = pf.refresh_holdings(quotes)
    print(f"已更新 {updated} 只持仓的价格")

    df = pf.list_holdings()
    print(f"\n共 {len(df)} 只持仓：")
    print(df.to_string(index=False))


def cmd_summary(args):
    """持仓总览：总市值、总盈亏、仓位分布"""

    pf = _portfolio_cls()()
    df = pf.list_holdings()
    if df.empty:
        print("持仓为空")
        return

    # 基本指标
    total_market_value = df["market_value"].sum()
    total_cost = (df["shares"] * df["cost_price"]).sum()
    total_pnl = total_market_value - total_cost
    total_pnl_pct = (total_pnl / total_cost * 100) if total_cost > 0 else 0

    print("=" * 50)
    print("  MoatX 持仓总览")
    print("=" * 50)
    print(f"  持仓数量：{len(df)} 只")
    print(f"  总市值：  {total_market_value:>12,.2f} 元")
    print(f"  总成本：  {total_cost:>12,.2f} 元")
    print(f"  总盈亏：  {total_pnl:>12,.2f} 元  ({total_pnl_pct:+.2f}%)")

    # 当日盈亏
    if "daily_pnl_val" in df.columns:
        daily_total = df["daily_pnl_val"].sum()
        for _, row in df.iterrows():
            prev = row.get("current_price", 0) / (1 + row.get("daily_pnl_ratio", 0) / 100) if row.get("daily_pnl_ratio", 0) != 0 else row.get("current_price", 0)
            if prev > 0:
                _daily_pct = df["daily_pnl_ratio"].mean()
                break
        print(f"  当日盈亏：{daily_total:>12,.2f} 元")

    print("-" * 50)
    print(f"  {'代码':<8} {'名称':<10} {'市值':>10} {'占比':>8} {'当日涨跌':>8}")
    print(f"  {'-'*6} {'-'*8} {'-'*10} {'-'*6} {'-'*8}")
    df_sorted = df.sort_values("market_value", ascending=False)
    for _, row in df_sorted.head(8).iterrows():
        pct = row["market_value"] / total_market_value * 100 if total_market_value > 0 else 0
        daily = f"{row.get('daily_pnl_ratio', 0):+.2f}%" if "daily_pnl_ratio" in row else ""
        print(f"  {row['symbol']:<8} {row['name']:<10} {row['market_value']:>10,.2f} {pct:>7.1f}%  {daily:>8}")
    print("-" * 50)
    print()


def cmd_trade(args):
    """记录一笔交易（买入/卖出），买入自动加入持仓，卖出自动删除持仓"""
    # 日期校验
    import re
    from datetime import datetime
    date_str = args.date
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", date_str):
        _logger.error("日期格式无效，请使用 YYYY-MM-DD 格式，例如 2026-04-25")
        return
    try:
        trade_date = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        _logger.error("日期无效: %s", date_str)
        return
    if trade_date.date() > datetime.now().date():
        _logger.error("交易日期不能晚于今天: %s", date_str)
        return

    pf = _portfolio_cls()()
    sym = pf._normalize_symbol(args.symbol)

    pf.record_trade(
        date=date_str,
        action=args.action,
        symbol=sym,
        name=args.name or "",
        shares=float(args.shares),
        price=float(args.price),
        amount=float(args.amount),
        fee=float(args.fee or 0),
        stamp_duty=float(args.stamp_duty or 0),
        transfer_fee=float(args.transfer_fee or 0),
        trade_levy=float(args.trade_levy or 0),
    )

    print(f"已记录：{date_str} {args.action.upper()} {sym} {args.name} "
          f"{args.shares}股 @{args.price}，成交额 {args.amount}")
