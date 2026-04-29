"""Health monitoring dashboard for MoatX."""

from __future__ import annotations

from datetime import datetime


def cmd_monitor(args):
    from modules.config import cfg as _cfg
    from modules.db import DatabaseManager
    from pathlib import Path

    db = DatabaseManager(_cfg().data.warehouse_path)

    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    print(f"=== MoatX 健康监控 — {now} ===")
    print("-" * 50)

    # Scheduler status
    try:
        from modules.scheduler import _scheduler_ref
        if _scheduler_ref and _scheduler_ref.running:
            started = getattr(_scheduler_ref, "_started_at", None)
            if started:
                delta = datetime.now() - started
                hours = int(delta.total_seconds() // 3600)
                mins = int((delta.total_seconds() % 3600) // 60)
                print(f"调度器: 🟢 运行中 (已运行 {hours}h {mins}m)")
            else:
                print("调度器: 🟢 运行中")
        else:
            print("调度器: ⚪ 未启动")
    except Exception:
        print("调度器: ⚪ 未知")

    # Recent task executions
    try:
        runs = db.task_log().recent_runs(limit=5)
        if not runs.empty:
            last = runs.iloc[0]
            ok_icon = "✅" if last.get("success") else "❌"
            print(f"上次任务: {last.get('task_name', '?')} ({ok_icon}, {last.get('finished_at', '')[:16]})")
    except Exception:
        pass

    # Log file size
    log_path = Path(_cfg().data.warehouse_path).parent / "moatx.log"
    if log_path.exists():
        size_mb = log_path.stat().st_size / (1024 * 1024)
        print(f"日志: {log_path.name} ({size_mb:.1f}MB)")
    else:
        print("日志: 无")

    # Data source status (simple connectivity check)
    print("数据源: ", end="")
    sources_status = _check_data_sources()
    print(" | ".join(sources_status))

    # Recent alerts from alert log file
    try:
        alert_log = Path(_cfg().data.warehouse_path).parent / "alerts.log"
        if alert_log.exists():
            lines = alert_log.read_text(encoding="utf-8").strip().split("\n")
            if lines:
                recent = [entry for entry in lines if entry.strip()][-3:]
                print(f"近期预警: {len(recent)} 条")
                for line in recent:
                    print(f"  {line[:80]}")
        else:
            print("近期预警: 无")
    except Exception:
        print("预警: 未知")

    # Feishu push health
    try:
        from modules.alerter import get_push_stats
        stats = get_push_stats()
        total = stats.get("total", 0)
        success = stats.get("success", 0)
        if total > 0:
            rate = success / total * 100
            print(f"飞书推送: {success}/{total} 成功 ({rate:.0f}%)")
        else:
            print("飞书推送: 无记录")
    except Exception:
        pass

    print("-" * 50)


def _check_data_sources() -> list[str]:
    """Check connectivity of major data sources."""
    import requests
    results = []
    sources = [
        ("Sina", "https://stock.finance.sina.com.cn"),
        ("Tencent", "https://qt.gtimg.cn"),
        ("EastMoney", "https://push2.eastmoney.com"),
        ("CNINFO", "https://www.cninfo.com.cn"),
    ]
    for name, url in sources:
        try:
            r = requests.get(url, timeout=5)
            if r.status_code < 500:
                results.append(f"{name} 🟢")
            else:
                results.append(f"{name} 🔴")
        except Exception:
            results.append(f"{name} ⚪")
    return results
