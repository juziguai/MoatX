"""MoatX 自动化调度器

基于 APScheduler，支持 cron + interval 任务。

用法:
    python -m modules.scheduler --list          # 列出所有任务
    python -m modules.scheduler --start         # 启动调度器（前台运行）
    python -m modules.scheduler --start --daemon # 后台运行
"""

from __future__ import annotations

import logging
import os
import signal
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, TypedDict

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

from modules.config import cfg

_logger = logging.getLogger("moatx.scheduler")

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_PID_FILE = _PROJECT_ROOT / "data" / "scheduler.pid"
_DAEMON_LOG = _PROJECT_ROOT / "data" / "scheduler_daemon.log"


def _hidden_subprocess_kwargs(*, new_group: bool = False) -> dict[str, Any]:
    """Hide scheduler child process console windows on Windows."""
    if os.name != "nt":
        return {}
    flags = subprocess.CREATE_NO_WINDOW
    if new_group:
        flags |= subprocess.CREATE_NEW_PROCESS_GROUP
    startupinfo = subprocess.STARTUPINFO()
    startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    startupinfo.wShowWindow = 0
    return {"creationflags": flags, "startupinfo": startupinfo}


class TaskDict(TypedDict):
    id: str
    name: str
    fn: Callable[..., Any]
    trigger: Any
    enabled: bool


class _SubprocessResult:
    """Subprocess execution result."""
    def __init__(self, returncode: int, stdout: str, stderr: str):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr

    @property
    def ok(self) -> bool:
        return self.returncode == 0


# ─────────────────────────────────────────────
# 任务回调函数
# ─────────────────────────────────────────────


def _log_task(task_id: str, task_name: str, fn: Callable[..., Any]) -> Callable[..., Any]:
    """Wrap a task function with execution logging."""
    import functools

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        if cfg().data.enable_warehouse:
            try:
                from modules.db import DatabaseManager
                db = DatabaseManager(cfg().data.warehouse_path)
                log_id = db.task().start_run(task_id, task_name)
                start = time.time()
                try:
                    result = fn(*args, **kwargs)
                    ok = getattr(result, "ok", True)
                    out = getattr(result, "stdout", "") or ""
                    err = getattr(result, "stderr", "") or ""
                    # stdout 截断避免过长
                    db.task().finish_run(
                        log_id, ok,
                        output=out[:500] if out else "",
                        error=err[:500] if err else "",
                        duration_ms=int((time.time() - start) * 1000),
                    )
                except Exception as e:
                    db.task().finish_run(
                        log_id, False, error=str(e),
                        duration_ms=int((time.time() - start) * 1000),
                    )
                    raise
            except Exception as e:
                _logger.warning("仓库记录失败，任务继续执行: %s", e)
                fn(*args, **kwargs)
        else:
            fn(*args, **kwargs)
    return wrapper


def scan_candidates() -> _SubprocessResult:
    """09:30 开盘扫描候选股"""
    return _run_script("set_pending.py", ["--scan"])


def mark_pending() -> _SubprocessResult:
    """14:50 标记待验证"""
    return _run_script("set_pending.py", ["--set"])


def verify_close() -> _SubprocessResult:
    """15:10 收盘验证涨跌"""
    return _run_script("verify_candidates.py", [])


def reset_pending() -> _SubprocessResult:
    """09:20 次日清除残留标记"""
    return _run_script("set_pending.py", ["--reset"])


def check_alerts() -> _SubprocessResult:
    """每 5 分钟盘中预警"""
    return _run_module("modules.cli_portfolio", ["alert", "check"])


def snapshot_portfolio() -> _SubprocessResult:
    """15:30 日终刷新持仓"""
    return _run_module("modules.cli_portfolio", ["refresh"])


def generate_signals() -> _SubprocessResult:
    """15:05 生成交易信号"""
    return _run_module("modules.cli_portfolio", ["tool", "signal", "run"])


# ─────────────────────────────────────────────
# 仿真交易任务回调
# ─────────────────────────────────────────────

def sim_scan_and_buy(*args, **kwargs) -> _SubprocessResult:
    """09:30 开盘扫描+模拟买入"""
    from modules.simulation import scan_and_buy
    import json
    try:
        result = scan_and_buy()
        return _SubprocessResult(0, json.dumps(result, ensure_ascii=False), "")
    except Exception as e:
        return _SubprocessResult(1, "", str(e))


def sim_monitor_holdings(*args, **kwargs) -> _SubprocessResult:
    """盘中实时监控（每30分钟）"""
    import json
    from modules.config import cfg as _cfg
    from modules.db import DatabaseManager
    from modules.alert_manager import AlertManager
    try:
        db = DatabaseManager(_cfg().data.warehouse_path)
        holdings = db.signal().all_paper_holdings()
        if holdings.empty:
            return _SubprocessResult(0, json.dumps({"alerts": 0}), "")
        am = AlertManager(db.conn())
        alerts = am.check_paper_alerts(holdings)
        for alert in alerts:
            am._log_alert(alert)
        return _SubprocessResult(0, json.dumps({"alerts": len(alerts)}), "")
    except Exception as e:
        return _SubprocessResult(1, "", str(e))


def sim_generate_sell_signals(*args, **kwargs) -> _SubprocessResult:
    """14:55 卖出信号生成"""
    import json
    from modules.simulation import generate_sell_signals
    try:
        result = generate_sell_signals()
        return _SubprocessResult(0, json.dumps(result, ensure_ascii=False), "")
    except Exception as e:
        return _SubprocessResult(1, "", str(e))


def sim_execute_signals(*args, **kwargs) -> _SubprocessResult:
    """15:00 执行未处理交易信号"""
    import json
    from modules.simulation import execute_signals
    try:
        result = execute_signals()
        return _SubprocessResult(0, json.dumps(result, ensure_ascii=False), "")
    except Exception as e:
        return _SubprocessResult(1, "", str(e))


def sim_daily_snapshot(*args, **kwargs) -> _SubprocessResult:
    """15:10 账户快照"""
    import json
    from modules.simulation import daily_snapshot
    try:
        result = daily_snapshot()
        return _SubprocessResult(0, json.dumps(result, ensure_ascii=False), "")
    except Exception as e:
        return _SubprocessResult(1, "", str(e))


def sim_daily_report(*args, **kwargs) -> _SubprocessResult:
    """15:30 每日交易报告（飞书推送）"""
    from modules.simulation import daily_report
    from modules.alerter import Alerter
    try:
        report = daily_report()
        # Try Feishu push
        try:
            Alerter().send(report, "MoatX 模拟交易日报")
        except Exception:
            pass
        return _SubprocessResult(0, report[:2000], "")
    except Exception as e:
        return _SubprocessResult(1, "", str(e))


def event_collect_news(*args, **kwargs) -> _SubprocessResult:
    """Collect configured macro event news sources."""
    return _run_module("modules.cli", ["tool", "event", "collect", "--json"])


def event_extract_signals(*args, **kwargs) -> _SubprocessResult:
    """Extract macro event signals from collected news."""
    return _run_module("modules.cli", ["tool", "event", "extract", "--json"])


def event_update_states(*args, **kwargs) -> _SubprocessResult:
    """Update macro event probability states."""
    return _run_module("modules.cli", ["tool", "event", "states", "--json"])


def event_scan_opportunities(*args, **kwargs) -> _SubprocessResult:
    """Scan event-driven stock opportunities."""
    return _run_module(
        "modules.cli",
        [
            "tool",
            "event",
            "opportunities",
            "--min-probability",
            str(cfg().event_intelligence.notify_probability_threshold),
            "--json",
        ],
    )


def event_cycle(*args, **kwargs) -> _SubprocessResult:
    """Run event intelligence cycle and dry-run notification candidates."""
    settings = cfg().event_intelligence
    return _run_module(
        "modules.cli",
        [
            "tool",
            "event",
            "run",
            "--notify",
            "--min-probability",
            str(settings.notify_probability_threshold),
            "--probability-threshold",
            str(settings.notify_probability_threshold),
            "--opportunity-threshold",
            str(settings.notify_opportunity_threshold),
            "--json",
        ],
    )


def event_notify(*args, **kwargs) -> _SubprocessResult:
    """Send event notifications that pass thresholds and cooldown checks."""
    settings = cfg().event_intelligence
    return _run_module(
        "modules.cli",
        [
            "tool",
            "event",
            "notify",
            "--send",
            "--probability-threshold",
            str(settings.notify_probability_threshold),
            "--opportunity-threshold",
            str(settings.notify_opportunity_threshold),
            "--json",
        ],
    )


# ─────────────────────────────────────────────
# 任务定义
# ─────────────────────────────────────────────

TASKS: list[TaskDict] = [
    # ── 旧任务（已禁用，保留函数不删除）────────────────────
    {
        "id": "scan_candidates",
        "name": "开盘扫描候选股",
        "fn": _log_task("scan_candidates", "开盘扫描候选股", scan_candidates),
        "trigger": CronTrigger(hour=9, minute=30, day_of_week="mon-fri"),
        "enabled": False,
    },
    {
        "id": "mark_pending",
        "name": "标记待验证",
        "fn": _log_task("mark_pending", "标记待验证", mark_pending),
        "trigger": CronTrigger(hour=14, minute=50, day_of_week="mon-fri"),
        "enabled": False,
    },
    {
        "id": "verify_close",
        "name": "收盘验证涨跌",
        "fn": _log_task("verify_close", "收盘验证涨跌", verify_close),
        "trigger": CronTrigger(hour=15, minute=10, day_of_week="mon-fri"),
        "enabled": False,
    },
    {
        "id": "reset_pending",
        "name": "清除残留标记",
        "fn": _log_task("reset_pending", "清除残留标记", reset_pending),
        "trigger": CronTrigger(hour=9, minute=20, day_of_week="mon-fri"),
        "enabled": False,
    },
    {
        "id": "check_alerts",
        "name": "盘中预警（实盘）",
        "fn": _log_task("check_alerts", "盘中预警（实盘）", check_alerts),
        "trigger": IntervalTrigger(minutes=5),
        "enabled": False,
    },
    {
        "id": "snapshot_portfolio",
        "name": "日终持仓快照（实盘）",
        "fn": _log_task("snapshot_portfolio", "日终持仓快照（实盘）", snapshot_portfolio),
        "trigger": CronTrigger(hour=15, minute=30, day_of_week="mon-fri"),
        "enabled": False,
    },
    {
        "id": "generate_signals",
        "name": "生成交易信号（实盘）",
        "fn": _log_task("generate_signals", "生成交易信号（实盘）", generate_signals),
        "trigger": CronTrigger(hour=15, minute=5, day_of_week="mon-fri"),
        "enabled": False,
    },
    # ── 仿真交易任务（已启用）──────────────────────────────
    {
        "id": "sim_scan_and_buy",
        "name": "开盘扫描+模拟买入",
        "fn": _log_task("sim_scan_and_buy", "开盘扫描+模拟买入", sim_scan_and_buy),
        "trigger": CronTrigger(hour=9, minute=30, day_of_week="mon-fri"),
        "enabled": True,
    },
    {
        "id": "sim_monitor_holdings",
        "name": "盘中实时监控",
        "fn": _log_task("sim_monitor_holdings", "盘中实时监控", sim_monitor_holdings),
        "trigger": IntervalTrigger(minutes=30),
        "enabled": True,
    },
    {
        "id": "sim_generate_sell_signals",
        "name": "卖出信号生成",
        "fn": _log_task("sim_generate_sell_signals", "卖出信号生成", sim_generate_sell_signals),
        "trigger": CronTrigger(hour=14, minute=55, day_of_week="mon-fri"),
        "enabled": True,
    },
    {
        "id": "sim_execute_signals",
        "name": "执行交易信号",
        "fn": _log_task("sim_execute_signals", "执行交易信号", sim_execute_signals),
        "trigger": CronTrigger(hour=15, minute=0, day_of_week="mon-fri"),
        "enabled": True,
    },
    {
        "id": "sim_daily_snapshot",
        "name": "账户快照",
        "fn": _log_task("sim_daily_snapshot", "账户快照", sim_daily_snapshot),
        "trigger": CronTrigger(hour=15, minute=10, day_of_week="mon-fri"),
        "enabled": True,
    },
    {
        "id": "sim_daily_report",
        "name": "每日交易报告",
        "fn": _log_task("sim_daily_report", "每日交易报告", sim_daily_report),
        "trigger": CronTrigger(hour=15, minute=30, day_of_week="mon-fri"),
        "enabled": True,
    },
    {
        "id": "event_collect_news",
        "name": "宏观事件新闻采集",
        "fn": _log_task("event_collect_news", "宏观事件新闻采集", event_collect_news),
        "trigger": IntervalTrigger(minutes=10),
        "enabled": True,
    },
    {
        "id": "event_extract_signals",
        "name": "宏观事件信号抽取",
        "fn": _log_task("event_extract_signals", "宏观事件信号抽取", event_extract_signals),
        "trigger": IntervalTrigger(minutes=10),
        "enabled": True,
    },
    {
        "id": "event_update_states",
        "name": "宏观事件状态更新",
        "fn": _log_task("event_update_states", "宏观事件状态更新", event_update_states),
        "trigger": IntervalTrigger(minutes=10),
        "enabled": True,
    },
    {
        "id": "event_scan_opportunities",
        "name": "宏观事件机会扫描",
        "fn": _log_task("event_scan_opportunities", "宏观事件机会扫描", event_scan_opportunities),
        "trigger": IntervalTrigger(minutes=10),
        "enabled": True,
    },
    {
        "id": "event_cycle",
        "name": "宏观事件闭环",
        "fn": _log_task("event_cycle", "宏观事件闭环", event_cycle),
        "trigger": IntervalTrigger(minutes=15),
        "enabled": True,
    },
    {
        "id": "event_notify",
        "name": "宏观事件推送检查",
        "fn": _log_task("event_notify", "宏观事件推送检查", event_notify),
        "trigger": IntervalTrigger(minutes=15),
        "enabled": True,
    },
]


def _run_script(script_name: str, args: list[str]) -> _SubprocessResult:
    """Run a script from the scripts/ directory."""
    import subprocess
    script_path = _PROJECT_ROOT / "scripts" / script_name
    cmd = [sys.executable, str(script_path)] + args
    _logger.info("Running: %s", " ".join(cmd))
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=120,
            **_hidden_subprocess_kwargs(),
        )
    except subprocess.TimeoutExpired as e:
        return _SubprocessResult(-1, "", f"超时 120s: {e}")
    except Exception as e:
        _logger.error("Script %s exception: %s", script_name, e)
        return _SubprocessResult(-1, "", str(e))
    if result.returncode != 0:
        _logger.error("Script %s failed (rc=%d): %s", script_name, result.returncode, result.stderr)
    else:
        _logger.info("Script %s OK: %s", script_name, result.stdout.strip()[:200])
    return _SubprocessResult(result.returncode, result.stdout, result.stderr)


def _run_module(module: str, args: list[str]) -> _SubprocessResult:
    """Run a Python module."""
    import subprocess
    cmd = [sys.executable, "-m", module] + args
    _logger.info("Running: %s", " ".join(cmd))
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=cfg().crawler.timeout * 6,
            **_hidden_subprocess_kwargs(),
        )
    except subprocess.TimeoutExpired as e:
        return _SubprocessResult(-1, "", f"超时 {cfg().crawler.timeout * 6}s: {e}")
    except Exception as e:
        _logger.error("Module %s exception: %s", module, e)
        return _SubprocessResult(-1, "", str(e))
    if result.returncode != 0:
        _logger.error("Module %s failed (rc=%d): %s", module, result.returncode, result.stderr)
    else:
        _logger.info("Module %s OK: %s", module, result.stdout.strip()[:200])
    return _SubprocessResult(result.returncode, result.stdout, result.stderr)


__scheduler_ref: BlockingScheduler | None = None


def build_scheduler(enabled_only: bool = True) -> BlockingScheduler:
    """Build and configure the APScheduler instance."""
    global _scheduler_ref
    from modules.db import DatabaseManager

    scheduler = BlockingScheduler(timezone="Asia/Shanghai")
    scheduler._logger = _logger
    _scheduler_ref = scheduler

    def _send_pause_alert(task_id: str, task_name: str, error: str) -> None:
        """连续失败达到阈值时发送飞书预警。"""
        try:
            from modules.alerter import AlertManager
            from modules.portfolio import Portfolio
            pf = Portfolio()
            am = AlertManager(pf.db)
            msg = (
                f"⚠️ MoatX 调度器告警\n\n"
                f"任务「{task_name}」（{task_id}）连续失败 3 次，已自动暂停。\n"
                f"最后错误：{error[:200]}"
            )
            am.send_text(msg)
            pf.close()
        except Exception as e:
            _logger.warning("发送暂停预警飞书失败: %s", e)

    if cfg().data.enable_warehouse:
        db = DatabaseManager(cfg().data.warehouse_path)
        tracker = db.failure_tracker()

        # 跳过已暂停的任务
        for task in TASKS:
            if enabled_only and not task.get("enabled", True):
                continue
            job_id = task["id"]
            if tracker.is_paused(job_id):
                _logger.warning("  [跳过] %s (%s) — 已连续失败暂停", job_id, task["name"])
                continue
            trigger = task["trigger"]
            scheduler.add_job(
                task["fn"],
                trigger=trigger,
                id=job_id,
                name=task["name"],
                replace_existing=True,
            )
            _logger.info("  [%s] %s → %s", job_id, task["name"], trigger)
        db.close()

        # 监听器
        def _on_job_event(event):
            job_id = event.job_id
            if job_id not in {t["id"] for t in TASKS}:
                return
            try:
                db2 = DatabaseManager(cfg().data.warehouse_path)
                t = db2.failure_tracker()
                if event.exception:
                    # 失败：记录并检查是否应暂停
                    t.record_failure(job_id, str(event.exception))
                    if t.should_pause(job_id):
                        t.mark_paused(job_id)
                        # 暂停 APScheduler job
                        if _scheduler_ref and _scheduler_ref.get_job(job_id):
                            _scheduler_ref.pause_job(job_id)
                            _logger.error("任务 %s (%s) 连续失败 3 次，已暂停调度", job_id, job_id)
                        # 发送飞书预警
                        task_name = next((x["name"] for x in TASKS if x["id"] == job_id), job_id)
                        _send_pause_alert(job_id, task_name, str(event.exception))
                else:
                    # 成功：重置失败计数
                    t.record_success(job_id)
                db2.close()
            except Exception as e:
                _logger.warning("任务失败追踪异常: %s", e)

        scheduler.add_listener(_on_job_event, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
    else:
        for task in TASKS:
            if enabled_only and not task.get("enabled", True):
                continue
            trigger = task["trigger"]
            job_id = task["id"]
            scheduler.add_job(
                task["fn"],
                trigger=trigger,
                id=job_id,
                name=task["name"],
                replace_existing=True,
            )
            _logger.info("  [%s] %s → %s", job_id, task["name"], trigger)

    return scheduler


def list_tasks() -> str:
    lines = [
        "MoatX 调度任务",
        "=" * 50,
    ]
    for t in TASKS:
        status = "✓" if t.get("enabled", True) else "✗"
        trigger = str(t["trigger"])
        lines.append(f"  {status} {t['id']:20s} {t['name']:<16s} {trigger}")
    lines.append("=" * 50)
    lines.append(f"共 {len(TASKS)} 个任务")
    return "\n".join(lines)


def _pid_is_running(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        result = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}", "/NH"],
            capture_output=True,
            text=True,
            timeout=10,
            **_hidden_subprocess_kwargs(),
        )
        return str(pid) in result.stdout
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def scheduler_status() -> str:
    if not _PID_FILE.exists():
        return "scheduler: stopped (no pid file)"
    try:
        pid = int(_PID_FILE.read_text(encoding="utf-8").strip())
    except ValueError:
        return "scheduler: stopped (invalid pid file)"
    state = "running" if _pid_is_running(pid) else "stopped"
    return f"scheduler: {state} pid={pid}"


def start_daemon() -> None:
    """Start scheduler in the background and persist its pid."""
    if _PID_FILE.exists():
        try:
            pid = int(_PID_FILE.read_text(encoding="utf-8").strip())
            if _pid_is_running(pid):
                print(f"MoatX scheduler already running, pid={pid}")
                return
        except ValueError:
            pass

    _DAEMON_LOG.parent.mkdir(parents=True, exist_ok=True)
    log = open(_DAEMON_LOG, "a", encoding="utf-8")
    proc = subprocess.Popen(
        [sys.executable, "-m", "modules.scheduler", "--start"],
        cwd=str(_PROJECT_ROOT),
        stdout=log,
        stderr=subprocess.STDOUT,
        stdin=subprocess.DEVNULL,
        **_hidden_subprocess_kwargs(new_group=True),
    )
    _PID_FILE.write_text(str(proc.pid), encoding="utf-8")
    print(f"MoatX scheduler started, pid={proc.pid}, log={_DAEMON_LOG}")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="MoatX 调度器")
    parser.add_argument("--list", action="store_true", help="列出所有任务")
    parser.add_argument("--start", action="store_true", help="启动调度器")
    parser.add_argument("--daemon", action="store_true", help="后台运行")
    parser.add_argument("--status", action="store_true", help="查看后台调度器状态")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )

    if args.list:
        print(list_tasks())
        return

    if args.status:
        print(scheduler_status())
        return

    if args.daemon:
        start_daemon()
        return

    if args.start:
        scheduler = build_scheduler()
        _logger.info("MoatX 调度器启动于 %s", datetime.now())
        _logger.info("\n" + list_tasks())
        _logger.info("")

        def _handle_sig(signum, frame):
            _logger.info("\n收到退出信号，调度器关闭中...")
            scheduler.shutdown(wait=True)
            sys.exit(0)

        signal.signal(signal.SIGINT, _handle_sig)
        signal.signal(signal.SIGTERM, _handle_sig)

        try:
            scheduler.start()
        except KeyboardInterrupt:
            _logger.info("调度器已停止")


if __name__ == "__main__":
    main()
