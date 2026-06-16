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
_INTRADAY_PID_FILE = _PROJECT_ROOT / "data" / "intraday_monitor.pid"
_INTRADAY_DAEMON_LOG = _PROJECT_ROOT / "data" / "intraday_monitor.log"
_SCHEDULER_PROFILES: dict[str, tuple[str, ...] | None] = {
    "default": None,
    "intraday": ("swing_monitor_watchlist", "swing_tail_scan", "sim_monitor_holdings"),
}


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
        log_id: int | None = None
        db = None
        start = time.time()
        if cfg().data.enable_warehouse:
            try:
                from modules.db import DatabaseManager
                db = DatabaseManager(cfg().data.warehouse_path)
                log_id = db.task().start_run(task_id, task_name)
            except Exception as exc:
                _logger.warning("仓库开始记录失败，任务继续执行一次: %s", exc)
                db = None

        try:
            result = fn(*args, **kwargs)
        except Exception as exc:
            if db is not None and log_id is not None:
                try:
                    db.task().finish_run(
                        log_id,
                        False,
                        error=str(exc),
                        duration_ms=int((time.time() - start) * 1000),
                    )
                except Exception as log_exc:
                    _logger.warning("仓库结束记录失败: %s", log_exc)
            raise

        if db is not None and log_id is not None:
            try:
                ok = getattr(result, "ok", True)
                out = getattr(result, "stdout", "") or ""
                err = getattr(result, "stderr", "") or ""
                db.task().finish_run(
                    log_id,
                    ok,
                    output=out[:500] if out else "",
                    error=err[:500] if err else "",
                    duration_ms=int((time.time() - start) * 1000),
                )
            except Exception as exc:
                _logger.warning("仓库结束记录失败: %s", exc)
        return result
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


def event_news_factors(*args, **kwargs) -> _SubprocessResult:
    """Build and persist news intelligence sector factors."""
    return _run_module("modules.cli", ["tool", "event", "news-factors", "--json"])


def event_topic_memory(*args, **kwargs) -> _SubprocessResult:
    """Update topic memory and materialized topic events."""
    return _run_module("modules.cli", ["tool", "event", "topics", "--json"])


def event_llm_review_dry_run(*args, **kwargs) -> _SubprocessResult:
    """Preview LLM semantic-review candidates without external model calls."""
    return _run_module("modules.cli", ["tool", "event", "llm-review", "--json"])


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

def swing_daily_watchlist(*args, **kwargs) -> _SubprocessResult:
    """Generate and push tomorrow's swing watchlist after close."""
    return _run_module(
        "modules.cli",
        [
            "tool",
            "swing",
            "watchlist",
            "--limit",
            "10",
            "--pool-limit",
            "120",
            "--deadline-seconds",
            "180",
            "--min-score",
            "55",
            "--send",
            "--json",
        ],
    )


def swing_tail_scan(*args, **kwargs) -> _SubprocessResult:
    """Scan close-buy swing candidates during the 14:00 tail window."""
    return _run_module(
        "modules.cli",
        [
            "tool",
            "swing",
            "tail-scan",
            "--limit",
            "10",
            "--pool-limit",
            "120",
            "--deadline-seconds",
            "120",
            "--min-score",
            "55",
            "--send",
            "--json",
        ],
    )


def swing_monitor_watchlist(*args, **kwargs) -> _SubprocessResult:
    """Monitor active swing watchlist for target/stop alerts."""
    return _run_module(
        "modules.cli",
        [
            "tool",
            "swing",
            "monitor",
            "--send",
            "--json",
        ],
    )




def source_health_check(*args, **kwargs) -> _SubprocessResult:
    try:
        from modules.source_health import run_health_check
        results = run_health_check()
        ok = all(r.healthy for r in results)
        msg = "; ".join(f"{r.source}: {'OK' if r.healthy else 'FAIL'}({r.latency_ms:.0f}ms)" for r in results)
        return _SubprocessResult(0 if ok else 1, msg, "")
    except Exception as e:
        return _SubprocessResult(1, "", str(e))


def quick_decision_evaluate(*args, **kwargs) -> _SubprocessResult:
    """Persist T+1/T+3/T+5 post-evaluation for quick intraday decisions."""
    return _run_module(
        "modules.cli",
        [
            "tool",
            "quick-decision",
            "evaluate",
            "--horizons",
            "1,3,5",
            "--limit",
            "300",
            "--save-evaluation",
            "--json",
        ],
    )


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
    {
        "id": "source_health_check",
        "name": "数据源健康检查",
        "fn": _log_task("source_health_check", "数据源健康检查", source_health_check),
        "trigger": CronTrigger(hour=8, minute=30, day_of_week="mon-fri"),
        "enabled": True,
    },
    {
        "id": "quick_decision_evaluate",
        "name": "极速决策后验评价",
        "fn": _log_task("quick_decision_evaluate", "极速决策后验评价", quick_decision_evaluate),
        "trigger": CronTrigger(hour=15, minute=45, day_of_week="mon-fri"),
        "enabled": True,
    },
    {
        "id": "swing_daily_watchlist",
        "name": "短线盘后观察名单",
        "fn": _log_task("swing_daily_watchlist", "短线盘后观察名单", swing_daily_watchlist),
        "trigger": CronTrigger(hour=15, minute=25, day_of_week="mon-fri"),
        "enabled": True,
    },
    {
        "id": "swing_tail_scan",
        "name": "短线尾盘收盘买入扫描",
        "fn": _log_task("swing_tail_scan", "短线尾盘收盘买入扫描", swing_tail_scan),
        "trigger": CronTrigger(hour=14, minute="0,15,30,45,55", day_of_week="mon-fri"),
        "enabled": True,
    },
    {
        "id": "swing_monitor_watchlist",
        "name": "短线盘中目标止损监控",
        "fn": _log_task("swing_monitor_watchlist", "短线盘中目标止损监控", swing_monitor_watchlist),
        "trigger": IntervalTrigger(minutes=5),
        "enabled": True,
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
        "id": "event_news_factors",
        "name": "新闻情报因子物化",
        "fn": _log_task("event_news_factors", "新闻情报因子物化", event_news_factors),
        "trigger": IntervalTrigger(minutes=10),
        "enabled": True,
    },
    {
        "id": "event_topic_memory",
        "name": "新闻主题记忆更新",
        "fn": _log_task("event_topic_memory", "新闻主题记忆更新", event_topic_memory),
        "trigger": IntervalTrigger(minutes=15),
        "enabled": True,
    },
    {
        "id": "event_llm_review_dry_run",
        "name": "LLM语义评审预览",
        "fn": _log_task("event_llm_review_dry_run", "LLM语义评审预览", event_llm_review_dry_run),
        "trigger": IntervalTrigger(minutes=30),
        "enabled": False,
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


def build_scheduler(
    enabled_only: bool = True,
    task_ids: set[str] | None = None,
    run_immediate_task_ids: set[str] | None = None,
) -> BlockingScheduler:
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
        for task in _selected_tasks(task_ids=task_ids):
            if enabled_only and not task.get("enabled", True):
                continue
            job_id = task["id"]
            if tracker.is_paused(job_id):
                _logger.warning("  [跳过] %s (%s) — 已连续失败暂停", job_id, task["name"])
                continue
            trigger = task["trigger"]
            extra: dict[str, Any] = {}
            if run_immediate_task_ids and job_id in run_immediate_task_ids:
                extra["next_run_time"] = datetime.now()
            scheduler.add_job(
                task["fn"],
                trigger=trigger,
                id=job_id,
                name=task["name"],
                replace_existing=True,
                **extra,
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
            except Exception as e:
                _logger.warning("任务失败追踪异常: %s", e)

        scheduler.add_listener(_on_job_event, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
    else:
        for task in _selected_tasks(task_ids=task_ids):
            if enabled_only and not task.get("enabled", True):
                continue
            trigger = task["trigger"]
            job_id = task["id"]
            extra: dict[str, Any] = {}
            if run_immediate_task_ids and job_id in run_immediate_task_ids:
                extra["next_run_time"] = datetime.now()
            scheduler.add_job(
                task["fn"],
                trigger=trigger,
                id=job_id,
                name=task["name"],
                replace_existing=True,
                **extra,
            )
            _logger.info("  [%s] %s → %s", job_id, task["name"], trigger)

    return scheduler


def _selected_tasks(*, task_ids: set[str] | None = None) -> list[TaskDict]:
    if not task_ids:
        return list(TASKS)
    return [task for task in TASKS if task["id"] in task_ids]


def _profile_task_ids(profile: str) -> set[str] | None:
    value = _SCHEDULER_PROFILES.get(profile)
    return set(value) if value else None


def _profile_immediate_task_ids(profile: str) -> set[str]:
    if profile == "intraday":
        return {"swing_monitor_watchlist", "sim_monitor_holdings"}
    return set()


def _normalize_profile(profile: str | None) -> str:
    value = str(profile or "default").strip().lower()
    if value in {"monitor", "盘中", "intraday"}:
        return "intraday"
    return value if value in _SCHEDULER_PROFILES else "default"


def list_tasks(profile: str = "default") -> str:
    profile = _normalize_profile(profile)
    task_ids = _profile_task_ids(profile)
    lines = [
        f"MoatX 调度任务 [{profile}]",
        "=" * 50,
    ]
    for t in _selected_tasks(task_ids=task_ids):
        status = "✓" if t.get("enabled", True) else "✗"
        trigger = str(t["trigger"])
        lines.append(f"  {status} {t['id']:20s} {t['name']:<16s} {trigger}")
    lines.append("=" * 50)
    lines.append(f"共 {len(_selected_tasks(task_ids=task_ids))} 个任务")
    return "\n".join(lines)


def _process_command_line(pid: int) -> str:
    if pid <= 0:
        return ""
    if os.name == "nt":
        try:
            result = subprocess.run(
                [
                    "powershell",
                    "-NoProfile",
                    "-Command",
                    f"(Get-CimInstance Win32_Process -Filter \"ProcessId={pid}\").CommandLine",
                ],
                capture_output=True,
                text=True,
                timeout=10,
                **_hidden_subprocess_kwargs(),
            )
            return (result.stdout or "").strip()
        except Exception:
            return ""
    return ""


def _pid_status_from_command_line(command_line: str, *, profile: str = "default") -> tuple[bool, str]:
    command_line = str(command_line or "").strip()
    if not command_line:
        return False, "process missing"
    if "modules.scheduler" not in command_line:
        return False, "pid belongs to another process"
    if profile == "intraday" and "--profile intraday" not in command_line:
        return False, "pid is scheduler for another profile"
    return True, ""


def _pid_state(pid: int, *, profile: str = "default") -> tuple[str, str]:
    if pid <= 0:
        return "stopped", "invalid pid"
    if os.name == "nt":
        running, reason = _pid_status_from_command_line(_process_command_line(pid), profile=profile)
        return ("running", "") if running else ("stopped", reason)
    try:
        os.kill(pid, 0)
    except OSError:
        return "stopped", "process missing"
    return "running", ""


def _pid_is_running(pid: int, *, profile: str = "default") -> bool:
    return _pid_state(pid, profile=profile)[0] == "running"


def _pid_file_for(profile: str) -> Path:
    return _INTRADAY_PID_FILE if profile == "intraday" else _PID_FILE


def _daemon_log_for(profile: str) -> Path:
    return _INTRADAY_DAEMON_LOG if profile == "intraday" else _DAEMON_LOG


def scheduler_status(profile: str = "default") -> str:
    profile = _normalize_profile(profile)
    pid_file = _pid_file_for(profile)
    lines = [f"scheduler[{profile}] status"]
    if not pid_file.exists():
        lines.append("state: stopped (no pid file)")
        lines.extend(_runtime_status_lines(profile))
        return "\n".join(lines)
    try:
        pid = int(pid_file.read_text(encoding="utf-8").strip())
    except ValueError:
        lines.append(f"state: stopped (invalid pid file: {pid_file})")
        lines.extend(_runtime_status_lines(profile))
        return "\n".join(lines)
    state, reason = _pid_state(pid, profile=profile)
    suffix = f" ({reason})" if reason else ""
    lines.append(f"state: {state} pid={pid}{suffix}")
    lines.append(f"pid_file: {pid_file}")
    lines.append(f"log: {_daemon_log_for(profile)}")
    lines.extend(_runtime_status_lines(profile))
    return "\n".join(lines)


def _runtime_status_lines(profile: str) -> list[str]:
    lines: list[str] = []
    task_ids = _profile_task_ids(profile) or {task["id"] for task in TASKS}
    if profile == "intraday":
        lines.extend(_active_watchlist_lines())
    try:
        from modules.db import DatabaseManager

        db = DatabaseManager(cfg().data.warehouse_path)
        recent = db.task().recent_runs(limit=80)
        db.close()
        if recent.empty:
            lines.append("last_runs: none")
            return lines
        lines.append("last_runs:")
        for task_id in sorted(task_ids):
            rows = recent[recent["task_id"] == task_id]
            if rows.empty:
                lines.append(f"- {task_id}: no recent run")
                continue
            row = rows.iloc[0]
            success = "ok" if int(row.get("success") or 0) == 1 else "fail"
            finished = _status_text(row.get("finished_at"))
            started = _status_text(row.get("started_at"))
            duration = int(row.get("duration_ms") or 0)
            lines.append(f"- {task_id}: {success} start={started} finish={finished or 'running/unfinished'} {duration}ms")
    except Exception as exc:
        lines.append(f"last_runs: unavailable ({exc})")
    return lines


def _status_text(value: Any) -> str:
    text = str(value or "").strip()
    return "" if text.lower() in {"nan", "nat", "none"} else text


def _active_watchlist_lines() -> list[str]:
    import json

    path = _PROJECT_ROOT / "data" / "swing_watchlist_latest.json"
    if not path.exists():
        return [f"watchlist: missing {path}"]
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return [f"watchlist: unreadable {path} ({exc})"]
    positions = payload.get("positions") or []
    generated_at = str(payload.get("generated_at") or "")
    status = str(payload.get("status") or "")
    lines = [f"watchlist: {status or 'unknown'} positions={len(positions)} generated_at={generated_at} path={path}"]
    if not positions:
        lines.append("watchlist_warning: empty positions, monitor has no target/stop symbols to alert")
    if generated_at[:10] and generated_at[:10] != datetime.now().strftime("%Y-%m-%d"):
        lines.append("watchlist_warning: stale watchlist date, run tail-scan/watchlist to refresh active symbols")
    return lines


def start_daemon(profile: str = "default", *, ensure: bool = False, immediate: bool | None = None) -> None:
    """Start scheduler in the background and persist its pid."""
    profile = _normalize_profile(profile)
    pid_file = _pid_file_for(profile)
    daemon_log = _daemon_log_for(profile)
    if pid_file.exists():
        try:
            pid = int(pid_file.read_text(encoding="utf-8").strip())
            if _pid_is_running(pid, profile=profile):
                action = "already running" if not ensure else "ok"
                print(f"MoatX scheduler[{profile}] {action}, pid={pid}")
                return
        except ValueError:
            pass

    daemon_log.parent.mkdir(parents=True, exist_ok=True)
    log = open(daemon_log, "a", encoding="utf-8")
    cmd = [sys.executable, "-m", "modules.scheduler", "--start", "--profile", profile]
    if immediate if immediate is not None else profile == "intraday":
        cmd.append("--immediate")
    proc = subprocess.Popen(
        cmd,
        cwd=str(_PROJECT_ROOT),
        stdout=log,
        stderr=subprocess.STDOUT,
        stdin=subprocess.DEVNULL,
        **_hidden_subprocess_kwargs(new_group=True),
    )
    pid_file.write_text(str(proc.pid), encoding="utf-8")
    print(f"MoatX scheduler[{profile}] started, pid={proc.pid}, log={daemon_log}")


def stop_daemon(profile: str = "default") -> None:
    """Stop a background scheduler profile if it is running."""
    profile = _normalize_profile(profile)
    pid_file = _pid_file_for(profile)
    if not pid_file.exists():
        print(f"MoatX scheduler[{profile}] stopped (no pid file)")
        return
    try:
        pid = int(pid_file.read_text(encoding="utf-8").strip())
    except ValueError:
        pid_file.unlink(missing_ok=True)
        print(f"MoatX scheduler[{profile}] stopped (removed invalid pid file)")
        return
    if _pid_is_running(pid, profile=profile):
        try:
            os.kill(pid, signal.SIGTERM)
        except Exception as exc:
            print(f"MoatX scheduler[{profile}] stop failed, pid={pid}: {exc}")
            return
        for _ in range(20):
            time.sleep(0.25)
            if not _pid_is_running(pid, profile=profile):
                break
    pid_file.unlink(missing_ok=True)
    print(f"MoatX scheduler[{profile}] stopped, pid={pid}")


def restart_daemon(profile: str = "default", *, immediate: bool | None = None) -> None:
    """Restart a background scheduler profile."""
    stop_daemon(profile=profile)
    start_daemon(profile=profile, immediate=immediate)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="MoatX 调度器")
    parser.add_argument("--list", action="store_true", help="列出所有任务")
    parser.add_argument("--start", action="store_true", help="启动调度器")
    parser.add_argument("--daemon", action="store_true", help="后台运行")
    parser.add_argument("--ensure", action="store_true", help="未运行则后台拉起，已运行则直接返回")
    parser.add_argument("--stop", action="store_true", help="停止后台调度器")
    parser.add_argument("--restart", action="store_true", help="重启后台调度器")
    parser.add_argument("--status", action="store_true", help="查看后台调度器状态")
    parser.add_argument("--profile", choices=["default", "intraday"], default="default", help="调度任务集")
    parser.add_argument("--immediate", action="store_true", help="启动后立即运行该 profile 的关键 interval 任务")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )

    if args.list:
        print(list_tasks(profile=args.profile))
        return

    if args.status:
        print(scheduler_status(profile=args.profile))
        return

    if args.ensure:
        start_daemon(profile=args.profile, ensure=True, immediate=args.immediate or None)
        print(scheduler_status(profile=args.profile))
        return

    if args.stop:
        stop_daemon(profile=args.profile)
        return

    if args.restart:
        restart_daemon(profile=args.profile, immediate=args.immediate or None)
        return

    if args.daemon:
        start_daemon(profile=args.profile, immediate=args.immediate or None)
        return

    if args.start:
        profile = _normalize_profile(args.profile)
        task_ids = _profile_task_ids(profile)
        immediate_task_ids = _profile_immediate_task_ids(profile) if args.immediate else set()
        scheduler = build_scheduler(task_ids=task_ids, run_immediate_task_ids=immediate_task_ids)
        _logger.info("MoatX 调度器启动于 %s", datetime.now())
        _logger.info("\n" + list_tasks(profile=args.profile))
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
