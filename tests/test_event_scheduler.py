from modules.scheduler import TASKS
from modules.scheduler import _pid_status_from_command_line


def test_event_scheduler_tasks_exist_and_enabled():
    expected = {
        "event_collect_news",
        "event_extract_signals",
        "event_update_states",
        "event_scan_opportunities",
        "event_news_factors",
        "event_topic_memory",
        "event_cycle",
        "event_notify",
    }

    tasks = {task["id"]: task for task in TASKS}

    assert expected <= set(tasks)
    assert all(tasks[task_id]["enabled"] is True for task_id in expected)


def test_event_notify_scheduler_sends_explicitly():
    task = next(task for task in TASKS if task["id"] == "event_notify")

    assert task["enabled"] is True


def test_event_llm_review_scheduler_is_dry_run_and_disabled_by_default():
    task = next(task for task in TASKS if task["id"] == "event_llm_review_dry_run")

    assert task["enabled"] is False


def test_quick_decision_evaluation_scheduler_is_enabled():
    task = next(task for task in TASKS if task["id"] == "quick_decision_evaluate")

    assert task["enabled"] is True


def test_scheduler_pid_status_reports_foreign_process():
    running, reason = _pid_status_from_command_line("powershell.exe -NoProfile", profile="intraday")

    assert running is False
    assert reason == "pid belongs to another process"


def test_scheduler_pid_status_reports_profile_mismatch():
    running, reason = _pid_status_from_command_line(
        "python -m modules.scheduler --start --profile default",
        profile="intraday",
    )

    assert running is False
    assert reason == "pid is scheduler for another profile"
