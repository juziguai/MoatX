from modules.scheduler import TASKS


def test_event_scheduler_tasks_exist_and_enabled():
    expected = {
        "event_collect_news",
        "event_extract_signals",
        "event_update_states",
        "event_scan_opportunities",
        "event_cycle",
        "event_notify",
    }

    tasks = {task["id"]: task for task in TASKS}

    assert expected <= set(tasks)
    assert all(tasks[task_id]["enabled"] is True for task_id in expected)


def test_event_notify_scheduler_sends_explicitly():
    task = next(task for task in TASKS if task["id"] == "event_notify")

    assert task["enabled"] is True
