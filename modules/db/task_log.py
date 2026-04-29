"""Scheduler task execution logging."""

from __future__ import annotations

import sqlite3
from datetime import datetime

import pandas as pd


class TaskLogStore:
    """Record and query scheduler task executions."""

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    def start_run(self, task_id: str, task_name: str) -> int:
        cursor = self._conn.cursor()
        cursor.execute(
            "INSERT INTO task_execution_log (task_id, task_name) VALUES (?, ?)",
            (task_id, task_name),
        )
        self._conn.commit()
        return cursor.lastrowid

    def finish_run(
        self,
        log_id: int,
        success: bool,
        output: str = "",
        error: str = "",
        duration_ms: int = 0,
    ) -> None:
        cursor = self._conn.cursor()
        cursor.execute(
            """UPDATE task_execution_log SET
               finished_at = datetime('now', 'localtime'),
               success = ?, output = ?, error = ?, duration_ms = ?
               WHERE id = ?""",
            (1 if success else 0, output, error, duration_ms, log_id),
        )
        self._conn.commit()

    def recent_runs(self, limit: int = 50) -> pd.DataFrame:
        df = pd.read_sql_query(
            "SELECT * FROM task_execution_log ORDER BY id DESC LIMIT ?",
            self._conn,
            params=(limit,),
        )
        return df

    def task_summary(self, task_id: str | None = None) -> dict:
        """Return summary stats per task."""
        if task_id:
            cursor = self._conn.cursor()
            cursor.execute(
                """SELECT COUNT(*), SUM(CASE WHEN success=1 THEN 1 ELSE 0 END),
                          AVG(CASE WHEN duration_ms > 0 THEN duration_ms ELSE NULL END)
                   FROM task_execution_log WHERE task_id=?""",
                (task_id,),
            )
            row = cursor.fetchone()
            total = row[0] or 0
            success = row[1] or 0
            avg_ms = row[2]
            return {
                "task_id": task_id,
                "total_runs": total,
                "success_count": success,
                "fail_count": total - success,
                "avg_duration_ms": round(avg_ms) if avg_ms else 0,
            }
        else:
            df = pd.read_sql_query(
                """SELECT task_id, COUNT(*) as total,
                          SUM(CASE WHEN success=1 THEN 1 ELSE 0 END) as success_count,
                          AVG(CASE WHEN duration_ms > 0 THEN duration_ms ELSE NULL END) as avg_duration_ms
                   FROM task_execution_log GROUP BY task_id""",
                self._conn,
            )
            return df.to_dict(orient="records")


class TaskFailureTracker:
    """
    追踪任务连续失败次数，达到阈值（默认 3 次）时自动标记暂停。
    """

    def __init__(self, conn: sqlite3.Connection, threshold: int = 3):
        self._conn = conn
        self._threshold = threshold

    def record_failure(self, task_id: str, error: str = "") -> int:
        """
        记录一次失败。
        Returns: 当前连续失败次数（达到 threshold 时返回 threshold 表示应暂停）。
        """
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cur = self._conn.execute(
            "SELECT consecutive_failures FROM task_failure_snapshot WHERE task_id=?",
            (task_id,),
        )
        row = cur.fetchone()
        if row:
            failures = row[0] + 1
            self._conn.execute(
                """UPDATE task_failure_snapshot
                   SET consecutive_failures=?, last_failure_at=?, last_error=?
                   WHERE task_id=?""",
                (failures, now, error[:500], task_id),
            )
        else:
            failures = 1
            self._conn.execute(
                """INSERT INTO task_failure_snapshot
                   (task_id, consecutive_failures, last_failure_at, last_error)
                   VALUES (?, ?, ?, ?)""",
                (task_id, failures, now, error[:500]),
            )
        self._conn.commit()
        return failures

    def record_success(self, task_id: str) -> None:
        """记录一次成功，重置该任务的连续失败计数。"""
        cur = self._conn.execute(
            "SELECT consecutive_failures FROM task_failure_snapshot WHERE task_id=?",
            (task_id,),
        )
        row = cur.fetchone()
        if row and row[0] > 0:
            self._conn.execute(
                "UPDATE task_failure_snapshot SET consecutive_failures=0 WHERE task_id=?",
                (task_id,),
            )
            self._conn.commit()

    def should_pause(self, task_id: str) -> bool:
        """检查是否应暂停（连续失败达到阈值）。"""
        cur = self._conn.execute(
            "SELECT consecutive_failures, paused FROM task_failure_snapshot WHERE task_id=?",
            (task_id,),
        )
        row = cur.fetchone()
        if not row:
            return False
        return row[0] >= self._threshold and not row[1]

    def mark_paused(self, task_id: str) -> None:
        """标记任务已暂停。"""
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self._conn.execute(
            """INSERT OR REPLACE INTO task_failure_snapshot
               (task_id, consecutive_failures, last_failure_at, paused, paused_at)
               VALUES (?, 0, '', 1, ?)""",
            (task_id, now),
        )
        self._conn.commit()

    def is_paused(self, task_id: str) -> bool:
        """检查任务是否已被暂停。"""
        cur = self._conn.execute(
            "SELECT paused FROM task_failure_snapshot WHERE task_id=?",
            (task_id,),
        )
        row = cur.fetchone()
        return bool(row and row[0])

    def get_all_paused(self) -> list[tuple]:
        """返回所有已暂停的任务。"""
        cur = self._conn.execute(
            "SELECT task_id, last_failure_at, last_error FROM task_failure_snapshot WHERE paused=1"
        )
        return cur.fetchall()

