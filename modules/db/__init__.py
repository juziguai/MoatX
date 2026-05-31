"""数据仓库 — SQLite 持久化层。

管理 OHLCV 日线、回测记录、任务日志、信号日志。
与 data/portfolio.db 独立，各自管理不同数据域。
"""

from __future__ import annotations

import os
import sqlite3
from threading import Lock

from .backtest_store import BacktestStore
from .event_store import EventStore
from .migrations import run_migrations
from .price_store import PriceStore
from .signal_store import SignalStore
from .task_log import TaskFailureTracker, TaskLogStore

_DEFAULT_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "data",
    "warehouse.db",
)



class SourceHealthStore:
    def __init__(self, conn):
        self._conn = conn

    def log(self, source: str, healthy: bool, latency_ms: float = 0.0,
            error: str = "", sample_count: int = 0) -> None:
        self._conn.execute(
            """INSERT INTO source_health_log
               (source, healthy, latency_ms, error, sample_count)
               VALUES (?, ?, ?, ?, ?)""",
            (source, int(healthy), latency_ms, error, sample_count),
        )
        self._conn.commit()

    def latest(self, source: str | None = None) -> list[dict]:
        if source:
            cur = self._conn.execute(
                """SELECT * FROM source_health_log
                   WHERE source = ? ORDER BY checked_at DESC LIMIT 1""",
                (source,),
            )
        else:
            cur = self._conn.execute(
                """SELECT * FROM source_health_log
                   WHERE checked_at = (SELECT MAX(checked_at) FROM source_health_log)""",
            )
        columns = [desc[0] for desc in cur.description] if cur.description else []
        return [dict(zip(columns, row)) for row in cur.fetchall()]

    def recent_failures(self, source: str, limit: int = 10) -> list[dict]:
        cur = self._conn.execute(
            """SELECT * FROM source_health_log
               WHERE source = ? AND healthy = 0
               ORDER BY checked_at DESC LIMIT ?""",
            (source, limit),
        )
        columns = [desc[0] for desc in cur.description] if cur.description else []
        return [dict(zip(columns, row)) for row in cur.fetchall()]

    def consecutive_failures(self, source: str) -> int:
        rows = self._conn.execute(
            """SELECT healthy FROM source_health_log
               WHERE source = ? ORDER BY checked_at DESC LIMIT 20""",
            (source,),
        ).fetchall()
        count = 0
        for (healthy,) in rows:
            if healthy:
                break
            count += 1
        return count



class DatabaseManager:
    """外观类，管理 warehouse 连接和子存储。"""

    _instances: dict[str, "DatabaseManager"] = {}
    _lock = Lock()

    def __new__(cls, db_path: str | None = None):
        path = db_path or _DEFAULT_PATH
        with cls._lock:
            if path not in cls._instances:
                instance = super().__new__(cls)
                instance._initialized = False
                cls._instances[path] = instance
            return cls._instances[path]

    def __init__(self, db_path: str | None = None):
        if getattr(self, "_initialized", False):
            return
        self._path = db_path or _DEFAULT_PATH
        os.makedirs(os.path.dirname(self._path), exist_ok=True)
        self._conn = sqlite3.connect(self._path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        run_migrations(self._conn)

        self._price: PriceStore | None = None
        self._backtest: BacktestStore | None = None
        self._task: TaskLogStore | None = None
        self._signal: SignalStore | None = None
        self._event: EventStore | None = None
        self._failure: TaskFailureTracker | None = None
        self._source_health: SourceHealthStore | None = None
        self._initialized = True

    def price(self) -> PriceStore:
        if self._price is None:
            self._price = PriceStore(self._conn)
        return self._price

    def backtest(self) -> BacktestStore:
        if self._backtest is None:
            self._backtest = BacktestStore(self._conn)
        return self._backtest

    def task(self) -> TaskLogStore:
        if self._task is None:
            self._task = TaskLogStore(self._conn)
        return self._task

    def signal(self) -> SignalStore:
        if self._signal is None:
            self._signal = SignalStore(self._conn)
        return self._signal

    def event(self) -> EventStore:
        if self._event is None:
            self._event = EventStore(self._conn)
        return self._event

    def failure_tracker(self) -> TaskFailureTracker:
        if self._failure is None:
            self._failure = TaskFailureTracker(self._conn)
        return self._failure

    def source_health(self) -> SourceHealthStore:
        if self._source_health is None:
            self._source_health = SourceHealthStore(self._conn)
        return self._source_health

    @property
    def conn(self) -> sqlite3.Connection:
        return self._conn

    def close(self) -> None:
        self._conn.close()
        with self._lock:
            self._instances.pop(self._path, None)

    @classmethod
    def close_all(cls) -> None:
        with cls._lock:
            for inst in cls._instances.values():
                inst._conn.close()
            cls._instances.clear()
