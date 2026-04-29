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
