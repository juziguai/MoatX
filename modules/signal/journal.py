"""信号日志 — 基于 warehouse 的信号查询和记录。"""

from __future__ import annotations

import pandas as pd

from modules.db import DatabaseManager
from modules.config import cfg
from .engine import Signal


class SignalJournal:
    """记录和查询交易信号历史。"""

    def __init__(self, db: DatabaseManager | None = None):
        self._db = db or DatabaseManager(cfg().data.warehouse_path)

    def record(self, signal: Signal) -> int:
        """记录一条信号到 warehouse。"""
        return self._db.signal().record_signal(
            symbol=signal.symbol,
            strategy_name=signal.strategy_name,
            signal_type=signal.signal_type,
            price=signal.price,
            reason=signal.reason,
            confidence=signal.confidence,
            indicators=signal.indicators,
        )

    def recent(self, limit: int = 50, signal_type: str | None = None,
               symbol: str | None = None) -> pd.DataFrame:
        """查询最近信号。"""
        return self._db.signal().list_signals(limit, signal_type, symbol)

    def mark_executed(self, signal_id: int) -> None:
        """标记信号已执行。"""
        self._db.signal().mark_executed(signal_id)
