"""Signal journal storage."""

from __future__ import annotations

import json
import sqlite3

import pandas as pd


class SignalStore:
    """Persist trading signals."""

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    def record_signal(
        self,
        symbol: str,
        strategy_name: str,
        signal_type: str,
        price: float,
        reason: str,
        confidence: float = 0.0,
        indicators: dict | None = None,
    ) -> int:
        cursor = self._conn.cursor()
        cursor.execute(
            """INSERT INTO signal_journal
               (symbol, strategy_name, signal_type, price, confidence, reason, indicators)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                symbol,
                strategy_name,
                signal_type,
                price,
                confidence,
                reason,
                json.dumps(indicators or {}, ensure_ascii=False),
            ),
        )
        self._conn.commit()
        return cursor.lastrowid

    def list_signals(
        self,
        limit: int = 50,
        signal_type: str | None = None,
        symbol: str | None = None,
    ) -> pd.DataFrame:
        conditions: list[str] = []
        params: list = []
        if signal_type:
            conditions.append("signal_type = ?")
            params.append(signal_type)
        if symbol:
            conditions.append("symbol = ?")
            params.append(symbol)

        where = (" WHERE " + " AND ".join(conditions)) if conditions else ""
        query = f"SELECT * FROM signal_journal{where} ORDER BY id DESC LIMIT ?"
        params.append(limit)
        df = pd.read_sql_query(query, self._conn, params=params)
        return df

    def mark_executed(self, signal_id: int) -> None:
        cursor = self._conn.cursor()
        cursor.execute("UPDATE signal_journal SET executed=1 WHERE id=?", (signal_id,))
        self._conn.commit()

    def paper_holding(self, symbol: str) -> dict | None:
        cursor = self._conn.cursor()
        cursor.execute("SELECT * FROM paper_holdings WHERE symbol=?", (symbol,))
        row = cursor.fetchone()
        if row is None:
            return None
        return dict(zip([d[0] for d in cursor.description], row))

    def all_paper_holdings(self) -> pd.DataFrame:
        return pd.read_sql_query("SELECT * FROM paper_holdings", self._conn)

    def upsert_paper_holding(
        self, symbol: str, name: str, shares: int, avg_cost: float
    ) -> None:
        cursor = self._conn.cursor()
        cursor.execute(
            """INSERT INTO paper_holdings (symbol, name, shares, avg_cost)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(symbol) DO UPDATE SET
               name=excluded.name, shares=excluded.shares, avg_cost=excluded.avg_cost""",
            (symbol, name, shares, avg_cost),
        )
        self._conn.commit()

    def delete_paper_holding(self, symbol: str) -> None:
        cursor = self._conn.cursor()
        cursor.execute("DELETE FROM paper_holdings WHERE symbol=?", (symbol,))
        self._conn.commit()

    def record_paper_trade(
        self,
        symbol: str,
        direction: str,
        price: float,
        shares: int,
        value: float,
        fee: float,
        reason: str = "",
        signal_id: int = 0,
    ) -> int:
        cursor = self._conn.cursor()
        cursor.execute(
            """INSERT INTO paper_trades
               (symbol, direction, price, shares, value, fee, reason, signal_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (symbol, direction, price, shares, value, fee, reason, signal_id or None),
        )
        self._conn.commit()
        return cursor.lastrowid

    def paper_trades(self, symbol: str | None = None, limit: int = 50) -> pd.DataFrame:
        if symbol:
            df = pd.read_sql_query(
                "SELECT * FROM paper_trades WHERE symbol=? ORDER BY id DESC LIMIT ?",
                self._conn, params=(symbol, limit),
            )
        else:
            df = pd.read_sql_query(
                "SELECT * FROM paper_trades ORDER BY id DESC LIMIT ?",
                self._conn, params=(limit,),
            )
        return df

    def save_paper_snapshot(
        self,
        snapshot_date: str,
        total_value: float,
        cash: float,
        market_value: float,
        holdings_detail: list,
        positions_detail: list,
        total_return_pct: float,
    ) -> int:
        cursor = self._conn.cursor()
        cursor.execute(
            """INSERT INTO paper_daily_snapshots
               (snapshot_date, total_value, cash, market_value,
                holdings_detail, positions_detail, total_return_pct)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(snapshot_date) DO UPDATE SET
               total_value=excluded.total_value,
               cash=excluded.cash,
               market_value=excluded.market_value,
               holdings_detail=excluded.holdings_detail,
               positions_detail=excluded.positions_detail,
               total_return_pct=excluded.total_return_pct""",
            (
                snapshot_date,
                total_value,
                cash,
                market_value,
                json.dumps(holdings_detail, ensure_ascii=False),
                json.dumps(positions_detail, ensure_ascii=False),
                total_return_pct,
            ),
        )
        self._conn.commit()
        return cursor.lastrowid

    def get_paper_snapshot(self, snapshot_date: str) -> dict | None:
        cursor = self._conn.cursor()
        cursor.execute(
            "SELECT * FROM paper_daily_snapshots WHERE snapshot_date=?",
            (snapshot_date,),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return dict(zip([d[0] for d in cursor.description], row))

    def list_paper_snapshots(self, limit: int = 30) -> pd.DataFrame:
        return pd.read_sql_query(
            "SELECT snapshot_date, total_value, cash, market_value, total_return_pct "
            "FROM paper_daily_snapshots ORDER BY snapshot_date DESC LIMIT ?",
            self._conn, params=(limit,),
        )
