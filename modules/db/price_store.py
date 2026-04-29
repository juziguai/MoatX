"""OHLCV and indicator storage."""

from __future__ import annotations

import sqlite3

import pandas as pd


class PriceStore:
    """Read/write daily OHLCV and indicator data to warehouse."""

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    def save_daily_batch(
        self, df: pd.DataFrame, symbol: str, adjust: str = "qfq"
    ) -> int:
        """Save daily OHLCV rows. Returns count of inserted rows."""
        required = {"date", "open", "high", "low", "close"}
        if not required.intersection(df.columns):
            return 0

        rows = 0
        cursor = self._conn.cursor()
        for _, row in df.iterrows():
            d = row.get("date")
            if d is None:
                continue
            trade_date = str(d.date()) if hasattr(d, "date") else str(d)[:10]
            cursor.execute(
                """INSERT OR IGNORE INTO price_daily
                   (symbol, trade_date, open, high, low, close, volume, amount, turn, pct_change, adjust)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    symbol,
                    trade_date,
                    row.get("open"),
                    row.get("high"),
                    row.get("low"),
                    row.get("close"),
                    row.get("volume"),
                    row.get("amount"),
                    row.get("turn"),
                    row.get("pct_change"),
                    adjust,
                ),
            )
            rows += cursor.rowcount
        self._conn.commit()
        return rows

    def load_daily(
        self,
        symbol: str,
        start_date: str | None = None,
        end_date: str | None = None,
        adjust: str = "qfq",
    ) -> pd.DataFrame:
        """Load daily OHLCV data."""
        conditions = ["symbol = ?", "adjust = ?"]
        params: list = [symbol, adjust]
        if start_date:
            conditions.append("trade_date >= ?")
            params.append(start_date)
        if end_date:
            conditions.append("trade_date <= ?")
            params.append(end_date)

        where = " AND ".join(conditions)
        query = f"SELECT trade_date as date, open, high, low, close, volume, amount, turn, pct_change FROM price_daily WHERE {where} ORDER BY trade_date"
        df = pd.read_sql_query(query, self._conn, params=params)
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"])
        return df

    def has_data(self, symbol: str, trade_date: str, adjust: str = "qfq") -> bool:
        """Check if data exists for a specific date."""
        cursor = self._conn.cursor()
        cursor.execute(
            "SELECT 1 FROM price_daily WHERE symbol=? AND trade_date=? AND adjust=?",
            (symbol, trade_date, adjust),
        )
        return cursor.fetchone() is not None

    def latest_date(self, symbol: str, adjust: str = "qfq") -> str | None:
        """Get the latest trade_date for a symbol."""
        cursor = self._conn.cursor()
        cursor.execute(
            "SELECT MAX(trade_date) FROM price_daily WHERE symbol=? AND adjust=?",
            (symbol, adjust),
        )
        row = cursor.fetchone()
        return row[0] if row[0] else None

    def save_indicators_batch(
        self, df: pd.DataFrame, symbol: str
    ) -> int:
        """Save indicator values. df must have 'date' column + indicator columns."""
        if "date" not in df.columns:
            return 0
        indicator_cols = [c for c in df.columns if c not in (
            "date", "open", "high", "low", "close", "volume", "amount", "turn", "pct_change"
        )]
        if not indicator_cols:
            return 0

        cursor = self._conn.cursor()
        rows = 0
        for _, row in df.iterrows():
            d = row.get("date")
            if d is None:
                continue
            trade_date = str(d.date()) if hasattr(d, "date") else str(d)[:10]
            for col in indicator_cols:
                val = row.get(col)
                if val is None or pd.isna(val):
                    continue
                cursor.execute(
                    """INSERT OR IGNORE INTO indicator_values
                       (symbol, trade_date, name, value) VALUES (?, ?, ?, ?)""",
                    (symbol, trade_date, col, float(val)),
                )
                rows += cursor.rowcount
        self._conn.commit()
        return rows

    def load_indicators(
        self,
        symbol: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        """Load indicator values as a pivoted DataFrame."""
        conditions = ["symbol = ?"]
        params: list = [symbol]
        if start_date:
            conditions.append("trade_date >= ?")
            params.append(start_date)
        if end_date:
            conditions.append("trade_date <= ?")
            params.append(end_date)

        where = " AND ".join(conditions)
        query = f"SELECT trade_date as date, name, value FROM indicator_values WHERE {where} ORDER BY trade_date"
        df = pd.read_sql_query(query, self._conn, params=params)
        if df.empty:
            return df
        df["date"] = pd.to_datetime(df["date"])
        pivoted = df.pivot_table(
            index="date", columns="name", values="value", aggfunc="first"
        ).reset_index()
        pivoted.columns.name = None
        return pivoted

    def delete_older_than(self, symbol: str, trade_date: str) -> int:
        """Remove data older than a given date (for cache invalidation)."""
        cursor = self._conn.cursor()
        cursor.execute(
            "DELETE FROM price_daily WHERE symbol=? AND trade_date<?",
            (symbol, trade_date),
        )
        affected = cursor.rowcount
        self._conn.commit()
        return affected
