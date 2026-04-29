"""Backtest strategy and run persistence."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field


@dataclass
class BacktestStrategyRecord:
    id: int = 0
    name: str = ""
    class_path: str = ""
    params: dict = field(default_factory=dict)
    created_at: str = ""


@dataclass
class BacktestRunRecord:
    id: int = 0
    strategy_id: int = 0
    strategy_name: str = ""
    symbols: list[str] = field(default_factory=list)
    start_date: str = ""
    end_date: str = ""
    initial_capital: float = 100_000
    results: dict = field(default_factory=dict)
    equity_curve: list[dict] = field(default_factory=list)
    duration_ms: int = 0
    created_at: str = ""


@dataclass
class OptimizationRecord:
    id: int = 0
    strategy_name: str = ""
    symbols: list[str] = field(default_factory=list)
    best_params: dict = field(default_factory=dict)
    best_result: dict = field(default_factory=dict)
    total_runs: int = 0
    duration_ms: int = 0
    created_at: str = ""


class BacktestStore:
    """Persist backtest strategy configs, runs, and optimization results."""

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    # -- Strategies --

    def save_strategy(self, name: str, class_path: str, params: dict | None = None) -> int:
        cursor = self._conn.cursor()
        cursor.execute(
            "INSERT INTO backtest_strategies (name, class_path, params) VALUES (?, ?, ?)",
            (name, class_path, json.dumps(params or {}, ensure_ascii=False)),
        )
        self._conn.commit()
        return cursor.lastrowid

    def list_strategies(self) -> list[BacktestStrategyRecord]:
        cursor = self._conn.cursor()
        cursor.execute(
            "SELECT id, name, class_path, params, created_at FROM backtest_strategies ORDER BY id"
        )
        return [
            BacktestStrategyRecord(
                id=row[0], name=row[1], class_path=row[2],
                params=json.loads(row[3] or "{}"), created_at=row[4] or "",
            )
            for row in cursor.fetchall()
        ]

    # -- Runs --

    def save_run(
        self,
        strategy_id: int,
        symbols: list[str],
        start: str,
        end: str,
        capital: float,
        results: dict,
        equity_curve: list[dict],
        duration_ms: int,
    ) -> int:
        cursor = self._conn.cursor()
        cursor.execute(
            """INSERT INTO backtest_runs
               (strategy_id, symbols, start_date, end_date, initial_capital, results, equity_curve, duration_ms)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                strategy_id,
                ",".join(symbols),
                start,
                end,
                capital,
                json.dumps(results, ensure_ascii=False, default=str),
                json.dumps(equity_curve, ensure_ascii=False, default=str),
                duration_ms,
            ),
        )
        self._conn.commit()
        return cursor.lastrowid

    def list_runs(self, strategy_id: int | None = None, limit: int = 20) -> list[BacktestRunRecord]:
        cursor = self._conn.cursor()
        if strategy_id:
            cursor.execute(
                """SELECT r.id, r.strategy_id, s.name, r.symbols, r.start_date, r.end_date,
                          r.initial_capital, r.results, r.equity_curve, r.duration_ms, r.created_at
                   FROM backtest_runs r
                   LEFT JOIN backtest_strategies s ON r.strategy_id = s.id
                   WHERE r.strategy_id = ?
                   ORDER BY r.id DESC LIMIT ?""",
                (strategy_id, limit),
            )
        else:
            cursor.execute(
                """SELECT r.id, r.strategy_id, s.name, r.symbols, r.start_date, r.end_date,
                          r.initial_capital, r.results, r.equity_curve, r.duration_ms, r.created_at
                   FROM backtest_runs r
                   LEFT JOIN backtest_strategies s ON r.strategy_id = s.id
                   ORDER BY r.id DESC LIMIT ?""",
                (limit,),
            )
        return [
            BacktestRunRecord(
                id=row[0], strategy_id=row[1], strategy_name=row[2] or "",
                symbols=row[3].split(",") if row[3] else [],
                start_date=row[4] or "", end_date=row[5] or "",
                initial_capital=row[6] or 100_000,
                results=json.loads(row[7] or "{}"),
                equity_curve=json.loads(row[8] or "[]"),
                duration_ms=row[9] or 0, created_at=row[10] or "",
            )
            for row in cursor.fetchall()
        ]

    def get_run(self, run_id: int) -> BacktestRunRecord | None:
        cursor = self._conn.cursor()
        cursor.execute(
            """SELECT r.id, r.strategy_id, s.name, r.symbols, r.start_date, r.end_date,
                      r.initial_capital, r.results, r.equity_curve, r.duration_ms, r.created_at
               FROM backtest_runs r
               LEFT JOIN backtest_strategies s ON r.strategy_id = s.id
               WHERE r.id = ?""",
            (run_id,),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return BacktestRunRecord(
            id=row[0], strategy_id=row[1], strategy_name=row[2] or "",
            symbols=row[3].split(",") if row[3] else [],
            start_date=row[4] or "", end_date=row[5] or "",
            initial_capital=row[6] or 100_000,
            results=json.loads(row[7] or "{}"),
            equity_curve=json.loads(row[8] or "[]"),
            duration_ms=row[9] or 0, created_at=row[10] or "",
        )

    def delete_run(self, run_id: int) -> None:
        cursor = self._conn.cursor()
        cursor.execute("DELETE FROM backtest_runs WHERE id=?", (run_id,))
        self._conn.commit()

    # -- Optimizations --

    def save_optimization(
        self,
        strategy_name: str,
        symbols: list[str],
        start: str,
        end: str,
        target_metric: str,
        best_params: dict,
        best_result: dict,
        total_runs: int,
        duration_ms: int,
    ) -> int:
        cursor = self._conn.cursor()
        cursor.execute(
            """INSERT INTO backtest_optimizations
               (strategy_name, symbols, start_date, end_date, target_metric,
                parameter_count, best_params, best_result, total_runs, duration_ms)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                strategy_name,
                ",".join(symbols),
                start,
                end,
                target_metric,
                len(best_params),
                json.dumps(best_params, ensure_ascii=False),
                json.dumps(best_result, ensure_ascii=False, default=str),
                total_runs,
                duration_ms,
            ),
        )
        self._conn.commit()
        return cursor.lastrowid

    def list_optimizations(self, limit: int = 10) -> list[OptimizationRecord]:
        cursor = self._conn.cursor()
        cursor.execute(
            """SELECT id, strategy_name, symbols, best_params, best_result,
                      total_runs, duration_ms, created_at
               FROM backtest_optimizations
               ORDER BY id DESC LIMIT ?""",
            (limit,),
        )
        return [
            OptimizationRecord(
                id=row[0], strategy_name=row[1],
                symbols=row[2].split(",") if row[2] else [],
                best_params=json.loads(row[3] or "{}"),
                best_result=json.loads(row[4] or "{}"),
                total_runs=row[5] or 0, duration_ms=row[6] or 0, created_at=row[7] or "",
            )
            for row in cursor.fetchall()
        ]
