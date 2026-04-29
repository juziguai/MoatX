"""
candidate.py - 候选股管理
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime
from typing import Any

import pandas as pd

from modules.utils import normalize_symbol

_logger = logging.getLogger("moatx.candidate")


class CandidateManager:
    """候选股 CRUD，与 Portfolio.db 共用 portfolio.db"""

    def __init__(self, db: sqlite3.Connection):
        self.db = db

    def add_candidate(
        self,
        symbol: str,
        name: str = "",
        rec_rank: int = 0,
        entry_price: float = 0,
        rec_pct_change: float = 0,
        pe_ratio: float | None = None,
        kdj_j: float | None = None,
        rsi6: float | None = None,
        boll_position: float | None = None,
        macd_signal: str = "",
        buy_signal_score: int = 0,
        risk_score: int | None = None,
    ) -> bool:
        """
        添加候选股到候选池（INSERT，symbol 重复则忽略，数据只增不删）。
        Returns True if inserted, False if duplicate/exists.
        """
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        today = datetime.now().strftime("%Y-%m-%d")
        sym = normalize_symbol(symbol)
        try:
            self.db.execute("""
                INSERT OR IGNORE INTO candidates
                (symbol, name, rec_date, rec_rank, entry_price, rec_pct_change,
                 pe_ratio, kdj_j, rsi6, boll_position, macd_signal,
                 buy_signal_score, risk_score, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (sym, name, today, rec_rank, entry_price, rec_pct_change,
                  pe_ratio, kdj_j, rsi6, boll_position, macd_signal,
                  buy_signal_score, risk_score, now))
            self.db.commit()
            return self.db.total_changes > 0
        except sqlite3.IntegrityError:
            return False  # 重复插入
        except sqlite3.Error as e:
            _logger.error("添加候选股失败: %s - %s", sym, e)
            raise

    def list_candidates(self, unverified_only: bool = False) -> pd.DataFrame:
        """列出候选股（unverified_only=True 时从 candidate_results 判断是否有验证记录）"""
        if unverified_only:
            df = pd.read_sql("""
                SELECT c.* FROM candidates c
                LEFT JOIN candidate_results r ON c.symbol = r.symbol
                WHERE r.symbol IS NULL
                ORDER BY c.rec_rank
            """, self.db)
        else:
            df = pd.read_sql("SELECT * FROM candidates ORDER BY rec_rank", self.db)
        return df

    def update_candidate_result(self, symbol: str, result_price: float, result_pct: float) -> None:
        """写入候选股验证结果（INSERT，不覆盖历史，result_date 为今天）"""
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        today = datetime.now().strftime("%Y-%m-%d")
        sym = normalize_symbol(symbol)
        self.db.execute("""
            INSERT INTO candidate_results (symbol, result_date, result_price, result_pct, verified, created_at)
            VALUES (?, ?, ?, ?, 1, ?)
        """, (sym, today, result_price, result_pct, now))
        self.db.commit()

    def delete_candidate(self, symbol: str) -> bool:
        """从候选池删除指定股票"""
        sym = normalize_symbol(symbol)
        cur = self.db.execute("DELETE FROM candidates WHERE symbol=?", (sym,))
        self.db.commit()
        return cur.rowcount > 0

    def verify_candidates(self) -> list[dict[str, Any]]:
        """返回候选股验证报告（JOIN candidate_results 取最新验证结果）"""
        df = pd.read_sql("""
            SELECT
                c.rec_rank,
                c.symbol,
                c.name,
                c.rec_date,
                c.entry_price,
                c.rec_pct_change,
                r.result_date,
                r.result_price,
                r.result_pct,
                r.verified
            FROM candidates c
            LEFT JOIN (
                SELECT symbol, result_date, result_price, result_pct, verified,
                       ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY created_at DESC) as rn
                FROM candidate_results
            ) r ON c.symbol = r.symbol AND r.rn = 1
            ORDER BY c.rec_rank
        """, self.db)
        if df.empty:
            return []
        for idx, row in df.iterrows():
            if row.get("result_price") and row.get("entry_price"):
                df.loc[idx, "actual_pct"] = (
                    float(row["result_price"]) - float(row["entry_price"])
                ) / max(float(row["entry_price"]), 0.001) * 100
        return df.to_dict("records")

    def get_pending(self) -> list[tuple]:
        """返回所有 pending_close=1 的候选股"""
        cur = self.db.execute(
            "SELECT symbol, name, rec_rank, rec_date, entry_price FROM candidates WHERE pending_close=1"
        )
        return cur.fetchall()

    def clear_pending(self, symbol: str | None = None) -> int:
        """清除 pending_close=1 标记。symbol=None 时清除全部，否则只清指定股。"""
        if symbol:
            cur = self.db.execute(
                "UPDATE candidates SET pending_close=0 WHERE symbol=? AND pending_close=1",
                (normalize_symbol(symbol),)
            )
        else:
            cur = self.db.execute("UPDATE candidates SET pending_close=0 WHERE pending_close=1")
        self.db.commit()
        return cur.rowcount

    def mark_verified(self, symbol: str, result_price: float, result_pct: float) -> None:
        """标记候选股为已验证（同步 candidates 表 + 写入 candidate_results）"""
        sym = normalize_symbol(symbol)
        today = datetime.now().strftime("%Y-%m-%d")
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # 写入 candidate_results（INSERT，不覆盖历史）
        self.db.execute("""
            INSERT INTO candidate_results (symbol, result_date, result_price, result_pct, verified, created_at)
            VALUES (?, ?, ?, ?, 1, ?)
        """, (sym, today, result_price, result_pct, now))
        # 更新 candidates 表标记
        self.db.execute("""
            UPDATE candidates
            SET pending_close=0, result_verified=1, result_price=?, result_pct=?
            WHERE symbol=?
        """, (result_price, result_pct, sym))
        self.db.commit()
