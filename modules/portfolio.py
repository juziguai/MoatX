"""
portfolio.py - 持仓管理 + 技术预警引擎
"""

import logging
import sqlite3
import os
from datetime import datetime
from typing import Any, Optional

import pandas as pd

from modules.utils import normalize_symbol

_logger = logging.getLogger("moatx.portfolio")


class Portfolio:
    """持仓管理 + 预警引擎"""

    def __init__(self, db_path: str = "data/portfolio.db"):
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        self.db = sqlite3.connect(db_path)
        self._init_db()
        # 委托子管理器（延迟导入避免循环）
        self._candidate_mgr = None
        self._alert_mgr = None
        self._risk_controller = None

    def _get_candidate_manager(self):
        if self._candidate_mgr is None:
            from modules.candidate import CandidateManager
            self._candidate_mgr = CandidateManager(self.db)
        return self._candidate_mgr

    def _get_alert_manager(self):
        if self._alert_mgr is None:
            from modules.alert_manager import AlertManager
            self._alert_mgr = AlertManager(self.db)
        return self._alert_mgr

    def _get_risk_controller(self):
        if self._risk_controller is None:
            from modules.risk_controller import RiskController
            self._risk_controller = RiskController(self.db)
        return self._risk_controller

    def _migrate_table_with_check(self, table_name: str, new_create_sql: str) -> None:
        """
        重建表，保留数据，添加 CHECK 约束。

        SQLite 不支持 ALTER TABLE ADD CONSTRAINT，必须重建表：
        1. 创建新表（含 CHECK）
        2. 从旧表 COPY 数据到新表
        3. 删除旧表
        4. 重命名新表
        """
        try:
            # 检查新表定义是否已存在（即旧表是否已有 CHECK 约束）
            # 尝试从 sqlite_master 查找该表的 CREATE 语句
            cur = self.db.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
                (table_name,)
            )
            row = cur.fetchone()
            if row is None:
                # 表不存在，直接创建
                self.db.execute(new_create_sql)
                self.db.commit()
                return

            old_sql = row[0] or ""
            new_sql = new_create_sql.strip()

            # 对比新旧 CREATE 语句：如果新SQL有 CHECK 而旧SQL没有，则迁移
            new_has_check = "CHECK" in new_sql
            old_has_check = "CHECK" in old_sql

            if new_has_check and not old_has_check:
                # 需要迁移：创建临时表 → 复制数据 → 删除旧表 → 重命名
                temp_name = f"{table_name}__migration_backup"
                self.db.execute(f"ALTER TABLE {table_name} RENAME TO {temp_name}")
                self.db.execute(new_create_sql)
                # 获取旧表列名，显式映射到新表（多出的新列用 DEFAULT 值填充）
                cur2 = self.db.execute(f"PRAGMA table_info({temp_name})")
                old_cols = [row[1] for row in cur2.fetchall()]
                col_list = ", ".join(old_cols)
                self.db.execute(
                    f"INSERT INTO {table_name} ({col_list}) SELECT {col_list} FROM {temp_name}"
                )
                self.db.execute(f"DROP TABLE {temp_name}")
                self.db.commit()
            else:
                # 无需迁移，或表本来就不存在（CREATE TABLE IF NOT EXISTS 不会覆盖）
                self.db.execute(new_create_sql)
                self.db.commit()
        except sqlite3.OperationalError as e:
            # 并发或表损坏等异常，记录但不影响启动
            _logger.warning("表 %s 迁移失败: %s", table_name, e)
            # 回滚以保持 db 状态一致
            self.db.rollback()

    def _ensure_columns(self, table: str, columns: list[tuple[str, str]]) -> None:
        """给已有表追加新列（如果列不存在）。"""
        try:
            cur = self.db.execute(f"PRAGMA table_info({table})")
            existing = {row[1] for row in cur.fetchall()}
            for col_name, col_def in columns:
                if col_name not in existing:
                    self.db.execute(f"ALTER TABLE {table} ADD COLUMN {col_name} {col_def}")
            self.db.commit()
        except sqlite3.OperationalError:
            self.db.rollback()

    def _init_db(self) -> None:
        # ─── CHECK 约束定义 ───────────────────────────────────
        HOLDINGS_CHECK = """
            CREATE TABLE IF NOT EXISTS holdings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL UNIQUE,
                name TEXT DEFAULT "",
                shares REAL NOT NULL CHECK (shares >= 0),
                cost_price REAL NOT NULL CHECK (cost_price >= 0),
                updated_at TEXT,
                note TEXT DEFAULT "",
                stop_loss_pct REAL DEFAULT 0,
                position_limit_pct REAL DEFAULT 0
            )
        """
        TRADES_CHECK = """
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT,
                action TEXT,
                symbol TEXT,
                name TEXT,
                shares REAL NOT NULL CHECK (shares > 0),
                price REAL NOT NULL CHECK (price >= 0),
                amount REAL NOT NULL CHECK (amount >= 0),
                net_amount REAL,
                fee REAL DEFAULT 0,
                stamp_duty REAL DEFAULT 0,
                transfer_fee REAL DEFAULT 0,
                trade_levy REAL DEFAULT 0,
                created_at TEXT
            )
        """
        CANDIDATES_CHECK = """
            CREATE TABLE IF NOT EXISTS candidates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL UNIQUE,
                name TEXT,
                rec_date TEXT,
                rec_rank INTEGER,
                entry_price REAL,
                rec_pct_change REAL,
                pe_ratio REAL,
                kdj_j REAL,
                rsi6 REAL,
                boll_position REAL,
                macd_signal TEXT,
                buy_signal_score INTEGER,
                risk_score INTEGER,
                result_price REAL,
                result_pct REAL,
                result_verified INTEGER DEFAULT 0,
                pending_close INTEGER DEFAULT 0,
                created_at TEXT
            )
        """
        SNAPSHOTS_CHECK = """
            CREATE TABLE IF NOT EXISTS snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                symbol TEXT NOT NULL,
                name TEXT DEFAULT "",
                shares REAL DEFAULT 0,
                current_price REAL DEFAULT 0,
                cost_price REAL DEFAULT 0,
                market_value REAL DEFAULT 0,
                total_pnl REAL DEFAULT 0,
                pnl_ratio REAL DEFAULT 0,
                position_ratio REAL DEFAULT 0,
                created_at TEXT
            )
        """
        DAILY_PNL_CHECK = """
            CREATE TABLE IF NOT EXISTS daily_pnl (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                symbol TEXT NOT NULL,
                name TEXT DEFAULT "",
                daily_pnl REAL DEFAULT 0,
                pnl_ratio REAL DEFAULT 0,
                created_at TEXT
            )
        """
        DAILY_ASSETS_CHECK = """
            CREATE TABLE IF NOT EXISTS daily_assets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                total_asset REAL DEFAULT 0,
                market_value REAL DEFAULT 0,
                position_pnl REAL DEFAULT 0,
                daily_pnl REAL DEFAULT 0,
                available_cash REAL DEFAULT 0,
                withdrawable_cash REAL DEFAULT 0,
                position_ratio REAL DEFAULT 0,
                date TEXT NOT NULL UNIQUE,
                created_at TEXT DEFAULT (datetime('now', 'localtime'))
            )
        """

        # ─── 迁移：检测并重建缺少 CHECK 约束的表 ───────────────
        self._migrate_table_with_check("holdings", HOLDINGS_CHECK)
        self._migrate_table_with_check("trades", TRADES_CHECK)
        self._migrate_table_with_check("candidates", CANDIDATES_CHECK)
        self._migrate_table_with_check("snapshots", SNAPSHOTS_CHECK)
        self._migrate_table_with_check("daily_pnl", DAILY_PNL_CHECK)
        self._migrate_table_with_check("daily_assets", DAILY_ASSETS_CHECK)

        # ─── candidate_results 表（独立存储验证结果，数据只增不删） ─
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS candidate_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                result_date TEXT NOT NULL,
                result_price REAL,
                result_pct REAL,
                verified INTEGER DEFAULT 0,
                created_at TEXT
            )
        """)
        self._ensure_columns("candidate_results", [
            ("result_date", "TEXT NOT NULL DEFAULT ''"),
            ("result_price", "REAL DEFAULT 0"),
            ("result_pct", "REAL DEFAULT 0"),
            ("verified", "INTEGER DEFAULT 0"),
        ])

        # ─── 列扩展：旧库可能缺少新列，用 ALTER TABLE 补充 ─────
        self._ensure_columns("holdings", [
            ("current_price", "REAL DEFAULT 0"),
            ("market_value", "REAL DEFAULT 0"),
            ("total_pnl", "REAL DEFAULT 0"),
            ("pnl_ratio", "TEXT DEFAULT '0.00%'"),
            ("position_ratio", "TEXT DEFAULT '0.00%'"),
        ])

        # ─── 不需要迁移的表（无数值列，无需 CHECK）──────────────
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS alert_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                alert_type TEXT,
                message TEXT,
                created_at TEXT
            )
        """)
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS config (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS risk_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                symbol TEXT,
                event_type TEXT,
                level TEXT,
                triggered_value REAL DEFAULT 0,
                threshold_value REAL DEFAULT 0,
                message TEXT,
                created_at TEXT
            )
        """)
        self.db.commit()

    # ---------- 持仓 CRUD ----------

    def add_holding(
        self,
        symbol: str,
        name: str = "",
        shares: float = 0,
        cost_price: float = 0,
        note: str = ""
    ) -> None:
        # ─── 业务层防御性校验 ───────────────────────────────────
        if shares < 0:
            raise ValueError(f"shares 必须 >= 0，实际为 {shares}")
        if cost_price < 0:
            raise ValueError(f"cost_price 必须 >= 0，实际为 {cost_price}")

        symbol = normalize_symbol(symbol)
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.db.execute("""
            INSERT OR REPLACE INTO holdings (symbol, name, shares, cost_price, updated_at, note)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (normalize_symbol(symbol), name, shares, cost_price, now, note))
        self.db.commit()

    def remove_holding(self, symbol: str) -> None:
        symbol = normalize_symbol(symbol)
        self.db.execute("DELETE FROM holdings WHERE symbol = ?", (symbol,))
        self.db.commit()

    def refresh_holdings(self, quotes: dict[str, dict[str, Any]]) -> int:
        """
        用实时行情更新持仓价格（批量 SQL，单次 commit）。

        Args:
            quotes: {full_code: {code, name, price, change_pct, volume, ...}}
                full_code 格式如 "002261.SZ"，需要匹配数据库中的裸代码。

        Returns:
            更新成功的股票数量。
        """
        df = self.list_holdings()
        if df.empty:
            return 0

        # 建立 {裸代码: quote} 的映射（去掉 .SH/.SZ/.BJ 后缀）
        quote_map = {}
        for full_code, q in quotes.items():
            bare = normalize_symbol(full_code)
            quote_map[bare] = q

        # 向量化计算总市值（一次遍历，无 iterrows）
        prices_arr = df["symbol"].map(
            lambda s: float(quote_map[s]["price"]) if s in quote_map else 0.0
        )
        total_market_value = (prices_arr * df["shares"]).sum()

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # 收集所有 UPDATE 参数
        update_rows = []
        for _, row in df.iterrows():
            sym = row["symbol"]
            if sym not in quote_map:
                continue
            q = quote_map[sym]
            price = float(q["price"])
            shares = float(row["shares"])
            cost_price = float(row["cost_price"])
            market_value = price * shares
            total_pnl = (price - cost_price) * shares
            pnl_ratio = (price - cost_price) / cost_price * 100 if cost_price else 0
            position_ratio = (market_value / total_market_value * 100) if total_market_value else 0
            update_rows.append((
                price, market_value, total_pnl,
                f"{pnl_ratio:.2f}%", f"{position_ratio:.2f}%",
                now, sym
            ))

        if not update_rows:
            return 0

        # 批量执行，单次 commit
        self.db.executemany("""
            UPDATE holdings SET
                current_price = ?,
                market_value = ?,
                total_pnl = ?,
                pnl_ratio = ?,
                position_ratio = ?,
                updated_at = ?
            WHERE symbol = ?
        """, update_rows)
        self.db.commit()
        return len(update_rows)

    def list_holdings(self) -> pd.DataFrame:
        """
        返回持仓列表，自动带上当日盈亏（来自 daily_pnl 表的最新记录）。
        """
        df = pd.read_sql("SELECT * FROM holdings ORDER BY updated_at DESC", self.db)
        if df.empty:
            return df

        # 查询每只股票最新一条 daily_pnl 记录
        daily_df = pd.read_sql("""
            SELECT symbol, daily_pnl, pnl_ratio
            FROM daily_pnl d1
            WHERE created_at = (
                SELECT MAX(created_at) FROM daily_pnl d2 WHERE d2.symbol = d1.symbol
            )
        """, self.db)

        if not daily_df.empty:
            daily_df = daily_df.rename(columns={
                "daily_pnl": "daily_pnl_val",
                "pnl_ratio": "daily_pnl_ratio"
            })
            df = df.merge(daily_df, on="symbol", how="left")

        return df

    def get_holding(self, symbol: str) -> Optional[dict]:
        symbol = normalize_symbol(symbol)
        df = pd.read_sql(
            "SELECT * FROM holdings WHERE symbol = ?", self.db, params=(symbol,)
        )
        if df.empty:
            return None
        return df.iloc[0].to_dict()

    # ---------- 快照 / 盈亏写入 ----------

    def insert_snapshot(self, date: str, symbol: str, name: str, shares: float,
                        current_price: float, cost_price: float, market_value: float,
                        total_pnl: float, pnl_ratio: float, position_ratio: float) -> None:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.db.execute("""
            INSERT INTO snapshots
            (date, symbol, name, shares, current_price, cost_price, market_value,
             total_pnl, pnl_ratio, position_ratio, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (date, normalize_symbol(symbol), name, shares, current_price,
              cost_price, market_value, total_pnl, pnl_ratio, position_ratio, now))
        self.db.commit()

    def insert_daily_pnl(self, date: str, symbol: str, name: str,
                          daily_pnl: float, pnl_ratio: float) -> None:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.db.execute("""
            INSERT INTO daily_pnl
            (date, symbol, name, daily_pnl, pnl_ratio, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (date, normalize_symbol(symbol), name, daily_pnl, pnl_ratio, now))
        self.db.commit()

    def record_daily_pnl(self, quotes: dict[str, float] | None = None) -> int:
        """
        批量记录当日盈亏到 daily_pnl 表（批量 INSERT，单次 commit）。

        Args:
            quotes: dict of {symbol: current_price}，如果为 None 则从持仓 current_price 读取

        Returns:
            记录的股票数量
        """
        from .stock_data import StockData
        holdings = self.list_holdings()
        if holdings.empty:
            return 0

        today = datetime.now().strftime("%Y-%m-%d")
        sd = StockData()
        symbols = holdings["symbol"].tolist()

        # 批量获取近期日线（含 prev_close）
        price_data = sd.get_daily_prices(symbols, count=5)

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        insert_rows = []

        for _, row in holdings.iterrows():
            sym = row["symbol"]
            name = row.get("name", "")
            shares = float(row.get("shares", 0))
            current_price = quotes.get(sym, row.get("current_price", 0)) if quotes else row.get("current_price", 0)
            if not current_price:
                continue

            # 取最新一条日线记录的 prev_close（前一交易日收盘）
            prev_close = 0.0
            sym_data = price_data.get(sym, {})
            if sym_data:
                latest_date = max(sym_data.keys())
                prev_close = sym_data[latest_date].get("prev_close", 0)

            if prev_close > 0:
                daily_pnl = (current_price - prev_close) * shares
                pnl_ratio = (current_price - prev_close) / prev_close * 100
            else:
                # 没有昨日收盘，用成本价估算
                cost = float(row.get("cost_price", 0))
                if cost > 0:
                    daily_pnl = (current_price - cost) * shares
                    pnl_ratio = (current_price - cost) / cost * 100
                else:
                    continue

            insert_rows.append((
                today, normalize_symbol(sym), name, daily_pnl, pnl_ratio, now
            ))

        if not insert_rows:
            return 0

        # 批量 INSERT，单次 commit
        self.db.executemany("""
            INSERT INTO daily_pnl (date, symbol, name, daily_pnl, pnl_ratio, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, insert_rows)
        self.db.commit()
        return len(insert_rows)

    def insert_daily_asset(self, date: str, total_asset: float, market_value: float,
                            position_pnl: float, daily_pnl: float, available_cash: float,
                            position_ratio: float) -> None:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.db.execute("""
            INSERT OR REPLACE INTO daily_assets
            (date, total_asset, market_value, position_pnl, daily_pnl,
             available_cash, position_ratio, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (date, total_asset, market_value, position_pnl, daily_pnl,
              available_cash, position_ratio, now))
        self.db.commit()

    # ---------- 截图导入 ----------

    def record_trade(self, date: str, action: str, symbol: str, name: str,
                     shares: float, price: float, amount: float,
                     fee: float = 0, stamp_duty: float = 0,
                     transfer_fee: float = 0, trade_levy: float = 0) -> None:
        """记录一笔交易，同时自动处理持仓变化（SELL删仓，BUY加仓）。

        事务语义：trade INSERT 与持仓变更（BUY add/SELL delete）在同一事务内，
        Python sqlite3 的嵌套 commit 不关闭外层事务，全部成功才 commit，
        任意一步失败则 rollback。
        """
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sym = normalize_symbol(symbol)

        # 基本校验
        if not date or not action or not sym:
            raise ValueError(f"无效交易记录: date={date}, action={action}, symbol={sym}")
        if shares <= 0:
            raise ValueError(f"交易股数必须 > 0，实际为 {shares}")
        if price < 0:
            raise ValueError(f"交易价格不能为负，实际为 {price}")

        # 事务原子性：trade INSERT + 持仓变更必须在同一事务内
        try:
            self.db.execute("""
                INSERT INTO trades (date, action, symbol, name, shares, price, amount, net_amount,
                                    fee, stamp_duty, transfer_fee, trade_levy, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (date, action.upper(), sym, name,
                  shares, price, amount, amount,
                  fee, stamp_duty, transfer_fee, trade_levy, now))

            # 自动处理持仓
            if action.upper() == "SELL":
                holding = self.get_holding(sym)
                if holding is None:
                    _logger.warning("尝试卖出 %s 但持仓中不存在，已记录交易但未删除持仓", sym)
                else:
                    self.remove_holding(sym)
            elif action.upper() == "BUY":
                self.add_holding(symbol=sym, name=name, shares=shares,
                                 cost_price=price, note="trade_import")

            self.db.commit()
        except Exception:
            self.db.rollback()
            raise

    # ---------- 截图导入 ----------

    def import_from_screenshot(self, image_path: str) -> list[Any]:
        """
        调用 MiniMax understand_image 识别截图，返回 [(symbol, name, shares), ...]
        由 cli_portfolio.py 先行调用 mmx CLI 解析，再传入此方法
        """
        raise NotImplementedError(
            "请通过 cli_portfolio.py 的 import 命令导入截图，"
            "该方法仅负责存储结果"
        )

    def import_parsed_results(self, results: list[tuple[Any, ...]]) -> list[str]:
        """
        存储截图解析结果
        results: [(symbol, name, shares, cost_price), ...]
        """
        added = []
        for symbol, name, shares, cost_price in results:
            self.add_holding(
                symbol=symbol,
                name=name or "",
                shares=shares,
                cost_price=cost_price,
                note="screenshot_import"
            )
            added.append(symbol)
        return added

    def import_trades(self, trades_df: pd.DataFrame) -> int:
        """
        批量导入交易记录（整体事务：任意一行失败全部回滚，已成功的行不受影响）。

        事务语义：所有行在同一事务内执行，Python sqlite3 会在首次 DML 时开启隐式事务，
        循环结束后统一 commit。发生异常时 rollback，已 commit 的行不受影响
        （即失败点之前的行已持久化，失败点之后的行不会被执行）。

        trades_df: DataFrame with columns [date, action, code/symbol, name, shares, price, amount, net_amount, fee, stamp_duty, transfer_fee, trade_levy]
        Returns: number of rows inserted
        """
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        rows = 0
        try:
            for _, row in trades_df.iterrows():
                # Support both 'code' and 'symbol' column names
                code = str(row.get("symbol", row.get("code", "")))
                sym = normalize_symbol(code)
                if not sym:
                    _logger.warning("跳过无效股票代码: '%s'，所在行: %s", code, dict(row))
                    continue
                self.db.execute("""
                    INSERT INTO trades (date, action, symbol, name, shares, price, amount, net_amount, fee, stamp_duty, transfer_fee, trade_levy, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    row["date"], row["action"], sym,
                    row["name"], row["shares"], row["price"], row["amount"],
                    row["net_amount"], row["fee"], row["stamp_duty"],
                    row["transfer_fee"], row["trade_levy"], now
                ))
                rows += 1
            if rows > 0:
                self.db.commit()
        except Exception as e:
            self.db.rollback()
            _logger.error("批量导入事务失败，已回滚: %s", e)
            return 0
        return rows

    def list_trades(self, symbol: str = None, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """查询交易记录，支持按股票和日期过滤"""
        query = "SELECT * FROM trades WHERE 1=1"
        params = []
        if symbol:
            query += " AND symbol = ?"
            params.append(normalize_symbol(symbol))
        if start_date:
            query += " AND date >= ?"
            params.append(start_date)
        if end_date:
            query += " AND date <= ?"
            params.append(end_date)
        query += " ORDER BY date ASC"
        return pd.read_sql(query, self.db, params=params)

    # ---------- 预警检测（委托给 AlertManager） ----------

    def check_alerts(self, max_workers: int = 6) -> list[Any]:
        """对所有持仓并行运行 analyze()，检测预警条件"""
        holdings = self.list_holdings()
        return self._get_alert_manager().check_alerts(holdings, max_workers=max_workers)

    def get_alert_history(self, limit: int = 50) -> pd.DataFrame:
        return self._get_alert_manager().get_alert_history(limit=limit)

    # ---------- 配置 ----------

    def set_config(self, key: str, value: str) -> None:
        self.db.execute(
            "INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)",
            (key, value)
        )
        self.db.commit()

    def get_config(self, key: str, default: str = "") -> str:
        row = self.db.execute(
            "SELECT value FROM config WHERE key = ?", (key,)
        ).fetchone()
        return row[0] if row else default

    # ---------- 工具 ----------

    def add_candidate(
        self,
        symbol: str,
        name: str = "",
        rec_rank: int = 0,
        entry_price: float = 0,
        rec_pct_change: float = 0,
        pe_ratio: float = None,
        kdj_j: float = None,
        rsi6: float = None,
        boll_position: float = None,
        macd_signal: str = "",
        buy_signal_score: int = 0,
        risk_score: int = None,
    ) -> bool:
        return self._get_candidate_manager().add_candidate(
            symbol, name, rec_rank, entry_price, rec_pct_change,
            pe_ratio, kdj_j, rsi6, boll_position, macd_signal,
            buy_signal_score, risk_score)

    def list_candidates(self, unverified_only: bool = False) -> pd.DataFrame:
        return self._get_candidate_manager().list_candidates(unverified_only)

    def update_candidate_result(self, symbol: str, result_price: float, result_pct: float) -> None:
        return self._get_candidate_manager().update_candidate_result(symbol, result_price, result_pct)

    def delete_candidate(self, symbol: str) -> bool:
        return self._get_candidate_manager().delete_candidate(symbol)

    def verify_candidates(self) -> list[Any]:
        return self._get_candidate_manager().verify_candidates()

    def get_pending_candidates(self) -> list[tuple]:
        """返回所有 pending_close=1 的候选股 (symbol, name, rec_rank, rec_date, entry_price)"""
        return self._get_candidate_manager().get_pending()

    def clear_candidate_pending(self, symbol: str | None = None) -> int:
        """清除 pending_close=1 标记。None 清除全部，否则只清指定股。"""
        return self._get_candidate_manager().clear_pending(symbol)

    def mark_candidate_verified(self, symbol: str, result_price: float, result_pct: float) -> None:
        """标记候选股已验证（写入 candidate_results + 更新 candidates 表）。"""
        self._get_candidate_manager().mark_verified(symbol, result_price, result_pct)

    def close(self) -> None:
        self.db.close()
