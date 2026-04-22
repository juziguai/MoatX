"""
portfolio.py - 持仓管理 + 技术预警引擎
"""

import sqlite3
import os
from datetime import datetime
from typing import Optional

import pandas as pd

from .analyzer import MoatXAnalyzer


class Portfolio:
    """持仓管理 + 预警引擎"""

    def __init__(self, db_path: str = "data/portfolio.db"):
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        self.db = sqlite3.connect(db_path)
        self._init_db()

    def _init_db(self):
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS holdings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL UNIQUE,
                name TEXT DEFAULT "",
                shares REAL DEFAULT 0,
                cost_price REAL DEFAULT 0,
                added_at TEXT,
                note TEXT DEFAULT ""
            )
        """)
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
        self.db.commit()

    # ---------- 持仓 CRUD ----------

    def add_holding(
        self,
        symbol: str,
        name: str = "",
        shares: float = 0,
        cost_price: float = 0,
        note: str = ""
    ):
        symbol = self._normalize_symbol(symbol)
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.db.execute("""
            INSERT OR REPLACE INTO holdings (symbol, name, shares, cost_price, added_at, note)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (symbol, name, shares, cost_price, now, note))
        self.db.commit()

    def remove_holding(self, symbol: str):
        symbol = self._normalize_symbol(symbol)
        self.db.execute("DELETE FROM holdings WHERE symbol = ?", (symbol,))
        self.db.commit()

    def list_holdings(self) -> pd.DataFrame:
        df = pd.read_sql("SELECT * FROM holdings ORDER BY added_at DESC", self.db)
        return df

    def get_holding(self, symbol: str) -> Optional[dict]:
        symbol = self._normalize_symbol(symbol)
        df = pd.read_sql(
            "SELECT * FROM holdings WHERE symbol = ?", self.db, params=(symbol,)
        )
        if df.empty:
            return None
        return df.iloc[0].to_dict()

    # ---------- 截图导入 ----------

    def import_from_screenshot(self, image_path: str) -> list:
        """
        调用 MiniMax understand_image 识别截图，返回 [(symbol, name, shares), ...]
        由 cli_portfolio.py 先行调用 mmx CLI 解析，再传入此方法
        """
        raise NotImplementedError(
            "请通过 cli_portfolio.py 的 import 命令导入截图，"
            "该方法仅负责存储结果"
        )

    def import_parsed_results(self, results: list):
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

    # ---------- 预警检测 ----------

    def check_alerts(self) -> list:
        """对所有持仓运行 analyze()，检测预警条件"""
        alerts = []
        holdings = self.list_holdings()
        if holdings.empty:
            return alerts

        analyzer = MoatXAnalyzer()
        for _, row in holdings.iterrows():
            symbol = row["symbol"]
            try:
                report = analyzer.analyze(symbol)
                detected = self._detect_alerts(report)
                for alert in detected:
                    alert["symbol"] = symbol
                    alerts.append(alert)
                    self._log_alert(alert)
            except Exception as e:
                alerts.append({
                    "symbol": symbol,
                    "type": "error",
                    "msg": f"分析失败: {e}"
                })
        return alerts

    def _detect_alerts(self, report: dict) -> list:
        alerts = []
        kdj = report.get("kdj", {})
        rsi = report.get("rsi", {})
        boll = report.get("boll", {})
        signals = report.get("signals", {})
        score = signals.get("composite_score", 50)
        price = report.get("price", 0)

        if kdj.get("j", 0) > 90:
            alerts.append({
                "type": "kdj_overbought",
                "msg": f"KDJ 超买 J={kdj['j']:.1f}，现价 {price}元"
            })
        if kdj.get("j", 0) < 10:
            alerts.append({
                "type": "kdj_oversold",
                "msg": f"KDJ 超卖 J={kdj['j']:.1f}，现价 {price}元"
            })
        if rsi.get("rsi12", 50) > 75:
            alerts.append({
                "type": "rsi_overbought",
                "msg": f"RSI 超买 RSI12={rsi['rsi12']:.1f}"
            })
        if rsi.get("rsi12", 50) < 25:
            alerts.append({
                "type": "rsi_oversold",
                "msg": f"RSI 超卖 RSI12={rsi['rsi12']:.1f}"
            })
        if boll.get("position", 50) < 5:
            alerts.append({
                "type": "boll_lower_break",
                "msg": f"触及布林下轨 position={boll['position']:.1f}%"
            })
        if boll.get("position", 50) > 95:
            alerts.append({
                "type": "boll_upper_break",
                "msg": f"触及布林上轨 position={boll['position']:.1f}%"
            })
        if score < 25:
            alerts.append({
                "type": "low_score",
                "msg": f"综合评分低 {score}分"
            })

        macd_sig = signals.get("macd_signal", "")
        if "死叉" in macd_sig:
            alerts.append({"type": "macd_death_cross", "msg": f"MACD 死叉信号"})
        elif "金叉" in macd_sig:
            alerts.append({"type": "macd_golden_cross", "msg": f"MACD 金叉信号"})

        return alerts

    def _log_alert(self, alert: dict):
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.db.execute("""
            INSERT INTO alert_log (symbol, alert_type, message, created_at)
            VALUES (?, ?, ?, ?)
        """, (alert.get("symbol", ""), alert.get("type", ""),
              alert.get("msg", ""), now))
        self.db.commit()

    def get_alert_history(self, limit: int = 50) -> pd.DataFrame:
        df = pd.read_sql(
            "SELECT * FROM alert_log ORDER BY created_at DESC LIMIT ?",
            self.db, params=(limit,)
        )
        return df

    # ---------- 配置 ----------

    def set_config(self, key: str, value: str):
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

    @staticmethod
    def _normalize_symbol(symbol: str) -> str:
        """标准化股票代码：去掉后缀，只保留6位数字"""
        s = symbol.strip().upper()
        for suffix in [".SH", ".SZ", "SH", "SZ"]:
            if s.endswith(suffix):
                s = s[:-len(suffix)]
        return s

    def close(self):
        self.db.close()
