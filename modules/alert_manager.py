"""
alert_manager.py - 持仓预警检测引擎
"""
from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any

import pandas as pd


class AlertManager:
    """持仓预警检测，对接 MoatXAnalyzer"""

    def __init__(self, db: sqlite3.Connection, analyzer: Any = None) -> None:
        """
        Args:
            db: SQLite 数据库连接
            analyzer: 可选的 MoatXAnalyzer 实例（由外部共享注入，避免重复创建 StockData 缓存）。
                     不传时内部创建（向后兼容）。
        """
        self.db: sqlite3.Connection = db
        self._analyzer = analyzer

    def check_alerts(
        self, holdings: pd.DataFrame, max_workers: int = 6
    ) -> list[dict[str, Any]]:
        """
        对所有持仓并行运行 analyze()，检测预警条件。

        Args:
            holdings: Portfolio.list_holdings() 返回的 DataFrame
            max_workers: 并行线程数
        """
        from modules.analyzer import MoatXAnalyzer

        alerts: list[dict[str, Any]] = []
        if holdings.empty:
            return alerts

        symbols: list[str] = holdings["symbol"].tolist()
        # 复用注入的 analyzer，避免每次创建新的 StockData 实例
        analyzer: MoatXAnalyzer = self._analyzer if self._analyzer is not None else MoatXAnalyzer()

        def analyze_one(symbol: str) -> tuple[str, dict[str, Any]]:
            report: dict[str, Any] = analyzer.analyze(symbol)
            return symbol, report

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(analyze_one, sym): sym for sym in symbols}
            for future in as_completed(futures):
                symbol: str = futures[future]
                try:
                    _, report = future.result()
                    detected: list[dict[str, Any]] = self._detect_alerts(report)
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

    def check_paper_alerts(self, paper_holdings: pd.DataFrame, max_workers: int = 6) -> list[dict[str, Any]]:
        """Alias for check_alerts that accepts paper_holdings DataFrame.

        paper_holdings comes from db.signal().all_paper_holdings() and has the
        same 'symbol' column that check_alerts() expects.
        """
        return self.check_alerts(paper_holdings, max_workers=max_workers)

    def get_alert_history(self, limit: int = 50) -> pd.DataFrame:
        df: pd.DataFrame = pd.read_sql(
            "SELECT * FROM alert_log ORDER BY created_at DESC LIMIT ?",
            self.db, params=(limit,)
        )
        return df

    # ---------- 内部方法 ----------

    def _num(self, value: Any, default: Any | None = None) -> float | Any | None:
        if value is None:
            return default
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def _detect_alerts(self, report: dict[str, Any]) -> list[dict[str, Any]]:
        alerts: list[dict[str, Any]] = []
        kdj: dict[str, Any] = report.get("kdj", {})
        rsi: dict[str, Any] = report.get("rsi", {})
        boll: dict[str, Any] = report.get("boll", {})
        signals: dict[str, Any] = report.get("signals", {})
        price: float | int | dict[str, Any] = report.get("price", 0)
        if isinstance(price, dict):
            price = price.get("current", 0)
        j: float | Any | None = self._num(kdj.get("j"))
        rsi12: float | Any | None = self._num(rsi.get("rsi12"))
        boll_pos: float | Any | None = self._num(boll.get("position"))
        kdj["j"] = j
        rsi["rsi12"] = rsi12
        boll["position"] = boll_pos

        if j is not None and j > 90:
            alerts.append({
                "type": "kdj_overbought",
                "msg": f"KDJ 超买 J={kdj['j']:.1f}，现价 {price}元"
            })
        if j is not None and j < 10:
            alerts.append({
                "type": "kdj_oversold",
                "msg": f"KDJ 超卖 J={kdj['j']:.1f}，现价 {price}元"
            })
        if rsi12 is not None and rsi12 > 75:
            alerts.append({
                "type": "rsi_overbought",
                "msg": f"RSI12 超买 {rsi12:.1f}，现价 {price}元"
            })
        if rsi12 is not None and rsi12 < 30:
            alerts.append({
                "type": "rsi_oversold",
                "msg": f"RSI12 超卖 {rsi12:.1f}，现价 {price}元"
            })
        if boll_pos is not None:
            if boll_pos > 0.95:
                alerts.append({
                    "type": "boll_upper",
                    "msg": f"布林带上轨突破 {boll_pos:.0%}，现价 {price}元"
                })
            elif boll_pos < 0.05:
                alerts.append({
                    "type": "boll_lower",
                    "msg": f"布林带下轨支撑 {boll_pos:.0%}，现价 {price}元"
                })

        risk: Any = signals.get("risk_level", "")
        if risk in ("high", "极高"):
            alerts.append({
                "type": "risk_high",
                "msg": f"风险等级: {risk}，现价 {price}元"
            })

        # 综合评分
        score: float | Any | None = self._num(signals.get("composite_score"), 50)
        if score < 25:
            alerts.append({
                "type": "low_score",
                "msg": f"综合评分低 {score}分"
            })

        # MACD 信号
        macd_sig: Any = signals.get("macd_signal", "")
        if "死叉" in macd_sig:
            alerts.append({"type": "macd_death_cross", "msg": "MACD 死叉信号"})
        elif "金叉" in macd_sig:
            alerts.append({"type": "macd_golden_cross", "msg": "MACD 金叉信号"})

        return alerts

    def _log_alert(self, alert: dict[str, Any]) -> None:
        now: str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.db.execute("""
            INSERT INTO alert_log (symbol, alert_type, message, created_at)
            VALUES (?, ?, ?, ?)
        """, (alert.get("symbol", ""), alert.get("type", ""),
              alert.get("msg", ""), now))
        self.db.commit()
