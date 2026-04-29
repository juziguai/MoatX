"""
risk_controller.py - 持仓风控检测引擎
止损预警 / 仓位上限检查 / 单日最大回撤检查
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Optional

import pandas as pd


@dataclass
class RiskEvent:
    """一次风控事件的统一定格式"""
    level: Literal["warning", "critical"]
    event_type: Literal[
        "stop_loss", "position_limit", "total_position", "daily_drawdown"
    ]
    symbol: Optional[str]        # None = 全局事件
    message: str
    triggered_value: float      # 实际触发值（例：35.5 即 35.5%）
    threshold_value: float      # 阈值（例：30.0 即 30%）

    def to_dict(self) -> dict:
        return {
            "level": self.level,
            "type": self.event_type,
            "symbol": self.symbol,
            "message": self.message,
            "triggered_value": self.triggered_value,
            "threshold_value": self.threshold_value,
        }


class RiskController:
    """
    持仓风控检测。

    用法：
        rc = RiskController(db, alerter)
        events = rc.check_and_alert(holdings, quotes, cfg().risk_control)
    """

    def __init__(self, db: sqlite3.Connection, alerter: Optional[object] = None):
        self.db = db
        self.alerter = alerter

    def check_all(
        self,
        holdings: pd.DataFrame,
        quotes: dict[str, dict],
        risk_cfg,  # RiskControlSettings instance
    ) -> list[RiskEvent]:
        """
        执行全部风控检查。

        Args:
            holdings: Portfolio.list_holdings() 返回的 DataFrame
            quotes: {symbol: {price, ...}} 实时行情
            risk_cfg: RiskControlSettings 实例（从 cfg().risk_control 传入）

        Returns:
            RiskEvent 列表（无预警时为空列表）
        """
        events: list[RiskEvent] = []

        if holdings.empty:
            return events

        # ── 1. 止损检查（浮亏超限）─────────────────────────
        events += self._check_stop_loss(holdings, quotes, risk_cfg)

        # ── 2. 单只仓位上限检查 ─────────────────────────────
        events += self._check_position_limit(holdings, risk_cfg)

        # ── 3. 总仓位上限检查 ───────────────────────────────
        events += self._check_total_position(holdings, risk_cfg)

        # ── 4. 单日回撤检查 ────────────────────────────────
        events += self._check_daily_drawdown(holdings, risk_cfg)

        return events

    def check_and_alert(
        self,
        holdings: pd.DataFrame,
        quotes: dict[str, dict],
        risk_cfg,
    ) -> list[RiskEvent]:
        """
        执行风控检查并发送告警。

        Args:
            holdings: Portfolio.list_holdings() 返回的 DataFrame
            quotes: {symbol: {price, change_pct, ...}} 实时行情
            risk_cfg: RiskControlSettings 实例

        Returns:
            RiskEvent 列表
        """
        events = self.check_all(holdings, quotes, risk_cfg)
        if events and self.alerter is not None:
            for event in events:
                report = self._format_event(event, holdings, quotes)
                level_str = "CRITICAL" if event.level == "critical" else "WARNING"
                self.alerter.send(report, title=f"🚨 风控预警 [{level_str}]")
        return events

    def _format_event(
        self,
        event: RiskEvent,
        holdings: pd.DataFrame,
        quotes: dict[str, dict],
    ) -> str:
        """将 RiskEvent 格式化为告警文本。"""
        event_type_names = {
            "stop_loss": "止损预警",
            "position_limit": "仓位超限",
            "total_position": "总仓位超限",
            "daily_drawdown": "当日回撤",
        }
        type_name = event_type_names.get(event.event_type, event.event_type)

        sym = event.symbol
        quote = quotes.get(sym, {}) if sym else {}

        # 基础信息
        lines = [
            f"**事件类型：** {type_name}",
            f"**股票代码：** {sym or '全局'}",
        ]

        # 持仓详情（如果有）
        if sym:
            name = ""
            cost = 0.0
            shares = 0.0
            for _, row in holdings.iterrows():
                if row.get("symbol") == sym:
                    name = row.get("name", "")
                    cost = float(row.get("cost_price", 0))
                    shares = float(row.get("shares", 0))
                    break

            if name:
                lines.append(f"**股票名称：** {name}")
            if cost > 0:
                lines.append(f"**持仓成本：** {cost:.2f} 元")
            if shares > 0:
                lines.append(f"**持仓数量：** {shares:.0f} 股")

        # 实时行情
        if quote:
            price = quote.get("price", 0)
            change = quote.get("change_pct", 0)
            if price > 0:
                lines.append(f"**当前价格：** {price:.2f} 元 ({change:+.2f}%)")

        # 亏损/超限详情
        lines.append(f"**触发值：** {event.triggered_value:.2f}%")
        lines.append(f"**阈值：** {event.threshold_value:.2f}%")

        # 消息描述
        lines.append(f"**描述：** {event.message}")

        return "\n".join(lines)

    def _check_stop_loss(
        self,
        holdings: pd.DataFrame,
        quotes: dict[str, dict],
        risk_cfg,
    ) -> list[RiskEvent]:
        """持仓成本 vs 当前价，计算浮亏比例是否触发止损"""
        events = []
        for _, row in holdings.iterrows():
            sym = row["symbol"]
            cost = float(row.get("cost_price", 0))
            shares = float(row.get("shares", 0))
            if cost <= 0 or shares <= 0:
                continue

            # 个性化止损阈值（per-symbol overrides）
            stop_pct = float(row.get("stop_loss_pct", 0)) or risk_cfg.stop_loss_pct
            if stop_pct <= 0:
                stop_pct = risk_cfg.stop_loss_pct

            quote = quotes.get(sym, {})
            current_price = float(quote.get("price", 0))
            if current_price <= 0:
                continue

            loss_pct = (current_price - cost) / cost * 100  # 负数 = 亏损

            if loss_pct <= -abs(stop_pct):
                triggered = abs(loss_pct)
                events.append(RiskEvent(
                    level="critical" if loss_pct <= -abs(stop_pct) * 1.5 else "warning",
                    event_type="stop_loss",
                    symbol=sym,
                    message=(
                        f"止损预警 [{sym}] {row.get('name', '')}："
                        f"成本 {cost:.2f}，现价 {current_price:.2f}，"
                        f"浮亏 {loss_pct:.2f}%（阈值 -{stop_pct:.1f}%）"
                    ),
                    triggered_value=triggered,
                    threshold_value=stop_pct,
                ))
        return events

    def _check_position_limit(
        self,
        holdings: pd.DataFrame,
        risk_cfg,
    ) -> list[RiskEvent]:
        """单只股票仓位占比是否超过上限"""
        events = []
        if "market_value" not in holdings.columns or holdings.empty:
            return events

        total_mv = holdings["market_value"].sum()
        if total_mv <= 0:
            return events

        for _, row in holdings.iterrows():
            sym = row["symbol"]
            mv = float(row.get("market_value", 0))
            # 个性化仓位上限
            limit_pct = float(row.get("position_limit_pct", 0)) or risk_cfg.max_single_position_pct
            if limit_pct <= 0:
                limit_pct = risk_cfg.max_single_position_pct

            position_pct = mv / total_mv * 100

            if position_pct > limit_pct:
                events.append(RiskEvent(
                    level="critical" if position_pct > limit_pct * 1.2 else "warning",
                    event_type="position_limit",
                    symbol=sym,
                    message=(
                        f"仓位超限 [{sym}] {row.get('name', '')}："
                        f"占比 {position_pct:.1f}%（上限 {limit_pct:.1f}%）"
                    ),
                    triggered_value=position_pct,
                    threshold_value=limit_pct,
                ))
        return events

    def _check_total_position(
        self,
        holdings: pd.DataFrame,
        risk_cfg,
    ) -> list[RiskEvent]:
        """总仓位检查（需外部传入 position_ratio，暂不实现）"""
        # 总仓位精确检查依赖：总市值 + 可用现金 → 总资产
        # 这类数据从 daily_assets 表读取，由调用方在 scheduler 层提供
        # 占位：后续可在 RiskController 构造时传入 available_cash / total_asset
        return []

    def _check_daily_drawdown(
        self,
        holdings: pd.DataFrame,
        risk_cfg,
    ) -> list[RiskEvent]:
        """当日亏损是否超过阈值（需要 daily_pnl 数据）"""
        events = []
        if holdings.empty:
            return events

        # daily_pnl_val 在 list_holdings() 时已 JOIN 进 DataFrame
        if "daily_pnl_val" not in holdings.columns:
            return events

        today_total_loss = holdings["daily_pnl_val"].sum()
        if today_total_loss >= 0:
            return events  # 当日盈利或持平，不触发

        # 需要总资产来算亏损比例；从 daily_assets 取最新一条
        row = self.db.execute(
            "SELECT total_asset FROM daily_assets ORDER BY date DESC LIMIT 1"
        ).fetchone()
        if not row:
            return events

        total_asset = float(row[0])
        if total_asset <= 0:
            return events

        loss_pct = abs(today_total_loss) / total_asset * 100
        max_daily = risk_cfg.max_daily_loss_pct

        if loss_pct > max_daily:
            events.append(RiskEvent(
                level="critical" if loss_pct > max_daily * 1.5 else "warning",
                event_type="daily_drawdown",
                symbol=None,  # 全局事件
                message=(
                    f"当日回撤预警：总亏损 {today_total_loss:.2f} 元，"
                    f"占总资产 {loss_pct:.2f}%（阈值 {max_daily:.1f}%）"
                ),
                triggered_value=loss_pct,
                threshold_value=max_daily,
            ))
        return events

    # ── 事件记录 ────────────────────────────────────────────

    def log_event(self, event: RiskEvent) -> None:
        """写入 risk_events 表"""
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        today = datetime.now().strftime("%Y-%m-%d")
        self.db.execute("""
            INSERT INTO risk_events
            (date, symbol, event_type, level, triggered_value, threshold_value, message, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            today,
            event.symbol or "",
            event.event_type,
            event.level,
            event.triggered_value,
            event.threshold_value,
            event.message,
            now,
        ))
        self.db.commit()

    def log_events(self, events: list[RiskEvent]) -> None:
        """批量写入事件（全部在同一事务内）"""
        if not events:
            return
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        today = datetime.now().strftime("%Y-%m-%d")
        for event in events:
            self.db.execute("""
                INSERT INTO risk_events
                (date, symbol, event_type, level, triggered_value, threshold_value, message, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                today,
                event.symbol or "",
                event.event_type,
                event.level,
                event.triggered_value,
                event.threshold_value,
                event.message,
                now,
            ))
        self.db.commit()

    def get_event_history(self, limit: int = 50) -> pd.DataFrame:
        """查询风控事件历史"""
        return pd.read_sql(
            "SELECT * FROM risk_events ORDER BY created_at DESC LIMIT ?",
            self.db, params=(limit,)
        )
