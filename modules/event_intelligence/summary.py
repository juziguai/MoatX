"""Compact event intelligence summary for intraday dashboards."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd

from modules.config import cfg
from modules.db import DatabaseManager

from .models import now_ts


def build_event_monitor_summary(
    *,
    db: DatabaseManager | None = None,
    top_n: int | None = None,
    probability_threshold: float | None = None,
    opportunity_threshold: float | None = None,
) -> dict[str, Any]:
    """Return Top-N macro events with related sectors and opportunity stocks."""
    settings = cfg().event_intelligence
    if not settings.monitor_enabled:
        return {
            "enabled": False,
            "generated_at": now_ts(),
            "top_events": [],
            "message": "event intelligence monitor disabled",
        }

    top_n = int(top_n or settings.monitor_top_events)
    probability_threshold = (
        settings.notify_probability_threshold
        if probability_threshold is None
        else float(probability_threshold)
    )
    opportunity_threshold = (
        settings.notify_opportunity_threshold
        if opportunity_threshold is None
        else float(opportunity_threshold)
    )

    owns_db = db is None
    db = db or DatabaseManager(cfg().data.warehouse_path)
    try:
        event_store = db.event()
        states = event_store.list_states(limit=100)
        opportunities = event_store.list_opportunities(limit=500)
        notifications = event_store.list_notifications(limit=100)
        rows = _top_events(
            states=states,
            opportunities=opportunities,
            top_n=top_n,
            probability_threshold=probability_threshold,
            opportunity_threshold=opportunity_threshold,
        )
        return {
            "enabled": True,
            "generated_at": now_ts(),
            "thresholds": {
                "probability": probability_threshold,
                "opportunity_score": opportunity_threshold,
            },
            "stale": _is_stale(states),
            "latest_state_at": _latest_ts(states, "updated_at"),
            "notifications": _records(notifications.head(5) if not notifications.empty else notifications),
            "top_events": rows,
        }
    finally:
        if owns_db:
            db.close()


def format_event_monitor_summary(summary: dict[str, Any]) -> list[str]:
    """Format monitor summary into short human-readable lines."""
    if not summary.get("enabled", True):
        return ["宏观事件：监控未启用"]
    top_events = summary.get("top_events") or []
    if not top_events:
        return ["宏观事件：暂无达到阈值的事件机会"]

    stale = "（数据可能偏旧）" if summary.get("stale") else ""
    lines = [f"宏观事件 Top{len(top_events)}{stale}："]
    for idx, item in enumerate(top_events, start=1):
        sectors = "、".join(item.get("sectors") or []) or "暂无板块"
        stocks = "、".join(
            f"{op.get('name') or op.get('symbol')}({float(op.get('score') or 0):.1f})"
            for op in (item.get("opportunities") or [])[:3]
        ) or "暂无标的"
        mark = "提醒" if item.get("alert") else "观察"
        lines.append(
            f"{idx}. {item.get('name') or item.get('event_id')} "
            f"P={float(item.get('probability') or 0):.2f} "
            f"机会={float(item.get('opportunity_score') or 0):.1f} "
            f"[{mark}] | 板块：{sectors} | 标的：{stocks}"
        )
    return lines


def _top_events(
    *,
    states: pd.DataFrame,
    opportunities: pd.DataFrame,
    top_n: int,
    probability_threshold: float,
    opportunity_threshold: float,
) -> list[dict[str, Any]]:
    if states.empty:
        return []

    opportunities_by_event: dict[str, list[dict[str, Any]]] = {}
    if not opportunities.empty:
        normalized = opportunities.where(pd.notna(opportunities), None)
        for _, row in normalized.iterrows():
            event_id = str(row.get("event_id") or "")
            if not event_id:
                continue
            opportunities_by_event.setdefault(event_id, []).append(
                {
                    "symbol": str(row.get("symbol") or ""),
                    "name": str(row.get("name") or ""),
                    "score": float(row.get("opportunity_score") or 0.0),
                    "sector_tags": _split_tags(row.get("sector_tags")),
                    "recommendation": str(row.get("recommendation") or ""),
                }
            )

    rows: list[dict[str, Any]] = []
    normalized_states = states.where(pd.notna(states), None)
    for _, row in normalized_states.iterrows():
        event_id = str(row.get("event_id") or "")
        event_opportunities = sorted(
            opportunities_by_event.get(event_id, []),
            key=lambda item: item["score"],
            reverse=True,
        )
        opportunity_score = event_opportunities[0]["score"] if event_opportunities else 0.0
        probability = float(row.get("probability") or 0.0)
        alert = probability >= probability_threshold or opportunity_score >= opportunity_threshold
        sectors: list[str] = []
        for item in event_opportunities:
            for tag in item.get("sector_tags") or []:
                if tag and tag not in sectors:
                    sectors.append(tag)
        rows.append(
            {
                "event_id": event_id,
                "name": str(row.get("name") or event_id),
                "probability": probability,
                "impact_strength": float(row.get("impact_strength") or 0.0),
                "status": str(row.get("status") or ""),
                "evidence_count": int(row.get("evidence_count") or 0),
                "sources_count": int(row.get("sources_count") or 0),
                "last_signal_at": str(row.get("last_signal_at") or ""),
                "updated_at": str(row.get("updated_at") or ""),
                "opportunity_score": opportunity_score,
                "alert": alert,
                "sectors": sectors[:6],
                "opportunities": event_opportunities[:5],
            }
        )

    rows.sort(
        key=lambda item: (
            item["alert"],
            item["opportunity_score"],
            item["probability"],
            item["impact_strength"],
        ),
        reverse=True,
    )
    return rows[:top_n]


def _split_tags(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    return [item.strip() for item in str(value).replace("，", ",").split(",") if item.strip()]


def _records(df: pd.DataFrame) -> list[dict[str, Any]]:
    if df.empty:
        return []
    return df.where(pd.notna(df), None).to_dict(orient="records")


def _latest_ts(df: pd.DataFrame, column: str) -> str:
    if df.empty or column not in df:
        return ""
    values = [str(value) for value in df[column].dropna().tolist() if str(value)]
    return max(values) if values else ""


def _is_stale(states: pd.DataFrame) -> bool:
    latest = _latest_ts(states, "updated_at")
    if not latest:
        return False
    try:
        latest_dt = datetime.strptime(latest, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return False
    return (datetime.now() - latest_dt).total_seconds() > 60 * 60
