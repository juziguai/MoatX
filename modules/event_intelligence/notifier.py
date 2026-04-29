"""Notification and cooldown logic for event intelligence reports."""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta
from typing import Any

import pandas as pd

from modules.alerter import Alerter
from modules.config import cfg
from modules.db import DatabaseManager

from .models import now_ts
from .reporter import EventReporter


class EventNotifier:
    """Decide whether to send event reports and maintain cooldown state."""

    def __init__(
        self,
        db: DatabaseManager | None = None,
        reporter: EventReporter | None = None,
        alerter: Alerter | None = None,
    ):
        self._db = db or DatabaseManager(cfg().data.warehouse_path)
        self._reporter = reporter or EventReporter(db=self._db)
        self._alerter = alerter or Alerter()

    def notify(
        self,
        *,
        send: bool = False,
        probability_threshold: float | None = None,
        opportunity_threshold: float | None = None,
        cooldown_hours: int | None = None,
        probability_delta: float | None = None,
        limit: int = 10,
    ) -> dict[str, Any]:
        """Dry-run or send a combined report for events that pass thresholds."""
        settings = cfg().event_intelligence
        probability_threshold = (
            settings.notify_probability_threshold
            if probability_threshold is None
            else probability_threshold
        )
        opportunity_threshold = (
            settings.notify_opportunity_threshold
            if opportunity_threshold is None
            else opportunity_threshold
        )
        cooldown_hours = settings.notify_cooldown_hours if cooldown_hours is None else cooldown_hours
        probability_delta = settings.notify_probability_delta if probability_delta is None else probability_delta
        report = self._reporter.report(limit=limit)
        report_hash = self._hash(report)
        states = self._db.event().list_states(limit=100)
        opportunities = self._db.event().list_opportunities(limit=200)
        candidates = self._eligible_events(
            states=states,
            opportunities=opportunities,
            probability_threshold=probability_threshold,
            opportunity_threshold=opportunity_threshold,
        )

        stats: dict[str, Any] = {
            "dry_run": not send,
            "candidates": len(candidates),
            "sent": 0,
            "skipped": 0,
            "items": [],
        }
        if not candidates:
            return stats

        now = datetime.now()
        eligible_to_send: list[dict[str, Any]] = []
        for candidate in candidates:
            decision = self._decision(
                candidate=candidate,
                report_hash=report_hash,
                now=now,
                probability_delta=probability_delta,
            )
            stats["items"].append(decision)
            if decision["allowed"]:
                eligible_to_send.append(candidate)
            else:
                stats["skipped"] += 1

        if not eligible_to_send:
            return stats

        if not send:
            return stats

        ok = self._alerter.send(report, "MoatX 宏观事件情报")
        status = "sent" if ok else "failed"
        sent_at = now_ts()
        cooldown_until = (now + timedelta(hours=cooldown_hours)).strftime("%Y-%m-%d %H:%M:%S")
        for candidate in eligible_to_send:
            self._db.event().upsert_notification(
                event_id=candidate["event_id"],
                report_hash=report_hash,
                status=status,
                last_sent_at=sent_at if ok else "",
                cooldown_until=cooldown_until if ok else "",
                last_probability=candidate["probability"],
                last_opportunity_score=candidate["opportunity_score"],
            )
        stats["sent"] = 1 if ok else 0
        return stats

    @staticmethod
    def _hash(report: str) -> str:
        return hashlib.sha256(report.encode("utf-8")).hexdigest()

    def _eligible_events(
        self,
        *,
        states: pd.DataFrame,
        opportunities: pd.DataFrame,
        probability_threshold: float,
        opportunity_threshold: float,
    ) -> list[dict[str, Any]]:
        if states.empty:
            return []

        max_opportunity: dict[str, float] = {}
        if not opportunities.empty:
            grouped = opportunities.groupby("event_id")["opportunity_score"].max()
            max_opportunity = {str(k): float(v) for k, v in grouped.items()}

        candidates: list[dict[str, Any]] = []
        for _, row in states.iterrows():
            event_id = str(row.get("event_id") or "")
            probability = float(row.get("probability") or 0.0)
            opportunity_score = max_opportunity.get(event_id, 0.0)
            if probability >= probability_threshold or opportunity_score >= opportunity_threshold:
                candidates.append(
                    {
                        "event_id": event_id,
                        "name": str(row.get("name") or event_id),
                        "probability": probability,
                        "opportunity_score": opportunity_score,
                    }
                )
        return candidates

    def _decision(
        self,
        *,
        candidate: dict[str, Any],
        report_hash: str,
        now: datetime,
        probability_delta: float,
    ) -> dict[str, Any]:
        event_id = candidate["event_id"]
        existing = self._db.event().get_notification(event_id)
        decision = {
            **candidate,
            "allowed": True,
            "reason": "eligible",
        }
        if not existing:
            return decision

        last_hash = str(existing.get("report_hash") or "")
        cooldown_until = self._parse_ts(existing.get("cooldown_until"))
        last_probability = float(existing.get("last_probability") or 0.0)
        probability_rise = candidate["probability"] - last_probability

        if cooldown_until and now < cooldown_until and probability_rise >= probability_delta:
            decision["reason"] = "probability_rise"
            return decision

        if last_hash == report_hash:
            decision["allowed"] = False
            decision["reason"] = "duplicate_report"
            return decision

        if cooldown_until and now < cooldown_until:
            decision["allowed"] = False
            decision["reason"] = "cooldown"
            return decision

        return decision

    @staticmethod
    def _parse_ts(value: Any) -> datetime | None:
        if not value:
            return None
        try:
            return datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None


def notify_event_report(send: bool = False) -> dict[str, Any]:
    """Convenience entry point for CLI/scheduler."""
    return EventNotifier().notify(send=send)
