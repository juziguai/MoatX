"""Rule-based event signal extraction."""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import Any

from modules.config import cfg
from modules.db import DatabaseManager

from .models import EventDefinition, EventSignal
from .transmission import EventTransmissionMap

_logger = logging.getLogger("moatx.event_intelligence.extractor")


_ESCALATION_WORDS = [
    "升级",
    "扩大",
    "加剧",
    "升温",
    "紧张",
    "封锁",
    "袭击",
    "扣押",
    "制裁",
    "报复",
    "部署",
    "威胁",
    "中断",
    "禁运",
    "close",
    "closure",
    "blockade",
    "attack",
    "seize",
    "sanction",
    "escalate",
    "threaten",
]
_CONFIRMED_WORDS = [
    "已",
    "已经",
    "确认",
    "宣布",
    "下令",
    "实施",
    "发生",
    "导致",
    "confirmed",
    "announced",
    "ordered",
    "implemented",
]
_RUMOR_WORDS = [
    "传闻",
    "据称",
    "或",
    "可能",
    "考虑",
    "不排除",
    "计划",
    "rumor",
    "may",
    "might",
    "could",
    "consider",
]
_NEGATION_WORDS = [
    "否认",
    "澄清",
    "不实",
    "未",
    "没有",
    "不会",
    "不认为",
    "无关",
    "辟谣",
    "deny",
    "denies",
    "denied",
    "not",
    "no plan",
]
_RELIEF_WORDS = [
    "恢复",
    "重开",
    "缓和",
    "停火",
    "和谈",
    "撤离",
    "解除",
    "reopen",
    "restore",
    "ceasefire",
    "ease",
    "de-escalate",
]
_TIME_URGENT_WORDS = [
    "刚刚",
    "突发",
    "最新",
    "今日",
    "当天",
    "目前",
    "now",
    "breaking",
    "latest",
    "today",
]
_TIME_STALE_WORDS = [
    "回顾",
    "历史上",
    "曾经",
    "去年",
    "多年前",
    "复盘",
    "review",
    "historical",
    "last year",
]
_INTENSITY_HIGH_WORDS = [
    "严重",
    "大规模",
    "全面",
    "极为",
    "重大",
    "危机",
    "暴涨",
    "飙升",
    "severe",
    "major",
    "massive",
    "crisis",
]
_INTENSITY_LOW_WORDS = [
    "小幅",
    "有限",
    "局部",
    "短暂",
    "minor",
    "limited",
    "temporary",
]


class EventExtractor:
    """Extract structured EventSignal records from unprocessed news."""

    def __init__(
        self,
        db: DatabaseManager | None = None,
        transmission_map: EventTransmissionMap | None = None,
    ):
        self._db = db or DatabaseManager(cfg().data.warehouse_path)
        self._map = transmission_map or EventTransmissionMap()

    def extract_unprocessed(self, limit: int = 200) -> dict[str, Any]:
        """Extract event signals from unprocessed news rows."""
        news_df = self._db.event().list_news(limit=limit, processed=0)
        stats = {"news": len(news_df), "signals": 0, "processed": 0, "stale_skipped": 0, "errors": []}
        processed_ids: list[int] = []

        for _, row in news_df.iterrows():
            news_id = int(row["id"])
            try:
                published_at = self._clean_text(row.get("published_at", ""))
                news_age_days = self._news_age_days(published_at)
                if self._is_stale_news(published_at):
                    processed_ids.append(news_id)
                    stats["stale_skipped"] += 1
                    continue

                text = f"{row.get('title', '')}\n{row.get('summary', '')}"
                signals = self.extract_text(
                    text,
                    news_id=news_id,
                    source=str(row.get("source", "")),
                    published_at=published_at,
                    news_age_days=news_age_days,
                )
                for signal in signals:
                    self._db.event().insert_signal(signal)
                    stats["signals"] += 1
                processed_ids.append(news_id)
            except Exception as exc:
                _logger.warning("extract news [%s] failed: %s", news_id, exc)
                stats["errors"].append(f"{news_id}: {exc}")

        self._db.event().mark_news_processed(processed_ids)
        source_ids = sorted({str(row.get("source", "")) for _, row in news_df.iterrows() if row.get("source", "")})
        self._db.event().refresh_source_quality_signal_hits(source_ids)
        stats["processed"] = len(processed_ids)
        return stats

    def extract_text(
        self,
        text: str,
        news_id: int | None = None,
        source: str = "",
        published_at: str = "",
        news_age_days: float | None = None,
    ) -> list[EventSignal]:
        """Extract signals from arbitrary text."""
        signals: list[EventSignal] = []
        for event in self._map.match_text(text):
            signal = self._build_signal(
                event,
                text,
                news_id=news_id,
                source=source,
                published_at=published_at,
                news_age_days=news_age_days,
            )
            if signal is not None:
                signals.append(signal)
        return signals

    def _build_signal(
        self,
        event: EventDefinition,
        text: str,
        news_id: int | None = None,
        source: str = "",
        published_at: str = "",
        news_age_days: float | None = None,
    ) -> EventSignal | None:
        keywords = self._map.matched_keywords(text, event)
        actions = self._map.matched_actions(text, event)
        if not keywords:
            return None

        stage_info = self._classify_stage(text, keywords, actions)
        severity = (
            self._severity(event, keywords, actions)
            * stage_info["severity_multiplier"]
            * stage_info["time_multiplier"]
            * stage_info["intensity_multiplier"]
        )
        confidence = self._confidence(keywords, actions) * stage_info["confidence_multiplier"]

        if not actions and len(keywords) < 2:
            severity *= 0.4
            confidence *= 0.5

        direction = "neutral"
        bullish = sum(1 for effect in event.effects if effect.direction == "bullish")
        bearish = sum(1 for effect in event.effects if effect.direction == "bearish")
        if bullish > bearish:
            direction = "bullish"
        elif bearish > bullish:
            direction = "bearish"
        if stage_info["stage"] in {"denied", "resolved"}:
            direction = "neutral"

        entities = {
            "source": source,
            "event_name": event.name,
            "effect_targets": [effect.target for effect in event.effects],
            "stage": stage_info["stage"],
            "stage_score": stage_info["stage_score"],
            "polarity": stage_info["polarity"],
            "matched_stage_words": stage_info["matched_stage_words"],
            "time_sensitivity": stage_info["time_sensitivity"],
            "intensity": stage_info["intensity"],
        }
        if published_at:
            entities["published_at"] = published_at
        if news_age_days is not None:
            entities["news_age_days"] = round(news_age_days, 3)

        return EventSignal(
            event_id=event.id,
            news_id=news_id,
            event_type=event.event_types[0] if event.event_types else "",
            entities=entities,
            matched_keywords=keywords,
            matched_actions=actions,
            severity=round(min(1.0, max(0.0, severity)), 3),
            confidence=round(min(1.0, max(0.0, confidence)), 3),
            direction=direction,
        )

    @staticmethod
    def _severity(
        event: EventDefinition,
        keywords: list[str],
        actions: list[str],
    ) -> float:
        return (
            event.base_probability
            + min(len(keywords), 5) * 0.08
            + min(len(actions), 4) * 0.14
            + max((effect.impact for effect in event.effects), default=0.0) * 0.25
        )

    @staticmethod
    def _confidence(keywords: list[str], actions: list[str]) -> float:
        return 0.25 + min(len(keywords), 5) * 0.08 + min(len(actions), 4) * 0.12

    @classmethod
    def _classify_stage(
        cls,
        text: str,
        keywords: list[str],
        actions: list[str],
    ) -> dict[str, Any]:
        """Classify whether an event mention is rumor, escalation, confirmed, denied, or easing."""
        content = str(text or "").lower()
        matched_negation = cls._matched_words(content, _NEGATION_WORDS)
        matched_relief = cls._matched_words(content, _RELIEF_WORDS)
        matched_confirmed = cls._matched_words(content, _CONFIRMED_WORDS)
        matched_escalation = cls._matched_words(content, _ESCALATION_WORDS)
        matched_rumor = cls._matched_words(content, _RUMOR_WORDS)
        time_profile = cls._time_profile(content)
        intensity_profile = cls._intensity_profile(content)

        if matched_negation and cls._near_event_terms(content, keywords + actions, matched_negation):
            return cls._with_profiles({
                "stage": "denied",
                "stage_score": -0.7,
                "polarity": "negative",
                "severity_multiplier": 0.25,
                "confidence_multiplier": 0.65,
                "matched_stage_words": matched_negation,
            }, time_profile, intensity_profile)
        if matched_relief and not matched_escalation:
            return cls._with_profiles({
                "stage": "resolved",
                "stage_score": -0.45,
                "polarity": "easing",
                "severity_multiplier": 0.4,
                "confidence_multiplier": 0.8,
                "matched_stage_words": matched_relief,
            }, time_profile, intensity_profile)
        if matched_confirmed and (matched_escalation or actions):
            return cls._with_profiles({
                "stage": "confirmed",
                "stage_score": 1.0,
                "polarity": "positive",
                "severity_multiplier": 1.2,
                "confidence_multiplier": 1.15,
                "matched_stage_words": matched_confirmed + matched_escalation,
            }, time_profile, intensity_profile)
        if matched_escalation or actions:
            return cls._with_profiles({
                "stage": "escalating",
                "stage_score": 0.75,
                "polarity": "positive",
                "severity_multiplier": 1.0,
                "confidence_multiplier": 1.0,
                "matched_stage_words": matched_escalation,
            }, time_profile, intensity_profile)
        if matched_rumor:
            return cls._with_profiles({
                "stage": "rumor",
                "stage_score": 0.35,
                "polarity": "uncertain",
                "severity_multiplier": 0.65,
                "confidence_multiplier": 0.75,
                "matched_stage_words": matched_rumor,
            }, time_profile, intensity_profile)
        return cls._with_profiles({
            "stage": "watching",
            "stage_score": 0.2,
            "polarity": "neutral",
            "severity_multiplier": 0.8,
            "confidence_multiplier": 0.85,
            "matched_stage_words": [],
        }, time_profile, intensity_profile)

    @staticmethod
    def _matched_words(content_lower: str, words: list[str]) -> list[str]:
        return [word for word in words if str(word).lower() in content_lower]

    @staticmethod
    def _near_event_terms(content_lower: str, event_terms: list[str], stage_words: list[str]) -> bool:
        terms = [str(term).lower() for term in event_terms if str(term)]
        if not terms:
            return True
        for stage_word in stage_words:
            stage = str(stage_word).lower()
            for term in terms:
                pattern = rf"(.{{0,18}}{re.escape(stage)}.{{0,18}}{re.escape(term)})|(.{{0,18}}{re.escape(term)}.{{0,18}}{re.escape(stage)})"
                if re.search(pattern, content_lower, re.S):
                    return True
        return False

    @classmethod
    def _is_stale_news(cls, published_at: str) -> bool:
        """Return True when a dated news item is too old for current-event extraction."""
        age_days = cls._news_age_days(published_at)
        if age_days is None:
            return False
        return age_days > cfg().event_intelligence.max_news_age_days

    @classmethod
    def _news_age_days(cls, published_at: str) -> float | None:
        published_dt = cls._parse_published_at(published_at)
        if published_dt is None:
            return None
        if published_dt.tzinfo is not None:
            published_dt = published_dt.astimezone().replace(tzinfo=None)
        age_seconds = (datetime.now() - published_dt).total_seconds()
        return max(0.0, age_seconds / 86400)

    @classmethod
    def _parse_published_at(cls, value: Any) -> datetime | None:
        text = cls._clean_text(value)
        if not text:
            return None

        try:
            return parsedate_to_datetime(text)
        except (TypeError, ValueError, IndexError, OverflowError):
            pass

        normalized = text.replace("年", "-").replace("月", "-").replace("日", "")
        normalized = normalized.replace("/", "-").replace("T", " ").replace("Z", "+00:00").strip()
        match = re.search(r"20\d{2}-\d{1,2}-\d{1,2}(?:\s+\d{1,2}:\d{2}(?::\d{2})?)?(?:[+-]\d{2}:?\d{2})?", normalized)
        if match:
            normalized = match.group(0)

        for fmt in (
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d",
            "%Y-%m-%d %H:%M:%S%z",
            "%Y-%m-%d %H:%M%z",
        ):
            try:
                return datetime.strptime(normalized, fmt)
            except ValueError:
                continue
        try:
            return datetime.fromisoformat(normalized)
        except ValueError:
            return None

    @staticmethod
    def _clean_text(value: Any) -> str:
        if value is None:
            return ""
        text = str(value).strip()
        return "" if text.lower() in {"nan", "nat", "none", "null"} else text

    @classmethod
    def _time_profile(cls, content_lower: str) -> dict[str, Any]:
        urgent = cls._matched_words(content_lower, _TIME_URGENT_WORDS)
        stale = cls._matched_words(content_lower, _TIME_STALE_WORDS)
        if urgent and not stale:
            return {"time_sensitivity": "urgent", "time_multiplier": 1.08}
        if stale:
            return {"time_sensitivity": "stale", "time_multiplier": 0.65}
        return {"time_sensitivity": "current", "time_multiplier": 1.0}

    @classmethod
    def _intensity_profile(cls, content_lower: str) -> dict[str, Any]:
        high = cls._matched_words(content_lower, _INTENSITY_HIGH_WORDS)
        low = cls._matched_words(content_lower, _INTENSITY_LOW_WORDS)
        if high and not low:
            return {"intensity": "high", "intensity_multiplier": 1.12}
        if low:
            return {"intensity": "low", "intensity_multiplier": 0.75}
        return {"intensity": "normal", "intensity_multiplier": 1.0}

    @staticmethod
    def _with_profiles(
        stage_info: dict[str, Any],
        time_profile: dict[str, Any],
        intensity_profile: dict[str, Any],
    ) -> dict[str, Any]:
        out = dict(stage_info)
        out.update(time_profile)
        out.update(intensity_profile)
        return out


def extract_events(limit: int = 200) -> dict[str, Any]:
    """Convenience entry point for scheduler/CLI."""
    return EventExtractor().extract_unprocessed(limit=limit)
