"""Sector resonance scoring for intraday radar signals."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from modules.sector_tags import SectorTagProvider
from modules.utils import normalize_symbol

from .models import RadarConfig

_GENERIC_TAGS = {"上海主板", "深圳主板", "北京证券交易所", "其他市场"}
_NAME_TAG_HINTS = (
    ("电力", "电力"),
    ("发电", "电力"),
    ("黄金", "黄金"),
    ("贵金属", "贵金属"),
)
_TAG_ALIASES = {
    "黄金": "贵金属",
    "黄金避险": "贵金属",
    "半导体": "芯片",
    "集成电路": "芯片",
    "先进封装": "芯片",
    "存储芯片": "芯片",
    "绿电": "电力",
    "绿色电力": "电力",
    "火电": "电力",
    "公用事业": "电力",
}


@dataclass(slots=True)
class _TagBucket:
    tag: str
    scanned: set[str] = field(default_factory=set)
    signaled: set[str] = field(default_factory=set)
    latest_pcts: list[float] = field(default_factory=list)
    signal_pcts: list[float] = field(default_factory=list)
    signal_scores: list[float] = field(default_factory=list)


class SectorResonanceScorer:
    """Boost signals when multiple stocks in the same theme move together."""

    def __init__(
        self,
        config: RadarConfig | None = None,
        *,
        sector_provider: SectorTagProvider | None = None,
    ) -> None:
        self.config = config or RadarConfig()
        self.sector_provider = sector_provider or SectorTagProvider(enable_live_bulk=False)

    def apply(self, *, results: list[dict[str, Any]], signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not results or not signals:
            return []

        latest_pct_by_code, name_by_code = self._latest_pct_and_names(results)
        signal_by_code = {
            self._code(signal.get("symbol")): signal
            for signal in signals
            if self._code(signal.get("symbol"))
        }
        codes = sorted(set(latest_pct_by_code) | set(signal_by_code))
        tags_by_code = {code: self._tags_for_code(code, name=name_by_code.get(code, "")) for code in codes}
        buckets = self._build_buckets(
            tags_by_code=tags_by_code,
            latest_pct_by_code=latest_pct_by_code,
            signal_by_code=signal_by_code,
        )
        resonance = self._summaries(buckets, name_by_code=name_by_code, signal_by_code=signal_by_code)
        resonance_by_tag = {str(row.get("tag")): row for row in resonance}

        for signal in signals:
            code = self._code(signal.get("symbol"))
            metrics = signal.setdefault("metrics", {})
            tags = tags_by_code.get(code, [])
            metrics["sector_tags"] = tags
            candidates = [resonance_by_tag[tag] for tag in tags if tag in resonance_by_tag]
            if not candidates:
                metrics["sector_boost"] = 0.0
                continue
            best = max(
                candidates,
                key=lambda row: (
                    float(row.get("boost") or 0.0),
                    int(row.get("signal_count") or 0),
                    float(row.get("avg_signal_pct") or 0.0),
                ),
            )
            boost = float(best.get("boost") or 0.0)
            if boost <= 0:
                metrics["sector_boost"] = 0.0
                continue
            signal["score"] = round(float(signal.get("score") or 0.0) + boost, 1)
            signal["level"] = _level(float(signal.get("score") or 0.0), float(signal.get("pct_change") or 0.0))
            metrics["sector_boost"] = round(boost, 1)
            metrics["sector_resonance"] = {
                "tag": best.get("tag"),
                "signal_count": best.get("signal_count"),
                "scanned_count": best.get("scanned_count"),
                "avg_signal_pct": best.get("avg_signal_pct"),
                "avg_latest_pct": best.get("avg_latest_pct"),
                "members": best.get("members"),
            }
            reason = (
                f"板块共振：{best.get('tag')} {best.get('signal_count')}只同步异动，"
                f"信号均涨幅 {float(best.get('avg_signal_pct') or 0.0):+.1f}%"
            )
            reasons = signal.setdefault("reasons", [])
            if reason not in reasons:
                reasons.insert(0, reason)
        return resonance

    def _build_buckets(
        self,
        *,
        tags_by_code: dict[str, list[str]],
        latest_pct_by_code: dict[str, float],
        signal_by_code: dict[str, dict[str, Any]],
    ) -> dict[str, _TagBucket]:
        buckets: dict[str, _TagBucket] = {}
        for code, tags in tags_by_code.items():
            for tag in tags:
                bucket = buckets.setdefault(tag, _TagBucket(tag=tag))
                bucket.scanned.add(code)
                if code in latest_pct_by_code:
                    bucket.latest_pcts.append(latest_pct_by_code[code])
                signal = signal_by_code.get(code)
                if signal:
                    bucket.signaled.add(code)
                    bucket.signal_pcts.append(float(signal.get("pct_change") or 0.0))
                    bucket.signal_scores.append(float(signal.get("score") or 0.0))
        return buckets

    def _summaries(
        self,
        buckets: dict[str, _TagBucket],
        *,
        name_by_code: dict[str, str],
        signal_by_code: dict[str, dict[str, Any]],
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        min_signals = max(2, int(self.config.sector_resonance_min_signals or 2))
        for bucket in buckets.values():
            signal_count = len(bucket.signaled)
            if signal_count < min_signals:
                continue
            avg_signal_pct = _avg(bucket.signal_pcts)
            avg_latest_pct = _avg(bucket.latest_pcts)
            boost = 18.0 if signal_count >= 3 else 10.0
            if avg_signal_pct >= 4.0 or avg_latest_pct >= 4.0:
                boost += 5.0
            boost = min(float(self.config.sector_resonance_boost_cap or 22.0), boost)
            members = sorted(
                bucket.signaled,
                key=lambda code: float(signal_by_code.get(code, {}).get("score") or 0.0),
                reverse=True,
            )
            rows.append(
                {
                    "tag": bucket.tag,
                    "boost": round(boost, 1),
                    "signal_count": signal_count,
                    "scanned_count": len(bucket.scanned),
                    "avg_signal_pct": round(avg_signal_pct, 3),
                    "avg_latest_pct": round(avg_latest_pct, 3),
                    "top_score": round(max(bucket.signal_scores or [0.0]), 1),
                    "members": [
                        {
                            "symbol": code,
                            "name": name_by_code.get(code, ""),
                            "score": round(float(signal_by_code.get(code, {}).get("score") or 0.0), 1),
                            "pct_change": round(float(signal_by_code.get(code, {}).get("pct_change") or 0.0), 3),
                        }
                        for code in members[:6]
                    ],
                }
            )
        rows.sort(
            key=lambda row: (
                float(row.get("boost") or 0.0),
                int(row.get("signal_count") or 0),
                float(row.get("avg_signal_pct") or 0.0),
            ),
            reverse=True,
        )
        return rows[:10]

    def _tags_for_code(self, code: str, *, name: str = "") -> list[str]:
        tags = set(self._all_code_tags().get(code, set()))
        for needle, tag in _NAME_TAG_HINTS:
            if needle in str(name or ""):
                tags.add(tag)
        informative = {
            normalized
            for tag in tags
            if (normalized := _normalize_tag(tag)) and normalized not in _GENERIC_TAGS
        }
        return sorted(informative)

    def _all_code_tags(self) -> dict[str, set[str]]:
        return self.sector_provider.build_code_to_tags()

    @staticmethod
    def _latest_pct_and_names(results: list[dict[str, Any]]) -> tuple[dict[str, float], dict[str, str]]:
        pct_by_code: dict[str, float] = {}
        name_by_code: dict[str, str] = {}
        for row in results:
            code = SectorResonanceScorer._code(row.get("symbol"))
            if not code:
                continue
            name_by_code[code] = str(row.get("name") or code)
            summary = row.get("summary") or {}
            pct_by_code[code] = float(summary.get("latest_pct") or 0.0)
        return pct_by_code, name_by_code

    @staticmethod
    def _code(value: Any) -> str:
        code = normalize_symbol(str(value or ""))
        return code.zfill(6) if code.isdigit() else code


def _normalize_tag(value: Any) -> str:
    tag = SectorTagProvider.canonical_tag(str(value or "").strip())
    return _TAG_ALIASES.get(tag, tag)


def _avg(values: list[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def _level(score: float, pct: float) -> str:
    if score >= 80 and pct < 8.0:
        return "强异动"
    if score >= 65:
        return "异动观察"
    return "记录"
