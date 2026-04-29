"""Convert full-stream news insights into sector-level scoring factors."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from modules.config import cfg
from modules.db import DatabaseManager

from .models import now_ts
from .news_intelligence import NewsIntelligenceEngine


@dataclass(slots=True)
class NewsSectorFactor:
    """Aggregated market factor for one sector/concept from high-value news."""

    sector: str
    factor_score: float
    direction: str
    insight_count: int
    avg_value_score: float
    top_topic: str
    top_titles: list[str] = field(default_factory=list)


class NewsFactorEngine:
    """Build sector factors consumed by event reports and Layer-4 scoring."""

    def __init__(self, db: DatabaseManager | None = None):
        self._owns_db = db is None
        self._db = db or DatabaseManager(cfg().data.warehouse_path)

    def close(self) -> None:
        if self._owns_db:
            self._db.close()

    def build(
        self,
        *,
        limit: int = 200,
        min_score: float = 55.0,
        top_n: int = 20,
    ) -> dict[str, Any]:
        """Return ranked sector factors derived from current high-value news."""
        try:
            payload = NewsIntelligenceEngine(db=self._db).analyze(limit=limit, min_score=min_score)
            insights = payload.get("insights") or []
            factors = self._aggregate(insights)
            return {
                "engine": "news_factor_v1",
                "generated_at": now_ts(),
                "news_scanned": payload.get("news_scanned", 0),
                "insight_count": len(insights),
                "factors": [asdict(item) for item in factors[:top_n]],
                "topic_summary": payload.get("topic_summary") or [],
                "message": self._message(insights, factors),
            }
        finally:
            self.close()

    def sector_boosts(
        self,
        *,
        limit: int = 200,
        min_score: float = 55.0,
    ) -> dict[str, float]:
        """Return {sector: boost_score} for EventDriver matching."""
        payload = self.build(limit=limit, min_score=min_score, top_n=100)
        return {
            str(row["sector"]): float(row["factor_score"])
            for row in payload.get("factors", [])
            if row.get("sector")
        }

    @staticmethod
    def _aggregate(insights: list[dict[str, Any]]) -> list[NewsSectorFactor]:
        buckets: dict[str, dict[str, Any]] = {}
        for item in insights:
            sectors = item.get("affected_sectors") or []
            if not sectors:
                continue
            sign = -1.0 if item.get("sentiment") == "bearish" else 1.0
            contribution = NewsFactorEngine._contribution(item) * sign
            for sector in sectors:
                sector_name = str(sector).strip()
                if not sector_name:
                    continue
                bucket = buckets.setdefault(
                    sector_name,
                    {
                        "score": 0.0,
                        "count": 0,
                        "value_total": 0.0,
                        "topics": {},
                        "titles": [],
                    },
                )
                bucket["score"] += contribution
                bucket["count"] += 1
                bucket["value_total"] += float(item.get("value_score") or 0.0)
                topic = str(item.get("topic") or "")
                bucket["topics"][topic] = bucket["topics"].get(topic, 0) + 1
                title = str(item.get("title") or "")[:48]
                if title and title not in bucket["titles"]:
                    bucket["titles"].append(title)

        factors: list[NewsSectorFactor] = []
        for sector, bucket in buckets.items():
            score = max(-25.0, min(25.0, bucket["score"]))
            if abs(score) < 0.1:
                continue
            topics = bucket["topics"]
            top_topic = max(topics, key=topics.get) if topics else ""
            direction = "bullish" if score > 0 else "bearish"
            factors.append(
                NewsSectorFactor(
                    sector=sector,
                    factor_score=round(score, 1),
                    direction=direction,
                    insight_count=int(bucket["count"]),
                    avg_value_score=round(bucket["value_total"] / max(1, bucket["count"]), 1),
                    top_topic=top_topic,
                    top_titles=bucket["titles"][:3],
                )
            )

        factors.sort(key=lambda item: abs(item.factor_score), reverse=True)
        return factors

    @staticmethod
    def _contribution(item: dict[str, Any]) -> float:
        value_score = float(item.get("value_score") or 0.0)
        confidence = float(item.get("confidence") or 0.0)
        impact_strength = float(item.get("impact_strength") or 0.0)
        score_part = max(0.0, value_score - 45.0) / 55.0
        return min(18.0, score_part * 24.0 * confidence * impact_strength)

    @staticmethod
    def _message(insights: list[dict[str, Any]], factors: list[NewsSectorFactor]) -> str:
        if not insights:
            return "暂无达到阈值的新闻洞察，未生成新闻板块因子。"
        if not factors:
            return "已发现新闻洞察，但暂未映射到可用 A 股板块因子。"
        return f"已从 {len(insights)} 条新闻洞察生成 {len(factors)} 个板块因子。"
