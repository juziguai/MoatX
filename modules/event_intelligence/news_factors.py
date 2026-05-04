"""Convert full-stream news insights into sector-level scoring factors."""

from __future__ import annotations

import json
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
    llm_adjustment: float = 1.0


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
            llm_reviews = self._latest_llm_reviews()
            factors = self._aggregate(insights, llm_reviews)
            self._persist_factors(factors)
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

    def list_persisted(self, *, limit: int = 50) -> list[dict[str, Any]]:
        """Return materialized news factors."""
        try:
            import pandas as pd

            df = pd.read_sql_query(
                """SELECT * FROM event_news_factors
                   ORDER BY ABS(factor_score) DESC, updated_at DESC LIMIT ?""",
                self._db.conn,
                params=[limit],
            )
            if df.empty:
                return []
            rows = df.where(pd.notna(df), None).to_dict(orient="records")
            for row in rows:
                try:
                    row["top_titles"] = json.loads(str(row.get("top_titles_json") or "[]"))
                except json.JSONDecodeError:
                    row["top_titles"] = []
            return rows
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
    def _aggregate(
        insights: list[dict[str, Any]],
        llm_reviews: dict[tuple[int, str], dict[str, Any]] | None = None,
    ) -> list[NewsSectorFactor]:
        buckets: dict[str, dict[str, Any]] = {}
        reviews = llm_reviews or {}
        for item in insights:
            sectors = item.get("affected_sectors") or []
            if not sectors:
                continue
            sign = -1.0 if item.get("sentiment") == "bearish" else 1.0
            llm_multiplier = NewsFactorEngine._llm_multiplier(item, reviews)
            if llm_multiplier <= 0:
                continue
            contribution = NewsFactorEngine._contribution(item) * sign * llm_multiplier
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
                        "llm_total": 0.0,
                        "llm_count": 0,
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
                bucket["llm_total"] += llm_multiplier
                bucket["llm_count"] += 1

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
                    llm_adjustment=round(bucket["llm_total"] / max(1, bucket["llm_count"]), 3),
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
    def _llm_multiplier(
        item: dict[str, Any],
        reviews: dict[tuple[int, str], dict[str, Any]],
    ) -> float:
        news_id = int(item.get("news_id") or 0)
        topic = str(item.get("topic") or "")
        review = reviews.get((news_id, topic)) or reviews.get((news_id, ""))
        if not review:
            return 1.0
        decision = str(review.get("decision") or "watch").lower()
        llm_score = float(review.get("llm_score") or 0.0)
        if decision == "ignore":
            return 0.0
        if llm_score and llm_score < 50:
            return 0.7
        if decision == "use":
            return 1.15
        return 1.0

    def _latest_llm_reviews(self) -> dict[tuple[int, str], dict[str, Any]]:
        rows = self._db.conn.execute(
            """SELECT news_id, topic, llm_score, decision
               FROM event_llm_reviews
               ORDER BY id DESC"""
        ).fetchall()
        reviews: dict[tuple[int, str], dict[str, Any]] = {}
        for news_id, topic, llm_score, decision in rows:
            key = (int(news_id or 0), str(topic or ""))
            if key in reviews:
                continue
            reviews[key] = {
                "llm_score": float(llm_score or 0.0),
                "decision": str(decision or ""),
            }
        return reviews

    def _persist_factors(self, factors: list[NewsSectorFactor]) -> None:
        now = now_ts()
        for item in factors:
            self._db.conn.execute(
                """INSERT INTO event_news_factors
                   (sector, factor_score, direction, insight_count, avg_value_score,
                    top_topic, top_titles_json, llm_adjustment, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(sector) DO UPDATE SET
                   factor_score=excluded.factor_score,
                   direction=excluded.direction,
                   insight_count=excluded.insight_count,
                   avg_value_score=excluded.avg_value_score,
                   top_topic=excluded.top_topic,
                   top_titles_json=excluded.top_titles_json,
                   llm_adjustment=excluded.llm_adjustment,
                   updated_at=excluded.updated_at""",
                (
                    item.sector,
                    item.factor_score,
                    item.direction,
                    item.insight_count,
                    item.avg_value_score,
                    item.top_topic,
                    json.dumps(item.top_titles, ensure_ascii=False),
                    item.llm_adjustment,
                    now,
                ),
            )
        self._db.conn.commit()

    @staticmethod
    def _message(insights: list[dict[str, Any]], factors: list[NewsSectorFactor]) -> str:
        if not insights:
            return "暂无达到阈值的新闻洞察，未生成新闻板块因子。"
        if not factors:
            return "已发现新闻洞察，但暂未映射到可用 A 股板块因子。"
        return f"已从 {len(insights)} 条新闻洞察生成 {len(factors)} 个板块因子。"
