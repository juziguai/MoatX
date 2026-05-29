"""Convert full-stream news insights into sector-level scoring factors."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from typing import Any

from modules.config import cfg
from modules.db import DatabaseManager

from .models import now_ts
from .news_intelligence import NewsIntelligenceEngine

_BEARISH_KEYWORDS = (
    "下跌", "下滑", "亏损", "预亏", "减持", "处罚", "立案", "调查", "制裁", "禁令",
    "不及预期", "暴跌", "风险", "违约", "退市", "召回", "事故", "停产", "降级",
    "债务", "商誉减值", "资产减值",
)

_SEVERE_BEARISH_KEYWORDS = (
    "立案", "制裁", "禁令", "违约", "退市", "召回", "停产", "商誉减值", "资产减值",
)


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
    avg_time_decay: float = 1.0


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

    def backfill_snapshots(
        self,
        *,
        start_date: str = "",
        end_date: str = "",
        lookback_days: int = 14,
        min_score: float = 55.0,
        top_n: int = 100,
    ) -> dict[str, Any]:
        """Backfill point-in-time daily factor snapshots from persisted news insights."""
        try:
            insights = self._historical_insights(min_score=min_score)
            dates = self._snapshot_dates(insights, start_date=start_date, end_date=end_date)
            reviews = self._latest_llm_reviews()
            rows: list[dict[str, Any]] = []
            for date_text in dates:
                as_of = datetime.strptime(date_text, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
                cutoff = as_of - timedelta(days=max(1, int(lookback_days or 1)))
                window = [
                    item
                    for item in insights
                    if self._in_snapshot_window(item, cutoff=cutoff, as_of=as_of)
                ]
                factors = self._aggregate(window, reviews, as_of=as_of)[: max(1, int(top_n or 1))]
                self._persist_factor_snapshot(date_text, factors, updated_at=as_of.strftime("%Y-%m-%d %H:%M:%S"))
                rows.append(
                    {
                        "snapshot_date": date_text,
                        "insight_count": len(window),
                        "factor_count": len(factors),
                        "top_factor": asdict(factors[0]) if factors else {},
                    }
                )
            return {
                "engine": "news_factor_snapshot_backfill_v1",
                "generated_at": now_ts(),
                "start_date": dates[0] if dates else "",
                "end_date": dates[-1] if dates else "",
                "lookback_days": int(lookback_days),
                "min_score": float(min_score),
                "insight_count": len(insights),
                "snapshot_count": len(rows),
                "snapshots": rows,
                "message": f"已回填 {len(rows)} 个新闻因子日快照。",
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
    def _aggregate(
        insights: list[dict[str, Any]],
        llm_reviews: dict[tuple[int, str], dict[str, Any]] | None = None,
        *,
        as_of: datetime | None = None,
    ) -> list[NewsSectorFactor]:
        buckets: dict[str, dict[str, Any]] = {}
        reviews = llm_reviews or {}
        for item in insights:
            sectors = item.get("affected_sectors") or []
            if not sectors:
                continue
            sign, bearish_hits = NewsFactorEngine._sentiment_sign(item)
            llm_multiplier = NewsFactorEngine._llm_multiplier(item, reviews)
            if llm_multiplier <= 0:
                continue
            time_decay = NewsFactorEngine._time_decay(
                item.get("published_at") or item.get("created_at") or "",
                now=as_of,
            )
            bearish_multiplier = 1.2 if bearish_hits & set(_SEVERE_BEARISH_KEYWORDS) else (1.1 if bearish_hits else 1.0)
            contribution = NewsFactorEngine._contribution(item) * sign * llm_multiplier * time_decay * bearish_multiplier
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
                        "time_total": 0.0,
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
                bucket["time_total"] += time_decay

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
                    avg_time_decay=round(bucket["time_total"] / max(1, bucket["count"]), 3),
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
    def _sentiment_sign(item: dict[str, Any]) -> tuple[float, set[str]]:
        text = " ".join(
            str(item.get(key) or "")
            for key in ("title", "summary", "reason")
        )
        bearish_hits = {keyword for keyword in _BEARISH_KEYWORDS if keyword in text}
        if str(item.get("sentiment") or "").lower() == "bearish" or bearish_hits:
            return -1.0, bearish_hits
        return 1.0, set()

    @staticmethod
    def _time_decay(value: Any, *, now: datetime | None = None) -> float:
        parsed = NewsFactorEngine._parse_time(value)
        if parsed is None:
            return 0.85
        reference = now or datetime.now()
        age_hours = max(0.0, (reference - parsed).total_seconds() / 3600)
        if age_hours <= 12:
            return 1.0
        if age_hours <= 24:
            return 0.9
        if age_hours <= 48:
            return 0.7
        if age_hours <= 72:
            return 0.5
        if age_hours <= 168:
            return 0.25
        return 0.1

    @staticmethod
    def _parse_time(value: Any) -> datetime | None:
        text = str(value or "").strip()
        if not text:
            return None
        text = text.replace("T", " ").replace("Z", "")
        if "." in text:
            text = text.split(".", 1)[0]
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
            try:
                return datetime.strptime(text, fmt)
            except ValueError:
                continue
        try:
            parsed = parsedate_to_datetime(text)
            if parsed is None:
                return None
            return parsed.replace(tzinfo=None) if parsed.tzinfo is not None else parsed
        except (TypeError, ValueError):
            return None

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

    def _historical_insights(self, *, min_score: float) -> list[dict[str, Any]]:
        import pandas as pd

        df = pd.read_sql_query(
            """SELECT i.news_id,
                      i.source,
                      i.title,
                      i.topic,
                      i.category,
                      i.value_score,
                      i.sentiment,
                      i.time_horizon,
                      i.affected_sectors_json,
                      i.affected_stocks_json,
                      i.reason,
                      i.llm_score,
                      i.llm_decision,
                      i.created_at,
                      i.updated_at,
                      COALESCE(NULLIF(n.published_at, ''), NULLIF(n.fetched_at, ''), i.created_at) AS published_at
               FROM event_news_insights i
               LEFT JOIN event_news n ON n.id = i.news_id
               WHERE i.value_score >= ?
               ORDER BY published_at, i.updated_at""",
            self._db.conn,
            params=[float(min_score)],
        )
        if df.empty:
            return []
        rows = df.where(pd.notna(df), None).to_dict(orient="records")
        for row in rows:
            try:
                row["affected_sectors"] = json.loads(str(row.get("affected_sectors_json") or "[]"))
            except json.JSONDecodeError:
                row["affected_sectors"] = []
            try:
                row["affected_stocks"] = json.loads(str(row.get("affected_stocks_json") or "[]"))
            except json.JSONDecodeError:
                row["affected_stocks"] = []
            row["confidence"] = min(1.0, max(0.35, float(row.get("value_score") or 0.0) / 100.0))
            row["impact_strength"] = min(1.0, max(0.45, len(row["affected_sectors"]) * 0.12))
        return rows

    @staticmethod
    def _snapshot_dates(insights: list[dict[str, Any]], *, start_date: str = "", end_date: str = "") -> list[str]:
        parsed_times = [
            NewsFactorEngine._parse_time(item.get("published_at") or item.get("created_at") or "")
            for item in insights
        ]
        parsed_times = [value for value in parsed_times if value is not None]
        if not parsed_times and not (start_date and end_date):
            return []
        start = (
            datetime.strptime(start_date, "%Y-%m-%d")
            if start_date
            else min(parsed_times).replace(hour=0, minute=0, second=0, microsecond=0)
        )
        end = (
            datetime.strptime(end_date, "%Y-%m-%d")
            if end_date
            else max(parsed_times).replace(hour=0, minute=0, second=0, microsecond=0)
        )
        if end < start:
            return []
        dates: list[str] = []
        current = start
        while current <= end:
            dates.append(current.strftime("%Y-%m-%d"))
            current += timedelta(days=1)
        return dates

    @staticmethod
    def _in_snapshot_window(item: dict[str, Any], *, cutoff: datetime, as_of: datetime) -> bool:
        parsed = NewsFactorEngine._parse_time(item.get("published_at") or item.get("created_at") or "")
        if parsed is None:
            return False
        return cutoff <= parsed <= as_of

    def _persist_factors(self, factors: list[NewsSectorFactor]) -> None:
        now = now_ts()
        for item in factors:
            self._db.conn.execute(
                """INSERT INTO event_news_factors
                   (sector, factor_score, direction, insight_count, avg_value_score,
                    top_topic, top_titles_json, llm_adjustment, avg_time_decay, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(sector) DO UPDATE SET
                   factor_score=excluded.factor_score,
                   direction=excluded.direction,
                   insight_count=excluded.insight_count,
                   avg_value_score=excluded.avg_value_score,
                   top_topic=excluded.top_topic,
                   top_titles_json=excluded.top_titles_json,
                   llm_adjustment=excluded.llm_adjustment,
                   avg_time_decay=excluded.avg_time_decay,
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
                    item.avg_time_decay,
                    now,
                ),
            )
        self._persist_factor_snapshot(now[:10], factors, updated_at=now, commit=False)
        self._db.conn.commit()

    def _persist_factor_snapshot(
        self,
        snapshot_date: str,
        factors: list[NewsSectorFactor],
        *,
        updated_at: str,
        commit: bool = True,
    ) -> None:
        for item in factors:
            self._db.conn.execute(
                """INSERT INTO event_news_factor_snapshots
                   (snapshot_date, sector, factor_score, direction, insight_count,
                    avg_value_score, top_topic, top_titles_json, llm_adjustment,
                    avg_time_decay, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(snapshot_date, sector) DO UPDATE SET
                   factor_score=excluded.factor_score,
                   direction=excluded.direction,
                   insight_count=excluded.insight_count,
                   avg_value_score=excluded.avg_value_score,
                   top_topic=excluded.top_topic,
                   top_titles_json=excluded.top_titles_json,
                   llm_adjustment=excluded.llm_adjustment,
                   avg_time_decay=excluded.avg_time_decay,
                   updated_at=excluded.updated_at""",
                (
                    snapshot_date,
                    item.sector,
                    item.factor_score,
                    item.direction,
                    item.insight_count,
                    item.avg_value_score,
                    item.top_topic,
                    json.dumps(item.top_titles, ensure_ascii=False),
                    item.llm_adjustment,
                    item.avg_time_decay,
                    updated_at,
                ),
            )
        if commit:
            self._db.conn.commit()

    @staticmethod
    def _message(insights: list[dict[str, Any]], factors: list[NewsSectorFactor]) -> str:
        if not insights:
            return "暂无达到阈值的新闻洞察，未生成新闻板块因子。"
        if not factors:
            return "已发现新闻洞察，但暂未映射到可用 A 股板块因子。"
        return f"已从 {len(insights)} 条新闻洞察生成 {len(factors)} 个板块因子。"
