"""Long-term topic memory and evolution tracking for news intelligence."""

from __future__ import annotations

import json
from typing import Any

import pandas as pd

from modules.config import cfg
from modules.db import DatabaseManager

from .models import now_ts
from .news_intelligence import NewsIntelligenceEngine


class TopicMemoryEngine:
    """Persist topic heat snapshots and track rising/cooling themes."""

    def __init__(self, db: DatabaseManager | None = None):
        self._owns_db = db is None
        self._db = db or DatabaseManager(cfg().data.warehouse_path)

    def close(self) -> None:
        if self._owns_db:
            self._db.close()

    def update(
        self,
        *,
        limit: int = 300,
        min_score: float = 45.0,
        top_n: int = 30,
    ) -> dict[str, Any]:
        """Analyze current news and persist topic memory evolution."""
        try:
            payload = NewsIntelligenceEngine(db=self._db).analyze(limit=limit, min_score=min_score)
            topic_summary = payload.get("topic_summary") or []
            now = now_ts()
            rows = [self._upsert_topic(row, payload.get("insights") or [], now) for row in topic_summary]
            return {
                "engine": "topic_memory_v1",
                "generated_at": now,
                "news_scanned": payload.get("news_scanned", 0),
                "updated": len(rows),
                "topics": self.list(limit=top_n),
                "message": self._message(rows),
            }
        finally:
            self.close()

    def list(self, *, limit: int = 30, trend: str | None = None) -> list[dict[str, Any]]:
        """Return persisted topic memory rows."""
        params: list[Any] = []
        where = ""
        if trend:
            where = " WHERE trend = ?"
            params.append(trend)
        params.append(limit)
        df = pd.read_sql_query(
            f"""SELECT * FROM event_topic_memory{where}
                ORDER BY momentum DESC, heat DESC, updated_at DESC LIMIT ?""",
            self._db.conn,
            params=params,
        )
        return self._records(df)

    def snapshots(self, *, topic: str = "", limit: int = 50) -> list[dict[str, Any]]:
        """Return recent heat snapshots for one topic or all topics."""
        params: list[Any] = []
        where = ""
        if topic:
            where = " WHERE topic = ?"
            params.append(topic)
        params.append(limit)
        df = pd.read_sql_query(
            f"""SELECT * FROM event_topic_snapshots{where}
                ORDER BY id DESC LIMIT ?""",
            self._db.conn,
            params=params,
        )
        return self._records(df)

    def _upsert_topic(self, row: dict[str, Any], insights: list[dict[str, Any]], now: str) -> dict[str, Any]:
        topic = str(row.get("topic") or "")
        category = str(row.get("category") or "")
        heat = float(row.get("heat") or 0.0)
        count = int(row.get("count") or 0)
        sectors = list(row.get("sectors") or [])
        titles = self._topic_titles(topic, insights)
        existing = self._get_topic(topic)
        previous_heat = float((existing or {}).get("heat") or 0.0)
        momentum = round(heat - previous_heat, 1)
        trend = self._trend(existing, momentum)
        first_seen_at = str((existing or {}).get("first_seen_at") or now)
        total_count = int((existing or {}).get("total_insight_count") or 0) + count
        payload = {
            "topic": topic,
            "category": category,
            "heat": round(heat, 1),
            "previous_heat": round(previous_heat, 1),
            "momentum": momentum,
            "insight_count": count,
            "total_insight_count": total_count,
            "first_seen_at": first_seen_at,
            "last_seen_at": now,
            "sectors": sectors,
            "top_titles": titles,
            "trend": trend,
            "updated_at": now,
        }
        self._db.conn.execute(
            """INSERT INTO event_topic_memory
               (topic, category, heat, previous_heat, momentum, insight_count,
                total_insight_count, first_seen_at, last_seen_at, sectors_json,
                top_titles_json, trend, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(topic) DO UPDATE SET
               category=excluded.category,
               heat=excluded.heat,
               previous_heat=excluded.previous_heat,
               momentum=excluded.momentum,
               insight_count=excluded.insight_count,
               total_insight_count=excluded.total_insight_count,
               last_seen_at=excluded.last_seen_at,
               sectors_json=excluded.sectors_json,
               top_titles_json=excluded.top_titles_json,
               trend=excluded.trend,
               updated_at=excluded.updated_at""",
            (
                topic,
                category,
                payload["heat"],
                payload["previous_heat"],
                momentum,
                count,
                total_count,
                first_seen_at,
                now,
                json.dumps(sectors, ensure_ascii=False),
                json.dumps(titles, ensure_ascii=False),
                trend,
                now,
            ),
        )
        self._db.conn.execute(
            """INSERT INTO event_topic_snapshots
               (topic, category, heat, insight_count, sectors_json, top_titles_json, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                topic,
                category,
                payload["heat"],
                count,
                json.dumps(sectors, ensure_ascii=False),
                json.dumps(titles, ensure_ascii=False),
                now,
            ),
        )
        self._db.conn.commit()
        return payload

    def _get_topic(self, topic: str) -> dict[str, Any] | None:
        cursor = self._db.conn.execute("SELECT * FROM event_topic_memory WHERE topic = ?", (topic,))
        row = cursor.fetchone()
        if row is None:
            return None
        columns = [item[0] for item in cursor.description]
        return dict(zip(columns, row))

    @staticmethod
    def _topic_titles(topic: str, insights: list[dict[str, Any]]) -> list[str]:
        titles: list[str] = []
        for item in insights:
            if item.get("topic") != topic:
                continue
            title = str(item.get("title") or "")[:60]
            if title and title not in titles:
                titles.append(title)
            if len(titles) >= 5:
                break
        return titles

    @staticmethod
    def _trend(existing: dict[str, Any] | None, momentum: float) -> str:
        if existing is None:
            return "new"
        if momentum >= 5:
            return "rising"
        if momentum <= -5:
            return "cooling"
        return "stable"

    @staticmethod
    def _records(df: pd.DataFrame) -> list[dict[str, Any]]:
        if df.empty:
            return []
        records = df.where(pd.notna(df), None).to_dict(orient="records")
        for row in records:
            for key in ("sectors_json", "top_titles_json"):
                value = row.get(key)
                try:
                    row[key.replace("_json", "")] = json.loads(str(value or "[]"))
                except json.JSONDecodeError:
                    row[key.replace("_json", "")] = []
        return records

    @staticmethod
    def _message(rows: list[dict[str, Any]]) -> str:
        if not rows:
            return "本轮未发现可更新的主题记忆。"
        rising = [row["topic"] for row in rows if row["trend"] in {"new", "rising"}]
        if rising:
            return f"已更新 {len(rows)} 个主题，升温/新增：{', '.join(rising[:5])}。"
        return f"已更新 {len(rows)} 个主题，暂无显著升温。"
