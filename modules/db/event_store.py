"""Event intelligence warehouse storage."""

from __future__ import annotations

import hashlib
import json
import sqlite3

import pandas as pd

from modules.event_intelligence.models import (
    EventOpportunity,
    EventSignal,
    EventState,
    NewsItem,
    now_ts,
)


class EventStore:
    """Persist macro event news, signals, states, and opportunities."""

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    @staticmethod
    def make_news_hash(source: str, title: str, url: str = "") -> str:
        """Build a stable duplicate key for a news item."""
        raw = f"{source}|{url}|{title}".strip().lower()
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def insert_news(self, item: NewsItem) -> int | None:
        """Insert a news item. Returns row id, or None if duplicate."""
        raw_hash = item.raw_hash or self.make_news_hash(item.source, item.title, item.url)
        cursor = self._conn.cursor()
        cursor.execute(
            """INSERT OR IGNORE INTO event_news
               (source, title, summary, url, published_at, fetched_at, language, raw_hash)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                item.source,
                item.title,
                item.summary,
                item.url,
                item.published_at,
                item.fetched_at or now_ts(),
                item.language,
                raw_hash,
            ),
        )
        self._conn.commit()
        return cursor.lastrowid if cursor.rowcount else None

    def list_news(self, limit: int = 50, processed: int | None = None) -> pd.DataFrame:
        """List recent news items, optionally filtered by processed flag."""
        params: list = []
        where = ""
        if processed is not None:
            where = " WHERE processed = ?"
            params.append(processed)
        params.append(limit)
        return pd.read_sql_query(
            f"SELECT * FROM event_news{where} ORDER BY id DESC LIMIT ?",
            self._conn,
            params=params,
        )

    def mark_news_processed(self, news_ids: list[int]) -> None:
        """Mark news rows as processed by the extractor."""
        if not news_ids:
            return
        placeholders = ",".join("?" for _ in news_ids)
        self._conn.execute(
            f"UPDATE event_news SET processed = 1 WHERE id IN ({placeholders})",
            news_ids,
        )
        self._conn.commit()

    def insert_signal(self, signal: EventSignal) -> int:
        """Insert one extracted event signal."""
        cursor = self._conn.cursor()
        cursor.execute(
            """INSERT INTO event_signals
               (event_id, news_id, event_type, entities_json, matched_keywords,
                matched_actions, severity, confidence, direction, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                signal.event_id,
                signal.news_id,
                signal.event_type,
                json.dumps(signal.entities, ensure_ascii=False),
                ",".join(signal.matched_keywords),
                ",".join(signal.matched_actions),
                signal.severity,
                signal.confidence,
                signal.direction,
                signal.created_at or now_ts(),
            ),
        )
        self._conn.commit()
        return cursor.lastrowid

    def list_signals(
        self,
        event_id: str | None = None,
        limit: int = 100,
    ) -> pd.DataFrame:
        """List recent event signals."""
        params: list = []
        where = ""
        if event_id:
            where = " WHERE event_id = ?"
            params.append(event_id)
        params.append(limit)
        return pd.read_sql_query(
            f"SELECT * FROM event_signals{where} ORDER BY id DESC LIMIT ?",
            self._conn,
            params=params,
        )

    def list_signal_evidence(
        self,
        event_id: str | None = None,
        limit: int = 50,
    ) -> pd.DataFrame:
        """List recent signals with their source news for explainable reports."""
        params: list = []
        where = ""
        if event_id:
            where = " WHERE s.event_id = ?"
            params.append(event_id)
        params.append(limit)
        return pd.read_sql_query(
            f"""SELECT s.id, s.event_id, s.event_type, s.entities_json,
                       s.matched_keywords, s.matched_actions, s.severity,
                       s.confidence, s.direction, s.created_at,
                       n.source, n.title, n.summary, n.url, n.published_at
                FROM event_signals s
                LEFT JOIN event_news n ON n.id = s.news_id
                {where}
                ORDER BY s.id DESC LIMIT ?""",
            self._conn,
            params=params,
        )

    def upsert_state(self, state: EventState) -> None:
        """Insert or update an aggregated event state."""
        self._conn.execute(
            """INSERT INTO event_states
               (event_id, name, probability, impact_strength, status,
                evidence_count, sources_count, last_signal_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(event_id) DO UPDATE SET
               name=excluded.name,
               probability=excluded.probability,
               impact_strength=excluded.impact_strength,
               status=excluded.status,
               evidence_count=excluded.evidence_count,
               sources_count=excluded.sources_count,
               last_signal_at=excluded.last_signal_at,
               updated_at=excluded.updated_at""",
            (
                state.event_id,
                state.name,
                state.probability,
                state.impact_strength,
                state.status,
                state.evidence_count,
                state.sources_count,
                state.last_signal_at,
                state.updated_at or now_ts(),
            ),
        )
        self._conn.commit()

    def list_states(self, limit: int = 50, status: str | None = None) -> pd.DataFrame:
        """List event states ordered by probability."""
        params: list = []
        where = ""
        if status:
            where = " WHERE status = ?"
            params.append(status)
        params.append(limit)
        return pd.read_sql_query(
            f"SELECT * FROM event_states{where} ORDER BY probability DESC, updated_at DESC LIMIT ?",
            self._conn,
            params=params,
        )

    def insert_opportunity(self, opportunity: EventOpportunity) -> int:
        """Insert one generated event opportunity."""
        cursor = self._conn.cursor()
        cursor.execute(
            """INSERT INTO event_opportunities
               (event_id, symbol, name, sector_tags, opportunity_score,
                event_score, exposure_score, underpricing_score, timing_score,
                risk_penalty, recommendation, evidence_json, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                opportunity.event_id,
                opportunity.symbol,
                opportunity.name,
                ",".join(opportunity.sector_tags),
                opportunity.opportunity_score,
                opportunity.event_score,
                opportunity.exposure_score,
                opportunity.underpricing_score,
                opportunity.timing_score,
                opportunity.risk_penalty,
                opportunity.recommendation,
                json.dumps(opportunity.evidence, ensure_ascii=False),
                opportunity.created_at or now_ts(),
            ),
        )
        self._conn.commit()
        return cursor.lastrowid

    def list_opportunities(
        self,
        event_id: str | None = None,
        limit: int = 50,
    ) -> pd.DataFrame:
        """List recent opportunities ordered by opportunity score."""
        params: list = []
        where = ""
        if event_id:
            where = " WHERE event_id = ?"
            params.append(event_id)
        params.append(limit)
        return pd.read_sql_query(
            f"""SELECT * FROM event_opportunities{where}
                ORDER BY opportunity_score DESC, id DESC LIMIT ?""",
            self._conn,
            params=params,
        )

    def delete_opportunities(self, event_id: str) -> int:
        """Delete existing generated opportunities for one event before refreshing."""
        cursor = self._conn.execute(
            "DELETE FROM event_opportunities WHERE event_id = ?",
            (event_id,),
        )
        self._conn.commit()
        return int(cursor.rowcount or 0)

    def get_notification(self, event_id: str) -> dict | None:
        """Return notification state for one event."""
        cursor = self._conn.execute(
            "SELECT * FROM event_notifications WHERE event_id = ?",
            (event_id,),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        columns = [d[0] for d in cursor.description]
        return dict(zip(columns, row))

    def upsert_notification(
        self,
        *,
        event_id: str,
        report_hash: str,
        status: str,
        last_sent_at: str = "",
        cooldown_until: str = "",
        last_probability: float = 0.0,
        last_opportunity_score: float = 0.0,
    ) -> None:
        """Insert or update notification cooldown state for one event."""
        self._conn.execute(
            """INSERT INTO event_notifications
               (event_id, report_hash, last_sent_at, cooldown_until, status,
                last_probability, last_opportunity_score, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(event_id) DO UPDATE SET
               report_hash=excluded.report_hash,
               last_sent_at=excluded.last_sent_at,
               cooldown_until=excluded.cooldown_until,
               status=excluded.status,
               last_probability=excluded.last_probability,
               last_opportunity_score=excluded.last_opportunity_score,
               updated_at=excluded.updated_at""",
            (
                event_id,
                report_hash,
                last_sent_at,
                cooldown_until,
                status,
                last_probability,
                last_opportunity_score,
                now_ts(),
            ),
        )
        self._conn.commit()

    def list_notifications(self, limit: int = 50) -> pd.DataFrame:
        """List recent notification states."""
        return pd.read_sql_query(
            """SELECT * FROM event_notifications
               ORDER BY updated_at DESC LIMIT ?""",
            self._conn,
            params=[limit],
        )

    def upsert_source_quality(
        self,
        *,
        source_id: str,
        name: str = "",
        category: str = "general",
        type: str = "",
        enabled: bool = False,
        fetched: int = 0,
        inserted: int = 0,
        duplicates: int = 0,
        errors: int = 0,
        last_success_at: str = "",
        last_error: str = "",
    ) -> None:
        """Insert or update source collection quality statistics."""
        existing = self.get_source_quality(source_id)
        signal_hits = int((existing or {}).get("signal_hits") or 0)
        hit_rate = float((existing or {}).get("hit_rate") or 0.0)
        quality_score, reliability = self._source_quality_score(
            fetched=fetched,
            errors=errors,
            hit_rate=hit_rate,
            last_error=last_error,
        )
        self._conn.execute(
            """INSERT INTO event_source_quality
               (source_id, name, category, type, enabled, fetched, inserted,
                duplicates, errors, signal_hits, hit_rate, last_success_at,
                last_error, quality_score, reliability, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(source_id) DO UPDATE SET
               name=excluded.name,
               category=excluded.category,
               type=excluded.type,
               enabled=excluded.enabled,
               fetched=excluded.fetched,
               inserted=excluded.inserted,
               duplicates=excluded.duplicates,
               errors=excluded.errors,
               signal_hits=excluded.signal_hits,
               hit_rate=excluded.hit_rate,
               last_success_at=excluded.last_success_at,
               last_error=excluded.last_error,
               quality_score=excluded.quality_score,
               reliability=excluded.reliability,
               updated_at=excluded.updated_at""",
            (
                source_id,
                name,
                category,
                type,
                1 if enabled else 0,
                fetched,
                inserted,
                duplicates,
                errors,
                signal_hits,
                hit_rate,
                last_success_at,
                last_error,
                quality_score,
                reliability,
                now_ts(),
            ),
        )
        self._conn.commit()

    def get_source_quality(self, source_id: str) -> dict | None:
        """Return quality statistics for one source."""
        cursor = self._conn.execute(
            "SELECT * FROM event_source_quality WHERE source_id = ?",
            (source_id,),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        columns = [d[0] for d in cursor.description]
        return dict(zip(columns, row))

    def refresh_source_quality_signal_hits(self, source_ids: list[str] | None = None) -> None:
        """Refresh source signal hit counts from persisted news/signals."""
        params: list = []
        where = ""
        if source_ids:
            placeholders = ",".join("?" for _ in source_ids)
            where = f" WHERE n.source IN ({placeholders})"
            params.extend(source_ids)

        rows = self._conn.execute(
            f"""SELECT n.source,
                       COUNT(DISTINCT CASE WHEN s.id IS NOT NULL THEN n.id END) AS signal_hits,
                       COUNT(DISTINCT n.id) AS news_count
                FROM event_news n
                LEFT JOIN event_signals s ON s.news_id = n.id
                {where}
                GROUP BY n.source""",
            params,
        ).fetchall()
        for source_id, signal_hits, news_count in rows:
            existing = self.get_source_quality(str(source_id))
            if not existing:
                continue
            hit_rate = float(signal_hits or 0) / int(news_count or 0) if news_count else 0.0
            quality_score, reliability = self._source_quality_score(
                fetched=int(existing.get("fetched") or 0),
                errors=int(existing.get("errors") or 0),
                hit_rate=hit_rate,
                last_error=str(existing.get("last_error") or ""),
            )
            self._conn.execute(
                """UPDATE event_source_quality
                   SET signal_hits = ?, hit_rate = ?, quality_score = ?, reliability = ?, updated_at = ?
                   WHERE source_id = ?""",
                (int(signal_hits or 0), hit_rate, quality_score, reliability, now_ts(), source_id),
            )
        self._conn.commit()

    def list_source_quality(self, limit: int = 50) -> pd.DataFrame:
        """List source quality statistics."""
        return pd.read_sql_query(
            """SELECT * FROM event_source_quality
               ORDER BY signal_hits DESC, fetched DESC, source_id LIMIT ?""",
            self._conn,
            params=[limit],
        )

    @staticmethod
    def _source_quality_score(
        *,
        fetched: int,
        errors: int,
        hit_rate: float,
        last_error: str = "",
    ) -> tuple[float, str]:
        """Score a source's operational quality for ranking and monitoring."""
        volume_score = min(max(fetched, 0), 50) / 50 * 30
        signal_score = min(max(hit_rate, 0.0), 0.8) / 0.8 * 55
        error_penalty = min(max(errors, 0), 3) * 15
        if last_error:
            error_penalty += 20
        score = round(max(0.0, min(100.0, volume_score + signal_score - error_penalty + 15)), 1)
        if score >= 75:
            reliability = "high"
        elif score >= 50:
            reliability = "medium"
        elif score > 0:
            reliability = "low"
        else:
            reliability = "unknown"
        return score, reliability

    def insert_elasticity_run(
        self,
        *,
        event_id: str = "",
        windows: list[int] | None = None,
        trigger_count: int = 0,
        sample_count: int = 0,
        summary: dict | None = None,
    ) -> int:
        """Persist one event elasticity backtest run."""
        cursor = self._conn.cursor()
        cursor.execute(
            """INSERT INTO event_backtest_runs
               (event_id, windows_json, trigger_count, sample_count, summary_json, created_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                event_id,
                json.dumps(windows or [], ensure_ascii=False),
                trigger_count,
                sample_count,
                json.dumps(summary or {}, ensure_ascii=False),
                now_ts(),
            ),
        )
        self._conn.commit()
        return int(cursor.lastrowid)

    def update_elasticity_run_summary(
        self,
        run_id: int,
        *,
        trigger_count: int,
        sample_count: int,
        summary: dict,
    ) -> None:
        """Update summary fields for a persisted elasticity run."""
        self._conn.execute(
            """UPDATE event_backtest_runs
               SET trigger_count = ?, sample_count = ?, summary_json = ?
               WHERE id = ?""",
            (
                trigger_count,
                sample_count,
                json.dumps(summary, ensure_ascii=False),
                run_id,
            ),
        )
        self._conn.commit()

    def insert_elasticity_sample(self, sample: dict) -> int:
        """Persist one event elasticity sample row."""
        cursor = self._conn.cursor()
        cursor.execute(
            """INSERT INTO event_elasticity_samples
               (run_id, event_id, symbol, name, trigger_date, entry_date,
                window_days, entry_close, exit_date, exit_close,
                forward_return, benchmark_return, excess_return,
                max_drawdown, success, source, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                sample.get("run_id"),
                sample.get("event_id", ""),
                sample.get("symbol", ""),
                sample.get("name", ""),
                sample.get("trigger_date", ""),
                sample.get("entry_date", ""),
                sample.get("window_days", 0),
                sample.get("entry_close"),
                sample.get("exit_date", ""),
                sample.get("exit_close"),
                sample.get("forward_return", 0.0),
                sample.get("benchmark_return", 0.0),
                sample.get("excess_return", 0.0),
                sample.get("max_drawdown", 0.0),
                1 if sample.get("success") else 0,
                sample.get("source", ""),
                now_ts(),
            ),
        )
        self._conn.commit()
        return int(cursor.lastrowid)

    def list_elasticity_runs(self, limit: int = 20) -> pd.DataFrame:
        """List recent elasticity backtest runs."""
        return pd.read_sql_query(
            """SELECT * FROM event_backtest_runs
               ORDER BY id DESC LIMIT ?""",
            self._conn,
            params=[limit],
        )

    def list_elasticity_samples(
        self,
        event_id: str | None = None,
        limit: int = 200,
    ) -> pd.DataFrame:
        """List recent elasticity sample rows."""
        params: list = []
        where = ""
        if event_id:
            where = " WHERE event_id = ?"
            params.append(event_id)
        params.append(limit)
        return pd.read_sql_query(
            f"""SELECT * FROM event_elasticity_samples{where}
                ORDER BY id DESC LIMIT ?""",
            self._conn,
            params=params,
        )
