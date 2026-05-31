"""Unified NewsManager — pure data layer with full observability + auto-recovery.

Architecture:
  NewsManager only does: collect → structure → keyword filter → observe.
  Semantic reasoning is delegated to the calling AI Agent (Codex CLI / scheduler).

Observability:
  RateLimitRegistry  → per-type rate limiting (RSS 2/s, JSON 5/s, HTML 1/s)
  HealthTracker      → auto-disable at 3 failures, auto-recover on success
  MetricsCollector   → latency / success rate / item count per source

Auto-Recovery:
  Sources that fail 3+ times are auto-disabled and skipped in collect().
  recovery_probe() re-tests disabled sources after 5 min cooldown.
  If probe succeeds → auto re-enabled. If fails → cooldown resets.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from modules.config import cfg
from modules.db import DatabaseManager
from modules.news_source import (
    NewsCapability,
    NewsHealthTracker,
    NewsMetricsCollector,
    NewsRateLimitRegistry,
)

logger = logging.getLogger("moatx.news_manager")


@dataclass
class NewsAnalysis:
    """Analysis result for one news item (keyword-only or externally injected)."""
    news_id: int
    title: str
    summary: str
    topic: str
    sectors: list[str] = field(default_factory=list)
    concepts: list[str] = field(default_factory=list)
    stocks: list[str] = field(default_factory=list)
    direction: str = "neutral"
    impact: float = 0.0
    confidence: float = 0.0
    reasoning: str = ""


class NewsManager:
    """Pure data layer with observability + auto-recovery.

    Semantic reasoning is delegated to the calling AI Agent.
    """

    def __init__(self, db: DatabaseManager | None = None):
        self._db = db or DatabaseManager(cfg().data.warehouse_path)
        self._rate_limiter = NewsRateLimitRegistry()
        self._health_tracker = NewsHealthTracker()
        self._metrics = NewsMetricsCollector()

    # ─── Collection ──────────────────────────────

    def collect(self) -> dict[str, Any]:
        """Collect news from all enabled sources.

        Each source: skip-disabled → rate-limit → fetch → metrics → health.
        """
        from modules.event_intelligence.source_registry import SourceRegistry

        registry = SourceRegistry()
        sources = list(registry.enabled())
        stats = {"sources": len(sources), "fetched": 0, "inserted": 0,
                 "duplicates": 0, "errors": [], "source_stats": [],
                 "disabled_skipped": 0}

        if not sources:
            stats["message"] = "no enabled news sources"
            return stats

        for source in sources:
            source_label = f"{source.type}:{source.id}"

            # Skip auto-disabled sources
            if self._health_tracker.is_disabled(source_label):
                logger.debug("skipping disabled source: %s", source_label)
                stats["disabled_skipped"] += 1
                stats["source_stats"].append({
                    "source_id": source.id, "name": source.name,
                    "type": source.type, "fetched": 0, "inserted": 0,
                    "error": "auto_disabled",
                })
                continue

            stat = {"source_id": source.id, "name": source.name,
                    "type": source.type, "fetched": 0, "inserted": 0,
                    "error": ""}
            t0 = time.time()

            # Rate limiting
            if not self._rate_limiter.acquire(source.type):
                logger.debug("rate limited: %s", source_label)
                stat["error"] = "rate_limited"
                stats["errors"].append(f"{source.id}: rate_limited")
                stats["source_stats"].append(stat)
                continue

            try:
                items = self._fetch_source(source)
                latency_ms = (time.time() - t0) * 1000
                item_count = len(items)
                stat["fetched"] = item_count
                stats["fetched"] += item_count

                for item in items:
                    inserted_id = self._db.event().insert_news(item)
                    if inserted_id is None:
                        stats["duplicates"] += 1
                    else:
                        stats["inserted"] += 1
                        stat["inserted"] += 1

                # Record success
                self._metrics.record(source_label, latency_ms, item_count, True)
                self._health_tracker.record_success(source_label)

            except Exception as exc:
                latency_ms = (time.time() - t0) * 1000
                logger.warning("news source [%s] failed: %s", source.id, exc)
                stat["error"] = str(exc)
                stats["errors"].append(f"{source.id}: {exc}")

                # Record failure (may trigger auto-disable)
                self._metrics.record(source_label, latency_ms, 0, False)
                self._health_tracker.record_failure(source_label, str(exc))

            stats["source_stats"].append(stat)

        return stats

    def _fetch_source(self, source):
        """Fetch news from one source using the appropriate provider."""
        from modules.news_sources import get_provider

        type_map = {
            "rss": ("rss", NewsCapability.RSS_FETCH),
            "http_json": ("http_json", NewsCapability.HTTP_JSON_FETCH),
            "api": ("http_json", NewsCapability.HTTP_JSON_FETCH),
            "jsonp": ("http_json", NewsCapability.JSONP_FETCH),
            "html": ("html", NewsCapability.HTML_SCRAPE),
        }

        provider_name, capability = type_map.get(source.type, (None, None))
        if provider_name is None:
            raise ValueError(f"unsupported source type: {source.type}")

        provider = get_provider(provider_name)
        if provider is None:
            raise ValueError(f"provider not found: {provider_name}")

        result = provider.fetch(capability, source=source)
        if not result.ok:
            raise RuntimeError(result.error or "fetch failed")
        return result.data or []

    # ─── Data Interface (for external AI Agent) ──

    # ─── Time range helpers ──────────────────────

    @staticmethod
    def _since_for(period: str) -> str | None:
        """Resolve human-readable period to ISO datetime string."""
        from datetime import datetime, timedelta
        now = datetime.now()
        if period == "today":
            return now.strftime("%Y-%m-%d 00:00:00")
        elif period == "3d":
            return (now - timedelta(days=3)).strftime("%Y-%m-%d 00:00:00")
        elif period == "7d":
            return (now - timedelta(days=7)).strftime("%Y-%m-%d 00:00:00")
        elif period == "month":
            return now.strftime("%Y-%m-01 00:00:00")
        elif period and len(period) >= 10:
            return period  # already ISO-like
        return None

    def list_news(self, limit: int = 50, period: str = "today") -> list[dict[str, Any]]:
        """Return raw news records as a list of dicts.

        Args:
            limit: max rows
            period: 'today' | '3d' | '7d' | 'month' | ISO datetime string
        """
        since = self._since_for(period)
        df = self._db.event().list_news(limit=limit, since=since)
        if df.empty:
            return []
        records = df.to_dict(orient="records")
        for r in records:
            for k, v in r.items():
                if hasattr(v, "item"):
                    r[k] = v.item()
        return records

    def cleanup_old_news(self, keep_days: int = 7) -> dict[str, Any]:
        """Delete news older than keep_days. Returns deleted count."""
        from datetime import datetime, timedelta
        cutoff = (datetime.now() - timedelta(days=keep_days)).strftime("%Y-%m-%d 00:00:00")
        deleted = self._db.event().delete_old_news(cutoff)
        return {"deleted": deleted, "cutoff": cutoff, "kept_days": keep_days}

    # ─── Keyword Analysis (fast fallback) ─────────

    def analyze(self, limit: int = 100, period: str = "today") -> dict[str, Any]:
        """Keyword-only fast filter using TOPIC_RULES."""
        since = self._since_for(period)
        df = self._db.event().list_news(limit=limit, since=since)
        if df.empty:
            return {"insights": [], "topic_summary": [], "stats": {"total": 0, "analyzed": 0}}

        insights: list[NewsAnalysis] = []
        for _idx, row in df.iterrows():
            try:
                analysis = self._keyword_analyze(row)
                if analysis and analysis.confidence > 0:
                    insights.append(analysis)
            except Exception as exc:
                logger.debug("analyze news %s failed: %s", row.get("id"), exc)

        insights.sort(key=lambda x: x.impact * x.confidence, reverse=True)

        return {
            "insights": [self._analysis_to_dict(a) for a in insights],
            "topic_summary": self._build_topic_summary(insights),
            "stats": {"total": len(df), "analyzed": len(insights)},
        }

    def _keyword_analyze(self, row: dict) -> NewsAnalysis | None:
        """Keyword-based analysis using deprecated TOPIC_RULES."""
        try:
            from modules.event_intelligence.news_intelligence import NewsIntelligenceEngine
            content = str(row.get("title", "")) + " " + str(row.get("summary", ""))
            engine = NewsIntelligenceEngine(db=self._db)
            matched = engine._match_topics(content)
            if not matched:
                return None

            rule, hits = matched[0]
            stocks = list(rule.stocks) if rule.stocks else []
            score = engine._topic_match_score(content, rule, hits)

            return NewsAnalysis(
                news_id=int(row.get("id", 0)),
                title=str(row.get("title", "")),
                summary=str(row.get("summary", "")),
                topic=rule.topic,
                sectors=list(rule.sectors),
                concepts=[],
                stocks=stocks,
                direction=rule.direction,
                impact=rule.base_importance,
                confidence=score / 100.0,
                reasoning="关键词匹配: " + str(hits),
            )
        except Exception:
            return None

    # ─── Sector → Stocks Resolution ──────────────

    def resolve_stocks(self, sectors: list[str]) -> list[str]:
        """Resolve sector/concept names to A-share stock codes via sector graph."""
        stocks: list[str] = []
        try:
            from modules.sector_tags import SectorTagProvider
            provider = SectorTagProvider()
            for sector in sectors:
                canonical = provider.canonical_tag(sector)
                target = canonical or sector
                for target_type in ("board", "industry", "concept"):
                    df = provider.get_members(target, target_type)
                    if not df.empty and "code" in df.columns:
                        codes = df["code"].dropna().astype(str).tolist()
                        stocks.extend(codes[:5])
                        if codes:
                            break
        except Exception:
            pass

        return list(dict.fromkeys(stocks))[:20]

    # ─── Agent Pipeline ─────────────────────────

    def enrich_insights(self, agent_insights: list[dict]) -> list[dict]:
        """Accept AI Agent's analysis, resolve sectors to stocks, return enriched."""
        enriched = []
        for ins in agent_insights:
            entry = dict(ins)
            sectors = entry.get("sectors", [])
            if sectors:
                entry["stocks"] = self.resolve_stocks(sectors)
            else:
                entry.setdefault("stocks", [])
            entry.setdefault("concepts", [])
            entry.setdefault("direction", "neutral")
            entry.setdefault("impact", 0.0)
            entry.setdefault("confidence", 0.0)
            entry.setdefault("reasoning", "")
            enriched.append(entry)
        return enriched

    def agent_full_report(self, agent_insights: list[dict]) -> str:
        """Full pipeline: agent analysis → enrich → markdown report."""
        enriched = self.enrich_insights(agent_insights)
        return self.report(insights=enriched)

    # ─── Reporting ──────────────────────────────

    def report(self, limit: int = 20,
               insights: list[dict] | None = None) -> str:
        """Generate markdown news intelligence report."""
        if insights is None:
            result = self.analyze(limit=limit)
            insights = result.get("insights", [])

        if not insights:
            return "## 新闻情报报告\n\n暂无高价值新闻信号。"

        lines = [
            "## 新闻情报报告",
            f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"分析条数: {len(insights)}",
            "",
        ]

        for i, insight in enumerate(insights[:limit], 1):
            topic = insight.get("topic", "未分类")
            title = insight.get("title", "")
            direction = insight.get("direction", "neutral")
            impact = insight.get("impact", 0)
            confidence = insight.get("confidence", 0)
            sectors = insight.get("sectors", [])
            stocks = insight.get("stocks", [])
            reasoning = insight.get("reasoning", "")

            dir_label = {"bullish": "看多", "bearish": "看空", "neutral": "中性"}.get(direction, direction)
            stars = "⭐" * min(5, int(confidence * 5 + 1))

            lines.append(f"### {i}. {topic} {stars}")
            lines.append(f"**{title}**")
            lines.append(f"方向: {dir_label} | 影响强度: {impact:.0%} | 置信度: {confidence:.0%}")
            if sectors:
                lines.append(f"影响板块: {', '.join(sectors)}")
            if stocks:
                lines.append(f"相关个股: {', '.join(stocks)}")
            if reasoning:
                lines.append(f"> {reasoning}")
            lines.append("")

        return "\n".join(lines)

    # ─── Observability ───────────────────────────

    def recovery_probe(self) -> dict[str, Any]:
        """Probe disabled sources that are due for recovery check.

        Returns dict of {source_label: {recovered: bool, error: str}}
        """
        due = self._health_tracker.due_for_recovery_probe()
        if not due:
            return {"probed": 0, "results": {}}

        results = {}
        probed = 0
        for source_label in due:
            parts = source_label.split(":", 1)
            if len(parts) != 2:
                continue
            source_type, source_id = parts

            try:
                from modules.event_intelligence.source_registry import SourceRegistry
                registry = SourceRegistry()
                source = registry.get(source_id)
                if source is None:
                    continue

                probed += 1
                items = self._fetch_source(source)
                self._health_tracker.record_success(source_label)
                self._metrics.record(source_label, 0, len(items), True)
                results[source_label] = {"recovered": True, "items": len(items)}
            except Exception as exc:
                self._health_tracker.record_failure(source_label, str(exc))
                self._metrics.record(source_label, 0, 0, False)
                results[source_label] = {"recovered": False, "error": str(exc)}

        return {"probed": probed, "results": results}

    def health_check(self) -> dict[str, dict]:
        """Run health check on all registered providers and persist."""
        from modules.news_sources import provider_names, get_provider

        results = {}
        for name in sorted(provider_names()):
            p = get_provider(name)
            if p is None:
                continue
            label = f"provider:{name}"
            t0 = time.time()
            try:
                h = p.health()
                latency_ms = (time.time() - t0) * 1000
                results[name] = {
                    "healthy": h.healthy,
                    "latency_ms": h.latency_ms,
                    "error": h.error,
                    "items_fetched": h.items_fetched,
                }
                self._health_tracker.record_success(label)
                self._metrics.record(label, latency_ms, h.items_fetched, h.healthy)
            except Exception as exc:
                latency_ms = (time.time() - t0) * 1000
                results[name] = {
                    "healthy": False,
                    "latency_ms": latency_ms,
                    "error": str(exc),
                }
                self._health_tracker.record_failure(label, str(exc))
                self._metrics.record(label, latency_ms, 0, False)

        return results

    def health_status(self) -> dict[str, dict]:
        """Get current health status snapshot (no network call).

        Includes disabled sources and their failure counts.
        """
        return self._health_tracker.status()

    def disabled_sources(self) -> list[str]:
        """List currently auto-disabled source labels."""
        return sorted(self._health_tracker.disabled_sources())

    def metrics_summary(self) -> dict:
        """Get per-source and aggregate metrics."""
        return self._metrics.stats()

    # ─── Helpers ────────────────────────────────

    @staticmethod
    def _analysis_to_dict(a: NewsAnalysis) -> dict:
        return {
            "news_id": a.news_id,
            "title": a.title,
            "topic": a.topic,
            "sectors": a.sectors,
            "concepts": a.concepts,
            "stocks": a.stocks,
            "direction": a.direction,
            "impact": a.impact,
            "confidence": a.confidence,
            "reasoning": a.reasoning,
        }

    @staticmethod
    def _build_topic_summary(insights: list[NewsAnalysis]) -> list[dict]:
        by_topic = {}
        for a in insights:
            if a.topic not in by_topic:
                by_topic[a.topic] = {"topic": a.topic, "count": 0,
                                     "total_impact": 0, "sectors": set(), "stocks": set()}
            by_topic[a.topic]["count"] += 1
            by_topic[a.topic]["total_impact"] += a.impact
            by_topic[a.topic]["sectors"].update(a.sectors)
            by_topic[a.topic]["stocks"].update(a.stocks)

        summary = []
        for t, data in sorted(by_topic.items(), key=lambda x: x[1]["total_impact"], reverse=True):
            summary.append({
                "topic": t, "count": data["count"],
                "total_impact": round(data["total_impact"], 2),
                "sectors": list(data["sectors"]), "stocks": list(data["stocks"]),
            })
        return summary
