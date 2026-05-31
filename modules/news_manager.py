"""Unified NewsManager — pure data layer for news collection and structured output.

Architecture:
  NewsManager 只负责采集 + 结构化 + 关键词快速过滤。
  语义推理（新闻→板块→个股→方向）交给上层 AI Agent（Codex CLI / 调度器）。

Usage:
    mgr = NewsManager()
    mgr.collect()              # fetch from all sources → DB
    news = mgr.list_news(50)   # raw news for AI Agent to reason over
    insights = mgr.analyze(50) # keyword-only fast filter
    report = mgr.report(20)    # markdown report
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from modules.config import cfg
from modules.db import DatabaseManager

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
    """Pure data layer: collect + list + keyword-filter + report.

    Semantic reasoning is delegated to the calling AI Agent.
    The Agent reads list_news(), does LLM inference externally,
    then feeds structured insights back via report(insights=...).
    """

    def __init__(self, db: DatabaseManager | None = None):
        self._db = db or DatabaseManager(cfg().data.warehouse_path)

    # ─── Collection ──────────────────────────────

    def collect(self) -> dict[str, Any]:
        """Collect news from all enabled sources via plugin providers."""
        from modules.event_intelligence.source_registry import SourceRegistry

        registry = SourceRegistry()
        sources = registry.enabled()
        stats = {"sources": len(sources), "fetched": 0, "inserted": 0,
                 "duplicates": 0, "errors": [], "source_stats": []}

        if not sources:
            stats["message"] = "no enabled news sources"
            return stats

        for source in sources:
            stat = {"source_id": source.id, "name": source.name,
                    "type": source.type, "fetched": 0, "inserted": 0,
                    "error": ""}
            try:
                items = self._fetch_source(source)
                stat["fetched"] = len(items)
                stats["fetched"] += len(items)
                for item in items:
                    inserted_id = self._db.event().insert_news(item)
                    if inserted_id is None:
                        stats["duplicates"] += 1
                    else:
                        stats["inserted"] += 1
                        stat["inserted"] += 1
            except Exception as exc:
                logger.warning("news source [%s] failed: %s", source.id, exc)
                stat["error"] = str(exc)
                stats["errors"].append(f"{source.id}: {exc}")
            stats["source_stats"].append(stat)

        return stats

    def _fetch_source(self, source):
        """Fetch news from one source using the appropriate provider."""
        from modules.news_source import NewsCapability
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

    def list_news(self, limit: int = 50) -> list[dict[str, Any]]:
        """Return raw news records as a list of dicts.

        The calling AI Agent reads this output and does its own
        LLM reasoning to infer topics / sectors / stocks / direction.
        """
        df = self._db.event().list_news(limit=limit)
        if df.empty:
            return []
        records = df.to_dict(orient="records")
        for r in records:
            for k, v in r.items():
                if hasattr(v, "item"):
                    r[k] = v.item()
        return records

    # ─── Keyword Analysis (fast fallback) ─────────

    def analyze(self, limit: int = 100) -> dict[str, Any]:
        """Keyword-only fast filter using TOPIC_RULES.

        Returns dict with insights list, topic_summary, stats.
        For full semantic reasoning, use list_news() + external LLM.
        """
        df = self._db.event().list_news(limit=limit)
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
        """Resolve sector/concept names to A-share stock codes via sector graph.

        Called by the AI Agent after it infers sectors from news content.
        """
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
        """Accept AI Agent's analysis, resolve sectors to stocks, return enriched.

        Each insight dict from the agent should have:
            news_id, title, topic, sectors, concepts, direction, impact, confidence, reasoning

        Returns the same list with "stocks" populated from sector graph.
        """
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
        """Generate markdown news intelligence report.

        If insights is provided (from external AI Agent), use them directly.
        Otherwise, fall back to keyword-only analyze().
        """
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

    def health_all(self) -> dict[str, dict]:
        """Health check all registered news providers."""
        from modules.news_sources import provider_names, get_provider
        result = {}
        for name in sorted(provider_names()):
            p = get_provider(name)
            if p is None:
                continue
            try:
                h = p.health()
                result[name] = {
                    "healthy": h.healthy,
                    "latency_ms": h.latency_ms,
                    "error": h.error,
                    "items_fetched": h.items_fetched,
                }
            except Exception as exc:
                result[name] = {"healthy": False, "latency_ms": 0, "error": str(exc)}
        return result

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
