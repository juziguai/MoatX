"""Unified NewsManager — single entry point for news collection, analysis, and reporting.

Replaces the old EventNewsCollector + NewsIntelligenceEngine duo with one
config-driven manager that routes through plugin providers and LLM reasoning.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from modules.db import DatabaseManager
from modules.config import cfg

logger = logging.getLogger("moatx.news_manager")


@dataclass
class NewsAnalysis:
    """LLM-driven analysis result for one news item."""
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
    """Unified news intelligence manager.

    Usage:
        mgr = NewsManager()
        stats = mgr.collect()                    # fetch from all sources
        insights = mgr.analyze(limit=50)         # LLM-driven analysis
        report = mgr.report(limit=20)            # markdown report
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

    # ─── LLM Analysis ───────────────────────────

    def analyze(self, limit: int = 100, use_llm: bool = True) -> dict[str, Any]:
        """Analyze recent news with LLM-driven topic extraction and sector mapping.

        Args:
            limit: max news items to analyze
            use_llm: use LLM for reasoning (falls back to keyword if False)

        Returns:
            dict with insights list, topic_summary, stats
        """
        df = self._db.event().list_news(limit=limit)
        if df.empty:
            return {"insights": [], "topic_summary": [], "stats": {"total": 0, "analyzed": 0}}

        insights: list[NewsAnalysis] = []
        for _idx, row in df.iterrows():
            try:
                if use_llm:
                    analysis = self._llm_analyze(row)
                else:
                    analysis = self._keyword_analyze(row)
                if analysis and analysis.confidence > 0:
                    insights.append(analysis)
            except Exception as exc:
                logger.debug("analyze news %s failed: %s", row.get("id"), exc)

        # Rank by impact * confidence
        insights.sort(key=lambda x: x.impact * x.confidence, reverse=True)

        topic_summary = self._build_topic_summary(insights)

        return {
            "insights": [self._analysis_to_dict(a) for a in insights],
            "topic_summary": topic_summary,
            "stats": {"total": len(df), "analyzed": len(insights)},
        }

    def _llm_analyze(self, row: dict) -> NewsAnalysis | None:
        """Use LLM to dynamically analyze news and infer A-share impact."""
        title = str(row.get("title", "") or "")
        summary = str(row.get("summary", "") or "")
        content = f"标题: {title}\n摘要: {summary}"

        prompt = self._build_llm_prompt(content)

        try:
            response = self._call_llm(prompt)
            parsed = self._parse_llm_response(response)
            if not parsed:
                return None

            # Resolve sectors to stocks via sector graph
            stocks = self._resolve_stocks(parsed.get("sectors", []))

            return NewsAnalysis(
                news_id=int(row.get("id", 0)),
                title=title,
                summary=summary,
                topic=parsed.get("topic", ""),
                sectors=parsed.get("sectors", []),
                concepts=parsed.get("concepts", []),
                stocks=stocks,
                direction=parsed.get("direction", "neutral"),
                impact=float(parsed.get("impact", 0)),
                confidence=float(parsed.get("confidence", 0)),
                reasoning=parsed.get("reasoning", ""),
            )
        except Exception as exc:
            logger.debug("LLM analysis failed: %s", exc)
            return self._keyword_analyze(row)

    def _build_llm_prompt(self, content: str) -> str:
        """Build prompt for LLM news analysis."""
        return f"""你是一位A股宏观事件分析师。请分析以下新闻，判断它对A股的影响。

新闻内容:
{content}

请以JSON格式返回:
{{
  "topic": "新闻主题(简短)",
  "sectors": ["受影响的A股行业板块名称"],
  "concepts": ["受影响的概念板块名称"],
  "direction": "bullish/neutral/bearish",
  "impact": 0.0-1.0之间的影响强度,
  "confidence": 0.0-1.0之间的分析置信度,
  "reasoning": "简短的分析理由(1-2句话)"
}}

注意: sectors和concepts请使用A股通用的行业/概念名称(如"半导体""人工智能""新能源""白酒""券商"等)。
只返回JSON,不要其他文字。"""

    def _call_llm(self, prompt: str) -> str:
        """Call LLM API for analysis."""
        import os
        import requests
        from modules.event_intelligence.llm_semantics import load_llm_settings

        settings = load_llm_settings()
        api_key = os.getenv(settings.api_key_env, "")
        if not api_key:
            logger.warning("LLM not configured, falling back to keyword analysis")
            raise RuntimeError("LLM not configured")

        url = settings.base_url.rstrip("/") + "/chat/completions"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": settings.model,
            "messages": [
                {"role": "system", "content": "你是一位A股宏观事件分析师。请严格以JSON格式回复。"},
                {"role": "user", "content": prompt},
            ],
            "temperature": settings.temperature,
            "max_tokens": settings.max_output_tokens,
        }

        try:
            resp = requests.post(
                url, headers=headers, json=payload,
                timeout=settings.timeout,
            )
            resp.raise_for_status()
            data = resp.json()
            content_text = data.get("choices", [{}])[0].get("message", {}).get("content", "")
            return content_text
        except requests.RequestException as exc:
            logger.warning("LLM API call failed: %s", exc)
            raise

    def _parse_llm_response(self, response: str) -> dict | None:
        """Parse LLM JSON response."""
        if not response:
            return None
        try:
            # Extract JSON block
            match = re.search(r"\{[\s\S]*\}", response)
            if match:
                return json.loads(match.group())
        except json.JSONDecodeError:
            pass
        return None

    def _keyword_analyze(self, row: dict) -> NewsAnalysis | None:
        """Fallback: keyword-based analysis using TOPIC_RULES."""
        try:
            from modules.event_intelligence.news_intelligence import (
                NewsIntelligenceEngine,
            )
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
                reasoning=f"关键词匹配: {hits}",
            )
        except Exception:
            return None

    def _resolve_stocks(self, sectors: list[str]) -> list[str]:
        """Resolve sector/concept names to A-share stock codes via sector graph."""
        stocks: list[str] = []
        try:
            from modules.sector_tags import SectorTagProvider
            provider = SectorTagProvider()
            for sector in sectors:
                # Try canonical tag match first
                canonical = provider.canonical_tag(sector)
                target = canonical or sector
                for target_type in ("board", "industry", "concept"):
                    df = provider.get_members(target, target_type)
                    if not df.empty and "code" in df.columns:
                        codes = df["code"].dropna().astype(str).tolist()
                        stocks.extend(codes[:5])  # top 5 per sector
                        if codes:
                            break  # found members, stop trying other types
        except Exception:
            pass

        return list(dict.fromkeys(stocks))[:20]  # dedup, cap at 20

    # ─── Reporting ──────────────────────────────

    def report(self, limit: int = 20) -> str:
        """Generate markdown news intelligence report with LLM analysis."""
        result = self.analyze(limit=limit, use_llm=True)
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
                by_topic[a.topic] = {"topic": a.topic, "count": 0, "total_impact": 0, "sectors": set(), "stocks": set()}
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
