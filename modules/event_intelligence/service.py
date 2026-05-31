"""Service orchestration for macro event intelligence.

Uses NewsManager for collection/analysis/reporting;
retains legacy engines for extraction/probability/opportunity.
"""

from __future__ import annotations

from typing import Any

from .context import EventContextBuilder
from .elasticity import EventElasticityBacktester
from .extractor import EventExtractor
from .manual_ingest import ingest_manual_news, ingest_news_file
from .llm_semantics import LLMSemanticReviewer
from .news_factors import NewsFactorEngine
from .notifier import EventNotifier
from .opportunity import EventOpportunityScanner
from .probability import EventProbabilityEngine
from .topic_memory import TopicMemoryEngine
from modules.news_manager import NewsManager


class EventIntelligenceService:
    """P0 orchestration service for event intelligence.

    Uses NewsManager for collection/analysis/reporting;
    retains legacy engines for extraction/probability/opportunity.
    """

    def __init__(self):
        from modules.db import DatabaseManager
        from modules.config import cfg
        self._db = DatabaseManager(cfg().data.warehouse_path)

    def collect_news(self) -> dict[str, Any]:
        """Collect news via NewsManager (plugin-based providers)."""
        return NewsManager(db=self._db).collect()

    def ingest_news(
        self,
        title: str = "",
        summary: str = "",
        url: str = "",
        source: str = "manual",
        published_at: str = "",
        file: str | None = None,
    ) -> dict[str, Any]:
        if file:
            return ingest_news_file(file, source=source)
        return ingest_manual_news(
            title=title,
            summary=summary,
            url=url,
            source=source,
            published_at=published_at,
        )

    def extract_events(self, limit: int = 200) -> dict[str, Any]:
        return EventExtractor().extract_unprocessed(limit=limit)

    def update_states(self) -> dict[str, Any]:
        return EventProbabilityEngine().update_states()

    def scan_opportunities(
        self,
        min_probability: float = 0.35,
        per_effect_limit: int = 20,
    ) -> dict[str, Any]:
        return EventOpportunityScanner().scan(
            min_probability=min_probability,
            per_effect_limit=per_effect_limit,
        )

    def report(self, limit: int = 20) -> str:
        """Generate markdown report via NewsManager."""
        return NewsManager(db=self._db).report(limit=limit)

    def news_intelligence(
        self,
        limit: int = 200,
        min_score: float = 45.0,
        topic: str | None = None,
    ) -> dict[str, Any]:
        """Analyze news via NewsManager (LLM-driven with keyword fallback)."""
        return NewsManager(db=self._db).analyze(limit=limit)

    def news_factors(
        self,
        limit: int = 200,
        min_score: float = 55.0,
        top_n: int = 20,
    ) -> dict[str, Any]:
        return NewsFactorEngine().build(limit=limit, min_score=min_score, top_n=top_n)

    def news_factor_backfill(
        self,
        start_date: str = "",
        end_date: str = "",
        lookback_days: int = 14,
        min_score: float = 55.0,
        top_n: int = 100,
    ) -> dict[str, Any]:
        return NewsFactorEngine().backfill_snapshots(
            start_date=start_date,
            end_date=end_date,
            lookback_days=lookback_days,
            min_score=min_score,
            top_n=top_n,
        )

    def topic_memory(
        self,
        limit: int = 200,
        min_score: float = 45.0,
        top_n: int = 30,
    ) -> dict[str, Any]:
        return TopicMemoryEngine().update(limit=limit, min_score=min_score, top_n=top_n)

    def llm_review(
        self,
        limit: int = 100,
        min_score: float = 45.0,
        send: bool = False,
    ) -> dict[str, Any]:
        return LLMSemanticReviewer().review(limit=limit, min_score=min_score, send=send)

    def notify(
        self,
        send: bool = False,
        limit: int = 10,
        probability_threshold: float | None = None,
        opportunity_threshold: float | None = None,
    ) -> dict[str, Any]:
        return EventNotifier().notify(
            send=send,
            limit=limit,
            probability_threshold=probability_threshold,
            opportunity_threshold=opportunity_threshold,
        )

    def context(self, limit: int = 20) -> dict[str, Any]:
        return EventContextBuilder().build(limit=limit)

    def elasticity(
        self,
        event_id: str = "",
        windows: list[int] | None = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        return EventElasticityBacktester().run(event_id=event_id, windows=windows, limit=limit)

    # ─── Agent Pipeline (for external AI Agent like Codex CLI) ──

    def list_news_for_agent(self, limit: int = 50, period: str = "today") -> list[dict[str, Any]]:
        """Return raw news for an external AI Agent to reason over.

        period: 'today' | '3d' | '7d' | 'month' | ISO datetime
        """
        return NewsManager(db=self._db).list_news(limit=limit, period=period)

    def cleanup_old_news(self, keep_days: int = 7) -> dict[str, Any]:
        """Delete news older than keep_days."""
        return NewsManager(db=self._db).cleanup_old_news(keep_days=keep_days)

    def resolve_sectors_for_agent(self, sectors: list[str]) -> list[str]:
        """Resolve sector/concept names to A-share stock codes."""
        return NewsManager(db=self._db).resolve_stocks(sectors)

    def agent_report(self, insights: list[dict]) -> str:
        """Agent pipeline: accept AI analysis → enrich stocks → produce report.

        The caller (Codex CLI / scheduler) does LLM reasoning externally,
        then calls this method with structured insights to get a markdown report.
        """
        return NewsManager(db=self._db).agent_full_report(insights)

    def news_health_check(self) -> dict[str, Any]:
        """Run health check on all news providers."""
        return NewsManager(db=self._db).health_check()

    def news_health_status(self) -> dict[str, Any]:
        """Get current health snapshot (no network call)."""
        return NewsManager(db=self._db).health_status()

    def news_recovery_probe(self) -> dict[str, Any]:
        """Probe disabled news sources for auto-recovery."""
        return NewsManager(db=self._db).recovery_probe()

    def news_disabled_sources(self) -> list[str]:
        """List auto-disabled news source labels."""
        return NewsManager(db=self._db).disabled_sources()

    def news_metrics(self) -> dict[str, Any]:
        """Get per-source and aggregate metrics."""
        return NewsManager(db=self._db).metrics_summary()

    def run_agent_cycle(self, limit: int = 50, collect_first: bool = False) -> dict[str, Any]:
        """One-stop cycle for AI Agent:
        1. Optionally collect fresh news
        2. Return raw news for agent reasoning
        3. (Agent reasons externally)
        4. Agent calls agent_report(insights) separately

        Returns: {"collected": ..., "raw_news": [...]}
        """
        result: dict[str, Any] = {}
        if collect_first:
            result["collected"] = self.collect_news()
        NewsManager(db=self._db).list_news(limit=limit, period="3d")
        result["news_count"] = len(result["raw_news"])
        result["hint"] = "Call agent_report(insights) with your LLM analysis to produce final report."
        return result


    def run_event_cycle(
        self,
        limit: int = 200,
        min_probability: float = 0.35,
        per_effect_limit: int = 20,
        notify: bool = False,
        send: bool = False,
        probability_threshold: float | None = None,
        opportunity_threshold: float | None = None,
    ) -> dict[str, Any]:
        """Run the current P0 cycle: collect, extract, update, report."""
        collect_stats = self.collect_news()
        extract_stats = self.extract_events(limit=limit)
        state_stats = self.update_states()
        news_stats = self.news_intelligence(limit=limit, min_score=45.0)
        news_factor_stats = self.news_factors(limit=limit, min_score=55.0, top_n=30)
        topic_stats = self.topic_memory(limit=limit, min_score=45.0, top_n=30)
        opportunity_stats = self.scan_opportunities(
            min_probability=min_probability,
            per_effect_limit=per_effect_limit,
        )
        report = self.report()
        result = {
            "collect": collect_stats,
            "extract": extract_stats,
            "states": state_stats,
            "news_intelligence": news_stats,
            "news_factors": news_factor_stats,
            "topic_memory": topic_stats,
            "opportunities": opportunity_stats,
            "report": report,
        }
        if notify:
            result["notify"] = self.notify(
                send=send,
                probability_threshold=probability_threshold,
                opportunity_threshold=opportunity_threshold,
            )
        return result


def run_event_cycle() -> dict[str, Any]:
    """Convenience entry point for scheduler/CLI."""
    return EventIntelligenceService().run_event_cycle()
