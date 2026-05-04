"""Service orchestration for macro event intelligence."""

from __future__ import annotations

from typing import Any

from .collector import EventNewsCollector
from .context import EventContextBuilder
from .elasticity import EventElasticityBacktester
from .extractor import EventExtractor
from .manual_ingest import ingest_manual_news, ingest_news_file
from .llm_semantics import LLMSemanticReviewer
from .news_factors import NewsFactorEngine
from .news_intelligence import NewsIntelligenceEngine
from .notifier import EventNotifier
from .opportunity import EventOpportunityScanner
from .probability import EventProbabilityEngine
from .reporter import EventReporter
from .topic_memory import TopicMemoryEngine


class EventIntelligenceService:
    """P0 orchestration service for event intelligence."""

    def collect_news(self) -> dict[str, Any]:
        return EventNewsCollector().collect()

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

    def report(self, limit: int = 10) -> str:
        return EventReporter().report(limit=limit)

    def news_intelligence(
        self,
        limit: int = 200,
        min_score: float = 45.0,
        topic: str | None = None,
    ) -> dict[str, Any]:
        return NewsIntelligenceEngine().analyze(limit=limit, min_score=min_score, topic=topic)

    def news_factors(
        self,
        limit: int = 200,
        min_score: float = 55.0,
        top_n: int = 20,
    ) -> dict[str, Any]:
        return NewsFactorEngine().build(limit=limit, min_score=min_score, top_n=top_n)

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
