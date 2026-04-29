"""Service orchestration for macro event intelligence."""

from __future__ import annotations

from typing import Any

from .collector import EventNewsCollector
from .context import EventContextBuilder
from .elasticity import EventElasticityBacktester
from .extractor import EventExtractor
from .manual_ingest import ingest_manual_news, ingest_news_file
from .notifier import EventNotifier
from .opportunity import EventOpportunityScanner
from .probability import EventProbabilityEngine
from .reporter import EventReporter


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
    ) -> dict[str, Any]:
        """Run the current P0 cycle: collect, extract, update, report."""
        collect_stats = self.collect_news()
        extract_stats = self.extract_events(limit=limit)
        state_stats = self.update_states()
        opportunity_stats = self.scan_opportunities(
            min_probability=min_probability,
            per_effect_limit=per_effect_limit,
        )
        report = self.report()
        result = {
            "collect": collect_stats,
            "extract": extract_stats,
            "states": state_stats,
            "opportunities": opportunity_stats,
            "report": report,
        }
        if notify:
            result["notify"] = self.notify(send=send)
        return result


def run_event_cycle() -> dict[str, Any]:
    """Convenience entry point for scheduler/CLI."""
    return EventIntelligenceService().run_event_cycle()
