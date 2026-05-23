"""CLI commands for macro event intelligence."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def cmd_event(args) -> None:
    """Run event intelligence actions from CLI."""
    action = args.event_action
    payload: dict[str, Any] | str

    if action == "collect":
        from modules.event_intelligence.collector import EventNewsCollector

        payload = EventNewsCollector().collect()
    elif action == "ingest":
        from modules.event_intelligence.manual_ingest import ingest_manual_news, ingest_news_file

        if args.file:
            payload = ingest_news_file(args.file, source=args.source)
        else:
            payload = ingest_manual_news(
                title=args.title or "",
                summary=args.summary or "",
                url=args.url or "",
                source=args.source,
                published_at=args.published_at or "",
            )
    elif action == "extract":
        from modules.event_intelligence.extractor import EventExtractor

        payload = EventExtractor().extract_unprocessed(limit=args.limit)
    elif action == "states":
        from modules.event_intelligence.probability import EventProbabilityEngine

        payload = EventProbabilityEngine().update_states()
    elif action == "opportunities":
        from modules.event_intelligence.opportunity import EventOpportunityScanner

        payload = EventOpportunityScanner().scan(
            min_probability=args.min_probability,
            per_effect_limit=args.per_effect_limit,
        )
    elif action == "report":
        from modules.event_intelligence.reporter import EventReporter

        payload = EventReporter().report(limit=args.limit)
    elif action == "news":
        from modules.event_intelligence.service import EventIntelligenceService

        payload = EventIntelligenceService().news_intelligence(
            limit=args.limit,
            topic=args.topic or None,
            min_score=args.min_score,
        )
    elif action == "news-report":
        from modules.event_intelligence.news_intelligence import NewsIntelligenceEngine

        payload = NewsIntelligenceEngine().report(
            limit=args.limit,
            topic=args.topic or None,
            min_score=args.min_score,
        )
    elif action == "news-factors":
        from modules.event_intelligence.service import EventIntelligenceService

        payload = EventIntelligenceService().news_factors(
            limit=args.limit,
            min_score=args.min_score,
            top_n=args.top_events or 20,
        )
    elif action == "topics":
        from modules.event_intelligence.service import EventIntelligenceService

        payload = EventIntelligenceService().topic_memory(
            limit=args.limit,
            min_score=args.min_score,
            top_n=args.top_events or 30,
        )
    elif action == "topic-snapshots":
        from modules.event_intelligence.topic_memory import TopicMemoryEngine

        payload = {
            "engine": "topic_memory_v1",
            "topic": args.topic or "",
            "snapshots": TopicMemoryEngine().snapshots(topic=args.topic or "", limit=args.limit),
        }
    elif action == "llm-status":
        from modules.event_intelligence.llm_semantics import llm_settings_status

        payload = llm_settings_status()
    elif action == "llm-review":
        from modules.event_intelligence.service import EventIntelligenceService

        payload = EventIntelligenceService().llm_review(
            limit=args.limit,
            min_score=args.min_score,
            send=args.send,
        )
    elif action == "llm-reviews":
        from modules.event_intelligence.llm_semantics import LLMSemanticReviewer

        payload = {
            "engine": "llm_semantic_review_v1",
            "reviews": LLMSemanticReviewer().list_reviews(limit=args.limit),
        }
    elif action == "sources":
        payload = _source_snapshot(limit=args.limit)
    elif action == "notify":
        from modules.event_intelligence.notifier import EventNotifier

        payload = EventNotifier().notify(
            send=args.send,
            probability_threshold=args.probability_threshold,
            opportunity_threshold=args.opportunity_threshold,
            limit=args.limit,
        )
    elif action == "context":
        from modules.event_intelligence.context import EventContextBuilder

        payload = EventContextBuilder().build(limit=args.limit)
    elif action == "summary":
        from modules.event_intelligence.summary import build_event_monitor_summary

        payload = build_event_monitor_summary(
            top_n=args.top_events,
            probability_threshold=args.probability_threshold,
            opportunity_threshold=args.opportunity_threshold,
        )
    elif action == "elasticity":
        from modules.event_intelligence.elasticity import EventElasticityBacktester

        payload = EventElasticityBacktester().run(
            event_id=args.event_id or "",
            windows=_parse_windows(args.windows),
            limit=args.limit,
        )
    elif action == "calibration":
        from modules.event_intelligence.elasticity import EventElasticityBacktester

        payload = EventElasticityBacktester().calibrate(
            event_id=args.event_id or "",
            windows=_parse_windows(args.windows),
            limit=args.limit,
        )
    elif action == "run":
        from modules.event_intelligence.service import EventIntelligenceService

        service = EventIntelligenceService()
        payload = service.run_event_cycle(
            limit=args.limit,
            min_probability=args.min_probability,
            per_effect_limit=args.per_effect_limit,
            notify=args.notify,
            send=args.send,
            probability_threshold=args.probability_threshold,
            opportunity_threshold=args.opportunity_threshold,
        )
    else:
        raise SystemExit(f"Unknown event action: {action}")

    _emit(payload, as_json=args.as_json, output=args.output)


def _emit(payload: dict[str, Any] | str, *, as_json: bool = False, output: str | None = None) -> None:
    if isinstance(payload, str) and not as_json:
        text = payload
    else:
        text = json.dumps(payload, ensure_ascii=False, indent=2)

    if output:
        Path(output).write_text(text, encoding="utf-8")
        print(f"written: {output}")
        return

    print(text)


def _parse_windows(value: str) -> list[int]:
    return [int(item.strip()) for item in str(value or "1,3,5,10").split(",") if item.strip()]


def _source_snapshot(limit: int = 200) -> dict[str, Any]:
    """Return configured sources plus recent quality statistics."""
    from modules.event_intelligence.context import EventContextBuilder
    from modules.event_intelligence.source_registry import SourceRegistry

    registry = SourceRegistry()
    sources = registry.load()
    quality = EventContextBuilder().build(limit=limit).get("source_quality", [])
    quality_by_id = {row.get("source_id"): row for row in quality}
    rows = []
    for source in sources:
        row = {
            "source_id": source.id,
            "name": source.name,
            "type": source.type,
            "category": source.category,
            "enabled": source.enabled,
            "url": source.url,
        }
        row.update({k: v for k, v in quality_by_id.get(source.id, {}).items() if k not in row})
        rows.append(row)
    return {
        "sources": len(rows),
        "enabled": sum(1 for row in rows if row["enabled"]),
        "disabled": sum(1 for row in rows if not row["enabled"]),
        "items": rows,
    }
