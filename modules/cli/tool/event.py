"""CLI commands for macro event intelligence."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from modules.event_intelligence.collector import EventNewsCollector
from modules.event_intelligence.context import EventContextBuilder
from modules.event_intelligence.elasticity import EventElasticityBacktester
from modules.event_intelligence.extractor import EventExtractor
from modules.event_intelligence.manual_ingest import ingest_manual_news, ingest_news_file
from modules.event_intelligence.news_factors import NewsFactorEngine
from modules.event_intelligence.news_intelligence import NewsIntelligenceEngine
from modules.event_intelligence.notifier import EventNotifier
from modules.event_intelligence.opportunity import EventOpportunityScanner
from modules.event_intelligence.probability import EventProbabilityEngine
from modules.event_intelligence.reporter import EventReporter
from modules.event_intelligence.service import EventIntelligenceService
from modules.event_intelligence.source_registry import SourceRegistry
from modules.event_intelligence.summary import build_event_monitor_summary
from modules.event_intelligence.topic_memory import TopicMemoryEngine


def cmd_event(args) -> None:
    """Run event intelligence actions from CLI."""
    action = args.event_action
    payload: dict[str, Any] | str

    if action == "collect":
        payload = EventNewsCollector().collect()
    elif action == "ingest":
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
        payload = EventExtractor().extract_unprocessed(limit=args.limit)
    elif action == "states":
        payload = EventProbabilityEngine().update_states()
    elif action == "opportunities":
        payload = EventOpportunityScanner().scan(
            min_probability=args.min_probability,
            per_effect_limit=args.per_effect_limit,
        )
    elif action == "report":
        payload = EventReporter().report(limit=args.limit)
    elif action == "news":
        payload = NewsIntelligenceEngine().analyze(
            limit=args.limit,
            topic=args.topic or None,
            min_score=args.min_score,
        )
    elif action == "news-report":
        payload = NewsIntelligenceEngine().report(
            limit=args.limit,
            topic=args.topic or None,
            min_score=args.min_score,
        )
    elif action == "news-factors":
        payload = NewsFactorEngine().build(
            limit=args.limit,
            min_score=args.min_score,
            top_n=args.top_events or 20,
        )
    elif action == "topics":
        payload = TopicMemoryEngine().update(
            limit=args.limit,
            min_score=args.min_score,
            top_n=args.top_events or 30,
        )
    elif action == "topic-snapshots":
        payload = {
            "engine": "topic_memory_v1",
            "topic": args.topic or "",
            "snapshots": TopicMemoryEngine().snapshots(topic=args.topic or "", limit=args.limit),
        }
    elif action == "sources":
        payload = _source_snapshot(limit=args.limit)
    elif action == "notify":
        payload = EventNotifier().notify(
            send=args.send,
            probability_threshold=args.probability_threshold,
            opportunity_threshold=args.opportunity_threshold,
            limit=args.limit,
        )
    elif action == "context":
        payload = EventContextBuilder().build(limit=args.limit)
    elif action == "summary":
        payload = build_event_monitor_summary(
            top_n=args.top_events,
            probability_threshold=args.probability_threshold,
            opportunity_threshold=args.opportunity_threshold,
        )
    elif action == "elasticity":
        payload = EventElasticityBacktester().run(
            event_id=args.event_id or "",
            windows=_parse_windows(args.windows),
            limit=args.limit,
        )
    elif action == "run":
        service = EventIntelligenceService()
        collect_stats = service.collect_news()
        extract_stats = service.extract_events(limit=args.limit)
        state_stats = service.update_states()
        opportunity_stats = service.scan_opportunities(
            min_probability=args.min_probability,
            per_effect_limit=args.per_effect_limit,
        )
        report = service.report(limit=args.limit)
        payload = {
            "collect": collect_stats,
            "extract": extract_stats,
            "states": state_stats,
            "opportunities": opportunity_stats,
            "report": report,
        }
        if args.notify:
            payload["notify"] = EventNotifier().notify(
                send=args.send,
                probability_threshold=args.probability_threshold,
                opportunity_threshold=args.opportunity_threshold,
                limit=args.limit,
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
