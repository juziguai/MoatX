"""Manual news ingestion helpers for event intelligence."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from modules.config import cfg
from modules.db import DatabaseManager

from .collector import EventNewsCollector
from .models import NewsItem


def build_news_item(
    title: str,
    summary: str = "",
    url: str = "",
    source: str = "manual",
    published_at: str = "",
) -> NewsItem:
    """Build a normalized manual news item."""
    return NewsItem(
        source=source or "manual",
        title=str(title or "").strip(),
        summary=str(summary or "").strip(),
        url=str(url or "").strip(),
        published_at=str(published_at or "").strip(),
    )


def ingest_manual_news(
    title: str,
    summary: str = "",
    url: str = "",
    source: str = "manual",
    published_at: str = "",
    db: DatabaseManager | None = None,
) -> dict[str, Any]:
    """Persist one manually supplied news item."""
    item = build_news_item(
        title=title,
        summary=summary,
        url=url,
        source=source,
        published_at=published_at,
    )
    if not item.title:
        return {"fetched": 0, "inserted": 0, "duplicates": 0, "errors": ["title is required"]}
    return EventNewsCollector(db=db or DatabaseManager(cfg().data.warehouse_path)).ingest_items([item])


def ingest_news_file(
    path: str | Path,
    source: str = "manual_file",
    db: DatabaseManager | None = None,
) -> dict[str, Any]:
    """Persist news from a UTF-8 JSON or text file.

    JSON may be a single object, a list of objects, or an object containing
    records/items/news/articles. Text files are ingested as one title per line.
    """
    p = Path(path)
    text = p.read_text(encoding="utf-8")
    items: list[NewsItem]
    if p.suffix.lower() == ".json":
        items = _items_from_json(json.loads(text), source=source)
    else:
        items = [
            build_news_item(title=line.strip(), source=source)
            for line in text.splitlines()
            if line.strip()
        ]
    return EventNewsCollector(db=db or DatabaseManager(cfg().data.warehouse_path)).ingest_items(items)


def _items_from_json(payload: Any, source: str) -> list[NewsItem]:
    records = _find_records(payload)
    items: list[NewsItem] = []
    for record in records:
        title = _first(record, ["title", "Title", "newsTitle", "name"])
        if not title:
            continue
        items.append(
            build_news_item(
                title=str(title),
                summary=str(_first(record, ["summary", "description", "content", "digest", "abstract"]) or ""),
                url=str(_first(record, ["url", "link", "articleUrl", "wapurl", "pcUrl"]) or ""),
                published_at=str(_first(record, ["published_at", "publishTime", "time", "date", "ShowDate", "pubDate"]) or ""),
                source=str(record.get("source") or source),
            )
        )
    return items


def _find_records(payload: Any) -> list[dict]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if not isinstance(payload, dict):
        return []
    if any(k in payload for k in ("title", "Title", "newsTitle", "name")):
        return [payload]
    for key in ("records", "items", "news", "articles", "data", "Data", "result", "list"):
        value = payload.get(key)
        nested = _find_records(value)
        if nested:
            return nested
    return []


def _first(record: dict, keys: list[str]) -> Any:
    for key in keys:
        value = record.get(key)
        if value not in (None, ""):
            return value
    return None
