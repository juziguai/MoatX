"""News collection for macro event intelligence."""

from __future__ import annotations

import logging
import json
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from modules.config import cfg
from modules.db import DatabaseManager

from .models import EventSource, NewsItem, now_ts
from .source_registry import SourceRegistry

_logger = logging.getLogger("moatx.event_intelligence.collector")


class EventNewsCollector:
    """Collect and normalize event-related news from configured sources."""

    def __init__(
        self,
        db: DatabaseManager | None = None,
        registry: SourceRegistry | None = None,
    ):
        self._db = db or DatabaseManager(cfg().data.warehouse_path)
        self._registry = registry or SourceRegistry()

    def collect(self) -> dict[str, Any]:
        """Collect from all enabled sources and persist deduplicated news."""
        stats = {
            "sources": 0,
            "fetched": 0,
            "inserted": 0,
            "duplicates": 0,
            "errors": [],
            "source_stats": [],
            "message": "",
        }
        sources = self._registry.enabled()
        stats["sources"] = len(sources)
        if not sources:
            stats["message"] = "no enabled event news sources"
            return stats

        for source in sources:
            source_stat = {
                "source_id": source.id,
                "name": source.name,
                "type": source.type,
                "category": source.category,
                "enabled": source.enabled,
                "fetched": 0,
                "inserted": 0,
                "duplicates": 0,
                "error": "",
                "signal_hits": 0,
                "hit_rate": 0.0,
            }
            try:
                items = self.fetch_source(source)
                source_stat["fetched"] = len(items)
                stats["fetched"] += len(items)
                for item in items:
                    inserted_id = self._db.event().insert_news(item)
                    if inserted_id is None:
                        stats["duplicates"] += 1
                        source_stat["duplicates"] += 1
                    else:
                        stats["inserted"] += 1
                        source_stat["inserted"] += 1
            except Exception as exc:
                _logger.warning("event source [%s] failed: %s", source.id, exc)
                source_stat["error"] = str(exc)
                stats["errors"].append(f"{source.id}: {exc}")
            finally:
                self._db.event().upsert_source_quality(
                    source_id=source.id,
                    name=source.name,
                    category=source.category,
                    type=source.type,
                    enabled=source.enabled,
                    fetched=int(source_stat["fetched"]),
                    inserted=int(source_stat["inserted"]),
                    duplicates=int(source_stat["duplicates"]),
                    errors=1 if source_stat["error"] else 0,
                    last_success_at=now_ts() if not source_stat["error"] else "",
                    last_error=str(source_stat["error"]),
                )
                self._db.event().refresh_source_quality_signal_hits([source.id])
                quality = self._db.event().get_source_quality(source.id) or {}
                source_stat["signal_hits"] = int(quality.get("signal_hits") or 0)
                source_stat["hit_rate"] = float(quality.get("hit_rate") or 0.0)
                stats["source_stats"].append(source_stat)

        return stats

    def ingest_items(self, items: list[NewsItem]) -> dict[str, Any]:
        """Persist externally supplied news items. Useful for tests and manual feeds."""
        stats = {"fetched": len(items), "inserted": 0, "duplicates": 0, "errors": []}
        for item in items:
            try:
                inserted_id = self._db.event().insert_news(item)
                if inserted_id is None:
                    stats["duplicates"] += 1
                else:
                    stats["inserted"] += 1
            except Exception as exc:
                stats["errors"].append(str(exc))
        return stats

    def fetch_source(self, source: EventSource) -> list[NewsItem]:
        """Fetch one configured source and normalize its records."""
        if source.type == "rss":
            return self._fetch_rss(source)
        if source.type in ("http_json", "api", "jsonp"):
            return self._fetch_http_json(source)
        if source.type == "html":
            return self._fetch_html(source)
        raise ValueError(f"unsupported event source type: {source.type}")

    def _fetch_rss(self, source: EventSource) -> list[NewsItem]:
        resp = self._get(source)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)

        items: list[NewsItem] = []
        for node in root.findall(".//item"):
            title = self._text(node, "title")
            if not title:
                continue
            url = self._text(node, "link")
            summary = self._text(node, "description")
            published_at = self._text(node, "pubDate")
            items.append(
                NewsItem(
                    source=source.id,
                    title=title,
                    summary=summary,
                    url=url,
                    published_at=published_at,
                    fetched_at=now_ts(),
                )
            )
        return items

    def _fetch_http_json(self, source: EventSource) -> list[NewsItem]:
        resp = self._get(source)
        resp.raise_for_status()
        payload = self._parse_json_payload(resp)
        records = self._find_json_records(payload, source.record_path)

        items: list[NewsItem] = []
        for record in records:
            title = self._first(record, self._field_keys(source, "title", ["title", "Title", "newsTitle", "name"]))
            if not title:
                continue
            items.append(
                NewsItem(
                    source=source.id,
                    title=str(title),
                    summary=str(self._first(record, self._field_keys(
                        source,
                        "summary",
                        ["summary", "description", "content", "digest", "abstract"],
                    )) or ""),
                    url=str(self._first(record, self._field_keys(
                        source,
                        "url",
                        ["url", "link", "articleUrl", "wapurl", "pcUrl"],
                    )) or ""),
                    published_at=self._normalize_timestamp(self._first(record, self._field_keys(
                        source,
                        "published_at",
                        [
                            "published_at",
                            "publishTime",
                            "focus_date",
                            "ctime",
                            "time",
                            "date",
                            "ShowDate",
                            "pubDate",
                        ],
                    ))),
                    fetched_at=now_ts(),
                )
            )
        return items

    def _fetch_html(self, source: EventSource) -> list[NewsItem]:
        resp = self._get(source)
        resp.raise_for_status()
        if not resp.encoding or resp.encoding.lower() == "iso-8859-1":
            resp.encoding = resp.apparent_encoding or "utf-8"
        soup = BeautifulSoup(resp.text, "html.parser")
        contains = source.field_map.get("link_contains", [])
        if isinstance(contains, str):
            contains = [contains]
        contains = [str(item) for item in contains if str(item)]
        max_items = int(source.field_map.get("max_items") or 30)

        items: list[NewsItem] = []
        seen: set[str] = set()
        for anchor in soup.find_all("a"):
            title = anchor.get_text(" ", strip=True)
            href = str(anchor.get("href") or "").strip()
            if len(title) < 8 or not href:
                continue
            if contains and not any(token in href for token in contains):
                continue
            url = urljoin(source.field_map.get("base_url") or source.url, href)
            if url in seen:
                continue
            seen.add(url)
            summary = anchor.parent.get_text(" ", strip=True) if anchor.parent else ""
            items.append(
                NewsItem(
                    source=source.id,
                    title=title,
                    summary=summary if summary != title else "",
                    url=url,
                    published_at=self._extract_date(summary),
                    fetched_at=now_ts(),
                )
            )
            if len(items) >= max_items:
                break
        return items

    @staticmethod
    def _get(source: EventSource) -> requests.Response:
        """Fetch a source with a small retry budget for flaky news hosts."""
        last_error: Exception | None = None
        for attempt in range(3):
            try:
                session = requests.Session()
                session.trust_env = False
                return session.get(
                    source.url,
                    headers=source.headers or None,
                    timeout=cfg().crawler.timeout,
                )
            except requests.RequestException as exc:
                last_error = exc
                if attempt < 2:
                    time.sleep(0.5 * (attempt + 1))
        assert last_error is not None
        raise last_error

    @staticmethod
    def _text(node: ET.Element, tag: str) -> str:
        child = node.find(tag)
        return (child.text or "").strip() if child is not None else ""

    @classmethod
    def _find_json_records(cls, payload: Any, record_path: str = "") -> list[dict]:
        """Find a plausible news-record list from common JSON response shapes."""
        if record_path:
            selected = cls._get_path(payload, record_path)
            if isinstance(selected, list):
                return [x for x in selected if isinstance(x, dict)]
            if isinstance(selected, dict):
                nested = cls._find_json_records(selected)
                if nested:
                    return nested

        if isinstance(payload, list):
            return [x for x in payload if isinstance(x, dict)]
        if not isinstance(payload, dict):
            return []

        for key in ["data", "Data", "items", "news", "articles", "result", "list"]:
            value = payload.get(key)
            if isinstance(value, list):
                return [x for x in value if isinstance(x, dict)]
            if isinstance(value, dict):
                nested = cls._find_json_records(value)
                if nested:
                    return nested
        return []

    @staticmethod
    def _parse_json_payload(resp: requests.Response) -> Any:
        try:
            return resp.json()
        except ValueError:
            raw = getattr(resp, "content", b"")
            text = (
                raw.decode("utf-8", errors="replace")
                if raw
                else str(getattr(resp, "text", ""))
            ).strip()
            match = re.match(r"^[\w$.]+\((.*)\);?$", text, re.S)
            if match:
                return json.loads(match.group(1))
            return json.loads(text)

    @staticmethod
    def _first(record: dict, keys: list[str]) -> Any:
        for key in keys:
            value = EventNewsCollector._get_path(record, key) if "." in key else record.get(key)
            if value not in (None, ""):
                return value
        return None

    @staticmethod
    def _field_keys(source: EventSource, name: str, defaults: list[str]) -> list[str]:
        configured = source.field_map.get(name)
        if isinstance(configured, str):
            return [configured] + defaults
        if isinstance(configured, list):
            return [str(x) for x in configured] + defaults
        return defaults

    @staticmethod
    def _get_path(payload: Any, path: str) -> Any:
        current = payload
        cleaned = str(path or "").strip()
        if cleaned.startswith("$."):
            cleaned = cleaned[2:]
        elif cleaned == "$":
            cleaned = ""
        for part in cleaned.split("."):
            if not part:
                continue
            if isinstance(current, dict):
                current = current.get(part)
            elif isinstance(current, list) and part.isdigit():
                idx = int(part)
                current = current[idx] if 0 <= idx < len(current) else None
            else:
                return None
            if current is None:
                return None
        return current

    @staticmethod
    def _normalize_timestamp(value: Any) -> str:
        if value in (None, ""):
            return ""
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(float(value)).strftime("%Y-%m-%d %H:%M:%S")
        text = str(value).strip()
        if re.fullmatch(r"\d{10}(\.\d+)?", text):
            return datetime.fromtimestamp(float(text)).strftime("%Y-%m-%d %H:%M:%S")
        if re.fullmatch(r"\d{13}", text):
            return datetime.fromtimestamp(float(text) / 1000).strftime("%Y-%m-%d %H:%M:%S")
        return text

    @staticmethod
    def _extract_date(text: str) -> str:
        if not text:
            return ""
        match = re.search(r"20\d{2}[-/年]\d{1,2}[-/月]\d{1,2}", text)
        if match:
            return match.group(0).replace("年", "-").replace("月", "-").replace("/", "-").rstrip("-")
        match = re.search(r"\d{1,2}-\d{1,2}", text)
        if match:
            return f"{datetime.now().year}-{match.group(0)}"
        return ""


def collect_news() -> dict[str, Any]:
    """Convenience entry point for scheduler/CLI."""
    return EventNewsCollector().collect()
