"""HTTP JSON / JSONP news provider."""

from __future__ import annotations

import json
import re
from typing import Any

import requests

from modules.news_source import NewsCapability, NewsSource
from modules.result import Result
from modules.event_intelligence.models import EventSource, NewsItem
from modules.config import cfg


class HttpJsonProvider(NewsSource):
    """HTTP JSON / JSONP endpoint news source."""

    @property
    def name(self) -> str:
        return "http_json"

    def capabilities(self) -> set[NewsCapability]:
        return {NewsCapability.HTTP_JSON_FETCH, NewsCapability.JSONP_FETCH}

    def fetch(self, capability: NewsCapability, **params: Any):
        if capability not in (NewsCapability.HTTP_JSON_FETCH, NewsCapability.JSONP_FETCH):
            return Result.fail(f"unsupported: {capability}", source=self.name)
        return self._fetch(params.get("source"))

    def _fetch(self, source: EventSource | None):
        if source is None:
            return Result.fail("no source config", source=self.name)

        try:
            session = requests.Session()
            session.trust_env = False
            session.proxies = {"http": None, "https": None}
            resp = session.get(
                source.url,
                headers=source.headers or {},
                timeout=cfg().crawler.timeout,
            )
            resp.raise_for_status()
            text = resp.text

            if source.type == "jsonp":
                match = re.search(r"\((.*)\)", text, re.DOTALL)
                text = match.group(1) if match else text

            data = json.loads(text)

            record_path = source.record_path or ""
            records = data
            for key in record_path.split("."):
                key = key.strip()
                if not key:
                    continue
                if isinstance(records, dict):
                    records = records.get(key, [])
                elif isinstance(records, list) and key.isdigit():
                    records = records[int(key)]
                else:
                    records = []
                    break

            if not isinstance(records, list):
                records = [records] if records else []

            fm = source.field_map or {}
            items = []
            for rec in records:
                if not isinstance(rec, dict):
                    continue
                title = str(self._get_field(rec, fm, "title", ""))
                if not title:
                    continue
                items.append(NewsItem(
                    source=source.id,
                    title=title.strip(),
                    summary=str(self._get_field(rec, fm, "summary", "")).strip(),
                    url=str(self._get_field(rec, fm, "url", "")),
                    published_at=str(self._get_field(rec, fm, "published_at", "")),
                    language="zh",
                ))

            return Result.ok(items, source=self.name)

        except Exception as exc:
            return Result.fail(str(exc), source=self.name)

    @staticmethod
    def _get_field(record: dict, field_map: dict, key: str, default: Any = ""):
        mapped = field_map.get(key, key)
        return record.get(mapped, default)
