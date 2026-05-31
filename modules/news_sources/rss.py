"""RSS news provider."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from typing import Any

import requests

from modules.news_source import NewsCapability, NewsSource
from modules.result import Result
from modules.event_intelligence.models import EventSource, NewsItem
from modules.config import cfg


class RSSProvider(NewsSource):
    """RSS/Atom feed news source."""

    @property
    def name(self) -> str:
        return "rss"

    def capabilities(self) -> set[NewsCapability]:
        return {NewsCapability.RSS_FETCH}

    def fetch(self, capability: NewsCapability, **params: Any):
        if capability != NewsCapability.RSS_FETCH:
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
            root = ET.fromstring(resp.content)

            items = []
            ns = {"atom": "http://www.w3.org/2005/Atom"}
            channel = root if root.tag == "feed" else root.find("channel")
            if channel is None:
                return Result.ok([], source=self.name)

            for entry in channel.findall("item") or channel.findall("atom:entry", ns) or []:
                title = self._text(entry, "title", ns)
                link = self._text(entry, "link", ns)
                desc = self._text(entry, "description", ns) or self._text(entry, "summary", ns)
                pub = self._text(entry, "pubDate", ns) or self._text(entry, "published", ns) or self._text(entry, "updated", ns)

                if not title:
                    continue

                items.append(NewsItem(
                    source=source.id,
                    title=title.strip(),
                    summary=(desc or "").strip(),
                    url=link or "",
                    published_at=pub or "",
                    language="zh",
                ))

            return Result.ok(items, source=self.name)

        except Exception as exc:
            return Result.fail(str(exc), source=self.name)

    @staticmethod
    def _text(element, tag, ns=None):
        el = element.find(tag, ns) if ns else element.find(tag)
        if el is not None and el.text:
            return el.text.strip()
        return ""
