"""HTML scraping news provider."""

from __future__ import annotations

from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from modules.news_source import NewsCapability, NewsSource
from modules.result import Result
from modules.event_intelligence.models import EventSource, NewsItem
from modules.config import cfg


class HtmlProvider(NewsSource):
    """HTML page scraping news source."""

    @property
    def name(self) -> str:
        return "html"

    def capabilities(self) -> set[NewsCapability]:
        return {NewsCapability.HTML_SCRAPE}

    def fetch(self, capability: NewsCapability, **params: Any):
        if capability != NewsCapability.HTML_SCRAPE:
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
            soup = BeautifulSoup(resp.content, "html.parser")

            fm = source.field_map or {}
            base_url = fm.get("base_url", source.url)
            link_contains = fm.get("link_contains", "")
            max_items = int(fm.get("max_items", 30))

            if isinstance(link_contains, str) and link_contains:
                link_contains = [link_contains]

            links = soup.find_all("a", href=True)
            items = []
            seen = set()

            for a in links:
                href = str(a.get("href", "")).strip()
                if not href:
                    continue

                full_url = urljoin(base_url, href)
                if link_contains:
                    if not any(pat in href for pat in link_contains):
                        continue

                if full_url in seen:
                    continue
                seen.add(full_url)

                title = a.get_text(strip=True)
                if not title or len(title) < 4:
                    continue

                items.append(NewsItem(
                    source=source.id,
                    title=title,
                    summary="",
                    url=full_url,
                    published_at="",
                    language="zh",
                ))

                if len(items) >= max_items:
                    break

            return Result.ok(items, source=self.name)

        except Exception as exc:
            return Result.fail(str(exc), source=self.name)
