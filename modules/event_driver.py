"""Event-driven sentiment engine for Layer 4 scoring.

Maps macro news → affected sectors, scans CNINFO announcements for
individual stock sentiment, and outputs event_score used as a multiplier
in ScoringEngine's Layer 4.

Design:
  - Keyword → sector mapping loaded from data/event_sector_map.toml
  - CNINFO announcements scanned for positive/negative keywords per stock
  - Time decay applied: recent events carry more weight
  - Output: per-stock event_boost (-40 to +40), applied as multiplier (0.6-1.4)
"""

from __future__ import annotations

import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import requests

from modules.config import cfg, tomllib
from modules.sector_tags import SectorTagProvider

_logger = logging.getLogger("moatx.event_driver")

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_EVENT_MAP_PATH = _PROJECT_ROOT / "data" / "event_sector_map.toml"

# ── 个股公告情绪关键词 ──────────────────────────────

POSITIVE_KW = sorted([
    ("业绩预增", 12), ("重大合同", 12), ("中标", 10), ("回购", 8),
    ("增持", 8), ("高分红", 6), ("新产品发布", 6), ("专利授权", 5),
    ("产能扩张", 7), ("战略合作", 7), ("行业龙头", 4), ("国产替代", 6),
    ("净利润增长", 10), ("营收增长", 8), ("毛利率提升", 7),
    ("获得批文", 6), ("技术突破", 8), ("进入供应链", 9),
], key=lambda x: len(x[0]), reverse=True)

NEGATIVE_KW = sorted([
    ("业绩预亏", -12), ("业绩下滑", -10), ("减持", -8), ("质押", -6),
    ("诉讼", -8), ("处罚", -10), ("立案调查", -15), ("退市风险", -20),
    ("商誉减值", -12), ("资产减值", -10), ("董事长辞职", -5),
    ("债务逾期", -12), ("担保风险", -8), ("监管函", -6),
    ("不能表示意见", -20), ("否定意见", -20),
], key=lambda x: len(x[0]), reverse=True)


class EventDriver:
    """Macro event → sector mapping + individual stock sentiment."""

    def __init__(self, sector_provider: SectorTagProvider | None = None):
        self._events = self._load_event_map()
        self._cache: dict[str, dict] = {}  # per-symbol sentiment cache
        self._sector_provider = sector_provider or SectorTagProvider()

    # ── Public API ────────────────────────────────

    # ── Multi-tag reverse lookup (industry + concept) ───────────────────────

    def _build_code_to_tags(self) -> dict[str, set[str]]:
        """Compatibility wrapper for old tests/callers."""
        return self._sector_provider.build_code_to_tags()

    def _get_tags(self, symbol: str) -> set[str]:
        """Return all industry + concept tags for a stock (cached lookup)."""
        return self._sector_provider.get_tags(symbol)

    # ── Public API ───────────────────────────────────────────────────────────

    def score_batch(self, symbols: list[str]) -> dict[str, float]:
        """Return per-symbol event_boost dict (-40 to +40)."""
        results = {}
        sector_boosts = self._active_sector_boosts()

        with ThreadPoolExecutor(max_workers=min(len(symbols), 8)) as ex:
            futures = {ex.submit(self.score_single, s, sector_boosts): s for s in symbols}
            for fut in as_completed(futures):
                sym = futures[fut]
                try:
                    results[sym] = fut.result()
                except Exception:
                    results[sym] = 0.0
        return results

    def score_single(self, symbol: str, sector_boosts: dict[str, float] | None = None) -> float:
        """Score a single stock's event sentiment. Returns -40 to +40."""
        if sector_boosts is None:
            sector_boosts = self._active_sector_boosts()

        score = 0.0

        # 1. Sector-level boost (macro news → sector mapping)
        # A stock can belong to multiple industries/concepts; accumulate each event tag once
        matched_event_tags: set[str] = set()
        for tag in self._get_tags(symbol):
            for event_tag, boost in sector_boosts.items():
                if event_tag in matched_event_tags:
                    continue
                if self._tag_matches(tag, event_tag):
                    score += boost
                    matched_event_tags.add(event_tag)

        # 2. Individual stock sentiment (CNINFO announcements)
        score += self._scan_announcement_sentiment(symbol)

        return max(-40.0, min(40.0, round(score, 1)))

    # ── Sector boost from macro events ─────────────

    def _active_sector_boosts(self) -> dict[str, float]:
        """Calculate sector boost scores from active macro events."""
        sector_scores: dict[str, float] = {}

        for event in self._events:
            name = event["name"]
            boost = event.get("boost_sectors", [])
            penalty = event.get("penalty_sectors", [])

            # Check if event is active (keywords hit in recent news)
            event_hit = False
            recent_ts = None
            for kw in event.get("keywords", []):
                hit, ts = self._check_keyword_in_news(kw)
                if hit:
                    event_hit = True
                    if recent_ts is None or ts > recent_ts:
                        recent_ts = ts
                    break

            if not event_hit:
                continue

            # Calculate impact with time decay
            days_ago = 0
            if recent_ts:
                days_ago = (datetime.now() - recent_ts).days
            decay = self._time_decay(days_ago)
            impact = event.get("impact_score", 15) * decay

            for s in boost:
                sector_scores[s] = sector_scores.get(s, 0) + impact
            for s in penalty:
                sector_scores[s] = sector_scores.get(s, 0) - impact

            _logger.info("事件 [%s] 命中: days=%d decay=%.1f impact=%.1f boost=%s penalty=%s",
                         name, days_ago, decay, impact, boost[:3], penalty[:3])

        try:
            from modules.event_intelligence.news_factors import NewsFactorEngine

            for sector, score in NewsFactorEngine().sector_boosts().items():
                sector_scores[sector] = sector_scores.get(sector, 0.0) + float(score)
        except Exception as e:
            _logger.debug("新闻因子评分加载失败: %s", e)

        return sector_scores

    def _check_keyword_in_news(self, keyword: str) -> tuple[bool, datetime | None]:
        """Check if a keyword appears in recent financial news headlines.

        Uses EastMoney news search as a lightweight probe.
        Returns (hit, approximate_timestamp).
        """
        try:
            session = requests.Session()
            session.trust_env = False
            url = "https://searchapi.eastmoney.com/bussiness/Web/GetCMSSearchResult"
            params = {
                "type": "8196",
                "pageindex": 1,
                "pagesize": 5,
                "keyword": keyword,
                "name": "zixun",
            }
            r = session.get(url, params=params, timeout=cfg().crawler.timeout)
            if r.status_code != 200:
                return False, None

            data = r.json()
            items = data.get("Data", [])
            if not items:
                return False, None

            # Check if any result is within 7 days
            now = datetime.now()
            for item in items:
                date_str = item.get("ShowDate", "")
                try:
                    item_date = datetime.strptime(date_str[:10], "%Y-%m-%d")
                    if (now - item_date).days <= 7:
                        return True, item_date
                except (ValueError, TypeError):
                    continue

            return True, None  # hit but couldn't parse date

        except Exception as e:
            _logger.debug("新闻查询 [%s] 失败: %s", keyword, e)
            return False, None

    # ── Individual stock sentiment ─────────────────

    def _scan_announcement_sentiment(self, symbol: str) -> float:
        """Scan CNINFO announcements for positive/negative keywords."""
        cache_key = f"{symbol}_{datetime.now().strftime('%Y%m%d')}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        score = 0.0
        try:
            code = symbol.split(".")[0] if "." in symbol else symbol
            end_date = datetime.now()
            start_date = end_date - timedelta(days=14)

            session = requests.Session()
            session.trust_env = False
            url = "http://www.cninfo.com.cn/new/hisAnnouncement/query"
            payload = {
                "pageNum": "1",
                "pageSize": "10",
                "column": "szse",
                "tabName": "fulltext",
                "plate": "",
                "stock": "",
                "searchkey": code,
                "secid": "",
                "category": "",
                "trade": "",
                "seDate": f"{start_date.strftime('%Y-%m-%d')}~{end_date.strftime('%Y-%m-%d')}",
                "sortName": "",
                "sortType": "",
                "isHLtitle": "true",
            }
            r = session.post(url, data=payload, timeout=cfg().crawler.timeout)
            data = r.json()
            notices = data.get("announcements") or []

            seen = set()
            for item in notices:
                title = str(item.get("announcementTitle", ""))
                title_clean = re.sub(r"<[^>]+>", "", title)
                if not title_clean or title_clean in seen:
                    continue
                seen.add(title_clean)

                # Positive keywords
                for kw, pts in POSITIVE_KW:
                    if kw in title_clean:
                        score += pts
                        break  # One match per announcement

                # Negative keywords
                for kw, pts in NEGATIVE_KW:
                    if kw in title_clean:
                        score += pts
                        break

                ts = item.get("announcementTime", 0)
                if ts:
                    t = datetime.fromtimestamp(ts / 1000)
                    days_ago = (datetime.now() - t).days
                    # Apply minor decay for older announcements
                    if days_ago > 7:
                        score *= 0.5

        except Exception as e:
            _logger.debug("公告情绪扫描 [%s] 失败: %s", symbol, e)

        score = max(-30.0, min(30.0, round(score, 1)))
        self._cache[cache_key] = score
        return score

    # ── Helpers ────────────────────────────────────

    def _load_event_map(self) -> list[dict[str, Any]]:
        """Load event → sector mapping from TOML config."""
        try:
            with _EVENT_MAP_PATH.open("rb") as f:
                raw = tomllib.load(f)
            return raw.get("events", [])
        except Exception as e:
            _logger.warning("加载 event_sector_map.toml 失败: %s", e)
            return []

    @staticmethod
    def _canonical_tag(tag: str) -> str:
        """Normalize board labels from different sources for fuzzy matching."""
        return SectorTagProvider.canonical_tag(tag)

    @classmethod
    def _tag_matches(cls, stock_tag: str, event_tag: str) -> bool:
        """Return True if a stock board tag should match an event board tag."""
        return SectorTagProvider.tag_matches(stock_tag, event_tag)

    @staticmethod
    def _time_decay(days_ago: int) -> float:
        """Exponential decay: 1.0 today → 0.3 after 7 days → 0 after 14 days."""
        if days_ago <= 0:
            return 1.0
        if days_ago <= 3:
            return 0.7
        if days_ago <= 7:
            return 0.3
        if days_ago <= 14:
            return 0.1
        return 0.0
