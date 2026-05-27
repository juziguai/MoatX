"""Event-driven sentiment engine for Layer 4 scoring.

Maps macro news → affected sectors, scans CNINFO announcements for
individual stock sentiment, and outputs event_score used as a multiplier
in ScoringEngine's Layer 4.

Design:
  - Keyword → sector mapping loaded from data/event_sector_map.toml
  - CNINFO announcements scanned for positive/negative keywords per stock
  - Time decay applied: recent events carry more weight
  - Output: per-stock event_boost (-40 to +30), applied as multiplier (0.6-1.3)
"""

from __future__ import annotations

import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from time import monotonic
from typing import Any

import requests

from modules.config import cfg, tomllib
from modules.event_intelligence.exposure import StockTopicExposureProvider
from modules.sector_tags import SectorTagProvider

_logger = logging.getLogger("moatx.event_driver")

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_EVENT_MAP_PATH = _PROJECT_ROOT / "data" / "event_sector_map.toml"
_LEGACY_NEWS_PROBE_SECONDS = 3.0
_LEGACY_NEWS_REQUEST_SECONDS = 1.0
_MATCH_DECAY_WEIGHTS = (1.0, 0.4, 0.2)
_MAX_EVENT_BOOST = 30.0
_MIN_EVENT_BOOST = -40.0
_MARKET_VALIDATION_SAMPLE_PER_SECTOR = 6
_MARKET_VALIDATION_MAX_CODES = 50

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

    def __init__(
        self,
        sector_provider: SectorTagProvider | None = None,
        exposure_provider: StockTopicExposureProvider | None = None,
    ):
        self._events = self._load_event_map()
        self._cache: dict[str, dict] = {}  # per-symbol sentiment cache
        self._news_probe_cache: dict[str, tuple[bool, datetime | None]] = {}
        self._sector_provider = sector_provider or SectorTagProvider()
        self._exposure_provider = exposure_provider or StockTopicExposureProvider()
        self._sector_boost_details: dict[str, dict[str, Any]] = {}

    # ── Public API ────────────────────────────────

    # ── Multi-tag reverse lookup (industry + concept) ───────────────────────

    def _build_code_to_tags(self) -> dict[str, set[str]]:
        """Compatibility wrapper for old tests/callers."""
        return self._sector_provider.build_code_to_tags()

    def _get_tags(self, symbol: str) -> set[str]:
        """Return all industry + concept tags for a stock (cached lookup)."""
        code = self._sector_provider.normalize_code(symbol)
        try:
            graph_tags = self._sector_provider._graph_tags_for_code(code)
            if graph_tags:
                return graph_tags
        except Exception:
            pass
        try:
            return self._sector_provider.get_tags(symbol)
        except Exception as e:
            _logger.debug("sector tags lookup failed [%s]: %s", symbol, e)
            return {self._sector_provider.market_fallback_tag(code)}

    # ── Public API ───────────────────────────────────────────────────────────

    def score_batch(self, symbols: list[str]) -> dict[str, float]:
        """Return per-symbol event_boost dict (-40 to +30)."""
        explanations = self.explain_batch(symbols)
        return {symbol: float(row.get("boost") or 0.0) for symbol, row in explanations.items()}

    def explain_batch(self, symbols: list[str]) -> dict[str, dict[str, Any]]:
        """Return explainable event boost details for each symbol."""
        results = {}
        sector_boosts = self._active_sector_boosts()

        with ThreadPoolExecutor(max_workers=min(len(symbols), 8)) as ex:
            futures = {ex.submit(self.explain_single, s, sector_boosts): s for s in symbols}
            for fut in as_completed(futures):
                sym = futures[fut]
                try:
                    results[sym] = fut.result()
                except Exception:
                    results[sym] = {
                        "symbol": sym,
                        "boost": 0.0,
                        "matched_factors": [],
                        "announcement_score": 0.0,
                        "reason": "",
                    }
        return results

    def score_single(self, symbol: str, sector_boosts: dict[str, float] | None = None) -> float:
        """Score a single stock's event sentiment. Returns -40 to +30."""
        return float(self.explain_single(symbol, sector_boosts).get("boost") or 0.0)

    def explain_single(self, symbol: str, sector_boosts: dict[str, float] | None = None) -> dict[str, Any]:
        """Explain a single stock's event sentiment. Returns boost and matched factors."""
        if sector_boosts is None:
            sector_boosts = self._active_sector_boosts()

        score = 0.0
        matched_factors: list[dict[str, Any]] = []

        # 1. Sector-level boost (macro news → sector mapping)
        # A stock can belong to multiple overlapping tags; use decayed top matches to avoid double-counting near synonyms.
        event_matches: dict[str, dict[str, Any]] = {}
        for tag in self._get_tags(symbol):
            for event_tag, boost in sector_boosts.items():
                if self._tag_matches(tag, event_tag):
                    self._remember_event_match(
                        event_matches,
                        symbol=symbol,
                        stock_tag=tag,
                        event_tag=event_tag,
                        boost=float(boost),
                        fallback_exposure=self._exposure_weight(tag, event_tag),
                    )

        for event_tag, boost in sector_boosts.items():
            detail = self._sector_boost_details.get(event_tag, {})
            exposure, row = self._exposure_provider.weight(
                symbol,
                topic=str(detail.get("top_topic") or ""),
                event_tag=event_tag,
                fallback=0.0,
            )
            if exposure <= 0 or row is None:
                continue
            self._remember_event_match(
                event_matches,
                symbol=symbol,
                stock_tag=str(row.get("sector_tag") or "configured"),
                event_tag=event_tag,
                boost=float(boost),
                fallback_exposure=exposure,
                exposure_row=row,
            )

        grouped_matches: dict[str, list[dict[str, Any]]] = {}
        for match in event_matches.values():
            grouped_matches.setdefault(self._theme_key(match["event_tag"], match.get("topic")), []).append(match)
        raw_matches = []
        for rows in grouped_matches.values():
            best = max(rows, key=lambda row: abs(float(row.get("raw_boost") or 0.0)))
            best["deduped_count"] = len(rows)
            raw_matches.append(best)
        raw_matches.sort(key=lambda row: abs(float(row.get("raw_boost") or 0.0)), reverse=True)
        for match, weight in zip(raw_matches[: len(_MATCH_DECAY_WEIGHTS)], _MATCH_DECAY_WEIGHTS):
            raw_boost = float(match.get("raw_boost") or 0.0)
            weighted_boost = raw_boost * weight
            score += weighted_boost
            matched_factors.append(
                {
                    "stock_tag": match["stock_tag"],
                    "event_tag": match["event_tag"],
                    "topic": match.get("topic", ""),
                    "boost": round(weighted_boost, 1),
                    "raw_boost": round(raw_boost, 1),
                    "source_boost": round(float(match.get("source_boost") or 0.0), 1),
                    "weight": weight,
                    "exposure": round(float(match.get("exposure") or 0.0), 2),
                    "deduped_count": int(match.get("deduped_count") or 1),
                    "market_validation": match.get("market_validation", ""),
                    "market_multiplier": round(float(match.get("market_multiplier") or 1.0), 2),
                    "exposure_source": match.get("exposure_source", "tag_match"),
                }
            )

        # 2. Individual stock sentiment (CNINFO announcements)
        announcement_score = self._scan_announcement_sentiment(symbol)
        score += announcement_score

        final_score = max(_MIN_EVENT_BOOST, min(_MAX_EVENT_BOOST, round(score, 1)))
        reason_parts = [
            f"{row['event_tag']}({row['boost']:+.1f})"
            for row in matched_factors[:5]
        ]
        if announcement_score:
            reason_parts.append(f"公告情绪({announcement_score:+.1f})")
        return {
            "symbol": symbol,
            "boost": final_score,
            "matched_factors": matched_factors,
            "announcement_score": round(float(announcement_score), 1),
            "reason": "；".join(reason_parts),
        }

    # ── Sector boost from macro events ─────────────

    def _remember_event_match(
        self,
        event_matches: dict[str, dict[str, Any]],
        *,
        symbol: str,
        stock_tag: str,
        event_tag: str,
        boost: float,
        fallback_exposure: float,
        exposure_row: dict[str, Any] | None = None,
    ) -> None:
        detail = self._sector_boost_details.get(event_tag, {})
        topic = str(detail.get("top_topic") or "")
        exposure, configured = self._exposure_provider.weight(
            symbol,
            topic=topic,
            event_tag=event_tag,
            stock_tag=stock_tag,
            fallback=fallback_exposure,
        )
        if exposure_row is not None:
            configured = exposure_row
            exposure = fallback_exposure
        raw_boost = boost * exposure
        previous = event_matches.get(event_tag)
        if previous is not None and abs(raw_boost) <= abs(float(previous.get("raw_boost") or 0.0)):
            return
        event_matches[event_tag] = {
            "stock_tag": stock_tag,
            "event_tag": event_tag,
            "topic": topic,
            "raw_boost": raw_boost,
            "source_boost": boost,
            "exposure": exposure,
            "exposure_source": (configured or {}).get("source", "tag_match"),
            "market_validation": detail.get("market_validation_status", ""),
            "market_multiplier": detail.get("market_multiplier", 1.0),
        }

    def _active_sector_boosts(self) -> dict[str, float]:
        """Calculate sector boost scores from active macro events."""
        sector_scores = self._news_factor_boosts()
        if not self._legacy_news_probe_enabled(has_news_factors=bool(sector_scores)):
            return sector_scores

        deadline = monotonic() + _LEGACY_NEWS_PROBE_SECONDS
        for event in self._events:
            if monotonic() >= deadline:
                _logger.debug("旧新闻关键词探测达到耗时预算，跳过剩余事件")
                break
            name = event["name"]
            boost = event.get("boost_sectors", [])
            penalty = event.get("penalty_sectors", [])

            # Check if event is active (keywords hit in recent news)
            event_hit = False
            recent_ts = None
            for kw in event.get("keywords", []):
                if monotonic() >= deadline:
                    break
                hit, ts = self._check_keyword_in_news(kw, timeout=_LEGACY_NEWS_REQUEST_SECONDS)
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

        return sector_scores

    @staticmethod
    def _legacy_news_probe_enabled(*, has_news_factors: bool) -> bool:
        raw_value = os.environ.get("MOATX_EVENT_DRIVER_LEGACY_NEWS_PROBE")
        if raw_value is None:
            return not has_news_factors
        value = raw_value.strip().lower()
        return value not in {"0", "false", "no", "off"}

    def _news_factor_boosts(self) -> dict[str, float]:
        sector_scores: dict[str, float] = {}
        self._sector_boost_details = {}
        try:
            from modules.event_intelligence.news_factors import NewsFactorEngine

            payload = NewsFactorEngine().build(limit=200, min_score=55.0, top_n=100)
            factors = payload.get("factors", [])
            sectors = [str(row.get("sector") or "").strip() for row in factors if row.get("sector")]
            validations = self._market_validation_by_sector(sectors)
            for row in factors:
                sector = str(row.get("sector") or "").strip()
                if not sector:
                    continue
                raw_score = float(row.get("factor_score") or 0.0)
                validation = validations.get(sector, {})
                market_multiplier, validation_status = self._market_validation_multiplier(raw_score, validation)
                score = raw_score * market_multiplier
                sector_scores[sector] = sector_scores.get(sector, 0.0) + score
                detail = dict(row)
                detail.update(
                    {
                        "raw_factor_score": round(raw_score, 1),
                        "adjusted_factor_score": round(score, 1),
                        "market_multiplier": round(market_multiplier, 3),
                        "market_validation": validation,
                        "market_validation_status": validation_status,
                    }
                )
                self._sector_boost_details[sector] = detail
        except Exception as e:
            _logger.debug("新闻因子评分加载失败: %s", e)
        return sector_scores

    def _market_validation_by_sector(self, sectors: list[str]) -> dict[str, dict[str, Any]]:
        codes_by_sector: dict[str, list[str]] = {}
        all_codes: list[str] = []
        for sector in sectors:
            codes = self._sector_member_codes(sector)[:_MARKET_VALIDATION_SAMPLE_PER_SECTOR]
            if not codes:
                continue
            codes_by_sector[sector] = codes
            for code in codes:
                if code not in all_codes and len(all_codes) < _MARKET_VALIDATION_MAX_CODES:
                    all_codes.append(code)
        if not all_codes:
            return {}

        try:
            from modules.datasource import QuoteManager
            from modules.utils import to_full_code

            quotes = QuoteManager(source_names=["sina"], mode="single").fetch_quotes(all_codes)
            rows: dict[str, dict[str, Any]] = {}
            for sector, codes in codes_by_sector.items():
                pcts: list[float] = []
                quote_rows: list[dict[str, Any]] = []
                amount = 0.0
                for code in codes:
                    quote = quotes.get(to_full_code(code))
                    if not quote:
                        continue
                    pct = float(quote.get("change_pct") or 0.0)
                    row_amount = float(quote.get("amount") or 0.0)
                    pcts.append(pct)
                    amount += row_amount
                    quote_rows.append(
                        {
                            "code": code,
                            "name": str(quote.get("name") or ""),
                            "pct": pct,
                            "amount": row_amount,
                        }
                    )
                if not pcts:
                    continue
                up = sum(1 for pct in pcts if pct > 0.2)
                down = sum(1 for pct in pcts if pct < -0.2)
                top = max(quote_rows, key=lambda row: float(row["pct"]))
                bottom = min(quote_rows, key=lambda row: float(row["pct"]))
                leader_amount = max((float(row["amount"]) for row in quote_rows), default=0.0)
                rows[sector] = {
                    "sample_count": len(pcts),
                    "up": up,
                    "down": down,
                    "flat": len(pcts) - up - down,
                    "up_ratio": round(up / len(pcts), 4),
                    "down_ratio": round(down / len(pcts), 4),
                    "avg_pct": round(sum(pcts) / len(pcts), 3),
                    "amount": round(amount, 2),
                    "amount_yi": round(amount / 100_000_000, 3),
                    "leader_share": round(leader_amount / amount, 4) if amount > 0 else 0.0,
                    "top_code": top["code"],
                    "top_name": top["name"],
                    "top_pct": round(float(top["pct"]), 3),
                    "bottom_code": bottom["code"],
                    "bottom_name": bottom["name"],
                    "bottom_pct": round(float(bottom["pct"]), 3),
                }
            return rows
        except Exception as e:
            _logger.debug("盘面验证加载失败: %s", e)
            return {}

    def _sector_member_codes(self, sector: str) -> list[str]:
        frames = []
        try:
            frames.append(self._sector_provider._graph_members(sector))
        except Exception:
            pass
        try:
            frames.append(SectorTagProvider._fallback_members(sector))
        except Exception:
            pass

        codes: list[str] = []
        for frame in frames:
            if frame is None or getattr(frame, "empty", True) or "code" not in frame.columns:
                continue
            for code in frame["code"].astype(str).tolist():
                normalized = self._sector_provider.normalize_code(code)
                if normalized and normalized not in codes:
                    codes.append(normalized)
        return codes

    @staticmethod
    def _market_validation_multiplier(score: float, validation: dict[str, Any]) -> tuple[float, str]:
        if not validation:
            return 1.0, "unavailable"
        confirmation = EventDriver._market_confirmation_score(score, validation)
        if score >= 0:
            if confirmation >= 0.72:
                return 1.0, "confirmed"
            if confirmation >= 0.55:
                return 0.85, "mild_confirmed"
            if confirmation >= 0.35:
                return 0.65, "mixed"
            return 0.45, "unconfirmed"

        if confirmation >= 0.72:
            return 1.1, "bearish_confirmed"
        if confirmation >= 0.55:
            return 1.0, "bearish_mild_confirmed"
        if confirmation >= 0.35:
            return 0.85, "bearish_mixed"
        return 0.75, "bearish_unconfirmed"

    @staticmethod
    def _market_confirmation_score(score: float, validation: dict[str, Any]) -> float:
        sample_count = max(1, int(validation.get("sample_count") or 0))
        avg_pct = float(validation.get("avg_pct") or 0.0)
        up_ratio = float(validation.get("up_ratio") or (int(validation.get("up") or 0) / sample_count))
        down_ratio = float(validation.get("down_ratio") or (int(validation.get("down") or 0) / sample_count))
        amount_yi = float(validation.get("amount_yi") or float(validation.get("amount") or 0.0) / 100_000_000)
        leader_share = float(validation.get("leader_share") or 0.0)

        direction_pct = avg_pct if score >= 0 else -avg_pct
        confirm_ratio = up_ratio if score >= 0 else down_ratio
        price_score = EventDriver._threshold_score(direction_pct, [(1.5, 1.0), (0.6, 0.75), (0.0, 0.45)], 0.15)
        breadth_score = EventDriver._threshold_score(confirm_ratio, [(0.67, 0.95), (0.5, 0.7), (0.34, 0.4)], 0.15)
        liquidity_score = EventDriver._threshold_score(amount_yi, [(50.0, 1.0), (15.0, 0.75), (3.0, 0.45)], 0.25)
        concentration_penalty = 0.15 if sample_count >= 3 and leader_share >= 0.65 else 0.0
        confirmation = price_score * 0.45 + breadth_score * 0.4 + liquidity_score * 0.15 - concentration_penalty
        return round(max(0.0, min(1.0, confirmation)), 4)

    @staticmethod
    def _threshold_score(value: float, thresholds: list[tuple[float, float]], default: float) -> float:
        for threshold, score in thresholds:
            if value >= threshold:
                return score
        return default

    def topic_exposure_table(self, symbols: list[str]) -> list[dict[str, Any]]:
        """Return stock-topic exposure rows for the current event factors."""
        sector_boosts = self._active_sector_boosts()
        rows: list[dict[str, Any]] = []
        for symbol in symbols:
            for tag in self._get_tags(symbol):
                for event_tag, boost in sector_boosts.items():
                    if not self._tag_matches(tag, event_tag):
                        continue
                    detail = self._sector_boost_details.get(event_tag, {})
                    exposure, exposure_row = self._exposure_provider.weight(
                        symbol,
                        topic=str(detail.get("top_topic") or ""),
                        event_tag=event_tag,
                        stock_tag=tag,
                        fallback=self._exposure_weight(tag, event_tag),
                    )
                    rows.append(
                        {
                            "symbol": symbol,
                            "stock_tag": tag,
                            "event_tag": event_tag,
                            "topic": detail.get("top_topic") or event_tag,
                            "exposure": round(exposure, 2),
                            "exposure_source": (exposure_row or {}).get("source", "tag_match"),
                            "boost": round(float(boost), 1),
                        }
                    )
            for exposure_row in self._exposure_provider.rows([symbol]):
                topic = str(exposure_row.get("topic") or "")
                for event_tag, boost in sector_boosts.items():
                    detail = self._sector_boost_details.get(event_tag, {})
                    runtime_topic = str(detail.get("top_topic") or event_tag)
                    if topic and topic != runtime_topic:
                        continue
                    sector_tag = str(exposure_row.get("sector_tag") or "")
                    if sector_tag and not self._tag_matches(sector_tag, event_tag):
                        continue
                    rows.append(
                        {
                            "symbol": symbol,
                            "stock_tag": sector_tag,
                            "event_tag": event_tag,
                            "topic": topic or runtime_topic,
                            "exposure": round(
                                float(exposure_row.get("exposure") or 0.0)
                                * float(exposure_row.get("confidence") or 1.0),
                                2,
                            ),
                            "exposure_source": exposure_row.get("source", "configured"),
                            "boost": round(float(boost), 1),
                        }
                    )
        deduped: dict[tuple[str, str, str], dict[str, Any]] = {}
        for row in rows:
            key = (str(row["symbol"]), str(row["topic"]), str(row["event_tag"]))
            current = deduped.get(key)
            if current is None or float(row["exposure"]) > float(current["exposure"]):
                deduped[key] = row
        rows = list(deduped.values())
        rows.sort(key=lambda row: (row["symbol"], str(row["topic"]), -float(row["exposure"])))
        return rows

    def _check_keyword_in_news(self, keyword: str, timeout: float | None = None) -> tuple[bool, datetime | None]:
        """Check if a keyword appears in recent financial news headlines.

        Uses EastMoney news search as a lightweight probe.
        Returns (hit, approximate_timestamp).
        """
        if keyword in self._news_probe_cache:
            return self._news_probe_cache[keyword]
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
            request_timeout = timeout or min(float(cfg().crawler.timeout), _LEGACY_NEWS_REQUEST_SECONDS)
            r = session.get(url, params=params, timeout=request_timeout)
            if r.status_code != 200:
                self._news_probe_cache[keyword] = (False, None)
                return self._news_probe_cache[keyword]

            data = r.json()
            items = data.get("Data", [])
            if not items:
                self._news_probe_cache[keyword] = (False, None)
                return self._news_probe_cache[keyword]

            # Check if any result is within 7 days
            now = datetime.now()
            for item in items:
                date_str = item.get("ShowDate", "")
                try:
                    item_date = datetime.strptime(date_str[:10], "%Y-%m-%d")
                    if (now - item_date).days <= 7:
                        self._news_probe_cache[keyword] = (True, item_date)
                        return self._news_probe_cache[keyword]
                except (ValueError, TypeError):
                    continue

            self._news_probe_cache[keyword] = (True, None)  # hit but couldn't parse date
            return self._news_probe_cache[keyword]

        except Exception as e:
            _logger.debug("新闻查询 [%s] 失败: %s", keyword, e)
            self._news_probe_cache[keyword] = (False, None)
            return self._news_probe_cache[keyword]

    # ── Individual stock sentiment ─────────────────

    def _scan_announcement_sentiment(self, symbol: str) -> float:
        """Scan CNINFO announcements for positive/negative keywords."""
        cache_key = f"{symbol}_{datetime.now().strftime('%Y%m%d')}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        score = 0.0
        try:
            from modules.announcement_risk import AnnouncementRiskScanner

            result = AnnouncementRiskScanner().scan(symbol, lookback_days=14, limit=10)
            score = float(result.get("sentiment_score") or 0.0)

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

    @classmethod
    def _exposure_weight(cls, stock_tag: str, event_tag: str) -> float:
        """Return how directly one stock tag is exposed to one event tag."""
        stock = str(stock_tag or "").strip()
        event = str(event_tag or "").strip()
        if not stock or not event:
            return 0.0
        if stock == event:
            return 1.0
        stock_norm = cls._canonical_tag(stock)
        event_norm = cls._canonical_tag(event)
        if stock_norm == event_norm:
            return 1.0
        stock_aliases = SectorTagProvider._graph_aliases(stock_norm)
        event_aliases = SectorTagProvider._graph_aliases(event_norm)
        if stock_aliases & event_aliases:
            return 0.9
        if event_norm in stock_norm or stock_norm in event_norm:
            return 0.75
        return 0.5

    @classmethod
    def _theme_key(cls, event_tag: str, topic: Any = "") -> str:
        """Collapse near-synonym sector labels into one scoring theme."""
        if str(topic or "").strip():
            return f"topic:{str(topic).strip()}"
        normalized = cls._canonical_tag(event_tag)
        aliases = SectorTagProvider._graph_aliases(normalized)
        if aliases:
            return min(aliases, key=lambda value: (len(value), value))
        return normalized

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
