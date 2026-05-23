"""CNINFO announcement risk scanner for single-stock decisions."""

from __future__ import annotations

import re
from datetime import datetime, timedelta
from typing import Any

import requests

from modules.config import cfg
from modules.utils import normalize_symbol

_RISK_KEYWORDS = sorted(
    [
        ("立案告知书", 35),
        ("立案调查", 35),
        ("行政处罚", 35),
        ("处罚事先告知书", 35),
        ("责令改正", 25),
        ("监管函", 20),
        ("警示函", 20),
        ("问询函", 10),
        ("关注函", 8),
        ("业绩预亏", 18),
        ("业绩下滑", 14),
        ("减持", 12),
        ("质押", 10),
        ("诉讼", 12),
        ("仲裁", 12),
        ("债务逾期", 25),
        ("无法表示意见", 35),
        ("否定意见", 35),
        ("会计差错", 15),
        ("更正", 10),
        ("风险提示", 10),
    ],
    key=lambda item: len(item[0]),
    reverse=True,
)

_POSITIVE_KEYWORDS = sorted(
    [
        ("回购", 8),
        ("增持", 8),
        ("中标", 8),
        ("重大合同", 10),
        ("业绩预增", 10),
        ("净利润增长", 8),
        ("战略合作", 6),
    ],
    key=lambda item: len(item[0]),
    reverse=True,
)


class AnnouncementRiskScanner:
    """Scan recent CNINFO announcements for direct company-level risk."""

    def __init__(self, session: requests.Session | None = None):
        self._session = session

    def scan(self, symbol: str, *, lookback_days: int = 45, limit: int = 30) -> dict[str, Any]:
        code = normalize_symbol(symbol)
        notices = self._fetch_notices(code, lookback_days=lookback_days, limit=limit)
        risk_score = 0
        sentiment_score = 0
        risk_items: list[tuple[int, str]] = []
        positive_flags: list[str] = []
        seen_keywords: set[str] = set()

        for notice in notices:
            title = str(notice.get("title") or "")
            matched_risk = self._matched_keywords(title, _RISK_KEYWORDS)
            matched_positive = self._matched_keywords(title, _POSITIVE_KEYWORDS)
            for keyword, points in matched_risk:
                if keyword in seen_keywords:
                    continue
                seen_keywords.add(keyword)
                risk_score += points
                sentiment_score -= min(20, points)
                risk_items.append((points, f"[{notice.get('date', '')}] {title}"))
                break
            for keyword, points in matched_positive:
                sentiment_score += points
                positive_flags.append(f"[{notice.get('date', '')}] {title}")
                break

        risk_score = min(100, risk_score)
        sentiment_score = max(-40, min(30, sentiment_score))
        risk_items.sort(key=lambda item: item[0], reverse=True)
        return {
            "symbol": code,
            "source": "cninfo",
            "lookback_days": lookback_days,
            "risk_score": risk_score,
            "risk_level": self._risk_level(risk_score),
            "is_buyable": risk_score < 30,
            "sentiment_score": sentiment_score,
            "red_flags": [item[1] for item in risk_items[:6]],
            "positive_flags": positive_flags[:6],
            "notices": notices[:8],
        }

    def _fetch_notices(self, symbol: str, *, lookback_days: int, limit: int) -> list[dict[str, Any]]:
        session = self._session or requests.Session()
        session.trust_env = False
        end_date = datetime.now()
        start_date = end_date - timedelta(days=lookback_days)
        payload = {
            "pageNum": "1",
            "pageSize": str(limit),
            "column": "szse" if symbol.startswith(("0", "3")) else "sse",
            "tabName": "fulltext",
            "plate": "",
            "stock": "",
            "searchkey": symbol,
            "secid": "",
            "category": "",
            "trade": "",
            "seDate": f"{start_date:%Y-%m-%d}~{end_date:%Y-%m-%d}",
            "sortName": "",
            "sortType": "",
            "isHLtitle": "true",
        }
        response = session.post(
            "http://www.cninfo.com.cn/new/hisAnnouncement/query",
            data=payload,
            timeout=cfg().crawler.timeout,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        response.raise_for_status()
        data = response.json()
        notices = []
        seen_titles: set[str] = set()
        for item in data.get("announcements") or []:
            title = re.sub(r"<[^>]+>", "", str(item.get("announcementTitle") or "")).strip()
            if not title or title in seen_titles:
                continue
            seen_titles.add(title)
            ts = item.get("announcementTime") or 0
            notice_date = ""
            if ts:
                try:
                    notice_date = datetime.fromtimestamp(int(ts) / 1000).strftime("%Y-%m-%d")
                except (TypeError, ValueError, OSError):
                    notice_date = ""
            notices.append(
                {
                    "title": title,
                    "date": notice_date,
                    "url": str(item.get("adjunctUrl") or ""),
                }
            )
        return notices

    @staticmethod
    def _matched_keywords(title: str, keywords: list[tuple[str, int]]) -> list[tuple[str, int]]:
        return [(keyword, points) for keyword, points in keywords if keyword in title]

    @staticmethod
    def _risk_level(score: int) -> str:
        if score >= 70:
            return "极高风险"
        if score >= 50:
            return "高风险"
        if score >= 30:
            return "中等风险"
        if score >= 15:
            return "低风险"
        return "基本无风险"
