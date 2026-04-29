"""Source quality governance helpers for event intelligence."""

from __future__ import annotations

from typing import Any


def source_recommendation(row: dict[str, Any]) -> dict[str, str]:
    """Return a simple operational recommendation for one source quality row."""
    fetched = int(row.get("fetched") or 0)
    errors = int(row.get("errors") or 0)
    hit_rate = float(row.get("hit_rate") or 0.0)
    score = float(row.get("quality_score") or 0.0)
    last_error = str(row.get("last_error") or "").strip()

    if errors > 0 and fetched == 0:
        return {
            "source_recommendation": "disable_candidate",
            "source_recommendation_reason": "最近抓取失败且无有效记录",
        }
    if score >= 65 and hit_rate >= 0.2:
        return {
            "source_recommendation": "promote",
            "source_recommendation_reason": "质量分和事件命中率较高",
        }
    if fetched >= 20 and hit_rate < 0.03:
        return {
            "source_recommendation": "watch_low_signal",
            "source_recommendation_reason": "抓取量充足但事件命中率偏低",
        }
    if score < 25 and (fetched > 0 or last_error):
        return {
            "source_recommendation": "disable_candidate",
            "source_recommendation_reason": "质量分偏低，建议观察后关闭",
        }
    if score >= 45:
        return {
            "source_recommendation": "keep",
            "source_recommendation_reason": "质量分处于可用区间",
        }
    return {
        "source_recommendation": "watch",
        "source_recommendation_reason": "数据仍需继续累计",
    }
