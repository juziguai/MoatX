"""Optional external-LLM semantic review for news intelligence."""

from __future__ import annotations

import json
import os
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable

import pandas as pd
import requests

from modules.config import cfg, tomllib
from modules.db import DatabaseManager

from .models import now_ts
from .news_intelligence import NewsIntelligenceEngine

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_LLM_CONFIG_PATH = _PROJECT_ROOT / "data" / "llm_intelligence.toml"

Transport = Callable[[str, dict[str, str], dict[str, Any], int], dict[str, Any]]


@dataclass(slots=True)
class LLMSettings:
    """Runtime settings for an OpenAI-compatible chat-completions endpoint."""

    enabled: bool = False
    provider: str = "openai_compatible"
    base_url: str = "https://api.openai.com/v1"
    model: str = ""
    api_key_env: str = "MOATX_LLM_API_KEY"
    timeout: int = 30
    max_items: int = 20
    max_output_tokens: int = 2000
    temperature: float = 0.1


class LLMSemanticReviewer:
    """Use an external LLM to judge market relevance and filter news insights."""

    def __init__(
        self,
        db: DatabaseManager | None = None,
        settings: LLMSettings | None = None,
        transport: Transport | None = None,
        api_key: str | None = None,
    ):
        self._owns_db = db is None
        self._db = db or DatabaseManager(cfg().data.warehouse_path)
        self._settings = settings or load_llm_settings()
        self._transport = transport or self._post_json
        self._api_key = api_key

    def close(self) -> None:
        if self._owns_db:
            self._db.close()

    def review(
        self,
        *,
        limit: int = 100,
        min_score: float = 45.0,
        send: bool = False,
        persist: bool = True,
    ) -> dict[str, Any]:
        """Review current high-value news with an optional external LLM call."""
        try:
            payload = NewsIntelligenceEngine(db=self._db).analyze(limit=limit, min_score=min_score)
            candidates = (payload.get("insights") or [])[: max(1, self._settings.max_items)]
            request_payload = self._request_payload(candidates)
            base = {
                "engine": "llm_semantic_review_v1",
                "enabled": self._settings.enabled,
                "send": send,
                "provider": self._settings.provider,
                "model": self._settings.model,
                "news_scanned": payload.get("news_scanned", 0),
                "candidate_count": len(candidates),
                "candidates": candidates,
            }
            if not candidates:
                return {**base, "status": "empty", "reviews": [], "message": "暂无可交给 LLM 复核的新闻洞察。"}
            if not send:
                return {
                    **base,
                    "status": "dry_run",
                    "request_preview": self._preview_request(request_payload),
                    "reviews": [],
                    "message": "dry-run：已生成 LLM 评审候选；传 --send 才会真正调用外部模型。",
                }
            if not self._settings.enabled:
                return {
                    **base,
                    "status": "disabled",
                    "reviews": [],
                    "message": "LLM 语义增强未启用；复制 data/llm_intelligence.toml.example 后启用。",
                }
            api_key = self._api_key or os.environ.get(self._settings.api_key_env, "")
            if not api_key:
                return {
                    **base,
                    "status": "missing_api_key",
                    "reviews": [],
                    "message": f"未找到环境变量 {self._settings.api_key_env}，未调用外部模型。",
                }
            response = self._call_llm(request_payload, api_key)
            reviews = self._parse_reviews(response)
            if persist:
                self._persist_reviews(candidates, reviews)
            return {
                **base,
                "status": "reviewed",
                "reviews": reviews,
                "raw_usage": response.get("usage", {}),
                "message": f"LLM 已复核 {len(reviews)} 条新闻洞察。",
            }
        finally:
            self.close()

    def list_reviews(self, *, limit: int = 50) -> list[dict[str, Any]]:
        """Return recent persisted LLM reviews."""
        try:
            df = pd.read_sql_query(
                """SELECT * FROM event_llm_reviews
                   ORDER BY id DESC LIMIT ?""",
                self._db.conn,
                params=[limit],
            )
            if df.empty:
                return []
            rows = df.where(pd.notna(df), None).to_dict(orient="records")
            for row in rows:
                try:
                    row["review"] = json.loads(str(row.get("review_json") or "{}"))
                except json.JSONDecodeError:
                    row["review"] = {}
            return rows
        finally:
            self.close()

    def _request_payload(self, candidates: list[dict[str, Any]]) -> dict[str, Any]:
        compact = [
            {
                "news_id": item.get("news_id"),
                "title": item.get("title"),
                "summary": str(item.get("summary") or "")[:280],
                "topic": item.get("topic"),
                "value_score": item.get("value_score"),
                "affected_sectors": item.get("affected_sectors"),
                "reason": item.get("reason"),
            }
            for item in candidates
        ]
        system = (
            "你是 A 股事件情报分析器。只做新闻价值判断，不给交易指令。"
            "请识别哪些新闻真正影响 A 股板块/产业链，过滤标题党、旧闻和弱相关内容。"
        )
        user = {
            "task": "review_news_insights",
            "rules": [
                "输出严格 JSON，不要 Markdown。",
                "decision 只能是 use/watch/ignore。",
                "llm_score 范围 0-100，越高表示越值得进入选股事件因子。",
                "说明利多/利空、影响板块、风险点和有效期。",
            ],
            "schema": {
                "reviews": [
                    {
                        "news_id": 0,
                        "decision": "use",
                        "llm_score": 80,
                        "rationale": "一句话理由",
                        "sentiment": "bullish|bearish|neutral",
                        "time_horizon": "short|mid|long",
                        "sectors": ["板块"],
                        "risks": ["风险"],
                    }
                ]
            },
            "candidates": compact,
        }
        return {
            "model": self._settings.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": json.dumps(user, ensure_ascii=False)},
            ],
            "temperature": self._settings.temperature,
            "max_tokens": self._settings.max_output_tokens,
            "response_format": {"type": "json_object"},
        }

    @staticmethod
    def _preview_request(payload: dict[str, Any]) -> dict[str, Any]:
        preview = dict(payload)
        messages = list(preview.get("messages") or [])
        if len(messages) > 1:
            content = str(messages[1].get("content") or "")
            messages[1] = {**messages[1], "content": content[:1200]}
        preview["messages"] = messages
        return preview

    def _call_llm(self, payload: dict[str, Any], api_key: str) -> dict[str, Any]:
        url = self._settings.base_url.rstrip("/") + "/chat/completions"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        return self._transport(url, headers, payload, self._settings.timeout)

    @staticmethod
    def _post_json(url: str, headers: dict[str, str], payload: dict[str, Any], timeout: int) -> dict[str, Any]:
        session = requests.Session()
        session.trust_env = False
        response = session.post(url, headers=headers, json=payload, timeout=timeout)
        response.raise_for_status()
        return response.json()

    @staticmethod
    def _parse_reviews(response: dict[str, Any]) -> list[dict[str, Any]]:
        content = ""
        choices = response.get("choices") or []
        if choices:
            content = str((choices[0].get("message") or {}).get("content") or "")
        if not content:
            content = json.dumps(response, ensure_ascii=False)
        try:
            parsed = json.loads(content)
        except json.JSONDecodeError:
            match = re.search(r"\{.*\}", content, re.S)
            parsed = json.loads(match.group(0)) if match else {}
        reviews = parsed.get("reviews", []) if isinstance(parsed, dict) else []
        normalized: list[dict[str, Any]] = []
        for row in reviews:
            if not isinstance(row, dict):
                continue
            decision = str(row.get("decision") or "watch").lower()
            if decision not in {"use", "watch", "ignore"}:
                decision = "watch"
            normalized.append(
                {
                    "news_id": int(row.get("news_id") or 0),
                    "decision": decision,
                    "llm_score": max(0.0, min(100.0, float(row.get("llm_score") or 0.0))),
                    "rationale": str(row.get("rationale") or ""),
                    "sentiment": str(row.get("sentiment") or "neutral"),
                    "time_horizon": str(row.get("time_horizon") or "mid"),
                    "sectors": [str(item) for item in row.get("sectors", []) if str(item)],
                    "risks": [str(item) for item in row.get("risks", []) if str(item)],
                }
            )
        return normalized

    def _persist_reviews(self, candidates: list[dict[str, Any]], reviews: list[dict[str, Any]]) -> None:
        by_id = {int(item.get("news_id") or 0): item for item in candidates}
        now = now_ts()
        for review in reviews:
            news_id = int(review.get("news_id") or 0)
            candidate = by_id.get(news_id, {})
            self._db.conn.execute(
                """INSERT INTO event_llm_reviews
                   (news_id, title, topic, value_score, llm_score, decision,
                    rationale, review_json, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    news_id,
                    str(candidate.get("title") or ""),
                    str(candidate.get("topic") or ""),
                    float(candidate.get("value_score") or 0.0),
                    float(review.get("llm_score") or 0.0),
                    str(review.get("decision") or ""),
                    str(review.get("rationale") or ""),
                    json.dumps(review, ensure_ascii=False),
                    now,
                ),
            )
        self._db.conn.commit()


def load_llm_settings(path: Path | None = None) -> LLMSettings:
    """Load optional LLM semantic-review settings from ignored local TOML."""
    config_path = path or _LLM_CONFIG_PATH
    if not config_path.exists():
        return LLMSettings()
    raw = tomllib.loads(config_path.read_text(encoding="utf-8"))
    section = raw.get("llm_intelligence", {})
    return LLMSettings(**section)


def llm_settings_status() -> dict[str, Any]:
    """Return safe non-secret LLM configuration status."""
    settings = load_llm_settings()
    return {
        **asdict(settings),
        "api_key_present": bool(os.environ.get(settings.api_key_env, "")),
        "config_path": str(_LLM_CONFIG_PATH),
    }
