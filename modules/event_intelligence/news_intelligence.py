"""Full-stream news value discovery for event intelligence v2."""

from __future__ import annotations

import json
import math
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import Any

import pandas as pd

from modules.config import cfg
from modules.db import DatabaseManager


@dataclass(slots=True)
class TopicRule:
    """Rule definition for one news intelligence topic."""

    topic: str
    category: str
    keywords: list[str]
    sectors: list[str]
    stocks: list[str] = field(default_factory=list)
    base_importance: float = 0.5
    direction: str = "bullish"


@dataclass(slots=True)
class NewsInsight:
    """One analyzed news item with market value metadata."""

    news_id: int
    source: str
    title: str
    summary: str
    url: str
    published_at: str
    topic: str
    category: str
    importance: float
    novelty: float
    market_relevance: float
    impact_strength: float
    source_quality: float
    freshness: float
    confidence: float
    value_score: float
    sentiment: str
    time_horizon: str
    affected_sectors: list[str]
    affected_stocks: list[str]
    reason: str


class NewsIntelligenceEngine:
    """Analyze all recent news and rank market-relevant insights."""

    def __init__(self, db: DatabaseManager | None = None):
        self._owns_db = db is None
        self._db = db or DatabaseManager(cfg().data.warehouse_path)

    def close(self) -> None:
        if self._owns_db:
            self._db.close()

    def analyze(
        self,
        *,
        limit: int = 200,
        topic: str | None = None,
        min_score: float = 45.0,
        persist: bool = True,
    ) -> dict[str, Any]:
        """Return high-value insights from recent full-stream news."""
        try:
            news = self._db.event().list_news(limit=limit)
            quality = self._db.event().list_source_quality(limit=500)
            source_quality = self._source_quality_map(quality)
            insights = self._analyze_frame(news, source_quality)
            if topic:
                insights = [item for item in insights if topic in item.topic or topic == item.category]
            insights = [item for item in insights if item.value_score >= min_score]
            insights.sort(key=self._insight_sort_key, reverse=True)
            topic_summary = self._topic_summary(insights)
            if persist:
                self._persist(insights, topic_summary)
            return {
                "engine": "news_intelligence_v2",
                "news_scanned": int(len(news)),
                "insights": [asdict(item) for item in insights],
                "topic_summary": topic_summary,
                "message": self._message(news, insights),
            }
        finally:
            self.close()

    def report(self, *, limit: int = 200, topic: str | None = None, min_score: float = 45.0) -> str:
        """Return a Markdown report for high-value news insights."""
        payload = self.analyze(limit=limit, topic=topic, min_score=min_score)
        lines = ["# MoatX 新闻价值发现报告", ""]
        lines.append(payload["message"])
        lines.append("")

        topic_summary = payload.get("topic_summary") or []
        if topic_summary:
            lines.extend([
                "## 高价值主题",
                "",
                "| 主题 | 分类 | 热度 | 新闻数 | 关联板块 |",
                "|---|---|---:|---:|---|",
            ])
            for row in topic_summary[:12]:
                lines.append(
                    f"| {row['topic']} | {row['category']} | {row['heat']:.1f} | "
                    f"{row['count']} | {', '.join(row['sectors'])} |"
                )
            lines.append("")

        insights = payload.get("insights") or []
        if not insights:
            lines.extend(["## 新闻洞察", "", "暂无达到阈值的高价值新闻。", ""])
        else:
            lines.extend([
                "## 新闻洞察",
                "",
                "| 分数 | 主题 | 新闻 | 关联板块 | 理由 |",
                "|---:|---|---|---|---|",
            ])
            for item in insights[:30]:
                title = str(item["title"])[:42]
                reason = str(item["reason"])[:54]
                lines.append(
                    f"| {float(item['value_score']):.1f} | {item['topic']} | {title} | "
                    f"{', '.join(item['affected_sectors'])} | {reason} |"
                )
            lines.append("")

        lines.extend([
            "## 使用边界",
            "",
            "- 新闻价值分只表示情报重要性和 A 股相关性，不是买卖指令。",
            "- 首版为规则系统，后续可接入更复杂 NLP 或外部大模型做语义增强。",
            "- 高价值主题会进入后续事件因子和选股评分链路。",
        ])
        return "\n".join(lines)

    def list_persisted_insights(self, *, limit: int = 50, topic: str = "") -> list[dict[str, Any]]:
        """Return persisted news insights for contexts and diagnostics."""
        try:
            params: list[Any] = []
            where = ""
            if topic:
                where = " WHERE topic = ?"
                params.append(topic)
            params.append(limit)
            df = pd.read_sql_query(
                f"""SELECT * FROM event_news_insights{where}
                    ORDER BY value_score DESC, updated_at DESC LIMIT ?""",
                self._db.conn,
                params=params,
            )
            return self._json_records(df)
        finally:
            self.close()

    def list_topic_events(self, *, limit: int = 20) -> list[dict[str, Any]]:
        """Return materialized current topic events."""
        try:
            df = pd.read_sql_query(
                """SELECT * FROM event_news_topic_events
                   ORDER BY heat DESC, updated_at DESC LIMIT ?""",
                self._db.conn,
                params=[limit],
            )
            return self._json_records(df)
        finally:
            self.close()

    def _analyze_frame(self, news: pd.DataFrame, source_quality: dict[str, float]) -> list[NewsInsight]:
        if news.empty:
            return []
        normalized = news.where(pd.notna(news), "")
        seen_titles: set[str] = set()
        insights: list[NewsInsight] = []
        for _, row in normalized.iterrows():
            title = str(row.get("title") or "")
            summary = str(row.get("summary") or "")
            content = f"{title} {summary}"
            if not title.strip():
                continue
            dedupe_key = self._dedupe_key(title)
            novelty = 0.35 if dedupe_key in seen_titles else 0.85
            seen_titles.add(dedupe_key)
            matched = self._match_topics(content)
            if not matched:
                continue
            matched = self._primary_matches(content, matched)
            for rule, keyword_hits in matched:
                source = str(row.get("source") or "")
                published_at = str(row.get("published_at") or "")
                source_score = source_quality.get(source, 45.0)
                freshness = self._freshness(published_at or str(row.get("fetched_at") or ""))
                importance = min(1.0, rule.base_importance + min(len(keyword_hits), 5) * 0.06)
                impact_strength = self._impact_strength(content, rule)
                market_relevance = min(1.0, 0.45 + len(rule.sectors) * 0.08 + len(rule.stocks) * 0.03)
                confidence = min(1.0, 0.45 + source_score / 200 + len(keyword_hits) * 0.05)
                value_score = self._value_score(
                    source_quality=source_score / 100,
                    freshness=freshness,
                    novelty=novelty,
                    market_relevance=market_relevance,
                    impact_strength=impact_strength,
                    confidence=confidence,
                )
                value_score = max(
                    0.0,
                    min(100.0, value_score + self._time_bonus(published_at or str(row.get("fetched_at") or ""))),
                )
                insights.append(
                    NewsInsight(
                        news_id=int(row.get("id") or 0),
                        source=source,
                        title=title,
                        summary=summary,
                        url=str(row.get("url") or ""),
                        published_at=published_at,
                        topic=rule.topic,
                        category=rule.category,
                        importance=round(importance, 3),
                        novelty=round(novelty, 3),
                        market_relevance=round(market_relevance, 3),
                        impact_strength=round(impact_strength, 3),
                        source_quality=round(source_score, 1),
                        freshness=round(freshness, 3),
                        confidence=round(confidence, 3),
                        value_score=round(value_score, 1),
                        sentiment=rule.direction,
                        time_horizon=self._time_horizon(content),
                        affected_sectors=rule.sectors,
                        affected_stocks=rule.stocks,
                        reason=self._reason(rule, keyword_hits),
                    )
                )
        return insights

    def _persist(self, insights: list[NewsInsight], topic_summary: list[dict[str, Any]]) -> None:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        reviews = self._latest_llm_reviews()
        for item in insights:
            review = reviews.get((item.news_id, item.topic)) or reviews.get((item.news_id, "")) or {}
            self._db.conn.execute(
                """INSERT INTO event_news_insights
                   (news_id, source, title, topic, category, value_score, sentiment,
                    time_horizon, affected_sectors_json, affected_stocks_json, reason,
                    llm_score, llm_decision, llm_rationale, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(news_id, topic) DO UPDATE SET
                   source=excluded.source,
                   title=excluded.title,
                   category=excluded.category,
                   value_score=excluded.value_score,
                   sentiment=excluded.sentiment,
                   time_horizon=excluded.time_horizon,
                   affected_sectors_json=excluded.affected_sectors_json,
                   affected_stocks_json=excluded.affected_stocks_json,
                   reason=excluded.reason,
                   llm_score=excluded.llm_score,
                   llm_decision=excluded.llm_decision,
                   llm_rationale=excluded.llm_rationale,
                   updated_at=excluded.updated_at""",
                (
                    item.news_id,
                    item.source,
                    item.title,
                    item.topic,
                    item.category,
                    item.value_score,
                    item.sentiment,
                    item.time_horizon,
                    json.dumps(item.affected_sectors, ensure_ascii=False),
                    json.dumps(item.affected_stocks, ensure_ascii=False),
                    item.reason,
                    float(review.get("llm_score") or 0.0),
                    str(review.get("decision") or ""),
                    str(review.get("rationale") or ""),
                    now,
                    now,
                ),
            )

        insight_by_topic: dict[str, list[NewsInsight]] = {}
        for item in insights:
            insight_by_topic.setdefault(item.topic, []).append(item)
        for row in topic_summary:
            topic = str(row.get("topic") or "")
            topic_items = insight_by_topic.get(topic, [])
            if not topic:
                continue
            avg_confidence = sum(item.confidence for item in topic_items) / max(1, len(topic_items))
            avg_relevance = sum(item.market_relevance for item in topic_items) / max(1, len(topic_items))
            sentiment_votes: dict[str, int] = {}
            latest_news: list[int] = []
            for item in topic_items[:10]:
                sentiment_votes[item.sentiment] = sentiment_votes.get(item.sentiment, 0) + 1
                if item.news_id:
                    latest_news.append(item.news_id)
            direction = max(sentiment_votes, key=sentiment_votes.get) if sentiment_votes else "neutral"
            self._db.conn.execute(
                """INSERT INTO event_news_topic_events
                   (topic, category, heat, confidence, market_relevance, direction,
                    insight_count, affected_sectors_json, latest_news_json, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(topic) DO UPDATE SET
                   category=excluded.category,
                   heat=excluded.heat,
                   confidence=excluded.confidence,
                   market_relevance=excluded.market_relevance,
                   direction=excluded.direction,
                   insight_count=excluded.insight_count,
                   affected_sectors_json=excluded.affected_sectors_json,
                   latest_news_json=excluded.latest_news_json,
                   updated_at=excluded.updated_at""",
                (
                    topic,
                    str(row.get("category") or ""),
                    float(row.get("heat") or 0.0),
                    round(avg_confidence, 3),
                    round(avg_relevance, 3),
                    direction,
                    int(row.get("count") or 0),
                    json.dumps(row.get("sectors") or [], ensure_ascii=False),
                    json.dumps(latest_news, ensure_ascii=False),
                    now,
                ),
            )
        self._db.conn.commit()

    def _latest_llm_reviews(self) -> dict[tuple[int, str], dict[str, Any]]:
        rows = self._db.conn.execute(
            """SELECT news_id, topic, llm_score, decision, rationale
               FROM event_llm_reviews
               ORDER BY id DESC"""
        ).fetchall()
        reviews: dict[tuple[int, str], dict[str, Any]] = {}
        for news_id, topic, llm_score, decision, rationale in rows:
            key = (int(news_id or 0), str(topic or ""))
            if key in reviews:
                continue
            reviews[key] = {
                "llm_score": float(llm_score or 0.0),
                "decision": str(decision or ""),
                "rationale": str(rationale or ""),
            }
        return reviews

    @staticmethod
    def _json_records(df: pd.DataFrame) -> list[dict[str, Any]]:
        if df.empty:
            return []
        records = df.where(pd.notna(df), None).to_dict(orient="records")
        for row in records:
            for key in ("affected_sectors_json", "affected_stocks_json", "latest_news_json"):
                if key not in row:
                    continue
                target = key.removesuffix("_json")
                try:
                    row[target] = json.loads(str(row.get(key) or "[]"))
                except json.JSONDecodeError:
                    row[target] = []
        return records

    @staticmethod
    def _source_quality_map(df: pd.DataFrame) -> dict[str, float]:
        if df.empty:
            return {}
        rows = df.where(pd.notna(df), None).to_dict(orient="records")
        return {
            str(row.get("source_id") or ""): float(row.get("quality_score") or 45.0)
            for row in rows
            if row.get("source_id")
        }

    def _match_topics(self, content: str) -> list[tuple[TopicRule, list[str]]]:
        lowered = content.lower()
        matched: list[tuple[TopicRule, list[str]]] = []
        for rule in TOPIC_RULES:
            hits = [keyword for keyword in rule.keywords if keyword.lower() in lowered]
            if hits:
                matched.append((rule, hits))
        return matched

    def _primary_matches(
        self,
        content: str,
        matched: list[tuple[TopicRule, list[str]]],
    ) -> list[tuple[TopicRule, list[str]]]:
        """Keep one primary topic per news item to avoid duplicated report rows."""
        if len(matched) <= 1:
            return matched
        primary = max(matched, key=lambda item: self._topic_match_score(content, item[0], item[1]))
        return [primary]

    @staticmethod
    def _topic_match_score(content: str, rule: TopicRule, hits: list[str]) -> float:
        lowered = content.lower()
        score = (
            len(hits) * 10
            + sum(len(keyword) for keyword in hits) * 0.2
            + rule.base_importance * 5
            + TOPIC_PRIORITY.get(rule.topic, 0)
        )

        real_estate_words = ["地产", "房地产", "住房", "购房", "房贷", "公积金"]
        if any(word in content for word in real_estate_words):
            if rule.topic == "金融地产政策":
                score += 18
            if rule.topic == "消费出海":
                score -= 18

        oil_words = ["原油", "油价", "crude", "oil", "barrel", "eia", "inventor"]
        if any(word in lowered for word in oil_words):
            if rule.topic == "能源商品":
                score += 18
            if rule.topic == "AI大模型" and {hit.lower() for hit in hits} <= {"api"}:
                score -= 30

        travel_words = ["文旅", "旅游", "假期", "出行", "铁路", "酒店", "景区"]
        if any(word in content for word in travel_words):
            if rule.topic == "消费出海":
                score += 8
            if rule.topic == "金融地产政策":
                score -= 8

        ai_words = ["大模型", "智能体", "openai", "deepseek", "gpt", "算力", "gpu"]
        if any(word in lowered for word in ai_words):
            if rule.topic in {"AI大模型", "算力基础设施"}:
                score += 8

        return score

    @staticmethod
    def _dedupe_key(title: str) -> str:
        clean = re.sub(r"\W+", "", title.lower())
        return clean[:48]

    @staticmethod
    def _freshness(value: str) -> float:
        if not value:
            return 0.55
        candidates = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
            "%a, %d %b %Y %H:%M:%S %z",
        ]
        parsed = None
        for fmt in candidates:
            try:
                parsed = datetime.strptime(value, fmt)
                break
            except ValueError:
                continue
        if parsed is None:
            return 0.6
        if parsed.tzinfo is not None:
            parsed = parsed.replace(tzinfo=None)
        age_hours = max(0.0, (datetime.now() - parsed).total_seconds() / 3600)
        return round(max(0.2, math.exp(-age_hours / 72)), 3)

    @classmethod
    def _insight_sort_key(cls, item: NewsInsight) -> tuple[int, float]:
        return (cls._time_bucket(item.published_at), item.value_score)

    @classmethod
    def _time_bonus(cls, value: str) -> float:
        bucket = cls._time_bucket(value)
        if bucket >= 5:
            return 6.0
        if bucket == 4:
            return 4.0
        if bucket == 3:
            return 2.0
        if bucket == 2:
            return -2.0
        if bucket == 1:
            return -8.0
        return -3.0

    @classmethod
    def _time_bucket(cls, value: str, now: datetime | None = None) -> int:
        """Rank news recency for report ordering: today first, then overnight, then older."""
        parsed = cls._parse_time(value)
        if parsed is None:
            return 0
        current = now or datetime.now()
        if parsed.date() == current.date():
            return 5
        age_days = (current.date() - parsed.date()).days
        if age_days == 1 and parsed.hour >= 15:
            return 4
        if age_days == 1:
            return 3
        if 1 < age_days <= 3:
            return 2
        return 1

    @staticmethod
    def _parse_time(value: str) -> datetime | None:
        text = str(value or "").strip()
        if not text:
            return None
        text = text.replace("T", " ").replace("Z", "")
        if "." in text:
            text = text.split(".", 1)[0]
        candidates = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d",
            "%a, %d %b %Y %H:%M:%S %z",
            "%a, %d %b %Y %H:%M:%S %Z",
        ]
        for fmt in candidates:
            try:
                parsed = datetime.strptime(text, fmt)
                return parsed.replace(tzinfo=None) if parsed.tzinfo is not None else parsed
            except ValueError:
                continue
        try:
            parsed = parsedate_to_datetime(text)
            return parsed.replace(tzinfo=None) if parsed else None
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _impact_strength(content: str, rule: TopicRule) -> float:
        high_words = [
            "发布", "推出", "突破", "上调", "涨价", "降价", "制裁", "禁令", "批准", "中标",
            "release", "launch", "upgrade", "breakthrough", "ban", "sanction", "surge",
        ]
        medium_words = ["计划", "拟", "有望", "合作", "订单", "投资", "扩产", "preview", "report"]
        score = rule.base_importance
        lowered = content.lower()
        if any(word.lower() in lowered for word in high_words):
            score += 0.25
        if any(word.lower() in lowered for word in medium_words):
            score += 0.12
        return min(1.0, score)

    @staticmethod
    def _time_horizon(content: str) -> str:
        lowered = content.lower()
        if any(word in lowered for word in ["盘中", "突发", "刚刚", "7x24", "urgent"]):
            return "short"
        if any(word in lowered for word in ["政策", "规划", "产业", "扩产", "infrastructure"]):
            return "long"
        return "mid"

    @staticmethod
    def _value_score(
        *,
        source_quality: float,
        freshness: float,
        novelty: float,
        market_relevance: float,
        impact_strength: float,
        confidence: float,
    ) -> float:
        return (
            source_quality * 0.20
            + freshness * 0.15
            + novelty * 0.15
            + market_relevance * 0.25
            + impact_strength * 0.20
            + confidence * 0.05
        ) * 100

    @staticmethod
    def _reason(rule: TopicRule, hits: list[str]) -> str:
        hit_text = "、".join(hits[:5]) if hits else "财经新闻"
        sectors = "、".join(rule.sectors[:5]) if rule.sectors else "待进一步映射"
        return f"命中“{hit_text}”，归入{rule.topic}，可能影响{sectors}。"

    @staticmethod
    def _topic_summary(insights: list[NewsInsight]) -> list[dict[str, Any]]:
        grouped: dict[str, dict[str, Any]] = {}
        for item in insights:
            bucket = grouped.setdefault(
                item.topic,
                {
                    "topic": item.topic,
                    "category": item.category,
                    "heat": 0.0,
                    "count": 0,
                    "sectors": [],
                },
            )
            bucket["heat"] += item.value_score
            bucket["count"] += 1
            for sector in item.affected_sectors:
                if sector not in bucket["sectors"]:
                    bucket["sectors"].append(sector)
        rows = list(grouped.values())
        for row in rows:
            row["heat"] = round(row["heat"] / max(1, row["count"]), 1)
            row["sectors"] = row["sectors"][:8]
        rows.sort(key=lambda row: (row["heat"], row["count"]), reverse=True)
        return rows

    @staticmethod
    def _message(news: pd.DataFrame, insights: list[NewsInsight]) -> str:
        if news.empty:
            return "暂无新闻数据，请先运行 collect。"
        if not insights:
            return "已扫描新闻，但暂无达到阈值的高价值 A 股相关主题。"
        return f"已从 {len(news)} 条新闻中发现 {len(insights)} 条高价值 A 股相关洞察。"


TOPIC_PRIORITY = {
    "军工地缘": 9,
    "AI大模型": 8,
    "算力基础设施": 7,
    "半导体": 7,
    "能源商品": 7,
    "金融地产政策": 7,
    "黄金贵金属": 6,
    "医药创新药": 6,
    "机器人": 6,
    "低空经济": 6,
    "并购重组国改": 5,
    "消费出海": 3,
}


# ============================================================================
# DEPRECATED (v1.4.0): Hard-coded topic rules are superseded by NewsManager's
# LLM-driven inference engine.  TOPIC_RULES remain as keyword-only fallback
# when the LLM endpoint is unreachable.
# New rules or topics should NOT be added here — update the LLM system prompt
# and sector graph (data/sector_graph.toml) instead.
# Will be removed in v2.0.0 after LLM stability is proven in production.
# ============================================================================

TOPIC_RULES = [
    TopicRule(
        topic="AI大模型",
        category="technology",
        keywords=[
            "GPT", "GPT-5", "GPT-5.5", "OpenAI", "DeepSeek", "V4", "Claude", "Gemini",
            "Qwen", "Kimi", "大模型", "开源模型", "模型发布", "智能体", "agent", "API",
        ],
        sectors=["算力", "光模块", "AI应用", "软件服务", "半导体", "传媒游戏"],
        stocks=["工业富联", "中际旭创", "新易盛", "浪潮信息", "科大讯飞", "金山办公"],
        base_importance=0.78,
    ),
    TopicRule(
        topic="算力基础设施",
        category="technology",
        keywords=["算力", "GPU", "服务器", "数据中心", "液冷", "CPO", "光模块", "HBM", "Ascend", "昇腾"],
        sectors=["算力", "光模块", "CPO", "液冷", "数据中心", "半导体"],
        stocks=["工业富联", "中际旭创", "新易盛", "浪潮信息", "寒武纪"],
        base_importance=0.75,
    ),
    TopicRule(
        topic="半导体",
        category="technology",
        keywords=["芯片", "半导体", "晶圆", "封装", "光刻机", "先进制程", "存储", "出口管制"],
        sectors=["半导体", "芯片", "集成电路", "信创"],
        stocks=["北方华创", "长电科技", "韦尔股份", "中芯国际"],
        base_importance=0.70,
    ),
    TopicRule(
        topic="机器人",
        category="technology",
        keywords=["机器人", "人形机器人", "具身智能", "减速器", "伺服", "传感器"],
        sectors=["机器人", "减速器", "伺服系统", "传感器"],
        stocks=["三花智控", "拓普集团", "绿的谐波", "鸣志电器"],
        base_importance=0.68,
    ),
    TopicRule(
        topic="低空经济",
        category="technology",
        keywords=["低空经济", "eVTOL", "无人机", "通航", "空域", "飞行汽车"],
        sectors=["低空经济", "无人机", "航空装备", "军工"],
        stocks=["中信海直", "万丰奥威", "宗申动力"],
        base_importance=0.66,
    ),
    TopicRule(
        topic="能源商品",
        category="commodity",
        keywords=[
            "原油", "天然气", "煤炭", "电力", "储能", "光伏", "油价",
            "OPEC", "Brent", "WTI", "crude", "oil", "barrel", "EIA", "inventory", "inventories",
        ],
        sectors=["石油行业", "天然气", "煤炭", "电力", "储能", "光伏"],
        stocks=["中国石油", "中国石化", "中国海油", "广汇能源", "中曼石油", "海油工程"],
        base_importance=0.68,
    ),
    TopicRule(
        topic="军工地缘",
        category="geopolitics",
        keywords=["战争", "冲突", "制裁", "军演", "导弹", "航母", "空袭", "红海", "霍尔木兹", "伊朗"],
        sectors=["国防军工", "黄金", "石油行业", "航运港口"],
        stocks=["中航沈飞", "航发动力", "中国船舶", "中兵红箭", "中国卫通", "中船防务"],
        base_importance=0.72,
    ),
    TopicRule(
        topic="黄金贵金属",
        category="commodity",
        keywords=["黄金", "白银", "贵金属", "避险", "央行购金", "美元", "美债"],
        sectors=["黄金", "贵金属"],
        stocks=["山东黄金", "紫金矿业", "中金黄金", "赤峰黄金", "银泰黄金", "湖南黄金"],
        base_importance=0.66,
    ),
    TopicRule(
        topic="医药创新药",
        category="healthcare",
        keywords=["创新药", "临床", "FDA", "医保", "CXO", "ADC", "医药", "药品审批"],
        sectors=["创新药", "CXO", "医药", "医疗服务"],
        stocks=["恒瑞医药", "药明康德", "百济神州", "长春高新", "康龙化成", "泰格医药"],
        base_importance=0.64,
    ),
    TopicRule(
        topic="金融地产政策",
        category="policy",
        keywords=["降准", "降息", "LPR", "MLF", "地产", "化债", "地方债", "房地产", "银行", "证券"],
        sectors=["证券", "银行", "房地产"],
        stocks=["招商银行", "中国平安", "万科A", "保利发展", "中信证券", "华泰证券"],
        base_importance=0.66,
    ),
    TopicRule(
        topic="消费出海",
        category="consumer",
        keywords=["消费", "出口", "关税", "汇率", "跨境电商", "补贴", "以旧换新"],
        sectors=["消费电子", "家电", "跨境电商", "汽车"],
        stocks=["比亚迪", "海尔智家", "美的集团", "安克创新", "石头科技", "传音控股"],
        base_importance=0.58,
    ),
    TopicRule(
        topic="并购重组国改",
        category="capital_market",
        keywords=["并购", "重组", "资产注入", "国企改革", "市值管理", "回购", "增持"],
        sectors=["央企改革", "国企改革", "券商"],
        stocks=["中国船舶", "中国交建", "中国中车", "中国联通", "中国铝业", "中国铁建"],
        base_importance=0.62,
    ),
]
