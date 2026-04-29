"""Markdown reporting for event intelligence."""

from __future__ import annotations

import json

from modules.config import cfg
from modules.db import DatabaseManager

from .history import EventHistoryRegistry
from .models import event_status_label
from .news_factors import NewsFactorEngine
from .news_intelligence import NewsIntelligenceEngine
from .source_quality import source_recommendation
from .topic_memory import TopicMemoryEngine


class EventReporter:
    """Generate human-readable event intelligence reports."""

    def __init__(self, db: DatabaseManager | None = None):
        self._db = db or DatabaseManager(cfg().data.warehouse_path)

    def report(self, limit: int = 10) -> str:
        states = self._db.event().list_states(limit=limit)
        opportunities = self._db.event().list_opportunities(limit=limit)
        source_quality = self._db.event().list_source_quality(limit=limit)
        signal_evidence = self._db.event().list_signal_evidence(limit=limit)
        elasticity_runs = self._db.event().list_elasticity_runs(limit=1)
        history = EventHistoryRegistry().list(limit=limit)
        news_scan_limit = max(100, limit * 20)
        news_intelligence = NewsIntelligenceEngine(db=self._db).analyze(limit=news_scan_limit, min_score=45.0)
        news_factors = NewsFactorEngine(db=self._db).build(limit=news_scan_limit, min_score=55.0, top_n=limit)
        topic_memory = TopicMemoryEngine(db=self._db).update(limit=news_scan_limit, min_score=45.0, top_n=limit)

        lines = ["# MoatX 宏观事件情报报告", ""]
        if states.empty:
            lines.extend(["暂无事件状态。", ""])
        else:
            lines.extend([
                "## 事件状态",
                "",
                "| 事件 | 概率 | 影响强度 | 状态 | 证据数 | 更新时间 |",
                "|---|---:|---:|---|---:|---|",
            ])
            for _, row in states.iterrows():
                lines.append(
                    f"| {row['name']} | {float(row['probability']):.0%} | "
                    f"{float(row['impact_strength']):.0%} | {event_status_label(row.get('status'))} | "
                    f"{int(row['evidence_count'])} | {row['updated_at']} |"
                )
            lines.append("")

        if opportunities.empty:
            lines.extend(["## 事件机会", "", "暂无候选机会。", ""])
        else:
            lines.extend([
                "## 事件机会",
                "",
                "| 事件ID | 代码 | 名称 | 机会分 | 标签 | 建议 |",
                "|---|---|---|---:|---|---|",
            ])
            for _, row in opportunities.iterrows():
                lines.append(
                    f"| {row['event_id']} | {row['symbol']} | {row.get('name', '')} | "
                    f"{float(row['opportunity_score']):.1f} | {row.get('sector_tags', '')} | "
                    f"{row.get('recommendation', '')} |"
                )
            lines.append("")

        if source_quality.empty:
            lines.extend(["## 源质量", "", "暂无源质量统计。", ""])
        else:
            lines.extend([
                "## 源质量",
                "",
                "| 源 | 分类 | 等级 | 质量分 | 抓取 | 信号命中 | 命中率 | 建议 | 最近错误 |",
                "|---|---|---|---:|---:|---:|---:|---|---|",
            ])
            for _, row in source_quality.iterrows():
                recommendation = source_recommendation(dict(row))
                lines.append(
                    f"| {row['source_id']} | {row.get('category', '')} | "
                    f"{row.get('reliability', '') or ''} | {float(row.get('quality_score') or 0):.1f} | "
                    f"{int(row.get('fetched') or 0)} | {int(row.get('signal_hits') or 0)} | "
                    f"{float(row.get('hit_rate') or 0):.1%} | "
                    f"{recommendation['source_recommendation']} | {row.get('last_error', '') or ''} |"
                )
            lines.append("")

        lines.extend(self._news_intelligence_section(news_intelligence, news_factors, limit))
        lines.extend(self._topic_memory_section(topic_memory, limit))

        if signal_evidence.empty:
            lines.extend(["## 最新证据链", "", "暂无最新事件证据。", ""])
        else:
            lines.extend([
                "## 最新证据链",
                "",
                "| 事件ID | 阶段 | 时效 | 强度 | 来源 | 标题 |",
                "|---|---|---|---|---|---|",
            ])
            for _, row in signal_evidence.iterrows():
                entities = self._json_dict(row.get("entities_json"))
                title = str(row.get("title") or "")[:42]
                lines.append(
                    f"| {row.get('event_id', '')} | {event_status_label(entities.get('stage'))} | "
                    f"{entities.get('time_sensitivity', '')} | {entities.get('intensity', '')} | "
                    f"{row.get('source', '')} | {title} |"
                )
            lines.append("")

        if elasticity_runs.empty:
            lines.extend(["## 历史弹性", "", "暂无事件弹性回测结果。", ""])
        else:
            latest = elasticity_runs.iloc[0]
            summary = json.loads(str(latest.get("summary_json") or "{}"))
            rows = summary.get("rows", []) if isinstance(summary, dict) else []
            lines.extend([
                "## 历史弹性",
                "",
                f"最近回测 Run #{latest['id']}，样本数 {int(latest.get('sample_count') or 0)}。",
                "",
            ])
            if rows:
                lines.extend([
                    "| 事件ID | 窗口 | 样本 | 平均收益 | 胜率 | 平均回撤 |",
                    "|---|---:|---:|---:|---:|---:|",
                ])
                for row in rows[:limit]:
                    lines.append(
                        f"| {row.get('event_id', '')} | T+{int(row.get('window_days') or 0)} | "
                        f"{int(row.get('sample_count') or 0)} | {float(row.get('avg_forward_return') or 0):.2f}% | "
                        f"{float(row.get('win_rate') or 0):.1%} | {float(row.get('avg_max_drawdown') or 0):.2f}% |"
                    )
                lines.append("")

        if history:
            lines.extend([
                "## 历史事件样本",
                "",
                "| 事件ID | 日期 | 样本 | 相关板块 |",
                "|---|---|---|---|",
            ])
            for row in history[:limit]:
                lines.append(
                    f"| {row.get('event_id', '')} | {row.get('trigger_date', '')} | "
                    f"{row.get('name', '')} | {', '.join(row.get('related_sectors', []))} |"
                )
            lines.append("")

        lines.extend([
            "## 风险提示",
            "",
            "- 宏观事件机会仅代表情报和产业传导判断，不构成自动交易指令。",
            "- 若事件快速缓和，事件溢价可能迅速回撤。",
            "- 若相关板块已大幅上涨，应优先判断追高风险。",
        ])
        return "\n".join(lines)

    @staticmethod
    def _json_dict(value) -> dict:
        try:
            payload = json.loads(str(value or "{}"))
            return payload if isinstance(payload, dict) else {}
        except Exception:
            return {}

    @staticmethod
    def _news_intelligence_section(
        news_intelligence: dict,
        news_factors: dict,
        limit: int,
    ) -> list[str]:
        lines = ["## 新闻价值发现", ""]
        factors = news_factors.get("factors") or []
        insights = news_intelligence.get("insights") or []
        if not factors and not insights:
            return lines + ["暂无达到阈值的高价值新闻洞察。", ""]

        if factors:
            lines.extend([
                "### 新闻板块因子",
                "",
                "| 板块 | 因子分 | 方向 | 洞察数 | 平均新闻分 | 主题 |",
                "|---|---:|---|---:|---:|---|",
            ])
            for row in factors[:limit]:
                lines.append(
                    f"| {row.get('sector', '')} | {float(row.get('factor_score') or 0):.1f} | "
                    f"{row.get('direction', '')} | {int(row.get('insight_count') or 0)} | "
                    f"{float(row.get('avg_value_score') or 0):.1f} | {row.get('top_topic', '')} |"
                )
            lines.append("")

        if insights:
            lines.extend([
                "### 高价值新闻",
                "",
                "| 分数 | 主题 | 新闻 | 关联板块 |",
                "|---:|---|---|---|",
            ])
            for row in insights[:limit]:
                title = str(row.get("title") or "")[:42]
                sectors = ", ".join(row.get("affected_sectors") or [])
                lines.append(
                    f"| {float(row.get('value_score') or 0):.1f} | {row.get('topic', '')} | "
                    f"{title} | {sectors} |"
                )
            lines.append("")
        return lines

    @staticmethod
    def _topic_memory_section(topic_memory: dict, limit: int) -> list[str]:
        lines = ["## 主题演化追踪", ""]
        topics = topic_memory.get("topics") or []
        if not topics:
            return lines + ["暂无主题记忆。", ""]
        lines.extend([
            "| 主题 | 热度 | 动量 | 趋势 | 累计洞察 | 关联板块 |",
            "|---|---:|---:|---|---:|---|",
        ])
        for row in topics[:limit]:
            sectors = ", ".join(row.get("sectors") or [])
            lines.append(
                f"| {row.get('topic', '')} | {float(row.get('heat') or 0):.1f} | "
                f"{float(row.get('momentum') or 0):+.1f} | {row.get('trend', '')} | "
                f"{int(row.get('total_insight_count') or 0)} | {sectors} |"
            )
        lines.append("")
        return lines


def generate_event_report(limit: int = 10) -> str:
    """Convenience entry point for scheduler/CLI."""
    return EventReporter().report(limit=limit)
