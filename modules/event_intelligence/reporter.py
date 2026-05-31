"""Markdown reporting for event intelligence."""

from __future__ import annotations

import json
import re
from datetime import datetime, time, timedelta
from difflib import SequenceMatcher
from email.utils import parsedate_to_datetime

from modules.config import cfg
from modules.data_source_manager import DataSourceManager
from modules.db import DatabaseManager
from modules.sector_tags import SectorTagProvider
from modules.utils import normalize_symbol

from .llm_semantics import llm_settings_status
from .models import event_status_label, now_ts
from .news_intelligence import NewsIntelligenceEngine
from .source_quality import source_recommendation


class EventReporter:
    """Generate human-readable event intelligence reports."""

    WATCH_MODULES = [
        "算力基础设施",
        "AI大模型",
        "能源商品",
        "医药创新药",
        "机器人",
        "军工地缘",
        "黄金贵金属",
        "储能新能源",
        "半导体",
        "金融地产政策",
        "消费出海",
    ]

    SOURCE_META = {
        "cls_telegraph_json": ("财联社", "中国"),
        "chinanews_china_rss": ("中国新闻网", "中国"),
        "chinanews_world_rss": ("中国新闻网", "中国"),
        "chinanews_finance_rss": ("中国新闻网", "中国"),
        "xinhua_world_rss": ("新华网", "中国"),
        "xinhua_fortune_rss": ("新华网", "中国"),
        "xinhua_politics_rss": ("新华网", "中国"),
        "cctv_news_jsonp": ("央视网", "中国"),
        "stcn_yw_html": ("证券时报", "中国"),
        "cnstock_home_html": ("上海证券报", "中国"),
        "oilprice_main_rss": ("OilPrice", "美国"),
        "pbc_news_html": ("中国人民银行", "中国"),
        "ndrc_news_html": ("国家发改委", "中国"),
        "ndrc_policy_html": ("国家发改委", "中国"),
        "stats_release_html": ("国家统计局", "中国"),
    }

    EVENT_PROFILES = [
        {
            "name": "gb300_power_order",
            "keywords": ["GB300", "电源", "批量订单"],
            "event_type": "订单/合同",
            "trigger": "GB300批量订单",
            "direct_tag": "服务器电源",
            "secondary": "液冷及数据中心基础设施",
            "core": "国产电源厂首次获得AI服务器高端平台批量配套订单，验证高端电源国产化从0到1的突破。",
            "editor": "该新闻标志着AI服务器电源国产替代取得实质订单，可能引发资金对电源链及液冷散热环节的重新定价。",
            "one_line": "电源环节国产替代加速，液冷和数据中心配套确定性抬升。",
        },
        {
            "name": "optical_passive_shortage",
            "keywords": ["光通信", "关键无源器件", "供应紧张"],
            "event_type": "供应紧缺",
            "trigger": "800G/1.6T光模块扩产拉动",
            "direct_tag": "光无源器件",
            "secondary": "高速光模块和CPO产能释放",
            "core": "高速光模块上游关键无源器件供应紧张，卡脖子环节可能限制下游扩产节奏。",
            "editor": "上游光无源器件产能受限，相关器件价格或有上行压力，利好有产能储备的光通信龙头。",
            "one_line": "光模块瓶颈从整机需求转向上游器件供给。",
        },
        {
            "name": "green_compute_policy",
            "keywords": ["绿色算力", "榜单"],
            "event_type": "政策/榜单",
            "trigger": "绿色算力设施榜单",
            "direct_tag": "绿色算力",
            "secondary": "智算中心、电力和液冷配套",
            "core": "六部委发布绿色算力设施榜单，官方梯队确认有望强化地方智算中心建设预期。",
            "editor": "政策首次明确绿色算力官方梯队，后续项目审批和资金支持可能向智算中心及节能配套倾斜。",
            "one_line": "绿色算力从概念走向官方示范，配套链条进入验证期。",
        },
        {
            "name": "oil_export_record",
            "keywords": ["原油出口", "创纪录"],
            "event_type": "数据新高",
            "trigger": "美国原油出口创纪录",
            "direct_tag": "石油行业",
            "secondary": "油服、航运和能源化工",
            "core": "美国原油出口升破600万桶/日，海外买家寻找中东替代供应的需求正在显性化。",
            "editor": "数据异动反映全球原油供给重定价，短期油价和油运链条受益，后续需要关注中东供应变量。",
            "one_line": "原油替代供应需求升温，能源链条波动率抬升。",
        },
        {
            "name": "sodium_battery_order",
            "keywords": ["钠电", "订单", "规模化量产"],
            "event_type": "订单/合同",
            "trigger": "钠电规模化订单",
            "direct_tag": "储能",
            "display_topic": "储能新能源",
            "secondary": "电池材料和储能系统",
            "core": "钠电大订单落地并给出量产时间表，验证新型储能从示范走向规模化交付。",
            "editor": "该新闻标志着钠电产业化取得实质订单，可能提升市场对储能系统和电池材料环节的关注。",
            "one_line": "钠电从主题预期进入订单验证阶段。",
        },
    ]

    def __init__(self, db: DatabaseManager | None = None):
        self._db = db or DatabaseManager(cfg().data.warehouse_path)

    def report(self, limit: int = 10) -> str:
        source_quality_all = self._db.event().list_source_quality(limit=1000)
        news_scan_limit = max(100, limit * 20)
        news_intelligence = NewsIntelligenceEngine(db=self._db).analyze(limit=news_scan_limit, min_score=45.0)
        lines = [self._report_title(), ""]
        insights = (news_intelligence.get("insights") or [])[:limit]
        lines.extend(self._strict_refresh_section(source_quality_all, news_intelligence, insights))
        lines.extend(self._market_validation_section(insights))
        lines.extend(self._strict_hotspot_sections(insights))
        lines.append("注：本报告基于算法新闻抓取与人工增强逻辑，不构成投资建议。")
        return "\n".join(lines)

    @staticmethod
    def _report_title(now_text: str | None = None) -> str:
        current = EventReporter._parse_time(now_text or now_ts())
        if current is None:
            current = datetime.now()
        if current.time() < time(9, 30):
            session = "盘前"
        elif current.time() <= time(15, 0):
            session = "盘中"
        else:
            session = "收盘后"
        return f"MoatX 热点速览 | {current.strftime('%Y-%m-%d')} {session}"

    @staticmethod
    def _strict_refresh_section(source_quality, news_intelligence: dict, insights: list[dict]) -> list[str]:
        active_sources = source_quality
        if not source_quality.empty and "enabled" in source_quality:
            active_sources = source_quality[source_quality["enabled"].fillna(0).astype(int) == 1]
        sources = len(active_sources) if not active_sources.empty else 0
        fetched = int(active_sources["fetched"].fillna(0).sum()) if not active_sources.empty and "fetched" in active_sources else 0
        counts = EventReporter._module_counts(insights)
        hot = [f"{topic}（{count}条）" for topic, count in counts if count > 0]
        inactive = [name for name in EventReporter.WATCH_MODULES if name not in dict(counts)]
        hot_text = "、".join(hot) if hot else "无触发阈值"
        inactive_text = f"{'、'.join(inactive[:5])}无触发阈值。" if inactive else ""
        return [
            f"新闻侧热度：本时段扫描{sources}个源，捕获{fetched}条资讯。今日高热聚焦：{hot_text}。{inactive_text}",
            "",
        ]

    @staticmethod
    def _market_validation_section(insights: list[dict]) -> list[str]:
        rows = EventReporter._market_validation_rows(insights)
        if not rows:
            return ["盘面验证：行情数据暂不可用，本报告仅保留新闻侧热度。", ""]

        confirmed = [row["topic"] for row in rows if row["status"] in ("资金共振", "温和确认")]
        pending = [row["topic"] for row in rows if row["status"] == "盘面未确认"]
        summary_parts = []
        if confirmed:
            summary_parts.append(f"{'、'.join(confirmed[:3])}获得盘面确认")
        if pending:
            summary_parts.append(f"{'、'.join(pending[:3])}新闻热但盘面未同步")
        summary = "；".join(summary_parts) if summary_parts else "主题表现分化，需继续观察资金确认"

        lines = [f"盘面验证：{summary}。", ""]
        for row in rows[:5]:
            lines.append(
                f"• {row['topic']}：{row['status']}，样本{row['sample_count']}只，"
                f"上涨{row['up']} / 下跌{row['down']} / 平盘{row['flat']}，"
                f"平均涨跌{row['avg_pct']:+.2f}%，成交{row['amount_yi']:.2f}亿；"
                f"最强 {row['top_name']}({row['top_code']}) {row['top_pct']:+.2f}%，"
                f"最弱 {row['bottom_name']}({row['bottom_code']}) {row['bottom_pct']:+.2f}%。"
            )
        lines.append("")
        return lines

    @staticmethod
    def _market_validation_rows(insights: list[dict]) -> list[dict]:
        topic_codes: dict[str, dict[str, str]] = {}
        for item in insights:
            profile = EventReporter._event_profile(item)
            topic = EventReporter._display_topic(item, profile)
            if not topic:
                continue
            targets = EventReporter._a_share_targets(item, profile, limit=5)
            bucket = topic_codes.setdefault(topic, {})
            for target in targets:
                match = re.search(r"\((\d{6})\)", target)
                if not match:
                    continue
                code = match.group(1)
                name = target.split("(", 1)[0].strip()
                bucket.setdefault(code, name)

        all_codes: list[str] = []
        for codes in topic_codes.values():
            for code in codes:
                if code not in all_codes:
                    all_codes.append(code)
        if not all_codes:
            return []

        try:
            quotes = DataSourceManager().fetch_quotes(all_codes[:40], mode="single", source_names=["sina"])
        except Exception:
            return []
        quotes_by_code = {normalize_symbol(key): value for key, value in quotes.items() if value}
        rows: list[dict] = []
        for topic, code_names in topic_codes.items():
            items = []
            for code, fallback_name in code_names.items():
                quote = quotes_by_code.get(code)
                if not quote:
                    continue
                pct = float(quote.get("change_pct") or 0)
                amount = float(quote.get("amount") or 0) / 100000000
                items.append({
                    "code": code,
                    "name": str(quote.get("name") or fallback_name),
                    "pct": pct,
                    "amount_yi": amount,
                })
            if not items:
                continue
            up = sum(1 for item in items if item["pct"] > 0)
            down = sum(1 for item in items if item["pct"] < 0)
            flat = len(items) - up - down
            avg_pct = sum(item["pct"] for item in items) / len(items)
            up_ratio = up / len(items)
            if avg_pct >= 1.0 and up_ratio >= 0.6:
                status = "资金共振"
            elif avg_pct > 0 and up > down:
                status = "温和确认"
            elif avg_pct <= -0.5 or down > up:
                status = "盘面未确认"
            else:
                status = "分化观察"
            top = max(items, key=lambda item: item["pct"])
            bottom = min(items, key=lambda item: item["pct"])
            rows.append({
                "topic": topic,
                "status": status,
                "sample_count": len(items),
                "up": up,
                "down": down,
                "flat": flat,
                "avg_pct": round(avg_pct, 2),
                "amount_yi": round(sum(item["amount_yi"] for item in items), 2),
                "top_name": top["name"],
                "top_code": top["code"],
                "top_pct": round(top["pct"], 2),
                "bottom_name": bottom["name"],
                "bottom_code": bottom["code"],
                "bottom_pct": round(bottom["pct"], 2),
            })
        return sorted(rows, key=lambda row: (-row["avg_pct"], -row["sample_count"]))

    @staticmethod
    def _strict_hotspot_sections(insights: list[dict]) -> list[str]:
        if not insights:
            return ["无触发阈值。", ""]

        groups = EventReporter._group_insights(insights)
        lines: list[str] = []
        index = 1
        for group in groups:
            title = EventReporter._module_title(group["topic"], len(group["items"]))
            lines.extend([title, ""])
            for item in group["items"]:
                lines.extend(EventReporter._hotspot_tuple(index, item))
                lines.append("")
                index += 1
            lines.append("")
        return lines

    @staticmethod
    def _hotspot_tuple(index: int, item: dict) -> list[str]:
        score = float(item.get("value_score") or 0)
        keyword = EventReporter._keyword_from_reason(str(item.get("reason") or ""))
        profile = EventReporter._event_profile(item)
        topic = EventReporter._display_topic(item, profile)
        if profile.get("keyword"):
            keyword = str(profile["keyword"])
        source, default_country = EventReporter._source_label(str(item.get("source") or ""))
        country = EventReporter._country_from_item(item, default_country)
        stocks = EventReporter._a_share_targets(item, profile)
        sectors = item.get("affected_sectors") or []
        first_sector = str(profile.get("direct_tag") or (sectors[0] if sectors else topic))
        next_sector = str(profile.get("secondary") or ("、".join(str(x) for x in sectors[1:4]) if len(sectors) > 1 else "相关产业链"))
        stock_hint = f"（{stocks[0]}等）" if stocks else ""
        title = EventReporter._compact_title(item)
        core = EventReporter._core_takeaway(item, profile)
        reason = EventReporter._editor_reason(item, keyword, topic, profile)
        trigger = str(profile.get("trigger") or EventReporter._trigger_point(item, keyword))
        return [
            f"{index}. {title} {EventReporter._time_badge(item)}",
            f"热度 {EventReporter._stars(score)} ({score:.0f}%) | {source} ({country})",
            f"核心看点：{core}",
            f"传导路径：{trigger} ➔ {first_sector}{stock_hint} ➔ {next_sector}",
            f"选中理由：命中关键词\"{keyword or topic}\"，自动归入\"{topic}\"模块。{reason}",
            f"可能涉及的A股：{'、'.join(stocks) if stocks else '暂无明确A股标的'}",
            f"一句话：{EventReporter._one_line(item, topic, profile)}",
        ]

    @staticmethod
    def _module_counts(insights: list[dict]) -> list[tuple[str, int]]:
        counts: dict[str, int] = {}
        totals: dict[str, float] = {}
        for item in insights:
            profile = EventReporter._event_profile(item)
            topic = EventReporter._display_topic(item, profile)
            if not topic:
                continue
            counts[topic] = counts.get(topic, 0) + 1
            totals[topic] = totals.get(topic, 0.0) + float(item.get("value_score") or 0)
        return sorted(counts.items(), key=lambda pair: (-pair[1], -totals.get(pair[0], 0.0)))

    @staticmethod
    def _group_insights(insights: list[dict]) -> list[dict]:
        buckets: dict[str, list[dict]] = {}
        for item in insights:
            profile = EventReporter._event_profile(item)
            topic = EventReporter._display_topic(item, profile) or "未分类"
            buckets.setdefault(topic, []).append(item)
        groups = [
            {
                "topic": topic,
                "items": sorted(items, key=lambda row: float(row.get("value_score") or 0), reverse=True),
                "total_heat": sum(float(row.get("value_score") or 0) for row in items),
            }
            for topic, items in buckets.items()
        ]
        return sorted(groups, key=lambda row: (-len(row["items"]), -float(row["total_heat"])))

    @staticmethod
    def _module_title(topic: str, count: int) -> str:
        if count <= 1:
            return f"🔥 {topic}"
        if topic == "算力基础设施" and count >= 3:
            return f"🔥 {topic} · 硬件突破与政策双轮驱动"
        catalyst = "双线驱动" if count == 2 else "三重催化" if count == 3 else "多线共振"
        return f"🔥 {topic} · {catalyst}"

    @staticmethod
    def _display_topic(item: dict, profile: dict | None = None) -> str:
        profile = profile or EventReporter._event_profile(item)
        return str(profile.get("display_topic") or item.get("topic") or "").strip()

    @staticmethod
    def _time_badge(item: dict, now_text: str | None = None) -> str:
        value = str(item.get("published_at") or item.get("fetched_at") or "").strip()
        if not value:
            return "【时间未知】"
        published = EventReporter._parse_time(value)
        current = EventReporter._parse_time(now_text or now_ts())
        if not published or not current:
            return f"【{value[:16]}】"

        if published.date() == current.date():
            if time(9, 30) <= published.time() <= time(15, 0):
                return f"【盘中新发 {published.strftime('%H:%M')}】"
            if published.time() < time(9, 30):
                return f"【盘前新发 {published.strftime('%H:%M')}】"
            return f"【收盘后 {published.strftime('%H:%M')}】"

        if published.date() == (current.date() - timedelta(days=1)):
            if published.time() >= time(15, 0):
                return f"【昨日盘后 {published.strftime('%H:%M')}】"
            return f"【昨日旧闻 {published.strftime('%H:%M')}】"

        age_days = (current.date() - published.date()).days
        if 1 < age_days <= 3:
            return f"【旧闻复热 {published.strftime('%m-%d %H:%M')}】"
        return f"【历史新闻 {published.strftime('%m-%d %H:%M')}】"

    @staticmethod
    def _parse_time(value: str) -> datetime | None:
        text = str(value or "").strip()
        if not text:
            return None
        text = text.replace("T", " ").replace("Z", "")
        if "." in text:
            text = text.split(".", 1)[0]
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M:%S", "%Y/%m/%d %H:%M"):
            try:
                return datetime.strptime(text[: len(datetime.now().strftime(fmt))], fmt)
            except ValueError:
                continue
        try:
            parsed = parsedate_to_datetime(text)
            return parsed.replace(tzinfo=None) if parsed else None
        except (TypeError, ValueError):
            pass
        return None

    @staticmethod
    def _compact_title(item: dict) -> str:
        profile = EventReporter._event_profile(item)
        if profile.get("name") == "gb300_power_order":
            return "麦格米特获GB300电源订单"
        if profile.get("name") == "optical_passive_shortage":
            return "光无源器件供应紧张"
        if profile.get("name") == "green_compute_policy":
            return "六部委发布绿色算力榜单"
        if profile.get("name") == "oil_export_record":
            return "美国原油出口创纪录"
        if profile.get("name") == "sodium_battery_order":
            return "钠电大订单落地"
        text = EventReporter._text_cell(item.get("title"), 60)
        text = re.sub(r"^【[^】]{1,12}】", "", text)
        text = re.sub(r"^(快讯|电报|解读|公告)[:：]", "", text).strip()
        text = text.replace("同比", "").replace("该细分领域是", "")
        if len(text) > 20:
            return EventReporter._safe_truncate(text, 20)
        return text or "热点新闻触发"

    @staticmethod
    def _core_takeaway(item: dict, profile: dict | None = None) -> str:
        profile = profile or EventReporter._event_profile(item)
        if profile.get("core"):
            return str(profile["core"])
        title = EventReporter._text_cell(item.get("title"), 70)
        summary = EventReporter._text_cell(item.get("summary"), 90)
        if summary and EventReporter._text_similarity(title, summary) <= 0.9:
            return summary
        return f"{title}（全文待扩展）"

    @staticmethod
    def _editor_reason(item: dict, keyword: str, topic: str, profile: dict | None = None) -> str:
        profile = profile or EventReporter._event_profile(item)
        if profile.get("editor"):
            return str(profile["editor"])
        sectors = item.get("affected_sectors") or []
        sector_text = "、".join(str(x) for x in sectors[:4])
        if keyword:
            return f"该新闻指向{keyword}环节的新增催化，可能提升市场对{sector_text or topic}方向的关注度。"
        return f"该新闻与{topic}主题相关，可能影响{sector_text or topic}方向的市场关注度。"

    @staticmethod
    def _trigger_point(item: dict, keyword: str) -> str:
        if keyword:
            return keyword
        return EventReporter._compact_title(item)

    @staticmethod
    def _one_line(item: dict, topic: str, profile: dict | None = None) -> str:
        profile = profile or EventReporter._event_profile(item)
        if profile.get("one_line"):
            return str(profile["one_line"])
        sectors = item.get("affected_sectors") or []
        if sectors:
            return f"{topic}热度抬升，{'、'.join(str(x) for x in sectors[:3])}链条需要观察资金持续性。"
        return f"{topic}出现新闻催化，需要观察后续资金确认。"

    @staticmethod
    def _keyword_from_reason(reason: str) -> str:
        match = re.search(r"命中[“\"]([^”\"]+)[”\"]", reason)
        return match.group(1) if match else ""

    @staticmethod
    def _stars(score: float) -> str:
        rounded = round(score)
        if rounded >= 85:
            count = 5
        elif rounded >= 75:
            count = 4
        elif rounded >= 65:
            count = 3
        elif rounded >= 55:
            count = 2
        else:
            count = 1
        return "⭐" * count

    @classmethod
    def _source_label(cls, source_id: str) -> tuple[str, str]:
        return cls.SOURCE_META.get(source_id, (source_id or "未知来源", "未知"))

    @staticmethod
    def _country_from_item(item: dict, default_country: str) -> str:
        text = f"{item.get('title') or ''} {item.get('summary') or ''}"
        if any(token in text for token in ["美国", "美联储", "华盛顿", "英伟达", "OpenAI"]):
            return "美国"
        if any(token in text for token in ["日本", "东京"]):
            return "日本"
        if any(token in text for token in ["韩国", "首尔"]):
            return "韩国"
        if any(token in text for token in ["欧盟", "欧洲"]):
            return "欧洲"
        if any(token in text for token in ["中国", "国内", "国务院", "国家", "工信部", "发改委", "央行", "A股"]):
            return "中国"
        return default_country

    @staticmethod
    def _a_share_targets(item: dict, profile: dict | None = None, limit: int = 5) -> list[str]:
        rows: list[str] = []
        provider = SectorTagProvider()
        direct_tag = str((profile or {}).get("direct_tag") or "")
        tags = [direct_tag] if direct_tag else []
        tags.extend(str(sector) for sector in item.get("affected_sectors") or [])
        direct_count = 0
        for tag_index, sector in enumerate(tags):
            if not sector:
                continue
            try:
                members = provider._graph_members(str(sector))
            except Exception:
                members = None
            if members is None or members.empty:
                continue
            for _, member in members.iterrows():
                code = str(member.get("code") or "")
                name = str(member.get("name") or "")
                if not code or not name:
                    continue
                indirect = tag_index > 0 and direct_count < 3
                label = f"{name}({code}){'*' if indirect else ''}"
                if label not in rows:
                    rows.append(label)
                    if tag_index == 0:
                        direct_count += 1
                if len(rows) >= limit:
                    return rows
            if tag_index == 0 and direct_count >= 3:
                return rows
        return rows

    @staticmethod
    def _event_profile(item: dict) -> dict:
        content = f"{item.get('title') or ''} {item.get('summary') or ''}"
        for profile in EventReporter.EVENT_PROFILES:
            hits = [keyword for keyword in profile["keywords"] if keyword in content]
            if len(hits) >= min(2, len(profile["keywords"])):
                enriched = dict(profile)
                enriched["keyword"] = hits[0]
                return enriched
        keyword = EventReporter._keyword_from_reason(str(item.get("reason") or ""))
        return {
            "keyword": keyword,
            "trigger": keyword or EventReporter._compact_title_without_profile(item),
            "direct_tag": keyword or ((item.get("affected_sectors") or [""])[0]),
        }

    @staticmethod
    def _compact_title_without_profile(item: dict) -> str:
        text = EventReporter._text_cell(item.get("title"), 60)
        text = re.sub(r"^【[^】]{1,12}】", "", text)
        text = re.sub(r"^(快讯|电报|解读|公告)[:：]", "", text).strip()
        return EventReporter._safe_truncate(text, 20) if len(text) > 20 else text

    @staticmethod
    def _safe_truncate(text: str, max_len: int) -> str:
        if len(text) <= max_len:
            return text
        cut = text[:max_len]
        for suffix in ["获取批", "成功获", "批量", "创纪录高", "同比增"]:
            if cut.endswith(suffix):
                cut = cut[: -len(suffix)]
                break
        cut = cut.rstrip("，。、；：:,. 的了和及与")
        return f"{cut}..."

    @staticmethod
    def _text_similarity(left: str, right: str) -> float:
        clean_left = re.sub(r"\W+", "", left)
        clean_right = re.sub(r"\W+", "", right)
        if not clean_left or not clean_right:
            return 0.0
        return SequenceMatcher(None, clean_left, clean_right).ratio()

    @staticmethod
    def _news_refresh_section(source_quality, news_intelligence: dict, news_factors: dict) -> list[str]:
        sources = len(source_quality) if not source_quality.empty else 0
        fetched = int(source_quality["fetched"].fillna(0).sum()) if not source_quality.empty and "fetched" in source_quality else 0
        counts = EventReporter._topic_counts(news_intelligence)
        triggered = [f"{topic}（{count}条）" for topic, count in counts[:5]]
        if not triggered:
            topics = EventReporter._top_topic_names(news_intelligence, news_factors, limit=5)
            triggered = [f"{topic}（因子触发）" for topic in topics]
        topic_text = "、".join(triggered) if triggered else "暂无明确主题"
        return [
            f"本时段扫描 {sources} 个源，捕获 {fetched} 条资讯。今日高热聚焦：{topic_text}。",
            "",
        ]

    @staticmethod
    def _topic_counts(news_intelligence: dict) -> list[tuple[str, int]]:
        counts: dict[str, int] = {}
        for row in news_intelligence.get("insights") or []:
            topic = str(row.get("topic") or "").strip()
            if topic:
                counts[topic] = counts.get(topic, 0) + 1
        return sorted(counts.items(), key=lambda item: item[1], reverse=True)

    @staticmethod
    def _top_topic_names(news_intelligence: dict, news_factors: dict, limit: int = 5) -> list[str]:
        names: list[str] = []
        for row in news_factors.get("factors") or []:
            topic = str(row.get("top_topic") or row.get("sector") or "").strip()
            if topic and topic not in names:
                names.append(topic)
            if len(names) >= limit:
                return names
        for row in news_intelligence.get("topic_summary") or []:
            topic = str(row.get("topic") or "").strip()
            if topic and topic not in names:
                names.append(topic)
            if len(names) >= limit:
                return names
        return names

    @staticmethod
    def _summary_section(states, opportunities, news_factors: dict, topic_memory: dict, llm_status: dict) -> list[str]:
        lines = ["一、核心摘要", ""]
        factors = news_factors.get("factors") or []
        topics = topic_memory.get("topics") or []

        if not states.empty:
            top_event = states.iloc[0]
            lines.append(
                f"• 最强宏观事件：{top_event.get('name', '')}，概率 {float(top_event.get('probability') or 0):.0%}，"
                f"影响强度 {float(top_event.get('impact_strength') or 0):.0%}，状态 {event_status_label(top_event.get('status'))}。"
            )
        else:
            lines.append("• 最强宏观事件：暂无可用事件状态。")

        if factors:
            top_factor = factors[0]
            lines.append(
                f"• 最强新闻因子：{top_factor.get('sector', '')}，分值 {float(top_factor.get('factor_score') or 0):.1f}，"
                f"主导主题 {top_factor.get('top_topic', '')}。"
            )
        else:
            lines.append("• 最强新闻因子：暂无达到阈值的板块新闻因子。")

        if topics:
            top_topic = topics[0]
            lines.append(
                f"• 主题热度：{top_topic.get('topic', '')} 热度 {float(top_topic.get('heat') or 0):.1f}，"
                f"趋势 {top_topic.get('trend', '') or '未知'}。"
            )

        if not opportunities.empty:
            top_opp = opportunities.iloc[0]
            lines.append(
                f"• 机会标的：{top_opp.get('symbol', '')} {top_opp.get('name', '')}，"
                f"机会分 {float(top_opp.get('opportunity_score') or 0):.1f}，建议：{top_opp.get('recommendation', '')}。"
            )
        else:
            lines.append("• 机会标的：暂无候选机会。")

        lines.extend([
            f"• LLM 语义评审：{'已启用' if llm_status.get('enabled') else '未启用'}；当前报告仍可由规则系统独立生成。",
            "",
        ])
        return lines

    @staticmethod
    def _event_radar_section(states, limit: int) -> list[str]:
        lines = ["四、宏观事件雷达", ""]
        if states.empty:
            return lines + ["暂无事件状态。", ""]
        for _, row in states.head(limit).iterrows():
            lines.append(
                f"• {row.get('name', '')}：概率 {float(row.get('probability') or 0):.0%}，"
                f"影响 {float(row.get('impact_strength') or 0):.0%}，状态 {event_status_label(row.get('status'))}，"
                f"证据 {int(row.get('evidence_count') or 0)} 条；更新时间 {row.get('updated_at', '')}。"
            )
        lines.append("")
        return lines

    @staticmethod
    def _opportunity_section(opportunities, limit: int) -> list[str]:
        lines = ["五、新闻/事件驱动机会", ""]
        if opportunities.empty:
            return lines + ["暂无候选机会。", ""]
        for _, row in opportunities.head(limit).iterrows():
            lines.append(
                f"• {row.get('symbol', '')} {row.get('name', '')}：机会分 {float(row.get('opportunity_score') or 0):.1f}，"
                f"事件 {row.get('event_id', '')}，标签 {row.get('sector_tags', '') or '未标注'}，建议：{row.get('recommendation', '')}。"
            )
        lines.append("")
        return lines

    @staticmethod
    def _source_quality_section(source_quality, limit: int) -> list[str]:
        lines = ["八、源质量", ""]
        if source_quality.empty:
            return lines + ["暂无源质量统计。", ""]
        for _, row in source_quality.head(limit).iterrows():
            recommendation = source_recommendation(dict(row))
            error = str(row.get("last_error") or "").strip()
            error_text = f"；最近错误：{EventReporter._text_cell(error, 72)}" if error else ""
            lines.append(
                f"• {row.get('source_id', '')}：质量分 {float(row.get('quality_score') or 0):.1f}，"
                f"抓取 {int(row.get('fetched') or 0)}，信号命中 {int(row.get('signal_hits') or 0)}，"
                f"命中率 {float(row.get('hit_rate') or 0):.1%}，建议 {recommendation['source_recommendation']}{error_text}。"
            )
        lines.append("")
        return lines

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
        lines = ["二、热点模块", ""]
        factors = news_factors.get("factors") or []
        insights = news_intelligence.get("insights") or []
        if not factors and not insights:
            return lines + ["暂无达到阈值的高价值新闻洞察。", ""]

        if factors:
            for idx, group in enumerate(EventReporter._factor_groups(factors)[:limit], start=1):
                topic = str(group.get("topic") or "")
                sectors = ", ".join(group.get("sectors") or [])
                related = EventReporter._related_insight(insights, "", topic)
                mapped_sectors = ", ".join(related.get("affected_sectors") or group.get("sectors") or [])
                reason = related.get("reason") or "命中新闻主题并映射到A股相关板块，需结合新闻详情复核。"
                title = related.get("title") or "暂无代表新闻"
                direction = "利多" if group.get("direction") == "bullish" else str(group.get("direction") or "中性")
                factor_text = "，".join(
                    f"{item.get('sector', '')} {float(item.get('factor_score') or 0):.1f}"
                    for item in group.get("items", [])[:5]
                )
                lines.append(
                    f"{idx}）{topic or sectors}"
                )
                lines.append(f"方向：{direction}")
                lines.append(f"新闻板块因子：{factor_text}")
                lines.append(f"关联产业链：{EventReporter._text_cell(mapped_sectors or sectors, 96)}")
                lines.append(f"为什么和A股有关：{EventReporter._text_cell(reason, 96)}")
                lines.append(f"代表新闻：{EventReporter._text_cell(title, 72)}")
                lines.append("")

        elif insights:
            for idx, row in enumerate(insights[:limit], start=1):
                title = EventReporter._text_cell(row.get("title"), max_len=42)
                topic = EventReporter._text_cell(row.get("topic"), max_len=24)
                sectors = EventReporter._text_cell(", ".join(row.get("affected_sectors") or []), max_len=48)
                reason = EventReporter._text_cell(
                    row.get("reason") or "命中新闻主题并映射到A股相关板块，需结合新闻详情复核。",
                    max_len=72,
                )
                lines.append(f"{idx}）{topic}")
                lines.append(f"价值分：{float(row.get('value_score') or 0):.1f}")
                lines.append(f"关联产业链：{sectors}")
                lines.append(f"为什么和A股有关：{reason}")
                lines.append(f"代表新闻：{title}")
                lines.append("")
        return lines

    @staticmethod
    def _top_news_section(news_intelligence: dict, limit: int) -> list[str]:
        lines = ["三、Top5 热点新闻", ""]
        insights = news_intelligence.get("insights") or []
        if not insights:
            return lines + ["暂无达到阈值的热点新闻。", ""]
        for idx, row in enumerate(insights[: min(5, limit)], start=1):
            title = EventReporter._text_cell(row.get("title"), 80)
            source = EventReporter._text_cell(row.get("source"), 32) or "未知来源"
            score = float(row.get("value_score") or 0)
            topic = EventReporter._text_cell(row.get("topic"), 32)
            sectors = EventReporter._text_cell(", ".join(row.get("affected_sectors") or []), 96)
            reason = EventReporter._text_cell(
                row.get("reason") or "命中新闻主题并映射到A股相关板块，需结合新闻详情复核。",
                96,
            )
            lines.append(f"{idx}）{title}")
            lines.append(f"热度值：{score:.0f}%")
            lines.append(f"消息来源：{source}")
            lines.append(f"所属模块：{topic}")
            lines.append(f"为什么抓取该新闻：{reason}")
            lines.append(f"相关股票方向：{sectors}")
            lines.append("")
        return lines

    @staticmethod
    def _related_insight(insights: list[dict], sector: str, topic: str) -> dict:
        for item in insights:
            sectors = item.get("affected_sectors") or []
            if sector in sectors or (topic and item.get("topic") == topic):
                return item
        return insights[0] if insights else {}

    @staticmethod
    def _factor_groups(factors: list[dict]) -> list[dict]:
        grouped: dict[str, dict] = {}
        for row in factors:
            topic = str(row.get("top_topic") or row.get("sector") or "新闻驱动")
            bucket = grouped.setdefault(
                topic,
                {"topic": topic, "items": [], "sectors": [], "max_score": 0.0, "direction": row.get("direction")},
            )
            bucket["items"].append(row)
            sector = str(row.get("sector") or "")
            if sector and sector not in bucket["sectors"]:
                bucket["sectors"].append(sector)
            bucket["max_score"] = max(float(bucket["max_score"]), float(row.get("factor_score") or 0))
            if row.get("direction") == "bullish":
                bucket["direction"] = "bullish"
        return sorted(grouped.values(), key=lambda item: float(item.get("max_score") or 0), reverse=True)

    @staticmethod
    def _text_cell(value: object, max_len: int = 64) -> str:
        text = str(value or "").replace("\r", " ").replace("\n", " ").replace("|", "｜").strip()
        text = " ".join(text.split())
        if len(text) > max_len:
            return text[: max_len - 1] + "…"
        return text

    @staticmethod
    def _topic_memory_section(topic_memory: dict, limit: int) -> list[str]:
        lines = ["六、主题演化追踪", ""]
        topics = topic_memory.get("topics") or []
        if not topics:
            return lines + ["暂无主题记忆。", ""]
        for row in topics[:limit]:
            sectors = ", ".join(row.get("sectors") or [])
            lines.append(
                f"• {row.get('topic', '')}：热度 {float(row.get('heat') or 0):.1f}，"
                f"动量 {float(row.get('momentum') or 0):+.1f}，趋势 {row.get('trend', '') or '未知'}，"
                f"累计洞察 {int(row.get('total_insight_count') or 0)}；关联板块：{sectors}。"
            )
        lines.append("")
        return lines

    @staticmethod
    def _llm_reviews_section(reviews: list[dict]) -> list[str]:
        status = llm_settings_status()
        lines = [
            "七、LLM 语义评审",
            "",
            f"状态：enabled={status.get('enabled')}，model={status.get('model') or '未配置'}，api_key_present={status.get('api_key_present')}",
            "",
        ]
        if not reviews:
            return lines + ["暂无 LLM 评审记录；可运行 python -m modules.cli tool event llm-review --json 预览，或加 --send 调用外部模型。", ""]
        for row in reviews:
            lines.append(
                f"• {EventReporter._text_cell(row.get('title'), 42)}：主题 {row.get('topic', '')}，"
                f"规则分 {float(row.get('value_score') or 0):.1f}，LLM分 {float(row.get('llm_score') or 0):.1f}，"
                f"决策 {row.get('decision', '')}；理由：{EventReporter._text_cell(row.get('rationale'), 64)}。"
            )
        lines.append("")
        return lines


def generate_event_report(limit: int = 10) -> str:
    """Convenience entry point for scheduler/CLI."""
    return EventReporter().report(limit=limit)
