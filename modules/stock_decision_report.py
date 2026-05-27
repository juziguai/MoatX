"""Single-stock decision report built from scoring, quote, event, and risk data."""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
from datetime import datetime
from typing import Any

from modules.datasource import QuoteManager
from modules.utils import normalize_symbol


class StockDecisionReporter:
    """Build a human-readable decision report for one stock."""

    def __init__(
        self,
        *,
        scoring_engine: Any | None = None,
        event_driver: Any | None = None,
        quote_manager: Any | None = None,
        announcement_scanner: Any | None = None,
    ):
        self._scoring_engine = scoring_engine
        self._event_driver = event_driver
        self._quote_manager = quote_manager
        self._announcement_scanner = announcement_scanner

    def build(self, symbol: str) -> dict[str, Any]:
        """Return a structured single-stock decision report."""
        code = normalize_symbol(symbol)
        quote, quote_warning = self._quote(code)
        score, score_warning = self._score(code)
        quote = self._enrich_quote_from_scoring_cache(code, quote)
        risk, risk_warning = self._risk(code, score)
        announcement_risk, announcement_warning = self._announcement_risk(code)
        risk = self._merge_risks(risk, announcement_risk)
        event, event_warning = self._event(code)
        data_warnings = [
            item
            for item in (quote_warning, score_warning, risk_warning, announcement_warning, event_warning)
            if item
        ]
        data_quality = self._data_quality(
            quote=quote,
            score=score,
            risk=risk,
            announcement_risk=announcement_risk,
            event=event,
            warnings=data_warnings,
        )

        decision = self._decision(score, risk, event, quote, data_quality)
        name = str(quote.get("name") or code)
        report = {
            "engine": "stock_decision_report_v1",
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "symbol": code,
            "name": name,
            "summary": decision["summary"],
            "new_position": decision["new_position"],
            "holding": decision["holding"],
            "risk_level": decision["risk_level"],
            "score": score,
            "quote": quote,
            "event": event,
            "risk": risk,
            "announcement_risk": announcement_risk,
            "data_quality": data_quality,
            "confidence": data_quality["confidence"],
            "key_points": decision["key_points"],
            "data_warnings": data_warnings,
        }
        report["markdown"] = self.format_markdown(report)
        return report

    def report(self, symbol: str) -> str:
        """Return a Markdown report for one stock."""
        return self.build(symbol)["markdown"]

    def _score(self, symbol: str) -> tuple[dict[str, Any], str]:
        try:
            engine = self._engine()
            score_obj = engine.score_single(symbol)
            score = asdict(score_obj) if is_dataclass(score_obj) else dict(score_obj)
            for key in ("total", "quality", "timing", "sentiment", "event_multiplier", "suggested_weight"):
                score[key] = round(float(score.get(key) or 0.0), 2)
            score["action"] = str(score.get("action") or "no_buy")
            score["vetoed"] = bool(score.get("vetoed", False))
            score["veto_reason"] = str(score.get("veto_reason") or "")
            return score, ""
        except Exception as exc:
            return {
                "symbol": symbol,
                "total": 0.0,
                "quality": 0.0,
                "timing": 0.0,
                "sentiment": 0.0,
                "event_multiplier": 1.0,
                "action": "no_buy",
                "suggested_weight": 0.0,
                "vetoed": False,
                "veto_reason": "",
                "reasons": [],
            }, f"score_unavailable: {exc}"

    def _risk(self, symbol: str, score: dict[str, Any]) -> tuple[dict[str, Any], str]:
        try:
            engine = self._engine()
            if hasattr(engine, "_get_risk_cached"):
                risk = dict(engine._get_risk_cached(symbol))
            else:
                risk = {}
            if not risk:
                return {}, "risk_unavailable"
            risk.setdefault("risk_score", 0)
            risk.setdefault("risk_level", "")
            risk.setdefault("red_flags", [])
            risk.setdefault("warnings", [])
            risk.setdefault("is_buyable", True)
            return risk, ""
        except Exception as exc:
            if score.get("veto_reason"):
                return {
                    "risk_score": 30,
                    "risk_level": "system_veto",
                    "red_flags": [score["veto_reason"]],
                    "warnings": [],
                    "is_buyable": False,
                }, ""
            return {}, f"risk_unavailable: {exc}"

    def _event(self, symbol: str) -> tuple[dict[str, Any], str]:
        try:
            driver = self._event_driver
            if driver is None:
                from modules.event_driver import EventDriver

                driver = EventDriver()
                self._event_driver = driver
            return dict(driver.explain_single(symbol)), ""
        except Exception as exc:
            return {"symbol": symbol, "boost": 0.0, "matched_factors": [], "announcement_score": 0.0, "reason": ""}, (
                f"event_unavailable: {exc}"
            )

    def _announcement_risk(self, symbol: str) -> tuple[dict[str, Any], str]:
        try:
            scanner = self._announcement_scanner
            if scanner is None:
                from modules.announcement_risk import AnnouncementRiskScanner

                scanner = AnnouncementRiskScanner()
                self._announcement_scanner = scanner
            return dict(scanner.scan(symbol)), ""
        except Exception as exc:
            return {
                "symbol": symbol,
                "source": "cninfo",
                "risk_score": 0,
                "risk_level": "未知",
                "is_buyable": True,
                "sentiment_score": 0,
                "red_flags": [],
                "positive_flags": [],
                "notices": [],
            }, f"announcement_risk_unavailable: {exc}"

    def _quote(self, symbol: str) -> tuple[dict[str, Any], str]:
        try:
            manager = self._quote_manager or QuoteManager(source_names=["sina"], mode="single")
            quotes = manager.fetch_quotes([symbol])
            for key, value in quotes.items():
                if normalize_symbol(str(key)) == symbol:
                    quote = dict(value)
                    quote["code"] = symbol
                    return quote, ""
            return {"code": symbol}, "quote_empty"
        except Exception as exc:
            return {"code": symbol}, f"quote_unavailable: {exc}"

    def _engine(self) -> Any:
        if self._scoring_engine is None:
            from modules.scoring_engine import ScoringEngine

            self._scoring_engine = ScoringEngine()
        return self._scoring_engine

    def _enrich_quote_from_scoring_cache(self, symbol: str, quote: dict[str, Any]) -> dict[str, Any]:
        try:
            engine = self._engine()
            if not hasattr(engine, "_get_spot_cached"):
                return quote
            spot = engine._get_spot_cached()
            if spot is None or getattr(spot, "empty", True) or "code" not in spot.columns:
                return quote
            row = spot[spot["code"].astype(str).eq(symbol)]
            if row.empty:
                return quote
            out = dict(quote)
            latest = row.iloc[0]
            for key in ("name", "price", "pct_change", "pe", "pb", "turnover"):
                current = out.get(key)
                cached = latest.get(key)
                if _missing_quote_value(current) and not _missing_quote_value(cached):
                    out[key] = cached
            return out
        except Exception:
            return quote

    @staticmethod
    def _merge_risks(risk: dict[str, Any], announcement_risk: dict[str, Any]) -> dict[str, Any]:
        financial_score = int(float(risk.get("risk_score") or 0))
        announcement_score = int(float(announcement_risk.get("risk_score") or 0))
        score = max(financial_score, announcement_score)
        red_flags = list(risk.get("red_flags") or []) + list(announcement_risk.get("red_flags") or [])
        warnings = list(risk.get("warnings") or [])
        if announcement_risk.get("positive_flags"):
            warnings.extend(f"正面公告：{item}" for item in announcement_risk.get("positive_flags") or [])
        return {
            **risk,
            "risk_score": score,
            "risk_level": _risk_level(score),
            "is_buyable": bool(risk.get("is_buyable", True)) and bool(announcement_risk.get("is_buyable", True)),
            "red_flags": red_flags,
            "warnings": warnings,
            "components": {
                "financial": risk,
                "announcement": announcement_risk,
            },
        }

    @staticmethod
    def _data_quality(
        *,
        quote: dict[str, Any],
        score: dict[str, Any],
        risk: dict[str, Any],
        announcement_risk: dict[str, Any],
        event: dict[str, Any],
        warnings: list[str],
    ) -> dict[str, Any]:
        checks = [
            ("quote", 20, not _missing_quote_value(quote.get("price")) and bool(quote.get("name"))),
            ("valuation", 15, not _missing_quote_value(quote.get("pe")) or not _missing_quote_value(quote.get("pb"))),
            ("score", 20, {"total", "quality", "timing", "sentiment"}.issubset(score.keys())),
            ("risk", 15, bool(risk)),
            ("announcement", 15, announcement_risk.get("source") == "cninfo" and "notices" in announcement_risk),
            ("event", 15, bool(event)),
        ]
        components = []
        total = 0
        for name, weight, ok in checks:
            components.append({"name": name, "weight": weight, "ok": bool(ok)})
            if ok:
                total += weight
        penalty = min(20, len(warnings) * 5)
        score_value = max(0, min(100, total - penalty))
        if score_value >= 80:
            confidence = "high"
        elif score_value >= 60:
            confidence = "medium"
        else:
            confidence = "low"
        return {
            "score": score_value,
            "confidence": confidence,
            "components": components,
            "warnings": warnings,
        }

    @classmethod
    def _decision(
        cls,
        score: dict[str, Any],
        risk: dict[str, Any],
        event: dict[str, Any],
        quote: dict[str, Any],
        data_quality: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        total = float(score.get("total") or 0.0)
        action = str(score.get("action") or "no_buy")
        risk_score = int(float(risk.get("risk_score") or 0))
        event_boost = float(event.get("boost") or 0.0)
        vetoed = bool(score.get("vetoed")) or risk_score >= 30 or not bool(risk.get("is_buyable", True))
        confidence = str((data_quality or {}).get("confidence") or "medium")
        pct_change = float(quote.get("change_pct") or quote.get("pct_change") or 0.0)
        strong_watch = action == "watch" and (
            pct_change >= 5.0
            or float(score.get("sentiment") or 0.0) >= 8.0
            or any("主力" in str(reason) for reason in score.get("reasons") or [])
        )

        key_points = cls._key_points(score, risk, event, quote)
        if vetoed:
            summary = "如果是新开仓，我不建议现在买；如果已经持有，也更偏向先处理风险暴露、设置止损，而不是加仓追。"
            new_position = "不建议新开仓"
            holding = "控制仓位，等待风险释放；走弱时优先执行止损纪律"
            risk_level = "high"
        elif action in {"no_buy", "watch"} or total < 41:
            if strong_watch:
                summary = "如果是新开仓，先不追高，等回踩或承接确认；如果已经持有，可以继续观察趋势确认，但要设好止盈/止损。"
                new_position = "不追高，等回踩/确认"
                holding = "持有观察，设好止盈/止损；放量承接不足先减风险"
            elif event_boost < 0:
                summary = "如果是新开仓，我不建议现在买；如果已经持有，也更偏向观察事件风险释放和设止损，而不是加仓追。"
                new_position = "不建议新开仓"
                holding = "观察为主，不加仓；跌破纪律位先控制回撤"
            else:
                summary = "如果是新开仓，我不建议现在买；如果已经持有，也更偏向观察走势确认和设止损，而不是加仓追。"
                new_position = "不建议新开仓"
                holding = "观察为主，不加仓；跌破纪律位先控制回撤"
            risk_level = "medium"
        elif action == "probe":
            summary = "如果是新开仓，只适合小仓试探；如果已经持有，可以继续观察确认，但不适合追高加仓。"
            new_position = "仅小仓试探"
            holding = "持有观察，等待量价和事件进一步确认"
            risk_level = "medium"
        elif action in {"normal", "heavy", "max_heavy"}:
            if confidence == "low":
                summary = "如果是新开仓，先不要按强信号处理；如果已经持有，可继续观察，但要等关键数据补齐后再决定是否加仓。"
                new_position = "数据不足，暂不按强信号开仓"
                holding = "持有观察，等待关键数据补齐"
                risk_level = "medium"
            else:
                summary = "如果是新开仓，可以按系统仓位分批参与；如果已经持有，可继续持有，但仍要跟踪风险和止损位。"
                new_position = "可按系统仓位分批参与"
                holding = "可继续持有，按风险纪律管理仓位"
                risk_level = "low"
        else:
            summary = "如果是新开仓，先观察；如果已经持有，按风险纪律管理仓位。"
            new_position = "观察"
            holding = "按风险纪律管理"
            risk_level = "medium"

        return {
            "summary": summary,
            "new_position": new_position,
            "holding": holding,
            "risk_level": risk_level,
            "key_points": key_points,
        }

    @staticmethod
    def _key_points(
        score: dict[str, Any],
        risk: dict[str, Any],
        event: dict[str, Any],
        quote: dict[str, Any],
    ) -> list[str]:
        points: list[str] = []
        total = float(score.get("total") or 0.0)
        action = str(score.get("action") or "no_buy")
        points.append(f"系统评分 {total:.1f}，动作 {action}，建议仓位 {float(score.get('suggested_weight') or 0):.0%}")

        risk_score = int(float(risk.get("risk_score") or 0))
        if risk_score >= 30:
            points.append(f"风险评分 {risk_score}，已进入买入否决/谨慎区间")
        elif risk:
            points.append(f"风险评分 {risk_score}，未触发硬否决")
        red_flags = list(risk.get("red_flags") or [])
        if red_flags:
            points.append(f"关键风险：{red_flags[0]}")

        event_boost = float(event.get("boost") or 0.0)
        if event_boost < 0:
            points.append(f"事件/公告情绪为负，事件加分 {event_boost:.1f}")
        elif event_boost > 0:
            points.append(f"事件/公告情绪为正，事件加分 +{event_boost:.1f}")

        pe = _as_float(quote.get("pe"))
        pb = _as_float(quote.get("pb"))
        turnover = _as_float(quote.get("turnover"))
        if pe is not None and pe > 100:
            points.append(f"PE {pe:.1f} 偏高，估值容错率低")
        if pb is not None and pb > 4:
            points.append(f"PB {pb:.2f} 偏高")
        if turnover is not None and turnover > 20:
            points.append(f"换手率 {turnover:.2f}% 偏高，短线博弈属性强")

        reasons = score.get("reasons") or []
        if reasons:
            points.append("技术/基本面提示：" + " / ".join(str(item) for item in reasons[:4]))
        return points[:6]

    @staticmethod
    def format_markdown(report: dict[str, Any]) -> str:
        quote = report.get("quote") or {}
        score = report.get("score") or {}
        event = report.get("event") or {}
        risk = report.get("risk") or {}
        data_quality = report.get("data_quality") or {}
        risk_label = {"low": "低", "medium": "中", "high": "高"}.get(str(report.get("risk_level")), "中")
        confidence_label = {"high": "高", "medium": "中", "low": "低"}.get(str(report.get("confidence")), "中")
        lines = [
            f"MoatX 单股综合报告 | {report['symbol']} {report['name']}",
            "",
            f"一句话：{report['summary']}",
            "",
            "核心数据：",
            f"- 数据质量：{data_quality.get('score', 'N/A')} / 100，结论置信度：{confidence_label}",
            f"- 评分：{float(score.get('total') or 0):.1f}，动作：{score.get('action', 'no_buy')}，建议仓位：{float(score.get('suggested_weight') or 0):.0%}",
            f"- 分项：质量 {float(score.get('quality') or 0):.1f} / 择时 {float(score.get('timing') or 0):.1f} / 情绪 {float(score.get('sentiment') or 0):.1f} / 事件乘数 {float(score.get('event_multiplier') or 1):.2f}",
            f"- 行情：现价 {quote.get('price', 'N/A')}，涨跌幅 {quote.get('change_pct', 'N/A')}%，PE {quote.get('pe', 'N/A')}，PB {quote.get('pb', 'N/A')}，换手 {quote.get('turnover', 'N/A')}%",
            f"- 综合风险：{risk_label}；财务/公告风险：{risk.get('risk_score', 'N/A')} 分，{risk.get('risk_level', '未知')}",
            f"- 事件：boost {float(event.get('boost') or 0):+.1f}，{event.get('reason') or '未命中明显事件'}",
            "",
            "操作口径：",
            f"- 新开仓：{report['new_position']}",
            f"- 已持有：{report['holding']}",
            "",
            "依据：",
        ]
        for item in report.get("key_points") or []:
            lines.append(f"- {item}")
        flags = list(risk.get("red_flags") or []) + list(risk.get("warnings") or [])
        if flags:
            lines.append("")
            lines.append("风险提示：")
            for item in flags[:5]:
                lines.append(f"- {item}")
        if report.get("data_warnings"):
            lines.append("")
            lines.append("数据降级：")
            for item in report["data_warnings"]:
                lines.append(f"- {item}")
        lines.append("")
        lines.append("注：本报告为系统信号汇总，不构成投资建议。")
        return "\n".join(lines)


def _as_float(value: Any) -> float | None:
    try:
        if value in (None, "", "-"):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _missing_quote_value(value: Any) -> bool:
    if value in (None, "", "-", "N/A"):
        return True
    try:
        numeric = float(value)
        return numeric == 0.0 or numeric != numeric
    except (TypeError, ValueError):
        return False


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
