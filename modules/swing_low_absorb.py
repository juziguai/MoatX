"""Low-absorb next-day swing scanner.

The model looks for main-board stocks that pulled back into a compact
MA10/MA20/MA30 support zone after a recent impulse move. It is a short-term
volatility setup, not a long-term buy rating.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
import json
import math
import logging
import time
from pathlib import Path
from typing import Any

import pandas as pd

from modules.announcement_risk import AnnouncementRiskScanner
from modules.config import tomllib
from modules.market_filters import filter_selection_universe, is_excluded_selection_board
from modules.utils import normalize_symbol, to_full_code


_logger = logging.getLogger("moatx.swing_low_absorb")

_DATA_DIR = Path(__file__).resolve().parent.parent / "data"
_WATCHLIST_DIR = _DATA_DIR / "swing_watchlists"
_WATCHLIST_LATEST = _DATA_DIR / "swing_watchlist_latest.json"
_WATCHLIST_ALERT_STATE = _DATA_DIR / "swing_watchlist_alert_state.json"


@dataclass
class LowAbsorbSwingPlan:
    symbol: str
    name: str
    trade_date: str
    score: float
    action: str
    setup: str
    plan: dict[str, Any]
    reasons: list[str]
    warnings: list[str]
    metrics: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class LowAbsorbSwingEngine:
    """Score low-absorb candidates for next-day morning spike exits."""

    def __init__(
        self,
        *,
        stock_data: Any | None = None,
        announcement_scanner: Any | None = None,
        sector_provider: Any | None = None,
        enable_sector_context: bool = True,
    ):
        if stock_data is None:
            from modules.stock_data import StockData

            stock_data = StockData()
        self._sd = stock_data
        self._announcement_scanner = announcement_scanner or AnnouncementRiskScanner()
        self._sector_provider = sector_provider
        self._enable_sector_context = enable_sector_context
        self._last_candidates_meta: dict[str, Any] = {}
        self._event_snapshot_cache: dict[str, tuple[str | None, list[dict[str, Any]]]] = {}

    def analyze(
        self,
        symbol: str,
        *,
        name: str = "",
        daily: pd.DataFrame | None = None,
        quote: dict[str, Any] | None = None,
        check_risk: bool = True,
        market_context: dict[str, Any] | None = None,
        sector_context: dict[str, Any] | None = None,
        event_context: dict[str, Any] | None = None,
        allow_breakout: bool = True,
    ) -> dict[str, Any]:
        code = normalize_symbol(symbol)
        if is_excluded_selection_board(code):
            return self._empty_plan(code, name=name, warning="创业板/科创板已按系统规则过滤")

        if daily is None:
            try:
                daily = self._sd.get_daily(code)
            except Exception as exc:
                return self._empty_plan(code, name=name, warning=f"日线不可用: {exc}")

        daily, quote_bar_appended = self._append_quote_bar(daily, quote)
        df = self._prepare_daily(daily)
        if len(df) < 35:
            return self._empty_plan(code, name=name, warning="日线不足 35 天")

        latest = df.iloc[-1]
        prev = df.iloc[-2]
        close = float(latest["close"])
        open_price = float(latest["open"])
        high = float(latest["high"])
        low = float(latest["low"])
        prev_close = float(prev["close"])
        if close <= 0 or open_price <= 0 or prev_close <= 0:
            return self._empty_plan(code, name=name, warning="价格数据无效")

        ma10 = float(df["close"].rolling(10).mean().iloc[-1])
        ma20 = float(df["close"].rolling(20).mean().iloc[-1])
        ma30 = float(df["close"].rolling(30).mean().iloc[-1])
        pct_change = (close / prev_close - 1.0) * 100
        day_range_pct = (high - low) / prev_close * 100 if prev_close else 0.0
        close_position = (close - low) / (high - low) if high > low else 0.5
        nearest_ma = min((ma10, ma20, ma30), key=lambda value: abs(close - value))
        nearest_ma_gap_pct = abs(close - nearest_ma) / close * 100
        ma10_ma20_gap_pct = abs(ma10 - ma20) / close * 100
        ma_spread_pct = (max(ma10, ma20, ma30) - min(ma10, ma20, ma30)) / close * 100
        volume = float(latest.get("volume") or 0.0)
        amount = float(latest.get("amount") or close * volume)
        turnover = float(latest.get("turn") or latest.get("turnover") or 0.0)
        recent = df.iloc[-21:-1].copy()
        recent_peak_volume = float(recent["volume"].max()) if not recent.empty else 0.0
        volume_to_peak = volume / recent_peak_volume if recent_peak_volume > 0 else 1.0
        recent_pct = recent["close"].pct_change() * 100
        prior_impulse_pct = float(recent_pct.max()) if not recent_pct.empty else 0.0

        score = 0.0
        reasons: list[str] = []
        warnings: list[str] = []
        if quote_bar_appended:
            warnings.append("日线末端使用实时行情临时补齐")

        if ma10 > ma20 > ma30:
            score += 14
            reasons.append("MA10/20/30 多头排列")
        elif ma10 > ma20 and ma20 >= ma30 * 0.995:
            score += 8
            reasons.append("均线接近多头")
        else:
            warnings.append("均线未形成低吸趋势底座")

        if ma_spread_pct <= 3:
            score += 6
            reasons.append("三线间距紧凑")
        elif ma_spread_pct <= 5:
            score += 3
        elif ma_spread_pct > 8:
            score -= 6
            warnings.append("三线间距偏大，低吸支撑区不够紧凑")

        if ma10_ma20_gap_pct > 3:
            score -= 4
            warnings.append("MA10 距 MA20 超过 3%，不符合紧凑均线口径")

        if nearest_ma_gap_pct <= 1.5:
            score += 7
            reasons.append("收盘贴近关键均线")
        elif nearest_ma_gap_pct <= 3:
            score += 4

        if close < open_price:
            score += 8
            reasons.append("尾盘阴线低吸形态")
        elif pct_change <= 1.0:
            score += 4
            reasons.append("小阳不追高")
        else:
            score -= 10
            warnings.append("涨幅超过 1%，不符合不追涨口径")

        if -4.0 <= pct_change <= 1.0:
            score += 6
            reasons.append("回调幅度可控")
        elif pct_change < -6.0:
            score -= 8
            warnings.append("跌幅过深，可能不是低吸而是破位")

        if close_position >= 0.45:
            score += 5
            reasons.append("收盘脱离日内低点")
        elif close_position < 0.25:
            score -= 5
            warnings.append("收盘靠近全天低点，承接偏弱")

        if prior_impulse_pct >= 7.0:
            score += 7
            reasons.append("前期有放量强势上攻")
        elif prior_impulse_pct >= 4.0:
            score += 4
            reasons.append("前期有上攻痕迹")

        recent_strength = float(recent_pct.tail(5).max()) if not recent_pct.empty else 0.0
        if (
            -3.8 <= pct_change <= 0.8
            and ma10 > ma20 > ma30
            and close >= ma10
            and recent_strength >= 4.0
            and close_position >= 0.4
        ):
            score += 7
            reasons.append("强势回踩守住MA10")

        if volume_to_peak <= 0.5:
            score += 9
            reasons.append("缩量回调至峰值 50% 以下")
        elif volume_to_peak <= 0.7:
            score += 5
            reasons.append("缩量回调")
        else:
            warnings.append("回调未明显缩量")

        if amount >= 200_000_000:
            score += 5
            reasons.append("成交额满足短线流动性")
        elif amount >= 50_000_000:
            score += 3
        else:
            score -= 10
            warnings.append("成交额不足，隔日兑现难度高")

        if 1.5 <= day_range_pct <= 6.0:
            score += 5
            reasons.append("日内振幅具备隔日差价空间")
        elif day_range_pct > 8.0:
            warnings.append("日内波动过大，止损纪律要更紧")

        market_adjust = self._market_score(market_context, reasons=reasons, warnings=warnings)
        score += market_adjust
        sector_adjust, sector_metrics = self._sector_score(code, sector_context, reasons=reasons, warnings=warnings)
        score += sector_adjust
        event_adjust, event_metrics = self._event_score(event_context, reasons=reasons, warnings=warnings)
        score += event_adjust

        risk_warnings: list[str] = []
        risk_adjust = self._risk_score(code, check_risk=check_risk, warnings=risk_warnings)
        low_absorb_score = score + risk_adjust
        low_absorb_warnings = warnings + risk_warnings
        setup = "低吸隔日冲高"
        exit_rule = "次日10:00前冲高分批兑现；跌破止损或冲高回落破分时均线则退出"
        profiles = []
        trend_profile = self._trend_continuation_profile(
            df=df,
            close=close,
            ma10=ma10,
            ma20=ma20,
            ma30=ma30,
            pct_change=pct_change,
            day_range_pct=day_range_pct,
            close_position=close_position,
            amount=amount,
            market_context=market_context,
            sector_metrics=sector_metrics,
            sector_adjust=sector_adjust,
            event_metrics=event_metrics,
            event_adjust=event_adjust,
            risk_adjust=risk_adjust,
            quote_bar_appended=quote_bar_appended,
        )
        if trend_profile:
            profiles.append(trend_profile)
        if allow_breakout:
            breakout_profile = self._breakout_ignition_profile(
                df=df,
                close=close,
                ma10=ma10,
                ma20=ma20,
                ma30=ma30,
                pct_change=pct_change,
                day_range_pct=day_range_pct,
                close_position=close_position,
                amount=amount,
                market_context=market_context,
                sector_metrics=sector_metrics,
                sector_adjust=sector_adjust,
                event_metrics=event_metrics,
                event_adjust=event_adjust,
                risk_adjust=risk_adjust,
                quote_bar_appended=quote_bar_appended,
            )
            if breakout_profile:
                profiles.append(breakout_profile)
        best_profile = max(profiles, key=lambda item: float(item["score"])) if profiles else None
        if best_profile and best_profile["score"] > low_absorb_score and best_profile["score"] >= 50:
            score = float(best_profile["score"])
            reasons = list(best_profile["reasons"])
            warnings = list(best_profile["warnings"]) + risk_warnings
            setup = str(best_profile.get("setup") or "强趋势延续观察")
            exit_rule = str(best_profile.get("exit_rule") or "趋势票只做回踩承接或盘中换手确认；跌破MA10或放量长阴退出")
        else:
            score = low_absorb_score
            warnings = low_absorb_warnings
        support = min(low, nearest_ma)
        raw_stop_loss = support * 0.99
        max_stop_loss = close * 0.98
        stop_loss = round(max(raw_stop_loss, max_stop_loss), 2)
        stop_loss_pct = (stop_loss / close - 1.0) * 100
        if raw_stop_loss < max_stop_loss:
            warnings.append("原始支撑止损超过 2%，已按短线纪律收紧")
        entry_low = round(max(stop_loss * 1.01, close * 0.995), 2)
        entry_high = round(close * 1.003, 2)
        target_1 = round(close * 1.015, 2)
        target_2 = round(close * 1.03, 2)

        historical_reference = self._historical_reference(
            df,
            setup=setup,
            current_close=close,
            target_1=target_1,
            stop_loss=stop_loss,
        )
        history_adjust = float(historical_reference.get("score_adjust") or 0.0)
        sample_count = int(historical_reference.get("sample_count") or 0)
        if sample_count >= 3:
            score += history_adjust
            summary = (
                f"历史相似{sample_count}次：目标命中{historical_reference.get('target_hit_rate_pct', 0):.0f}%，"
                f"均值{historical_reference.get('avg_next_return_pct', 0):+.2f}%"
            )
            if history_adjust < 0:
                warnings.insert(0, f"{summary}，小幅降权")
            else:
                reasons.insert(0, f"{summary}，小幅加权" if history_adjust > 0 else summary)
        elif sample_count > 0:
            warnings.append(f"历史相似样本仅{sample_count}次，暂不校准分数")
        else:
            warnings.append("暂无足够历史相似样本，暂不校准分数")

        setup_risk_adjust = self._setup_history_risk_adjust(setup, historical_reference)
        if setup_risk_adjust:
            score += setup_risk_adjust
            warnings.insert(0, f"历史同形态风险偏高，强趋势/突破降权{setup_risk_adjust:+.1f}")

        metrics_preview = {
            "close": close,
            "pct_change": pct_change,
            "ma10": ma10,
            "ma20": ma20,
            "ma_spread_pct": ma_spread_pct,
            "ma10_ma20_gap_pct": ma10_ma20_gap_pct,
            "day_range_pct": day_range_pct,
            "close_position": close_position,
            "event_adjust": event_adjust,
            "best_sector_pct": sector_metrics.get("best_sector_pct", 0.0),
            "historical_reference": historical_reference,
        }
        whipsaw_risk = self._trend_intraday_whipsaw_risk(setup, metrics_preview, warnings)
        if whipsaw_risk:
            score -= 8.0
            warnings.insert(0, "强趋势/突破日内振幅和乖离偏高，隔日冲高歧义风险降权")

        if setup.startswith("强趋势"):
            has_event_confirm = float(event_metrics.get("event_adjust") or 0.0) > 0.0
            best_sector_pct = float(sector_metrics.get("best_sector_pct") or 0.0)
            has_sector_confirm = best_sector_pct >= 0.5
            if not has_event_confirm and not has_sector_confirm:
                score -= 8.0
                warnings.append("强趋势缺少新闻/板块确认，降级观察等待盘口确认")

        attribution_gate = self._attribution_risk_gate(
            setup=setup,
            metrics=metrics_preview,
            historical_reference=historical_reference,
            warnings=warnings,
        )
        gate_adjust = float(attribution_gate.get("score_adjust") or 0.0)
        if gate_adjust:
            score += gate_adjust
        for note in reversed(attribution_gate.get("warnings") or []):
            warnings.insert(0, str(note))

        score = round(max(0.0, min(100.0, score)), 1)
        action = self._action(score, warnings)
        gate_decision = str(attribution_gate.get("decision") or "")
        if gate_decision == "skip":
            action = "skip"
        elif action == "candidate" and gate_decision == "watch":
            action = "watch"
        if action == "candidate" and self._should_downgrade_trend_setup(setup, historical_reference, warnings):
            action = "watch"
            warnings.insert(0, "强趋势/突破历史胜率不足，降级为观察，不作为优先建仓")
        if action == "candidate" and whipsaw_risk:
            action = "watch"
            warnings.insert(0, "隔日可能冲高后快速回落，降级观察等待分时承接确认")
        if market_context and market_context.get("state") == "severe" and action == "candidate":
            action = "watch"
            warnings.append("大盘宽度严重偏弱，候选降级为观察")

        trade_date = self._trade_date(df)
        return LowAbsorbSwingPlan(
            symbol=code,
            name=name or code,
            trade_date=trade_date,
            score=score,
            action=action,
            setup=setup,
            plan={
                "entry_low": entry_low,
                "entry_high": entry_high,
                "target_1": target_1,
                "target_2": target_2,
                "stop_loss": stop_loss,
                "exit_rule": exit_rule,
            },
            reasons=reasons[:8],
            warnings=warnings[:8],
            metrics={
                "close": round(close, 3),
                "pct_change": round(pct_change, 3),
                "ma10": round(ma10, 3),
                "ma20": round(ma20, 3),
                "ma30": round(ma30, 3),
                "nearest_ma_gap_pct": round(nearest_ma_gap_pct, 3),
                "ma10_ma20_gap_pct": round(ma10_ma20_gap_pct, 3),
                "ma_spread_pct": round(ma_spread_pct, 3),
                "volume_to_peak": round(volume_to_peak, 3),
                "prior_impulse_pct": round(prior_impulse_pct, 3),
                "day_range_pct": round(day_range_pct, 3),
                "close_position": round(close_position, 3),
                "amount": round(amount, 2),
                "turnover": round(turnover, 3),
                "stop_loss_pct": round(stop_loss_pct, 3),
                "market_state": (market_context or {}).get("state", ""),
                "market_adjust": round(market_adjust, 3),
                "historical_reference": historical_reference,
                **event_metrics,
                **sector_metrics,
            },
        ).to_dict()

    @staticmethod
    def _setup_history_risk_adjust(setup: str, historical_reference: dict[str, Any]) -> float:
        """Apply stricter history feedback for trend-chasing setups."""
        if not (str(setup).startswith("强趋势") or str(setup).startswith("放量突破")):
            return 0.0
        sample_count = int(historical_reference.get("sample_count") or 0)
        if sample_count < 4:
            return 0.0
        avg_return = float(historical_reference.get("avg_next_return_pct") or 0.0)
        stop_hit_rate = float(historical_reference.get("stop_hit_rate_pct") or 0.0)
        win_rate = float(historical_reference.get("win_rate_pct") or 0.0)
        if avg_return <= -0.8 or stop_hit_rate >= 65.0:
            return -8.0
        if avg_return <= 0.0 and stop_hit_rate >= 45.0:
            return -8.0
        if avg_return <= 0.2 and stop_hit_rate >= 40.0 and win_rate <= 60.0:
            return -6.0
        if avg_return < 0.0 and (stop_hit_rate >= 55.0 or win_rate < 45.0):
            return -5.0
        return 0.0

    @staticmethod
    def _should_downgrade_trend_setup(
        setup: str,
        historical_reference: dict[str, Any],
        warnings: list[str],
    ) -> bool:
        if not (str(setup).startswith("强趋势") or str(setup).startswith("放量突破")):
            return False
        sample_count = int(historical_reference.get("sample_count") or 0)
        if sample_count < 4:
            return False
        avg_return = float(historical_reference.get("avg_next_return_pct") or 0.0)
        stop_hit_rate = float(historical_reference.get("stop_hit_rate_pct") or 0.0)
        win_rate = float(historical_reference.get("win_rate_pct") or 0.0)
        target_hit_rate = float(historical_reference.get("target_hit_rate_pct") or 0.0)
        if avg_return < 0 and stop_hit_rate >= 50.0:
            return True
        if avg_return <= 0.0 and stop_hit_rate >= 45.0:
            return True
        if avg_return <= 0.2 and stop_hit_rate >= 40.0 and target_hit_rate <= 60.0:
            return True
        if win_rate < 45.0 and stop_hit_rate >= 45.0:
            return True
        return any("追高风险高" in str(item) and "历史相似" in " ".join(warnings) for item in warnings)

    @staticmethod
    def _trend_intraday_whipsaw_risk(setup: str, metrics: dict[str, Any], warnings: list[str]) -> bool:
        if not str(setup).startswith(("强趋势", "放量突破")):
            return False
        text = "；".join(str(item) for item in warnings)
        close = float(metrics.get("close") or 0.0)
        ma10 = float(metrics.get("ma10") or 0.0)
        ma20 = float(metrics.get("ma20") or 0.0)
        gap_ma10 = (close / ma10 - 1.0) * 100 if close > 0 and ma10 > 0 else 0.0
        gap_ma20 = (close / ma20 - 1.0) * 100 if close > 0 and ma20 > 0 else 0.0
        day_range = float(metrics.get("day_range_pct") or 0.0)
        heat_flags = 0
        if day_range >= 8.0 or "日内振幅偏大" in text:
            heat_flags += 1
        if gap_ma10 >= 18.0 or "偏离MA10" in text:
            heat_flags += 1
        if gap_ma20 >= 28.0 or "趋势过热" in text:
            heat_flags += 1
        if "涨幅已超过低吸口径" in text:
            heat_flags += 1
        return heat_flags >= 3 or (str(setup).startswith("放量突破") and day_range >= 7.0)

    @staticmethod
    def _attribution_risk_gate(
        *,
        setup: str,
        metrics: dict[str, Any],
        historical_reference: dict[str, Any],
        warnings: list[str],
    ) -> dict[str, Any]:
        text = "；".join(str(item) for item in warnings)
        setup_text = str(setup or "")
        sample_count = int(historical_reference.get("sample_count") or 0)
        avg_return = float(historical_reference.get("avg_next_return_pct") or 0.0)
        stop_hit_rate = float(historical_reference.get("stop_hit_rate_pct") or 0.0)
        both_hit_rate = float(historical_reference.get("both_hit_rate_pct") or 0.0)
        target_hit_rate = float(historical_reference.get("target_hit_rate_pct") or 0.0)
        day_range = float(metrics.get("day_range_pct") or 0.0)
        ma_spread = float(metrics.get("ma_spread_pct") or 0.0)
        ma10_ma20_gap = float(metrics.get("ma10_ma20_gap_pct") or 0.0)
        event_adjust = float(metrics.get("event_adjust") or 0.0)
        best_sector_pct = float(metrics.get("best_sector_pct") or 0.0)

        score_adjust = 0.0
        notes: list[str] = []
        decision = ""
        risk_points = 0

        if sample_count >= 3 and avg_return <= 0.0 and stop_hit_rate >= 45.0:
            risk_points += 3
            score_adjust -= 8.0
            notes.append("归因风控：历史相似负收益且止损率偏高，强降权")
        if sample_count >= 3 and both_hit_rate >= 35.0:
            risk_points += 2
            score_adjust -= 6.0
            notes.append("归因风控：历史同日目标/止损双触发偏多，降低隔日交易优先级")
        if target_hit_rate < 55.0 and stop_hit_rate >= 50.0 and sample_count >= 5:
            risk_points += 2
            score_adjust -= 5.0
            notes.append("归因风控：目标命中不足且止损高，候选需更强确认")
        if "三线间距偏大" in text or ma_spread > 8.0 or ma10_ma20_gap > 3.0:
            risk_points += 1
            score_adjust -= 3.0
            notes.append("归因风控：均线支撑不紧凑，低吸承接质量降权")
        if "追高风险高" in text or "趋势过热" in text or day_range >= 8.0:
            risk_points += 1
            score_adjust -= 4.0
            notes.append("归因风控：追高/高振幅信号命中亏损归因，降权")
        if setup_text.startswith(("强趋势", "放量突破")) and "涨幅已超过低吸口径" in text:
            risk_points += 1
            score_adjust -= 3.0
            notes.append("归因风控：趋势票涨幅已脱离低吸口径，要求更强确认")
        if setup_text.startswith("低吸"):
            if "均线未形成低吸趋势底座" in text:
                risk_points += 2
                score_adjust -= 8.0
                notes.append("归因风控：低吸缺少趋势底座，剔除优先级提高")
            if "收盘靠近全天低点" in text:
                risk_points += 1
                score_adjust -= 4.0
                notes.append("归因风控：收盘贴近日低，承接不足")
            if "原始支撑止损超过 2%" in text and (
                "收盘靠近全天低点" in text or "均线未形成低吸趋势底座" in text
            ):
                risk_points += 1
                score_adjust -= 3.0
                notes.append("归因风控：低位承接弱且止损距离被压缩，剔除优先级提高")
        if setup_text.startswith(("强趋势", "放量突破")) and event_adjust <= 0.0 and best_sector_pct < 0.5:
            risk_points += 2
            score_adjust -= 6.0
            notes.append("归因风控：趋势票缺少新闻和板块确认，降级观察")
        if setup_text.startswith("放量突破") and ("接近涨停追高区" in text or day_range >= 7.0):
            risk_points += 2
            score_adjust -= 8.0
            notes.append("归因风控：突破追高结构止损贡献高，默认压制")

        if risk_points >= 3:
            decision = "skip"
            notes.insert(0, "归因风控：风险信号叠加，剔除")
        elif risk_points >= 2:
            decision = "watch"
            notes.insert(0, "归因风控：风险信号叠加，降级观察")

        return {
            "decision": decision,
            "score_adjust": score_adjust,
            "risk_points": risk_points,
            "warnings": notes,
        }

    def candidates(
        self,
        *,
        limit: int = 20,
        pool_limit: int = 80,
        check_risk: bool = False,
        workers: int = 8,
        deadline_seconds: float | None = None,
        network_daily_fallback: bool = True,
        allow_breakout: bool = True,
    ) -> list[dict[str, Any]]:
        started = time.monotonic()
        deadline = started + float(deadline_seconds) if deadline_seconds and deadline_seconds > 0 else None
        self._last_candidates_meta = {
            "pool_limit": int(pool_limit or 0),
            "deadline_seconds": float(deadline_seconds or 0.0),
            "network_daily_fallback": bool(network_daily_fallback),
            "review_count": 0,
            "submitted_count": 0,
            "scanned_count": 0,
            "daily_cache_hits": 0,
            "daily_cache_misses": 0,
            "skipped_uncached": 0,
            "skipped_deadline": 0,
            "deadline_hit": False,
            "elapsed_seconds": 0.0,
        }
        spot = self._spot_snapshot()
        if spot is None or spot.empty:
            return []

        market_df = filter_selection_universe(spot.copy(), code_col="code")
        market_context = self._market_context(market_df)
        df = market_df.copy()
        if "price" in df.columns:
            df = df[pd.to_numeric(df["price"], errors="coerce") > 0]
        if "pct_change" in df.columns:
            pct = pd.to_numeric(df["pct_change"], errors="coerce")
            df = df[(pct >= -5.5) & (pct <= 10.5)]
        if "amount" in df.columns:
            df = df[pd.to_numeric(df["amount"], errors="coerce") >= 50_000_000]

        pool_codes = [normalize_symbol(str(code)) for code in df.get("code", [])]
        sector_context = self._sector_context(pool_codes, spot=market_df)
        event_context = self._event_context(pool_codes, sector_context=sector_context)
        self._last_candidates_meta["event_context_count"] = len(event_context)
        df = self._rank_prefilter_pool(df, sector_context, event_context)
        review_df = self._select_prefilter_pool(df, pool_limit)
        candidates = []
        for _, row in review_df.iterrows():
            code = normalize_symbol(str(row.get("code") or ""))
            if not code:
                continue
            candidates.append((code, str(row.get("name") or code)))

        self._last_candidates_meta["review_count"] = len(candidates)
        quotes = self._quote_snapshot([code for code, _ in candidates])
        daily_cache = self._daily_cache_snapshot([code for code, _ in candidates])
        work_items: list[tuple[str, str, pd.DataFrame | None]] = []
        for code, name in candidates:
            daily = daily_cache.get(code)
            if daily is not None and not daily.empty:
                self._last_candidates_meta["daily_cache_hits"] += 1
                work_items.append((code, name, daily))
            else:
                self._last_candidates_meta["daily_cache_misses"] += 1
                if network_daily_fallback:
                    work_items.append((code, name, None))
                else:
                    self._last_candidates_meta["skipped_uncached"] += 1

        rows: list[dict[str, Any]] = []
        max_workers = max(1, min(int(workers or 1), 8, len(work_items) or 1))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            for index, (code, name, daily) in enumerate(work_items):
                if deadline is not None and time.monotonic() >= deadline:
                    self._last_candidates_meta["deadline_hit"] = True
                    self._last_candidates_meta["skipped_deadline"] = len(work_items) - index
                    break
                future = executor.submit(
                    self.analyze,
                    code,
                    name=name,
                    daily=daily,
                    quote=quotes.get(code),
                    check_risk=check_risk,
                    market_context=market_context,
                    sector_context=sector_context,
                    event_context=event_context.get(code),
                    allow_breakout=allow_breakout,
                )
                futures[future] = code

            self._last_candidates_meta["submitted_count"] = len(futures)
            for future in as_completed(futures):
                try:
                    plan = future.result()
                except Exception as exc:
                    _logger.warning("low-absorb scan failed for %s: %s", futures[future], exc)
                    continue
                self._last_candidates_meta["scanned_count"] += 1
                if plan["action"] != "skip":
                    rows.append(plan)

        rows.sort(key=self._candidate_sort_key, reverse=True)
        self._last_candidates_meta["elapsed_seconds"] = round(time.monotonic() - started, 3)
        return rows[:limit]

    def _apply_comprehensive_score_gate(
        self,
        rows: list[dict[str, Any]],
        *,
        min_total: float = 35.0,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """Re-check swing candidates with the cross-module scoring/risk engine."""
        meta = {
            "enabled": True,
            "status": "skipped_empty",
            "min_total": float(min_total),
            "input_count": len(rows),
            "scored_count": 0,
            "passed_count": 0,
            "downgraded_count": 0,
            "failed_count": 0,
            "vetoed_count": 0,
            "unmatched_count": 0,
        }
        if not rows:
            return rows, meta

        try:
            from modules.scoring_engine import ScoringEngine

            spot_by_code = self._spot_by_code(self._spot_snapshot())
            records: list[dict[str, Any]] = []
            for row in rows:
                code = normalize_symbol(str(row.get("symbol") or row.get("code") or ""))
                if not code:
                    continue
                metrics = row.get("metrics") or {}
                spot_row = spot_by_code.loc[code] if not spot_by_code.empty and code in spot_by_code.index else {}

                def pick(metric_key: str, spot_key: str, default: Any = 0.0) -> Any:
                    value = metrics.get(metric_key)
                    if value not in (None, ""):
                        return value
                    if hasattr(spot_row, "get"):
                        value = spot_row.get(spot_key)
                        if value not in (None, ""):
                            return value
                    return default

                records.append(
                    {
                        "code": code,
                        "name": row.get("name") or pick("name", "name", code),
                        "price": pick("close", "price", 0.0),
                        "pct_change": pick("pct_change", "pct_change", 0.0),
                        "pe": pick("pe", "pe", 0.0),
                        "pb": pick("pb", "pb", 0.0),
                        "turnover": pick("turnover", "turnover", 0.0),
                        "amount": pick("amount", "amount", 0.0),
                    }
                )

            if not records:
                meta["status"] = "no_valid_symbols"
                return rows, meta

            score_df = ScoringEngine().score_batch(pd.DataFrame(records))
            score_by_code: dict[str, dict[str, Any]] = {}
            if score_df is not None and not score_df.empty:
                for _, score_row in score_df.iterrows():
                    code = normalize_symbol(str(score_row.get("code") or ""))
                    if code:
                        score_by_code[code] = dict(score_row)

            out: list[dict[str, Any]] = []
            for row in rows:
                code = normalize_symbol(str(row.get("symbol") or row.get("code") or ""))
                next_row = dict(row)
                next_row["metrics"] = dict(row.get("metrics") or {})
                next_row["reasons"] = list(row.get("reasons") or [])
                next_row["warnings"] = list(row.get("warnings") or [])
                score_row = score_by_code.get(code)
                if not score_row:
                    meta["unmatched_count"] += 1
                    next_row["metrics"]["score_gate"] = {
                        "decision": "unmatched",
                        "reason": "综合打分未返回该股票，保留短线候选原始结果",
                    }
                    next_row["warnings"].insert(0, "综合打分未返回该股票，暂按短线模型保留")
                    out.append(next_row)
                    continue

                gate = self._score_gate_decision(score_row, min_total=min_total)
                next_row["metrics"]["score_gate"] = gate
                meta["scored_count"] += 1
                decision = str(gate.get("decision") or "")
                if decision == "vetoed":
                    meta["vetoed_count"] += 1
                elif decision == "failed":
                    meta["failed_count"] += 1
                elif decision == "downgraded":
                    meta["downgraded_count"] += 1
                else:
                    meta["passed_count"] += 1

                score_adjust = float(gate.get("score_adjust") or 0.0)
                if score_adjust:
                    next_row["score"] = round(max(0.0, min(100.0, float(next_row.get("score") or 0.0) + score_adjust)), 1)

                total = float(gate.get("total") or 0.0)
                action_label = self._score_gate_action_label(str(gate.get("action") or ""))
                multiplier = float(gate.get("event_multiplier") or 1.0)
                if decision == "vetoed":
                    next_row["action"] = "skip"
                    next_row["warnings"].insert(0, f"综合风控否决：{gate.get('veto_reason') or '风险分层不可买'}")
                elif decision == "failed":
                    next_row["action"] = "skip"
                    next_row["warnings"].insert(0, f"综合打分未过门槛：总分{total:.1f}，低于{float(min_total):.1f}")
                elif decision == "downgraded":
                    if next_row.get("action") == "candidate":
                        next_row["action"] = "watch"
                    next_row["warnings"].insert(0, f"综合打分偏弱：总分{total:.1f}，系统动作{action_label}，候选降级观察")
                else:
                    next_row["reasons"].insert(
                        0,
                        f"综合打分确认：总分{total:.1f}，系统动作{action_label}，事件乘数{multiplier:.2f}",
                    )
                out.append(next_row)

            meta["status"] = "ok"
            return out, meta
        except Exception as exc:
            _logger.warning("comprehensive score gate unavailable: %s", exc)
            meta["status"] = "unavailable"
            meta["error"] = str(exc)
            return rows, meta

    @staticmethod
    def _score_gate_decision(score_row: dict[str, Any], *, min_total: float) -> dict[str, Any]:
        total = float(score_row.get("total") or 0.0)
        vetoed = bool(score_row.get("vetoed", False))
        action = str(score_row.get("action") or "")
        gate = {
            "decision": "passed",
            "score_adjust": 0.0,
            "total": round(total, 1),
            "quality": round(float(score_row.get("quality") or 0.0), 1),
            "timing": round(float(score_row.get("timing") or 0.0), 1),
            "sentiment": round(float(score_row.get("sentiment") or 0.0), 1),
            "event_multiplier": round(float(score_row.get("event_multiplier") or 1.0), 3),
            "action": action,
            "suggested_weight": round(float(score_row.get("suggested_weight") or 0.0), 3),
            "vetoed": vetoed,
            "veto_reason": str(score_row.get("veto_reason") or ""),
            "quality_reasons": str(score_row.get("quality_reasons") or ""),
        }
        if vetoed:
            gate["decision"] = "vetoed"
            gate["score_adjust"] = -100.0
        elif total < float(min_total):
            gate["decision"] = "failed"
            gate["score_adjust"] = -18.0
        elif total < 35.0:
            gate["decision"] = "downgraded"
            gate["score_adjust"] = -4.0
        elif total < 45.0:
            gate["decision"] = "downgraded"
            gate["score_adjust"] = -2.0
        elif total >= 86.0:
            gate["score_adjust"] = 7.0
        elif total >= 71.0:
            gate["score_adjust"] = 5.0
        elif total >= 56.0:
            gate["score_adjust"] = 3.0
        return gate

    @staticmethod
    def _score_gate_action_label(action: str) -> str:
        labels = {
            "no_buy": "不买",
            "watch": "观察",
            "probe": "试探",
            "normal": "正常仓位",
            "heavy": "加重仓位",
            "max_heavy": "最高仓位",
        }
        return labels.get(str(action or ""), str(action or "未知"))

    @staticmethod
    def _candidate_sort_key(row: dict[str, Any]) -> tuple[float, float, float]:
        action_rank = {"candidate": 2.0, "watch": 1.0}.get(str(row.get("action") or ""), 0.0)
        setup = str(row.get("setup") or "")
        setup_rank = 1.0 if setup.startswith("低吸") else 0.5 if setup.startswith("强趋势") else 0.25
        adjusted_score = float(row.get("score") or 0.0) - LowAbsorbSwingEngine._trend_heat_penalty(row)
        return (action_rank, adjusted_score, setup_rank)

    @staticmethod
    def _trend_heat_penalty(row: dict[str, Any]) -> float:
        setup = str(row.get("setup") or "")
        if not setup.startswith(("强趋势", "放量突破")):
            return 0.0
        metrics = row.get("metrics") or {}
        warnings_text = "；".join(str(item) for item in (row.get("warnings") or []))
        close = float(metrics.get("close") or 0.0)
        ma10 = float(metrics.get("ma10") or 0.0)
        ma20 = float(metrics.get("ma20") or 0.0)
        day_range = float(metrics.get("day_range_pct") or 0.0)
        gap_ma10 = (close / ma10 - 1.0) * 100 if close > 0 and ma10 > 0 else 0.0
        gap_ma20 = (close / ma20 - 1.0) * 100 if close > 0 and ma20 > 0 else 0.0
        historical = metrics.get("historical_reference") or {}

        penalty = 0.0
        if day_range >= 8.0 or "日内振幅偏大" in warnings_text:
            penalty += 5.0
        if gap_ma10 >= 18.0 or "偏离MA10" in warnings_text:
            penalty += 5.0
        if gap_ma20 >= 28.0 or "趋势过热" in warnings_text:
            penalty += 6.0
        if "涨幅已超过低吸口径" in warnings_text:
            penalty += 2.0
        if "新闻催化确认" in warnings_text and "确认+0.0" in warnings_text:
            penalty += 2.0
        if (
            int(historical.get("sample_count") or 0) >= 4
            and float(historical.get("avg_next_return_pct") or 0.0) <= 0.0
            and float(historical.get("stop_hit_rate_pct") or 0.0) >= 40.0
        ):
            penalty += 4.0
        return min(24.0, penalty)

    def build_paper_account(
        self,
        rows: list[dict[str, Any]],
        *,
        cash_per_stock: float = 10_000.0,
        lot_size: int = 100,
    ) -> dict[str, Any]:
        """Build an equal-cash paper account from swing candidate rows."""
        positions: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        created_at = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        for row in rows:
            metrics = row.get("metrics") or {}
            plan = row.get("plan") or {}
            close = float(metrics.get("close") or 0.0)
            if close <= 0:
                skipped.append({"symbol": row.get("symbol", ""), "reason": "missing close price"})
                continue
            quantity = int(math.floor(float(cash_per_stock) / close / lot_size) * lot_size)
            if quantity <= 0:
                skipped.append(
                    {
                        "symbol": row.get("symbol", ""),
                        "name": row.get("name", ""),
                        "close": close,
                        "reason": f"cash_per_stock below one lot cost {close * lot_size:.2f}",
                    }
                )
                continue
            cost = round(quantity * close, 2)
            target_1 = float(plan.get("target_1") or close * 1.015)
            target_2 = float(plan.get("target_2") or close * 1.03)
            stop_loss = float(plan.get("stop_loss") or close * 0.98)
            positions.append(
                {
                    "symbol": row.get("symbol", ""),
                    "name": row.get("name", ""),
                    "score": row.get("score", 0.0),
                    "action": row.get("action", ""),
                    "buy_date": row.get("trade_date", ""),
                    "buy_price": round(close, 3),
                    "quantity": quantity,
                    "cost": cost,
                    "target_sell_price": round(target_1, 3),
                    "target_2_price": round(target_2, 3),
                    "stop_loss": round(stop_loss, 3),
                    "target_profit": round((target_1 - close) * quantity, 2),
                    "target_return_pct": round((target_1 / close - 1.0) * 100, 2),
                    "stop_loss_risk": round((stop_loss - close) * quantity, 2),
                    "stop_loss_return_pct": round((stop_loss / close - 1.0) * 100, 2),
                    "reasons": row.get("reasons", [])[:6],
                    "warnings": row.get("warnings", [])[:6],
                }
            )

        deployed_cash = round(sum(item["cost"] for item in positions), 2)
        planned_cash = round(float(cash_per_stock) * len(rows), 2)
        return {
            "account_id": f"swing_low_absorb_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "account_name": "低吸隔日冲高等金额虚拟账号",
            "created_at": created_at,
            "strategy": "低吸隔日冲高",
            "cash_per_stock": float(cash_per_stock),
            "lot_size": lot_size,
            "planned_cash": planned_cash,
            "deployed_cash": deployed_cash,
            "cash_remaining": round(planned_cash - deployed_cash, 2),
            "positions": positions,
            "skipped": skipped,
            "status": "open",
        }

    def generate_watchlist(
        self,
        *,
        limit: int = 10,
        pool_limit: int = 120,
        workers: int = 4,
        min_score: float = 55.0,
        cash_per_stock: float = 10_000.0,
        lot_size: int = 100,
        check_risk: bool = False,
        include_watch: bool = True,
        deadline_seconds: float | None = 180.0,
        network_daily_fallback: bool = False,
        score_gate: bool = True,
        min_comprehensive_score: float = 20.0,
        allow_breakout: bool = True,
        output: str | Path | None = None,
        save_latest: bool = True,
    ) -> dict[str, Any]:
        """Generate tomorrow's next-day-spike watchlist from today's close."""
        rows = self.candidates(
            limit=max(int(limit or 1) * 3, int(limit or 1)),
            pool_limit=pool_limit,
            check_risk=check_risk,
            workers=workers,
            deadline_seconds=deadline_seconds,
            network_daily_fallback=network_daily_fallback,
            allow_breakout=allow_breakout,
        )
        scan_meta = dict(self._last_candidates_meta)
        allowed_actions = {"candidate", "watch"} if include_watch else {"candidate"}
        short_list = [
            row
            for row in rows
            if row.get("action") in allowed_actions and float(row.get("score") or 0.0) >= float(min_score)
        ]
        gate_meta: dict[str, Any] = {
            "enabled": bool(score_gate),
            "min_total": float(min_comprehensive_score),
            "input_count": len(short_list),
        }
        if score_gate:
            short_list, gate_meta = self._apply_comprehensive_score_gate(
                short_list,
                min_total=min_comprehensive_score,
            )
            short_list.sort(key=self._candidate_sort_key, reverse=True)
        selected = [
            row
            for row in short_list
            if row.get("action") in allowed_actions and float(row.get("score") or 0.0) >= float(min_score)
        ][: max(1, int(limit or 1))]
        account = self.build_paper_account(selected, cash_per_stock=cash_per_stock, lot_size=lot_size)
        generated_at = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        payload = {
            "watchlist_id": f"swing_watchlist_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "generated_at": generated_at,
            "trade_date": datetime.now().strftime("%Y-%m-%d"),
            "strategy": "低吸隔日冲高/强趋势延续",
            "summary": {
                "candidate_count": len(selected),
                "source_count": len(rows),
                "min_score": float(min_score),
                "include_watch": bool(include_watch),
                "cash_per_stock": float(cash_per_stock),
                "scan": scan_meta,
                "score_gate": gate_meta,
            },
            "positions": account.get("positions", []),
            "skipped": account.get("skipped", []),
            "raw_candidates": selected,
            "status": "active",
            "review_plan": {
                "entry": "next trading day intraday pullback/volume confirmation",
                "exit": "target_1/target_2/stop_loss",
                "monitor_fields": ["target_sell_price", "target_2_price", "stop_loss"],
            },
        }
        if save_latest:
            self._write_watchlist_payload(payload, _WATCHLIST_LATEST)
            dated_path = _WATCHLIST_DIR / f"{datetime.now().strftime('%Y%m%d')}.json"
            self._write_watchlist_payload(payload, dated_path)
        if output:
            self._write_watchlist_payload(payload, Path(output))
        return payload

    def monitor_watchlist(
        self,
        *,
        watchlist_path: str | Path | None = None,
        market_hours_only: bool = True,
    ) -> dict[str, Any]:
        """Check the latest swing watchlist against realtime prices."""
        now = datetime.now()
        if market_hours_only and not self._is_cn_market_session(now):
            return {
                "checked_at": now.strftime("%Y-%m-%dT%H:%M:%S"),
                "status": "outside_market_hours",
                "alerts": [],
            }

        path = Path(watchlist_path) if watchlist_path else _WATCHLIST_LATEST
        if not path.exists():
            return {
                "checked_at": now.strftime("%Y-%m-%dT%H:%M:%S"),
                "status": "no_watchlist",
                "watchlist_path": str(path),
                "alerts": [],
            }
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            return {
                "checked_at": now.strftime("%Y-%m-%dT%H:%M:%S"),
                "status": "watchlist_read_failed",
                "watchlist_path": str(path),
                "error": str(exc),
                "alerts": [],
            }

        positions = payload.get("positions") or []
        symbols = [str(row.get("symbol") or "") for row in positions if row.get("symbol")]
        quotes = self._quote_snapshot(symbols)
        state = self._load_watchlist_alert_state()
        alerts: list[dict[str, Any]] = []
        for row in positions:
            symbol = normalize_symbol(str(row.get("symbol") or ""))
            quote = quotes.get(symbol)
            if not quote:
                continue
            price = float(quote.get("price") or 0.0)
            if price <= 0:
                continue
            target_1 = float(row.get("target_sell_price") or 0.0)
            target_2 = float(row.get("target_2_price") or 0.0)
            stop_loss = float(row.get("stop_loss") or 0.0)
            checks = [
                ("target_2", target_2, price >= target_2 > 0, "达到第二目标，优先落袋"),
                ("target_1", target_1, price >= target_1 > 0, "达到第一目标，可分批兑现"),
                ("near_target_1", target_1, price >= target_1 * 0.995 > 0, "接近第一目标，盯盘兑现"),
                ("stop_loss", stop_loss, price <= stop_loss < target_1 if stop_loss > 0 else False, "跌破止损，执行纪律"),
            ]
            for alert_type, trigger_price, triggered, message in checks:
                if not triggered:
                    continue
                alert_key = f"{payload.get('watchlist_id')}:{symbol}:{alert_type}"
                if state.get(alert_key):
                    continue
                state[alert_key] = now.strftime("%Y-%m-%dT%H:%M:%S")
                alerts.append(
                    {
                        "type": alert_type,
                        "symbol": symbol,
                        "name": row.get("name", ""),
                        "price": round(price, 3),
                        "trigger_price": round(trigger_price, 3),
                        "target_1": round(target_1, 3),
                        "target_2": round(target_2, 3),
                        "stop_loss": round(stop_loss, 3),
                        "message": message,
                        "score": row.get("score", 0.0),
                        "action": row.get("action", ""),
                    }
                )
                if alert_type in {"target_2", "target_1", "stop_loss"}:
                    break

        if alerts:
            self._write_watchlist_alert_state(state)
        return {
            "checked_at": now.strftime("%Y-%m-%dT%H:%M:%S"),
            "status": "ok",
            "watchlist_path": str(path),
            "watchlist_id": payload.get("watchlist_id", ""),
            "trade_date": payload.get("trade_date", ""),
            "position_count": len(positions),
            "alert_count": len(alerts),
            "alerts": alerts,
        }

    @staticmethod
    def _write_watchlist_payload(payload: dict[str, Any], path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    @staticmethod
    def _load_watchlist_alert_state() -> dict[str, str]:
        if not _WATCHLIST_ALERT_STATE.exists():
            return {}
        try:
            return json.loads(_WATCHLIST_ALERT_STATE.read_text(encoding="utf-8"))
        except Exception:
            return {}

    @staticmethod
    def _write_watchlist_alert_state(state: dict[str, str]) -> None:
        _WATCHLIST_ALERT_STATE.parent.mkdir(parents=True, exist_ok=True)
        _WATCHLIST_ALERT_STATE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")

    @staticmethod
    def _is_cn_market_session(now: datetime) -> bool:
        if now.weekday() >= 5:
            return False
        current = now.hour * 60 + now.minute
        return (9 * 60 + 25 <= current <= 11 * 60 + 35) or (13 * 60 <= current <= 15 * 60 + 5)

    def backtest(
        self,
        *,
        start_date: str,
        end_date: str | None = None,
        symbols: list[str] | None = None,
        universe_limit: int = 300,
        pool_limit: int = 80,
        top_n: int = 5,
        min_score: float = 55.0,
        cash_per_trade: float = 10_000.0,
        initial_capital: float = 100_000.0,
        lot_size: int = 100,
        workers: int = 4,
        lookback_days: int = 160,
        check_risk: bool = False,
        include_watch: bool = True,
        slippage_pct: float = 0.001,
        intraday_policy: str = "conservative",
        use_event_context: bool = True,
        allow_breakout: bool = True,
        reentry_cooldown_days: int = 3,
    ) -> dict[str, Any]:
        """Replay the swing scanner day by day and simulate next-day exits."""
        context = self._prepare_backtest_context(
            start_date=start_date,
            end_date=end_date,
            symbols=symbols,
            universe_limit=universe_limit,
            lookback_days=lookback_days,
            workers=workers,
        )
        return self._backtest_with_context(
            context=context,
            pool_limit=pool_limit,
            top_n=top_n,
            min_score=min_score,
            cash_per_trade=cash_per_trade,
            initial_capital=initial_capital,
            lot_size=lot_size,
            check_risk=check_risk,
            include_watch=include_watch,
            slippage_pct=slippage_pct,
            intraday_policy=intraday_policy,
            use_event_context=use_event_context,
            allow_breakout=allow_breakout,
            reentry_cooldown_days=reentry_cooldown_days,
        )

    def backtest_variants(
        self,
        *,
        variants: list[dict[str, Any]],
        start_date: str,
        end_date: str | None = None,
        symbols: list[str] | None = None,
        universe_limit: int = 300,
        pool_limit: int = 80,
        top_n: int = 5,
        min_score: float = 55.0,
        cash_per_trade: float = 10_000.0,
        initial_capital: float = 100_000.0,
        lot_size: int = 100,
        workers: int = 4,
        lookback_days: int = 160,
        check_risk: bool = False,
        slippage_pct: float = 0.001,
        intraday_policy: str = "conservative",
        reentry_cooldown_days: int = 3,
    ) -> dict[str, Any]:
        """Run multiple backtest variants over one shared data load."""
        context = self._prepare_backtest_context(
            start_date=start_date,
            end_date=end_date,
            symbols=symbols,
            universe_limit=universe_limit,
            lookback_days=lookback_days,
            workers=workers,
        )
        results: list[dict[str, Any]] = []
        replay_started = time.monotonic()
        for variant in variants:
            payload = self._backtest_with_context(
                context=context,
                pool_limit=pool_limit,
                top_n=top_n,
                min_score=min_score,
                cash_per_trade=cash_per_trade,
                initial_capital=initial_capital,
                lot_size=lot_size,
                check_risk=check_risk,
                include_watch=bool(variant.get("include_watch", True)),
                slippage_pct=slippage_pct,
                intraday_policy=intraday_policy,
                use_event_context=bool(variant.get("use_event_context", True)),
                allow_breakout=bool(variant.get("allow_breakout", True)),
                reentry_cooldown_days=reentry_cooldown_days,
            )
            results.append(
                {
                    "name": str(variant.get("name") or ""),
                    "variant": dict(variant),
                    "payload": payload,
                }
            )
        return {
            "context": {
                "load_seconds": context.get("load_seconds", 0.0),
                "replay_seconds": round(time.monotonic() - replay_started, 3),
                "requested": len(context.get("requested_symbols") or []),
                "selected": len(context.get("universe") or []),
                "loaded": len(context.get("history") or {}),
                "trading_days": len(context.get("trading_dates") or []),
                "notes": context.get("universe_notes") or [],
            },
            "results": results,
        }

    def _prepare_backtest_context(
        self,
        *,
        start_date: str,
        end_date: str | None,
        symbols: list[str] | None,
        universe_limit: int,
        lookback_days: int,
        workers: int,
    ) -> dict[str, Any]:
        started = time.monotonic()
        start_ts = self._parse_backtest_date(start_date)
        end_ts = self._parse_backtest_date(end_date or datetime.now().strftime("%Y-%m-%d"))
        if end_ts <= start_ts:
            raise ValueError("end_date must be later than start_date")

        load_start = (start_ts - timedelta(days=max(60, int(lookback_days) * 2))).strftime("%Y%m%d")
        today_ts = pd.Timestamp(datetime.now().date())
        load_end_ts = min(end_ts + timedelta(days=10), today_ts)
        load_end = load_end_ts.strftime("%Y%m%d")
        universe, universe_notes = self._backtest_universe(symbols=symbols, universe_limit=universe_limit)
        history = self._load_backtest_daily_history(universe, load_start=load_start, load_end=load_end, workers=workers)
        trading_dates = self._backtest_trading_dates(history, start_ts=start_ts, end_ts=end_ts)
        return {
            "start_ts": start_ts,
            "end_ts": end_ts,
            "requested_symbols": list(symbols or []),
            "universe_limit": int(universe_limit),
            "universe": universe,
            "universe_notes": universe_notes,
            "history": history,
            "trading_dates": trading_dates,
            "load_start": load_start,
            "load_end": load_end,
            "load_seconds": round(time.monotonic() - started, 3),
            "day_cache": {},
            "analysis_cache": {},
        }

    def _backtest_with_context(
        self,
        *,
        context: dict[str, Any],
        pool_limit: int,
        top_n: int,
        min_score: float,
        cash_per_trade: float,
        initial_capital: float,
        lot_size: int,
        check_risk: bool,
        include_watch: bool,
        slippage_pct: float,
        intraday_policy: str,
        use_event_context: bool,
        allow_breakout: bool,
        reentry_cooldown_days: int,
    ) -> dict[str, Any]:
        start_ts = context["start_ts"]
        end_ts = context["end_ts"]
        universe = context.get("universe") or []
        universe_notes = context.get("universe_notes") or []
        history = context.get("history") or {}
        trading_dates = context.get("trading_dates") or []
        trades: list[dict[str, Any]] = []
        daily_rows: list[dict[str, Any]] = []
        skipped_days = 0
        event_snapshot_days = 0
        event_context_hits = 0
        event_snapshot_dates: set[str] = set()
        daily_pool_limit = max(1, int(pool_limit or 1))
        trades_per_day = max(1, int(top_n or 1))
        action_set = {"candidate", "watch"} if include_watch else {"candidate"}
        day_cache: dict[Any, dict[str, Any]] = context.setdefault("day_cache", {})
        analysis_cache: dict[Any, dict[str, Any]] = context.setdefault("analysis_cache", {})
        cooldown_days = max(0, int(reentry_cooldown_days or 0))
        cooldown_left: dict[str, int] = {}
        skipped_reentry_cooldown = 0

        for trade_date in trading_dates:
            day_key = str(pd.Timestamp(trade_date).date())
            cached_day = day_cache.get(day_key)
            if cached_day is None:
                snapshot = self._backtest_snapshot_for_date(history, universe, trade_date)
                if snapshot.empty:
                    day_cache[day_key] = {"empty": True}
                    skipped_days += 1
                    continue
                market_context = self._backtest_market_context(snapshot)
                review_by_limit: dict[int, pd.DataFrame] = {}
                sector_by_limit: dict[int, dict[str, Any]] = {}
                event_by_limit: dict[int, tuple[dict[str, dict[str, Any]], dict[str, Any]]] = {}
                cached_day = {
                    "empty": False,
                    "snapshot": snapshot,
                    "market_context": market_context,
                    "review_by_limit": review_by_limit,
                    "sector_by_limit": sector_by_limit,
                    "event_by_limit": event_by_limit,
                }
                day_cache[day_key] = cached_day
            elif cached_day.get("empty"):
                skipped_days += 1
                continue

            snapshot = cached_day["snapshot"]
            review_by_limit = cached_day.setdefault("review_by_limit", {})
            sector_by_limit = cached_day.setdefault("sector_by_limit", {})
            event_by_limit = cached_day.setdefault("event_by_limit", {})
            review_snapshot = review_by_limit.get(daily_pool_limit)
            if review_snapshot is None:
                review_snapshot = self._backtest_prefilter_snapshot(snapshot, daily_pool_limit)
                review_by_limit[daily_pool_limit] = review_snapshot
            if review_snapshot.empty:
                skipped_days += 1
                continue

            market_context = cached_day.get("market_context") or {}
            sector_context = sector_by_limit.get(daily_pool_limit)
            if use_event_context:
                if sector_context is None:
                    review_codes_for_sector = [
                        normalize_symbol(str(code))
                        for code in review_snapshot.get("code", [])
                        if normalize_symbol(str(code))
                    ]
                    sector_context = self._sector_context(review_codes_for_sector, spot=snapshot)
                    sector_by_limit[daily_pool_limit] = sector_context
                cached_event = event_by_limit.get(daily_pool_limit)
                if cached_event is None:
                    review_codes = [
                        normalize_symbol(str(code))
                        for code in review_snapshot.get("code", [])
                        if normalize_symbol(str(code))
                    ]
                    cached_event = self._historical_event_context(
                        review_codes,
                        trade_date,
                        sector_context=sector_context,
                    )
                    event_by_limit[daily_pool_limit] = cached_event
                event_context, event_meta = cached_event
            else:
                event_context, event_meta = {}, {}
                if sector_context is None:
                    review_codes_for_sector = [
                        normalize_symbol(str(code))
                        for code in review_snapshot.get("code", [])
                        if normalize_symbol(str(code))
                    ]
                    sector_context = self._sector_context(review_codes_for_sector, spot=snapshot)
                    sector_by_limit[daily_pool_limit] = sector_context
            if event_meta.get("snapshot_date"):
                event_snapshot_days += 1
                event_snapshot_dates.add(str(event_meta["snapshot_date"]))
                event_context_hits += len(event_context)
            plans: list[dict[str, Any]] = []
            for _, row in review_snapshot.iterrows():
                code = normalize_symbol(str(row.get("code") or ""))
                if not code:
                    continue
                daily = history.get(code)
                if daily is None or daily.empty:
                    continue
                idx = self._date_index_position(daily, trade_date)
                if idx is None or idx < 34 or idx + 1 >= len(daily):
                    continue
                analysis_key = (
                    day_key,
                    code,
                    bool(check_risk),
                    bool(use_event_context),
                    bool(allow_breakout),
                )
                plan = analysis_cache.get(analysis_key)
                if plan is None:
                    plan = self.analyze(
                        code,
                        name=str(row.get("name") or code),
                        daily=daily.iloc[: idx + 1].copy(),
                        quote=None,
                        check_risk=check_risk,
                        market_context=market_context,
                        sector_context=sector_context,
                        event_context=event_context.get(code),
                        allow_breakout=allow_breakout,
                    )
                    analysis_cache[analysis_key] = plan
                if plan.get("action") not in action_set:
                    continue
                if float(plan.get("score") or 0.0) < float(min_score):
                    continue
                if cooldown_days > 0 and cooldown_left.get(code, 0) > 0:
                    skipped_reentry_cooldown += 1
                    continue
                plans.append(plan)

            plans.sort(key=self._candidate_sort_key, reverse=True)
            selected = plans[:trades_per_day]
            day_trades: list[dict[str, Any]] = []
            for plan in selected:
                code = normalize_symbol(str(plan.get("symbol") or ""))
                daily = history.get(code)
                if daily is None or daily.empty:
                    continue
                idx = self._date_index_position(daily, trade_date)
                if idx is None or idx + 1 >= len(daily):
                    continue
                trade = self._simulate_backtest_trade(
                    plan,
                    next_row=daily.iloc[idx + 1],
                    cash_per_trade=cash_per_trade,
                    lot_size=lot_size,
                    slippage_pct=slippage_pct,
                    intraday_policy=intraday_policy,
                )
                if trade:
                    day_trades.append(trade)
                    trades.append(trade)

            if cooldown_days > 0:
                cooldown_left = {
                    code: left - 1
                    for code, left in cooldown_left.items()
                    if left - 1 > 0
                }
                for trade in day_trades:
                    code = normalize_symbol(str(trade.get("symbol") or ""))
                    if code:
                        cooldown_left[code] = cooldown_days

            if day_trades:
                deployed = sum(float(item["cost"]) for item in day_trades)
                pnl = sum(float(item["pnl"]) for item in day_trades)
                daily_rows.append(
                    {
                        "signal_date": str(trade_date.date()),
                        "exit_date": day_trades[0].get("exit_date", ""),
                        "trade_count": len(day_trades),
                        "deployed_cash": round(deployed, 2),
                        "pnl": round(pnl, 2),
                        "return_pct": round(pnl / deployed * 100, 3) if deployed > 0 else 0.0,
                        "symbols": [item["symbol"] for item in day_trades],
                    }
                )

        summary = self._summarize_backtest(
            trades,
            daily_rows=daily_rows,
            initial_capital=initial_capital,
            start_ts=start_ts,
            end_ts=end_ts,
        )
        return {
            "strategy": "swing_low_absorb_next_day_replay",
            "period": {
                "start": str(start_ts.date()),
                "end": str(end_ts.date()),
                "trading_days": len(trading_dates),
                "signal_days": len(daily_rows),
                "skipped_days": skipped_days,
            },
            "assumptions": {
                "entry": "signal_day_close",
                "exit": "next_trading_day_target_stop_or_close",
                "intraday_policy": intraday_policy,
                "cash_per_trade": float(cash_per_trade),
                "initial_capital": float(initial_capital),
                "lot_size": int(lot_size),
                "min_score": float(min_score),
                "top_n": trades_per_day,
                "include_watch": bool(include_watch),
                "check_risk": bool(check_risk),
                "slippage_pct": float(slippage_pct),
                "event_context": "historical_news_factor_snapshots_when_available" if use_event_context else "disabled",
                "allow_breakout": bool(allow_breakout),
                "reentry_cooldown_days": cooldown_days,
                "skipped_reentry_cooldown": int(skipped_reentry_cooldown),
                "event_snapshot_days": int(event_snapshot_days),
                "event_context_hits": int(event_context_hits),
                "event_snapshot_dates": sorted(event_snapshot_dates)[-10:],
                "point_in_time_note": "uses signal-day-and-prior OHLCV and news factor snapshots dated on or before signal day; current announcement risk is disabled unless requested",
            },
            "universe": {
                "requested": len(context.get("requested_symbols") or []),
                "selected": len(universe),
                "loaded": len(history),
                "limit": int(context.get("universe_limit") or 0),
                "notes": universe_notes,
                "stability_note": "explicit symbols are reproducible; dynamic universe depends on current spot snapshot and available daily cache",
            },
            "summary": summary,
            "setup_stats": self._backtest_setup_stats(trades),
            "daily": daily_rows,
            "trades": trades,
        }

    def _backtest_universe(
        self,
        *,
        symbols: list[str] | None,
        universe_limit: int,
    ) -> tuple[list[dict[str, str]], list[str]]:
        notes: list[str] = []
        limit = max(1, int(universe_limit or 1))
        spot = self._spot_snapshot()
        spot_by_code = self._spot_by_code(spot)

        if symbols:
            seen: set[str] = set()
            rows: list[dict[str, str]] = []
            for raw in symbols:
                code = normalize_symbol(str(raw))
                if not code or code in seen or is_excluded_selection_board(code):
                    continue
                seen.add(code)
                name = code
                if not spot_by_code.empty and code in spot_by_code.index:
                    name = str(spot_by_code.loc[code].get("name") or code)
                rows.append({"code": code, "name": name})
            notes.append("explicit_symbols")
            return rows[:limit], notes

        if spot is None or spot.empty:
            return [], ["spot_unavailable"]

        df = filter_selection_universe(spot.copy(), code_col="code")
        if "code" in df.columns:
            df["code"] = df["code"].map(normalize_symbol)
        if "price" in df.columns:
            df = df[pd.to_numeric(df["price"], errors="coerce") > 0]
        if "amount" in df.columns:
            df = df[pd.to_numeric(df["amount"], errors="coerce") >= 50_000_000]
        ranked = self._rank_prefilter_pool(df, {})
        selected = ranked.head(limit)
        notes.append("current_main_board_liquidity_universe")
        return [
            {"code": normalize_symbol(str(row.get("code") or "")), "name": str(row.get("name") or "")}
            for _, row in selected.iterrows()
            if normalize_symbol(str(row.get("code") or ""))
        ], notes

    def _load_backtest_daily_history(
        self,
        universe: list[dict[str, str]],
        *,
        load_start: str,
        load_end: str,
        workers: int,
    ) -> dict[str, pd.DataFrame]:
        if not universe:
            return {}
        history = self._load_backtest_daily_cache(universe, load_start=load_start, load_end=load_end)
        remaining = [
            row
            for row in universe
            if normalize_symbol(str(row.get("code") or "")) not in history
        ]
        if not remaining:
            return history

        def _load(row: dict[str, str]) -> tuple[str, pd.DataFrame | None]:
            code = normalize_symbol(str(row.get("code") or ""))
            if not code:
                return "", None
            try:
                daily = self._sd.get_daily(code, start_date=load_start, end_date=load_end)
                daily = self._prepare_daily(daily)
                if daily.empty or len(daily) < 36:
                    return code, None
                daily.index = pd.to_datetime(daily.index, errors="coerce").normalize()
                daily = daily[~daily.index.isna()]
                daily = daily[~daily.index.duplicated(keep="last")].sort_index()
                return code, daily
            except Exception as exc:
                _logger.warning("swing backtest daily load failed for %s: %s", code, exc)
                return code, None

        max_workers = max(1, min(int(workers or 1), 6, len(remaining)))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_load, row): row for row in remaining}
            for future in as_completed(futures):
                code, daily = future.result()
                if code and daily is not None and not daily.empty:
                    history[code] = daily

        return history

    def _load_backtest_daily_cache(
        self,
        universe: list[dict[str, str]],
        *,
        load_start: str,
        load_end: str,
    ) -> dict[str, pd.DataFrame]:
        codes = [normalize_symbol(str(row.get("code") or "")) for row in universe]
        codes = [code for code in dict.fromkeys(codes) if code]
        if not codes:
            return {}
        try:
            from modules.config import cfg as _cfg
            from modules.db import DatabaseManager

            settings = _cfg()
            if not settings.data.enable_warehouse:
                return {}

            variant_to_code: dict[str, str] = {}
            variants: list[str] = []
            for code in codes:
                for variant in (code, to_full_code(code)):
                    if variant and variant not in variant_to_code:
                        variant_to_code[variant] = code
                        variants.append(variant)

            start_date = self._backtest_store_date(load_start)
            end_date = self._backtest_store_date(load_end)
            end_ts = pd.Timestamp(end_date)
            cutoff = end_ts - pd.Timedelta(days=10)
            db = DatabaseManager(settings.data.warehouse_path)
            cached = db.price().load_daily_batch(variants, start_date=start_date, end_date=end_date)

            out: dict[str, pd.DataFrame] = {}
            for variant, daily in cached.items():
                code = variant_to_code.get(str(variant), normalize_symbol(str(variant)))
                if not code or daily is None or daily.empty:
                    continue
                prepared = self._prepare_daily(daily)
                if prepared.empty or len(prepared) < 36:
                    continue
                prepared.index = pd.to_datetime(prepared.index, errors="coerce").normalize()
                prepared = prepared[~prepared.index.isna()]
                prepared = prepared[~prepared.index.duplicated(keep="last")].sort_index()
                if prepared.empty or prepared.index.max() < cutoff:
                    continue
                current = out.get(code)
                if current is None or prepared.index.max() > current.index.max():
                    out[code] = prepared
            return out
        except Exception as exc:
            _logger.warning("backtest daily warehouse batch cache unavailable: %s", exc)
            return {}

    @staticmethod
    def _backtest_store_date(value: str | None) -> str | None:
        if not value:
            return None
        ts = pd.to_datetime(str(value).replace("-", ""), format="%Y%m%d", errors="coerce")
        if pd.isna(ts):
            return str(value)
        return str(ts.date())

    @staticmethod
    def _parse_backtest_date(value: str) -> pd.Timestamp:
        ts = pd.to_datetime(str(value), errors="coerce")
        if pd.isna(ts):
            raise ValueError(f"invalid date: {value}")
        return pd.Timestamp(ts).normalize()

    @classmethod
    def _backtest_trading_dates(
        cls,
        history: dict[str, pd.DataFrame],
        *,
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
    ) -> list[pd.Timestamp]:
        dates: set[pd.Timestamp] = set()
        for daily in history.values():
            if daily is None or daily.empty:
                continue
            for idx, trade_date in enumerate(pd.to_datetime(daily.index).normalize()):
                if idx + 1 >= len(daily):
                    continue
                if start_ts <= trade_date <= end_ts:
                    dates.add(pd.Timestamp(trade_date).normalize())
        return sorted(dates)

    @staticmethod
    def _date_index_position(df: pd.DataFrame, trade_date: pd.Timestamp) -> int | None:
        if df is None or df.empty:
            return None
        target = pd.Timestamp(trade_date).normalize()
        try:
            loc = df.index.get_loc(target)
        except KeyError:
            return None
        if isinstance(loc, int):
            return loc
        if isinstance(loc, slice):
            return int(loc.stop - 1)
        try:
            positions = [idx for idx, matched in enumerate(loc) if bool(matched)]
            return positions[-1] if positions else None
        except Exception:
            return None

    def _backtest_snapshot_for_date(
        self,
        history: dict[str, pd.DataFrame],
        universe: list[dict[str, str]],
        trade_date: pd.Timestamp,
    ) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []
        for item in universe:
            code = normalize_symbol(str(item.get("code") or ""))
            daily = history.get(code)
            if daily is None or daily.empty:
                continue
            idx = self._date_index_position(daily, trade_date)
            if idx is None or idx <= 0 or idx + 1 >= len(daily):
                continue
            row = daily.iloc[idx]
            prev = daily.iloc[idx - 1]
            close = float(row.get("close") or 0.0)
            prev_close = float(prev.get("close") or 0.0)
            if close <= 0 or prev_close <= 0:
                continue
            volume = float(row.get("volume") or 0.0)
            amount = float(row.get("amount") or 0.0)
            if amount <= 0 and volume > 0:
                amount = close * volume
            pct_change = row.get("pct_change")
            try:
                pct = float(pct_change)
            except Exception:
                pct = (close / prev_close - 1.0) * 100
            if pd.isna(pct):
                pct = (close / prev_close - 1.0) * 100
            rows.append(
                {
                    "code": code,
                    "name": str(item.get("name") or code),
                    "price": close,
                    "pct_change": pct,
                    "volume": volume,
                    "amount": amount,
                    "turnover": float(row.get("turnover") or row.get("turn") or 0.0),
                }
            )
        return pd.DataFrame(rows)

    def _backtest_prefilter_snapshot(self, snapshot: pd.DataFrame, pool_limit: int) -> pd.DataFrame:
        if snapshot is None or snapshot.empty:
            return pd.DataFrame()
        df = filter_selection_universe(snapshot.copy(), code_col="code")
        if "price" in df.columns:
            df = df[pd.to_numeric(df["price"], errors="coerce") > 0]
        if "pct_change" in df.columns:
            pct = pd.to_numeric(df["pct_change"], errors="coerce")
            df = df[(pct >= -5.5) & (pct <= 10.5)]
        if "amount" in df.columns:
            df = df[pd.to_numeric(df["amount"], errors="coerce") >= 50_000_000]
        ranked = self._rank_prefilter_pool(df, {})
        return self._select_prefilter_pool(ranked, pool_limit)

    @staticmethod
    def _backtest_market_context(snapshot: pd.DataFrame) -> dict[str, Any]:
        if snapshot is None or snapshot.empty or "pct_change" not in snapshot.columns:
            return {}
        pct = pd.to_numeric(snapshot["pct_change"], errors="coerce").dropna()
        if len(pct) < 50:
            return {}
        up = int((pct > 0).sum())
        down = int((pct < 0).sum())
        flat = int((pct == 0).sum())
        total = int(len(pct))
        down_ratio = down / total if total else 0.0
        down_up_ratio = down / max(up, 1)
        median_pct = float(pct.median())
        avg_pct = float(pct.mean())
        if down_ratio >= 0.68 or down_up_ratio >= 2.5 or median_pct <= -1.2:
            state = "severe"
        elif down_ratio >= 0.6 or down_up_ratio >= 1.8 or median_pct <= -0.8:
            state = "weak"
        elif down_ratio <= 0.45 and median_pct >= 0:
            state = "supportive"
        else:
            state = "neutral"
        return {
            "state": state,
            "total": total,
            "up": up,
            "down": down,
            "flat": flat,
            "down_ratio": round(down_ratio, 4),
            "down_up_ratio": round(down_up_ratio, 4),
            "median_pct": round(median_pct, 4),
            "avg_pct": round(avg_pct, 4),
        }

    @staticmethod
    def _simulate_backtest_trade(
        plan: dict[str, Any],
        *,
        next_row: pd.Series,
        cash_per_trade: float,
        lot_size: int,
        slippage_pct: float,
        intraday_policy: str,
    ) -> dict[str, Any] | None:
        from modules.backtest.fees import calc_buy_cost, calc_sell_proceeds

        metrics = plan.get("metrics") or {}
        trade_plan = plan.get("plan") or {}
        buy_price = float(metrics.get("close") or 0.0)
        if buy_price <= 0:
            return None
        buy_exec = buy_price * (1.0 + max(0.0, float(slippage_pct)))
        quantity = int(math.floor(float(cash_per_trade) / buy_exec / int(lot_size or 100)) * int(lot_size or 100))
        if quantity <= 0:
            return None

        next_high = float(next_row.get("high") or 0.0)
        next_low = float(next_row.get("low") or 0.0)
        next_close = float(next_row.get("close") or 0.0)
        if next_high <= 0 or next_low <= 0 or next_close <= 0:
            return None

        target_1 = float(trade_plan.get("target_1") or buy_price * 1.015)
        target_2 = float(trade_plan.get("target_2") or buy_price * 1.03)
        stop_loss = float(trade_plan.get("stop_loss") or buy_price * 0.98)
        target_hit = next_high >= target_1
        stop_hit = next_low <= stop_loss
        policy = str(intraday_policy or "conservative").lower()

        if target_hit and stop_hit:
            if policy in {"target_first", "optimistic"}:
                exit_price = target_1
                exit_reason = "target_1"
            elif policy == "close":
                exit_price = next_close
                exit_reason = "both_hit_close_exit"
            else:
                exit_price = stop_loss
                exit_reason = "both_hit_stop_first"
        elif target_hit:
            exit_price = target_1
            exit_reason = "target_1"
        elif stop_hit:
            exit_price = stop_loss
            exit_reason = "stop_loss"
        else:
            exit_price = next_close
            exit_reason = "next_close"

        sell_exec = exit_price * (1.0 - max(0.0, float(slippage_pct)))
        cost = calc_buy_cost(buy_exec, quantity)
        proceeds = calc_sell_proceeds(sell_exec, quantity)
        pnl = proceeds - cost
        return_pct = pnl / cost * 100 if cost > 0 else 0.0
        exit_date = next_row.name
        if isinstance(exit_date, pd.Timestamp):
            exit_date_text = str(exit_date.date())
        else:
            exit_date_text = str(exit_date)

        return {
            "symbol": plan.get("symbol", ""),
            "name": plan.get("name", ""),
            "setup": plan.get("setup", ""),
            "score": round(float(plan.get("score") or 0.0), 1),
            "action": plan.get("action", ""),
            "signal_date": plan.get("trade_date", ""),
            "exit_date": exit_date_text,
            "buy_price": round(buy_price, 3),
            "buy_exec_price": round(buy_exec, 3),
            "exit_price": round(exit_price, 3),
            "exit_exec_price": round(sell_exec, 3),
            "quantity": quantity,
            "cost": round(cost, 2),
            "proceeds": round(proceeds, 2),
            "pnl": round(pnl, 2),
            "return_pct": round(return_pct, 3),
            "target_1": round(target_1, 3),
            "target_2": round(target_2, 3),
            "stop_loss": round(stop_loss, 3),
            "target_hit": bool(target_hit),
            "stop_hit": bool(stop_hit),
            "exit_reason": exit_reason,
            "historical_reference": metrics.get("historical_reference") or {},
            "reasons": (plan.get("reasons") or [])[:5],
            "warnings": (plan.get("warnings") or [])[:5],
        }

    @classmethod
    def _summarize_backtest(
        cls,
        trades: list[dict[str, Any]],
        *,
        daily_rows: list[dict[str, Any]],
        initial_capital: float,
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
    ) -> dict[str, Any]:
        base = {
            "trade_count": 0,
            "win_rate_pct": 0.0,
            "target_hit_rate_pct": 0.0,
            "stop_hit_rate_pct": 0.0,
            "avg_return_pct": 0.0,
            "median_return_pct": 0.0,
            "total_pnl": 0.0,
            "total_return_on_capital_pct": 0.0,
            "total_return_on_deployed_pct": 0.0,
            "profit_factor": 0.0,
            "max_drawdown_pct": 0.0,
            "max_consecutive_losses": 0,
            "final_value": round(float(initial_capital), 2),
            "best_trade": None,
            "worst_trade": None,
            "monthly": [],
            "equity_curve": [],
        }
        if not trades:
            return base

        returns = pd.Series([float(item.get("return_pct") or 0.0) for item in trades])
        pnls = pd.Series([float(item.get("pnl") or 0.0) for item in trades])
        costs = pd.Series([float(item.get("cost") or 0.0) for item in trades])
        wins = int((pnls > 0).sum())
        losses = int((pnls < 0).sum())
        gross_profit = float(pnls[pnls > 0].sum())
        gross_loss = abs(float(pnls[pnls < 0].sum()))
        total_pnl = float(pnls.sum())
        total_cost = float(costs.sum())
        equity_curve, max_drawdown = cls._backtest_equity_curve(
            daily_rows,
            initial_capital=float(initial_capital),
            start_ts=start_ts,
            end_ts=end_ts,
        )
        best_trade = max(trades, key=lambda item: float(item.get("return_pct") or 0.0))
        worst_trade = min(trades, key=lambda item: float(item.get("return_pct") or 0.0))
        return {
            **base,
            "trade_count": len(trades),
            "win_rate_pct": round(wins / len(trades) * 100, 1),
            "target_hit_rate_pct": round(sum(bool(item.get("target_hit")) for item in trades) / len(trades) * 100, 1),
            "stop_hit_rate_pct": round(sum(bool(item.get("stop_hit")) for item in trades) / len(trades) * 100, 1),
            "avg_return_pct": round(float(returns.mean()), 3),
            "median_return_pct": round(float(returns.median()), 3),
            "total_pnl": round(total_pnl, 2),
            "total_return_on_capital_pct": round(total_pnl / float(initial_capital) * 100, 3)
            if initial_capital > 0
            else 0.0,
            "total_return_on_deployed_pct": round(total_pnl / total_cost * 100, 3) if total_cost > 0 else 0.0,
            "profit_factor": round(gross_profit / gross_loss, 3) if gross_loss > 0 else 0.0,
            "max_drawdown_pct": round(max_drawdown, 3),
            "max_consecutive_losses": cls._max_consecutive_losses(trades),
            "final_value": round(float(initial_capital) + total_pnl, 2),
            "best_trade": cls._compact_trade(best_trade),
            "worst_trade": cls._compact_trade(worst_trade),
            "monthly": cls._backtest_monthly(daily_rows),
            "equity_curve": equity_curve,
        }

    @staticmethod
    def _compact_trade(trade: dict[str, Any]) -> dict[str, Any]:
        return {
            "symbol": trade.get("symbol", ""),
            "name": trade.get("name", ""),
            "signal_date": trade.get("signal_date", ""),
            "exit_date": trade.get("exit_date", ""),
            "setup": trade.get("setup", ""),
            "return_pct": trade.get("return_pct", 0.0),
            "pnl": trade.get("pnl", 0.0),
            "exit_reason": trade.get("exit_reason", ""),
        }

    @staticmethod
    def _max_consecutive_losses(trades: list[dict[str, Any]]) -> int:
        current = 0
        max_seen = 0
        ordered = sorted(trades, key=lambda item: (str(item.get("exit_date") or ""), str(item.get("symbol") or "")))
        for trade in ordered:
            if float(trade.get("pnl") or 0.0) < 0:
                current += 1
                max_seen = max(max_seen, current)
            else:
                current = 0
        return max_seen

    @staticmethod
    def _backtest_equity_curve(
        daily_rows: list[dict[str, Any]],
        *,
        initial_capital: float,
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
    ) -> tuple[list[dict[str, Any]], float]:
        rows = [{"date": str(start_ts.date()), "pnl": 0.0, "total_value": round(initial_capital, 2)}]
        if not daily_rows:
            rows.append({"date": str(end_ts.date()), "pnl": 0.0, "total_value": round(initial_capital, 2)})
            return rows, 0.0
        df = pd.DataFrame(daily_rows)
        df["exit_date"] = pd.to_datetime(df["exit_date"], errors="coerce")
        df = df.dropna(subset=["exit_date"]).sort_values("exit_date")
        grouped = df.groupby("exit_date")["pnl"].sum().reset_index()
        total_value = float(initial_capital)
        peak = total_value
        max_drawdown = 0.0
        for _, row in grouped.iterrows():
            pnl = float(row["pnl"])
            total_value += pnl
            peak = max(peak, total_value)
            drawdown = (peak - total_value) / peak * 100 if peak > 0 else 0.0
            max_drawdown = max(max_drawdown, drawdown)
            rows.append(
                {
                    "date": str(pd.Timestamp(row["exit_date"]).date()),
                    "pnl": round(pnl, 2),
                    "total_value": round(total_value, 2),
                    "drawdown_pct": round(drawdown, 3),
                }
            )
        return rows, max_drawdown

    @staticmethod
    def _backtest_monthly(daily_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not daily_rows:
            return []
        df = pd.DataFrame(daily_rows)
        df["exit_date"] = pd.to_datetime(df["exit_date"], errors="coerce")
        df = df.dropna(subset=["exit_date"])
        if df.empty:
            return []
        df["month"] = df["exit_date"].dt.to_period("M").astype(str)
        rows = []
        for month, group in df.groupby("month"):
            pnl = float(group["pnl"].sum())
            deployed = float(group["deployed_cash"].sum())
            rows.append(
                {
                    "month": month,
                    "trade_days": int(len(group)),
                    "trade_count": int(group["trade_count"].sum()),
                    "pnl": round(pnl, 2),
                    "return_on_deployed_pct": round(pnl / deployed * 100, 3) if deployed > 0 else 0.0,
                }
            )
        return rows

    @staticmethod
    def _backtest_setup_stats(trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not trades:
            return []
        df = pd.DataFrame(trades)
        rows: list[dict[str, Any]] = []
        for setup, group in df.groupby("setup", dropna=False):
            pnls = pd.to_numeric(group["pnl"], errors="coerce").fillna(0.0)
            returns = pd.to_numeric(group["return_pct"], errors="coerce").fillna(0.0)
            costs = pd.to_numeric(group["cost"], errors="coerce").fillna(0.0)
            total_pnl = float(pnls.sum())
            total_cost = float(costs.sum())
            rows.append(
                {
                    "setup": str(setup or ""),
                    "trade_count": int(len(group)),
                    "win_rate_pct": round(float((pnls > 0).sum()) / len(group) * 100, 1),
                    "target_hit_rate_pct": round(float(group["target_hit"].astype(bool).sum()) / len(group) * 100, 1),
                    "stop_hit_rate_pct": round(float(group["stop_hit"].astype(bool).sum()) / len(group) * 100, 1),
                    "avg_return_pct": round(float(returns.mean()), 3),
                    "median_return_pct": round(float(returns.median()), 3),
                    "pnl": round(total_pnl, 2),
                    "return_on_deployed_pct": round(total_pnl / total_cost * 100, 3) if total_cost > 0 else 0.0,
                }
            )
        rows.sort(key=lambda item: (item["pnl"], item["trade_count"]), reverse=True)
        return rows

    @staticmethod
    def _market_context(spot: pd.DataFrame) -> dict[str, Any]:
        if spot is None or spot.empty or "pct_change" not in spot.columns:
            return {}
        df = filter_selection_universe(spot.copy(), code_col="code")
        pct = pd.to_numeric(df["pct_change"], errors="coerce").dropna()
        if len(pct) < 300:
            return {}
        up = int((pct > 0).sum())
        down = int((pct < 0).sum())
        flat = int((pct == 0).sum())
        total = int(len(pct))
        down_ratio = down / total if total else 0.0
        down_up_ratio = down / max(up, 1)
        median_pct = float(pct.median())
        avg_pct = float(pct.mean())
        if down_ratio >= 0.68 or down_up_ratio >= 2.5 or median_pct <= -1.2:
            state = "severe"
        elif down_ratio >= 0.6 or down_up_ratio >= 1.8 or median_pct <= -0.8:
            state = "weak"
        elif down_ratio <= 0.45 and median_pct >= 0:
            state = "supportive"
        else:
            state = "neutral"
        return {
            "state": state,
            "total": total,
            "up": up,
            "down": down,
            "flat": flat,
            "down_ratio": round(down_ratio, 4),
            "down_up_ratio": round(down_up_ratio, 4),
            "median_pct": round(median_pct, 4),
            "avg_pct": round(avg_pct, 4),
        }

    @staticmethod
    def _market_score(
        market_context: dict[str, Any] | None,
        *,
        reasons: list[str],
        warnings: list[str],
    ) -> float:
        if not market_context:
            return 0.0
        state = market_context.get("state")
        down = int(market_context.get("down") or 0)
        up = int(market_context.get("up") or 0)
        median_pct = float(market_context.get("median_pct") or 0.0)
        if state == "severe":
            warnings.append(f"大盘宽度严重偏弱：上涨{up}家/下跌{down}家，中位涨跌幅{median_pct:+.2f}%")
            return -14.0
        if state == "weak":
            warnings.append(f"大盘宽度偏弱：上涨{up}家/下跌{down}家，中位涨跌幅{median_pct:+.2f}%")
            return -8.0
        if state == "supportive":
            reasons.append("大盘宽度支持短线试错")
            return 3.0
        return 0.0

    @staticmethod
    def _rank_prefilter_pool(
        df: pd.DataFrame,
        sector_context: dict[str, Any] | None = None,
        event_context: dict[str, dict[str, Any]] | None = None,
    ) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame() if df is None else df

        ranked = df.copy()
        if "code" in ranked.columns:
            ranked["code"] = ranked["code"].map(normalize_symbol)
        for column in ("amount", "pct_change", "turnover"):
            if column in ranked.columns:
                ranked[column] = pd.to_numeric(ranked[column], errors="coerce").fillna(0.0)

        amount = ranked["amount"] if "amount" in ranked.columns else pd.Series(0.0, index=ranked.index)
        pct = ranked["pct_change"] if "pct_change" in ranked.columns else pd.Series(0.0, index=ranked.index)
        turnover = ranked["turnover"] if "turnover" in ranked.columns else pd.Series(0.0, index=ranked.index)

        amount_log = amount.clip(lower=0.0).map(math.log1p)
        max_amount_log = float(amount_log.max() or 0.0)
        amount_score = amount_log / max_amount_log if max_amount_log > 0 else pd.Series(0.0, index=ranked.index)
        momentum_score = (pct.clip(lower=0.0, upper=10.5) / 10.5).fillna(0.0)
        absorb_band = pct.between(-4.0, 1.2)
        absorb_score = pd.Series(0.0, index=ranked.index)
        absorb_score.loc[absorb_band] = (1.0 - (pct.loc[absorb_band] + 0.8).abs() / 4.4).clip(lower=0.0, upper=1.0)
        setup_score = pd.concat([momentum_score, absorb_score * 0.75], axis=1).max(axis=1)
        turnover_score = (turnover.clip(lower=0.0, upper=15.0) / 15.0).fillna(0.0)
        sector_score = ranked["code"].map(lambda code: LowAbsorbSwingEngine._prefilter_sector_score(str(code), sector_context))
        event_score = ranked["code"].map(lambda code: LowAbsorbSwingEngine._prefilter_event_score(str(code), event_context))

        ranked["_prefilter_score"] = (
            amount_score * 0.48
            + setup_score * 0.25
            + turnover_score * 0.12
            + sector_score * 0.08
            + event_score * 0.07
        )
        sort_columns = ["_prefilter_score"]
        ascending = [False]
        if "amount" in ranked.columns:
            sort_columns.append("amount")
            ascending.append(False)
        return ranked.sort_values(sort_columns, ascending=ascending)

    @staticmethod
    def _select_prefilter_pool(ranked: pd.DataFrame, pool_limit: int) -> pd.DataFrame:
        if ranked is None or ranked.empty:
            return pd.DataFrame() if ranked is None else ranked
        limit = max(1, int(pool_limit or 1))
        selected = [ranked.head(limit)]
        if "amount" in ranked.columns:
            liquidity_guard = min(max(limit // 4, 10), 30)
            selected.append(ranked.sort_values("amount", ascending=False).head(liquidity_guard))
        out = pd.concat(selected)
        if "code" in out.columns:
            out = out.drop_duplicates(subset=["code"])
        else:
            out = out.drop_duplicates()
        sort_columns = ["_prefilter_score"] if "_prefilter_score" in out.columns else []
        ascending = [False]
        if "amount" in out.columns:
            sort_columns.append("amount")
            ascending.append(False)
        return out.sort_values(sort_columns, ascending=ascending) if sort_columns else out

    @staticmethod
    def _prefilter_sector_score(code: str, sector_context: dict[str, Any] | None) -> float:
        if not sector_context:
            return 0.5
        tags = (sector_context.get("tags_by_code") or {}).get(code, set())
        boards = sector_context.get("boards") or {}
        scores = []
        for tag in tags:
            board = boards.get(tag)
            if not board:
                continue
            pct = float(board.get("pct_change") or 0.0)
            if pct >= 2.0:
                scores.append(1.0)
            elif pct >= 0.5:
                scores.append(0.75)
            elif pct <= -1.5:
                scores.append(0.2)
            else:
                scores.append(0.45)
        return max(scores) if scores else 0.5

    @staticmethod
    def _prefilter_event_score(code: str, event_context: dict[str, dict[str, Any]] | None) -> float:
        if not event_context:
            return 0.5
        row = event_context.get(normalize_symbol(code)) or {}
        boost = float(row.get("boost") or 0.0)
        if boost >= 12.0:
            return 1.0
        if boost >= 5.0:
            return 0.75
        if boost <= -8.0:
            return 0.15
        if boost < 0:
            return 0.35
        return 0.5

    def _event_context(
        self,
        symbols: list[str],
        *,
        sector_context: dict[str, Any] | None = None,
    ) -> dict[str, dict[str, Any]]:
        """Build current news/event catalyst context without per-stock announcement scans."""
        codes = [normalize_symbol(str(symbol)) for symbol in symbols if normalize_symbol(str(symbol))]
        if not codes:
            return {}
        try:
            from modules.event_driver import EventDriver

            driver = EventDriver()
            sector_boosts = driver._active_sector_boosts()
            if not sector_boosts:
                return {}
            details = getattr(driver, "_sector_boost_details", {}) or {}
            tags_by_code = (sector_context or {}).get("tags_by_code") or {}
            weights = (1.0, 0.4, 0.2)
            out: dict[str, dict[str, Any]] = {}
            for code in dict.fromkeys(codes):
                tags = {str(tag) for tag in tags_by_code.get(code, set()) if str(tag)}
                if not tags:
                    continue
                raw_matches: list[dict[str, Any]] = []
                for stock_tag in tags:
                    for event_tag, source_boost in sector_boosts.items():
                        if not driver._tag_matches(stock_tag, str(event_tag)):
                            continue
                        detail = details.get(event_tag, {})
                        topic = str(detail.get("top_topic") or event_tag)
                        fallback = driver._exposure_weight(stock_tag, str(event_tag))
                        try:
                            exposure, configured = driver._exposure_provider.weight(
                                code,
                                topic=topic,
                                event_tag=str(event_tag),
                                stock_tag=stock_tag,
                                fallback=fallback,
                            )
                        except Exception:
                            exposure, configured = fallback, None
                        raw_boost = float(source_boost) * float(exposure)
                        if abs(raw_boost) < 0.5:
                            continue
                        raw_matches.append(
                            {
                                "stock_tag": stock_tag,
                                "event_tag": str(event_tag),
                                "topic": topic,
                                "raw_boost": raw_boost,
                                "source_boost": float(source_boost),
                                "exposure": float(exposure),
                                "exposure_source": (configured or {}).get("source", "tag_match"),
                                "market_validation": detail.get("market_validation_status", ""),
                                "market_multiplier": float(detail.get("market_multiplier") or 1.0),
                            }
                        )
                if not raw_matches:
                    continue

                grouped: dict[str, list[dict[str, Any]]] = {}
                for match in raw_matches:
                    key = driver._theme_key(str(match["event_tag"]), match.get("topic"))
                    grouped.setdefault(key, []).append(match)
                best_matches = []
                for rows in grouped.values():
                    best = max(rows, key=lambda item: abs(float(item.get("raw_boost") or 0.0)))
                    best = dict(best)
                    best["deduped_count"] = len(rows)
                    best_matches.append(best)
                best_matches.sort(key=lambda item: abs(float(item.get("raw_boost") or 0.0)), reverse=True)

                boost = 0.0
                factors: list[dict[str, Any]] = []
                for match, weight in zip(best_matches[: len(weights)], weights):
                    raw_boost = float(match.get("raw_boost") or 0.0)
                    weighted = raw_boost * weight
                    boost += weighted
                    factors.append(
                        {
                            "stock_tag": match.get("stock_tag", ""),
                            "event_tag": match.get("event_tag", ""),
                            "topic": match.get("topic", ""),
                            "boost": round(weighted, 1),
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
                out[code] = {
                    "symbol": code,
                    "boost": max(-40.0, min(30.0, round(boost, 1))),
                    "matched_factors": factors,
                    "reason": "；".join(
                        f"{row.get('event_tag')}({float(row.get('boost') or 0.0):+.1f})"
                        for row in factors[:5]
                    ),
                }
            return out
        except Exception as exc:
            _logger.warning("swing event context unavailable: %s", exc)
            return {}

    def _historical_event_context(
        self,
        symbols: list[str],
        trade_date: Any,
        *,
        sector_context: dict[str, Any] | None = None,
    ) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
        """Build point-in-time event context from persisted factor snapshots."""
        snapshot_date, factors = self._event_factor_snapshot(trade_date)
        meta = {
            "snapshot_date": snapshot_date or "",
            "factor_count": len(factors),
            "context_count": 0,
        }
        if not factors:
            return {}, meta

        codes = [normalize_symbol(str(symbol)) for symbol in symbols if normalize_symbol(str(symbol))]
        if not codes:
            return {}, meta
        try:
            from modules.event_driver import EventDriver

            driver = EventDriver()
            tags_by_code = (sector_context or {}).get("tags_by_code") or {}
            weights = (1.0, 0.4, 0.2)
            out: dict[str, dict[str, Any]] = {}
            for code in dict.fromkeys(codes):
                tags = {str(tag) for tag in tags_by_code.get(code, set()) if str(tag)}
                if not tags:
                    continue
                raw_matches: list[dict[str, Any]] = []
                for stock_tag in tags:
                    for factor in factors:
                        event_tag = str(factor.get("sector") or "")
                        if not event_tag or not driver._tag_matches(stock_tag, event_tag):
                            continue
                        topic = str(factor.get("top_topic") or event_tag)
                        fallback = driver._exposure_weight(stock_tag, event_tag)
                        try:
                            exposure, configured = driver._exposure_provider.weight(
                                code,
                                topic=topic,
                                event_tag=event_tag,
                                stock_tag=stock_tag,
                                fallback=fallback,
                            )
                        except Exception:
                            exposure, configured = fallback, None
                        raw_boost = float(factor.get("factor_score") or 0.0) * float(exposure)
                        if abs(raw_boost) < 0.5:
                            continue
                        raw_matches.append(
                            {
                                "stock_tag": stock_tag,
                                "event_tag": event_tag,
                                "topic": topic,
                                "raw_boost": raw_boost,
                                "source_boost": float(factor.get("factor_score") or 0.0),
                                "exposure": float(exposure),
                                "exposure_source": (configured or {}).get("source", "historical_tag_match"),
                                "market_validation": f"snapshot:{snapshot_date}",
                                "market_multiplier": 1.0,
                            }
                        )
                if not raw_matches:
                    continue

                grouped: dict[str, list[dict[str, Any]]] = {}
                for match in raw_matches:
                    key = driver._theme_key(str(match["event_tag"]), match.get("topic"))
                    grouped.setdefault(key, []).append(match)
                best_matches = []
                for rows in grouped.values():
                    best = max(rows, key=lambda item: abs(float(item.get("raw_boost") or 0.0)))
                    best = dict(best)
                    best["deduped_count"] = len(rows)
                    best_matches.append(best)
                best_matches.sort(key=lambda item: abs(float(item.get("raw_boost") or 0.0)), reverse=True)

                boost = 0.0
                matched_factors: list[dict[str, Any]] = []
                for match, weight in zip(best_matches[: len(weights)], weights):
                    raw_boost = float(match.get("raw_boost") or 0.0)
                    weighted = raw_boost * weight
                    boost += weighted
                    matched_factors.append(
                        {
                            "stock_tag": match.get("stock_tag", ""),
                            "event_tag": match.get("event_tag", ""),
                            "topic": match.get("topic", ""),
                            "boost": round(weighted, 1),
                            "raw_boost": round(raw_boost, 1),
                            "source_boost": round(float(match.get("source_boost") or 0.0), 1),
                            "weight": weight,
                            "exposure": round(float(match.get("exposure") or 0.0), 2),
                            "deduped_count": int(match.get("deduped_count") or 1),
                            "market_validation": match.get("market_validation", ""),
                            "market_multiplier": round(float(match.get("market_multiplier") or 1.0), 2),
                            "exposure_source": match.get("exposure_source", "historical_tag_match"),
                        }
                    )
                out[code] = {
                    "symbol": code,
                    "boost": max(-40.0, min(30.0, round(boost, 1))),
                    "matched_factors": matched_factors,
                    "snapshot_date": snapshot_date,
                    "reason": "；".join(
                        f"{row.get('event_tag')}({float(row.get('boost') or 0.0):+.1f})"
                        for row in matched_factors[:5]
                    ),
                }
            meta["context_count"] = len(out)
            return out, meta
        except Exception as exc:
            _logger.warning("historical swing event context unavailable: %s", exc)
            meta["error"] = str(exc)
            return {}, meta

    def _event_factor_snapshot(self, trade_date: Any) -> tuple[str | None, list[dict[str, Any]]]:
        date_text = pd.Timestamp(trade_date).date().isoformat()
        if date_text in self._event_snapshot_cache:
            return self._event_snapshot_cache[date_text]
        try:
            from modules.config import cfg as _cfg
            from modules.db import DatabaseManager

            db = DatabaseManager(_cfg().data.warehouse_path)
            row = db.conn.execute(
                """SELECT MAX(snapshot_date)
                   FROM event_news_factor_snapshots
                   WHERE snapshot_date <= ?""",
                (date_text,),
            ).fetchone()
            snapshot_date = str(row[0] or "") if row else ""
            if not snapshot_date:
                result = (None, [])
                self._event_snapshot_cache[date_text] = result
                return result
            df = pd.read_sql_query(
                """SELECT *
                   FROM event_news_factor_snapshots
                   WHERE snapshot_date = ?
                   ORDER BY ABS(factor_score) DESC""",
                db.conn,
                params=[snapshot_date],
            )
            factors = df.where(pd.notna(df), None).to_dict(orient="records") if not df.empty else []
            result = (snapshot_date, factors)
            self._event_snapshot_cache[date_text] = result
            return result
        except Exception as exc:
            _logger.warning("event factor snapshot unavailable for %s: %s", date_text, exc)
            result = (None, [])
            self._event_snapshot_cache[date_text] = result
            return result

    def _daily_cache_snapshot(
        self,
        symbols: list[str],
        *,
        lookback_days: int = 420,
        max_stale_days: int = 10,
    ) -> dict[str, pd.DataFrame]:
        """Load recent daily bars from warehouse in one batch for fast scans."""
        codes = [normalize_symbol(str(symbol)) for symbol in symbols if normalize_symbol(str(symbol))]
        if not codes:
            return {}
        try:
            from modules.config import cfg as _cfg
            from modules.db import DatabaseManager

            settings = _cfg()
            if not settings.data.enable_warehouse:
                return {}

            variant_to_code: dict[str, str] = {}
            variants: list[str] = []
            for code in dict.fromkeys(codes):
                for variant in (code, to_full_code(code)):
                    if variant and variant not in variant_to_code:
                        variant_to_code[variant] = code
                        variants.append(variant)

            end_ts = pd.Timestamp(datetime.now().date())
            start_date = (end_ts - pd.Timedelta(days=max(60, int(lookback_days)))).date().isoformat()
            end_date = end_ts.date().isoformat()
            cutoff = end_ts - pd.Timedelta(days=max(0, int(max_stale_days)))

            db = DatabaseManager(settings.data.warehouse_path)
            cached = db.price().load_daily_batch(variants, start_date=start_date, end_date=end_date)
            out: dict[str, pd.DataFrame] = {}
            for variant, daily in cached.items():
                code = variant_to_code.get(str(variant), normalize_symbol(str(variant)))
                if not code or daily is None or daily.empty:
                    continue
                prepared = self._prepare_daily(daily)
                if prepared.empty or len(prepared) < 35:
                    continue
                prepared.index = pd.to_datetime(prepared.index, errors="coerce").normalize()
                prepared = prepared[~prepared.index.isna()]
                prepared = prepared[~prepared.index.duplicated(keep="last")].sort_index()
                if prepared.empty or prepared.index.max() < cutoff:
                    continue
                current = out.get(code)
                if current is None or prepared.index.max() > current.index.max():
                    out[code] = prepared
            return out
        except Exception as exc:
            _logger.warning("daily warehouse cache unavailable for swing scan: %s", exc)
            return {}

    def _spot_snapshot(self) -> pd.DataFrame:
        try:
            spot = self._sd.get_spot(use_cache=True)
            if spot is not None and not spot.empty:
                return spot
        except Exception as exc:
            _logger.warning("spot snapshot unavailable from StockData: %s", exc)

        try:
            from modules.crawler import cache

            cached = cache.read_df_cache("spot_sina", max_age_seconds=None)
            if cached.data is not None and not cached.data.empty:
                if not cached.ok:
                    _logger.warning("using stale spot_sina cache for swing context: %s", cached.error)
                return cached.data
        except Exception as exc:
            _logger.warning("spot_sina cache fallback unavailable: %s", exc)
        return pd.DataFrame()

    def _sector_context(self, symbols: list[str], *, spot: pd.DataFrame | None = None) -> dict[str, Any]:
        if not symbols or not self._enable_sector_context:
            return {}
        try:
            symbol_set = {normalize_symbol(symbol) for symbol in symbols}
            graph_path = Path(__file__).resolve().parent.parent / "data" / "sector_graph.toml"
            graph = tomllib.loads(graph_path.read_text(encoding="utf-8"))
            tags_by_code = {symbol: set() for symbol in symbol_set}
            boards: dict[str, dict[str, Any]] = {}
            spot_by_code = self._spot_by_code(spot)
            for node in graph.get("nodes", []):
                tag = str(node.get("tag") or "")
                members = [
                    normalize_symbol(str(member.get("code") or ""))
                    for member in node.get("members", [])
                    if member.get("code")
                ]
                if not tag or not members:
                    continue
                for code in symbol_set.intersection(members):
                    tags_by_code.setdefault(code, set()).add(tag)
                member_rows = spot_by_code.loc[spot_by_code.index.intersection(members)] if not spot_by_code.empty else pd.DataFrame()
                if not member_rows.empty and "pct_change" in member_rows.columns:
                    pct = pd.to_numeric(member_rows["pct_change"], errors="coerce")
                    boards[tag] = {
                        "pct_change": round(float(pct.mean(skipna=True)), 3),
                        "rise_count": int((pct > 0).sum()),
                        "fall_count": int((pct < 0).sum()),
                    }
            for code in symbol_set:
                if not tags_by_code.get(code):
                    tags_by_code[code] = {self._market_fallback_tag(code)}
            return {"tags_by_code": tags_by_code, "boards": boards}
        except Exception as exc:
            _logger.warning("sector context unavailable: %s", exc)
            return {}

    @staticmethod
    def _spot_by_code(spot: pd.DataFrame | None) -> pd.DataFrame:
        if spot is None or spot.empty or "code" not in spot.columns:
            return pd.DataFrame()
        out = spot.copy()
        out["code"] = out["code"].map(normalize_symbol)
        for column in ("pct_change", "price", "turnover", "amount"):
            if column in out.columns:
                out[column] = pd.to_numeric(out[column], errors="coerce")
        return out.drop_duplicates(subset=["code"]).set_index("code", drop=False)

    @staticmethod
    def _market_fallback_tag(code: str) -> str:
        if code.startswith("6"):
            return "上海主板"
        if code.startswith(("0", "2", "3")):
            return "深圳主板"
        if code.startswith(("8", "4", "9")):
            return "北京证券交易所"
        return "其他市场"

    @staticmethod
    def _sector_score(
        symbol: str,
        sector_context: dict[str, Any] | None,
        *,
        reasons: list[str],
        warnings: list[str],
    ) -> tuple[float, dict[str, Any]]:
        if not sector_context:
            return 0.0, {}
        tags = sorted(str(tag) for tag in (sector_context.get("tags_by_code") or {}).get(symbol, set()) if tag)
        informative_tags = [tag for tag in tags if tag not in {"上海主板", "深圳主板", "北京证券交易所", "其他市场"}]
        metrics: dict[str, Any] = {"sector_tags": informative_tags or tags}
        if not informative_tags:
            warnings.append("缺少可验证主题暴露，板块确认不足")
            metrics["sector_adjust"] = -6.0
            return -6.0, metrics

        boards = sector_context.get("boards") or {}
        matched = []
        for tag in informative_tags:
            board = boards.get(tag)
            if board:
                matched.append((tag, float(board.get("pct_change") or 0.0)))
        metrics["matched_sector_pct"] = {tag: round(pct, 3) for tag, pct in matched}
        if not matched:
            warnings.append("主题有标签但缺少实时板块强弱验证")
            metrics["sector_adjust"] = -2.0
            return -2.0, metrics

        best_tag, best_pct = max(matched, key=lambda item: item[1])
        worst_tag, worst_pct = min(matched, key=lambda item: item[1])
        metrics["best_sector"] = best_tag
        metrics["best_sector_pct"] = round(best_pct, 3)
        metrics["worst_sector"] = worst_tag
        metrics["worst_sector_pct"] = round(worst_pct, 3)
        if best_pct >= 2.0:
            reasons.append(f"所属主题{best_tag}强势确认({best_pct:+.2f}%)")
            metrics["sector_adjust"] = 6.0
            return 6.0, metrics
        if best_pct >= 0.5:
            reasons.append(f"所属主题{best_tag}偏强({best_pct:+.2f}%)")
            metrics["sector_adjust"] = 3.0
            return 3.0, metrics
        if worst_pct <= -1.5:
            warnings.append(f"所属主题{worst_tag}偏弱({worst_pct:+.2f}%)")
            metrics["sector_adjust"] = -8.0
            return -8.0, metrics
        warnings.append("所属主题没有明显强度确认")
        metrics["sector_adjust"] = -2.0
        return -2.0, metrics

    @staticmethod
    def _event_score(
        event_context: dict[str, Any] | None,
        *,
        reasons: list[str],
        warnings: list[str],
    ) -> tuple[float, dict[str, Any]]:
        if not event_context:
            return 0.0, {"event_boost": 0.0, "event_adjust": 0.0, "event_factors": []}

        boost = float(event_context.get("boost") or 0.0)
        factors = list(event_context.get("matched_factors") or [])[:3]
        metrics = {
            "event_boost": round(boost, 3),
            "event_factors": factors,
        }
        top = factors[0] if factors else {}
        topic = str(top.get("topic") or top.get("event_tag") or "")
        tag = str(top.get("event_tag") or "")
        validation = str(top.get("market_validation") or "")

        adjust = 0.0
        if boost >= 15.0:
            adjust = 8.0
        elif boost >= 8.0:
            adjust = 5.0
        elif boost >= 3.0:
            adjust = 3.0
        elif boost > 0:
            adjust = 1.0
        elif boost <= -15.0:
            adjust = -10.0
        elif boost <= -8.0:
            adjust = -6.0
        elif boost < 0:
            adjust = -3.0

        if adjust > 0:
            suffix = f"，盘面{validation}" if validation else ""
            reasons.insert(0, f"新闻催化：{topic or tag} 热度{boost:+.1f}，短线加分{adjust:+.1f}{suffix}")
        elif adjust < 0:
            suffix = f"，盘面{validation}" if validation else ""
            warnings.insert(0, f"新闻/公告事件偏负：{topic or tag} 热度{boost:+.1f}，短线降权{adjust:+.1f}{suffix}")

        metrics["event_adjust"] = round(adjust, 3)
        return adjust, metrics

    @staticmethod
    def _prepare_daily(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        if "date" in out.columns:
            out["date"] = pd.to_datetime(out["date"], errors="coerce")
            out = out.dropna(subset=["date"]).set_index("date")
        out = out.sort_index()
        for col in ("open", "high", "low", "close", "volume", "amount", "turn", "turnover"):
            if col in out.columns:
                out[col] = pd.to_numeric(out[col], errors="coerce")
        return out.dropna(subset=["open", "high", "low", "close", "volume"])

    @staticmethod
    def _append_quote_bar(
        daily: pd.DataFrame,
        quote: dict[str, Any] | None,
    ) -> tuple[pd.DataFrame, bool]:
        if daily is None or daily.empty or not quote:
            return daily, False

        today = pd.Timestamp(datetime.now().date())
        out = daily.copy()
        if "date" in out.columns:
            dates = pd.to_datetime(out["date"], errors="coerce")
            latest_date = dates.max()
        else:
            latest_date = pd.to_datetime(out.index, errors="coerce").max()
        if pd.isna(latest_date) or latest_date > today:
            return daily, False

        close = float(quote.get("price") or 0.0)
        if close <= 0:
            return daily, False
        open_price = float(quote.get("open") or close)
        high = float(quote.get("high") or max(open_price, close))
        low = float(quote.get("low") or min(open_price, close))
        prev_close = float(quote.get("prev_close") or 0.0)
        if prev_close <= 0 and "close" in out.columns:
            prev_close = float(pd.to_numeric(out["close"], errors="coerce").dropna().iloc[-1])

        high = max(high, open_price, close)
        low = min(low, open_price, close)
        volume = float(quote.get("volume") or 0.0)
        amount = float(quote.get("amount") or 0.0)
        if amount <= 0 and volume > 0:
            amount = close * volume
        turnover = float(quote.get("turnover") or 0.0)
        pct_change = quote.get("change_pct")
        if pct_change is None and prev_close > 0:
            pct_change = (close / prev_close - 1.0) * 100

        row = {
            "date": today,
            "open": open_price,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
            "amount": amount,
            "turn": turnover,
            "turnover": turnover,
            "pct_change": float(pct_change or 0.0),
        }
        if latest_date == today:
            current = LowAbsorbSwingEngine._latest_daily_row(out)
            if current is not None and not LowAbsorbSwingEngine._quote_bar_is_fresher(current, row):
                return daily, False
            if "date" in out.columns:
                dates = pd.to_datetime(out["date"], errors="coerce")
                out = out[dates.dt.normalize() != today]
            else:
                dates = pd.to_datetime(out.index, errors="coerce")
                out = out[dates.normalize() != today]

        if "date" in out.columns:
            out = pd.concat([out, pd.DataFrame([row])], ignore_index=True)
        else:
            row_indexed = pd.DataFrame([row]).set_index("date")
            out = pd.concat([out, row_indexed])
        return out, True

    @staticmethod
    def _latest_daily_row(df: pd.DataFrame) -> pd.Series | None:
        if df.empty:
            return None
        if "date" in df.columns:
            dates = pd.to_datetime(df["date"], errors="coerce")
            if dates.isna().all():
                return None
            return df.loc[dates.idxmax()]
        dates = pd.to_datetime(df.index, errors="coerce")
        if dates.isna().all():
            return None
        return df.iloc[int(dates.argmax())]

    @staticmethod
    def _quote_bar_is_fresher(current: pd.Series, quote_row: dict[str, Any]) -> bool:
        def number(value: Any) -> float:
            try:
                return float(value or 0.0)
            except Exception:
                return 0.0

        current_close = number(current.get("close"))
        quote_close = number(quote_row.get("close"))
        if current_close > 0 and abs(quote_close - current_close) / current_close >= 0.001:
            return True
        current_high = number(current.get("high"))
        current_low = number(current.get("low"))
        quote_high = number(quote_row.get("high"))
        quote_low = number(quote_row.get("low"))
        if current_high <= current_low and quote_high > quote_low:
            return True
        current_volume = number(current.get("volume"))
        quote_volume = number(quote_row.get("volume"))
        if quote_volume > max(current_volume * 1.2, current_volume + 1):
            return True
        current_amount = number(current.get("amount"))
        quote_amount = number(quote_row.get("amount"))
        return quote_amount > max(current_amount * 1.2, current_amount + 1)

    @staticmethod
    def _trend_continuation_profile(
        *,
        df: pd.DataFrame,
        close: float,
        ma10: float,
        ma20: float,
        ma30: float,
        pct_change: float,
        day_range_pct: float,
        close_position: float,
        amount: float,
        market_context: dict[str, Any] | None,
        sector_metrics: dict[str, Any],
        sector_adjust: float,
        event_metrics: dict[str, Any],
        event_adjust: float,
        risk_adjust: float,
        quote_bar_appended: bool,
    ) -> dict[str, Any] | None:
        if len(df) < 25 or close <= 0:
            return None

        closes = pd.to_numeric(df["close"], errors="coerce")
        pct_series = closes.pct_change() * 100
        limit_like_count = int((pct_series.tail(10) >= 9.2).sum())
        ret_5 = (close / float(closes.iloc[-6]) - 1.0) * 100 if len(closes) >= 6 and closes.iloc[-6] else 0.0
        ret_10 = (close / float(closes.iloc[-11]) - 1.0) * 100 if len(closes) >= 11 and closes.iloc[-11] else 0.0
        ma10_prev = float(closes.rolling(10).mean().iloc[-4]) if len(closes) >= 14 else ma10
        high_20 = float(pd.to_numeric(df["high"], errors="coerce").tail(20).max())
        high_20_position = close / high_20 if high_20 > 0 else 0.0
        gap_ma10 = (close / ma10 - 1.0) * 100 if ma10 > 0 else 0.0
        gap_ma20 = (close / ma20 - 1.0) * 100 if ma20 > 0 else 0.0

        score = 0.0
        reasons: list[str] = []
        warnings: list[str] = []
        if quote_bar_appended:
            warnings.append("日线末端使用实时行情临时补齐")
        if pct_change > 1.2:
            warnings.append("涨幅已超过低吸口径，趋势票需防追涨")

        if ma10 > ma20 > ma30:
            score += 14
            reasons.append("强趋势：MA10/20/30 多头排列")
        elif ma10 > ma20:
            score += 8
            reasons.append("强趋势：短中期均线向上")
        else:
            return None

        if close >= ma10:
            score += 8
            reasons.append("收盘站上MA10")
        elif close >= ma20:
            score += 4
            reasons.append("收盘仍在MA20上方")
        else:
            return None

        if ma10 > ma10_prev:
            score += 5
            reasons.append("MA10仍在上行")

        if ret_10 >= 30:
            score += 12
            reasons.append(f"10日涨幅{ret_10:.1f}%，趋势动能强")
        elif ret_10 >= 18:
            score += 8
            reasons.append(f"10日涨幅{ret_10:.1f}%，趋势延续")
        elif ret_5 >= 10:
            score += 6
            reasons.append(f"5日涨幅{ret_5:.1f}%，短线强势")

        if limit_like_count >= 3:
            score += 12
            reasons.append(f"近10日出现{limit_like_count}次涨停级别强势K")
        elif limit_like_count >= 2:
            score += 10
            reasons.append("近10日出现连续涨停级别强势K")
        elif limit_like_count >= 1:
            score += 7
            reasons.append("近10日出现涨停级别强势K")

        if high_20_position >= 1.0:
            score += 10
            reasons.append("股价刷新20日新高")
        elif high_20_position >= 0.97:
            score += 8
            reasons.append("股价接近20日新高")
        elif high_20_position >= 0.94:
            score += 5
            reasons.append("股价回落后仍在20日高位区")
        elif high_20_position >= 0.9:
            score += 2
            reasons.append("股价处于20日相对高位")

        if amount >= 1_000_000_000:
            score += 7
            reasons.append("成交额超过10亿，趋势承接活跃")
        elif amount >= 200_000_000:
            score += 5
            reasons.append("成交额满足趋势票流动性")

        strong_leader = (
            limit_like_count >= 2
            and (ret_10 >= 18 or ret_5 >= 10)
            and high_20 > 0
            and high_20_position >= 0.94
            and amount >= 200_000_000
        )

        if close_position >= 0.55:
            score += 5
            reasons.append("收盘位于日内中高位")
        elif close_position >= 0.35:
            score += 2
        else:
            score -= 5
            warnings.append("收盘位置偏弱，趋势接力风险高")

        if day_range_pct >= 8:
            warnings.append("日内振幅偏大，追高风险高")

        if sector_adjust > 0:
            score += min(6.0, sector_adjust)
            best_sector = sector_metrics.get("best_sector")
            best_pct = float(sector_metrics.get("best_sector_pct") or 0.0)
            if best_sector:
                reasons.append(f"所属主题{best_sector}获得盘面确认({best_pct:+.2f}%)")
        elif sector_adjust <= -8:
            if strong_leader:
                score -= 3
                warnings.append("所属主题偏弱，但个股强势独立，按逆势趋势票小幅降权")
            else:
                score -= 8
                warnings.append("所属主题偏弱，趋势延续确认不足")

        if event_adjust > 0:
            event_cap = 4.0
            if ret_10 >= 30 or gap_ma10 >= 18 or gap_ma20 >= 28:
                event_cap = 0.0
                warnings.append("新闻题材热但个股短线过热，趋势票不再额外加新闻分")
            event_points = min(event_cap, event_adjust)
            score += event_points
            event_factors = event_metrics.get("event_factors") or []
            top = event_factors[0] if event_factors else {}
            topic = str(top.get("topic") or top.get("event_tag") or "")
            if topic:
                text = f"新闻催化确认：{topic} 热度{float(event_metrics.get('event_boost') or 0):+.1f}，趋势确认{event_points:+.1f}"
                if event_points > 0:
                    reasons.insert(0, text)
                else:
                    warnings.insert(0, text)
        elif event_adjust < 0:
            score += event_adjust
            event_factors = event_metrics.get("event_factors") or []
            top = event_factors[0] if event_factors else {}
            topic = str(top.get("topic") or top.get("event_tag") or "")
            if topic:
                warnings.insert(
                    0,
                    f"新闻/事件偏负：{topic} 热度{float(event_metrics.get('event_boost') or 0):+.1f}，短线降权{event_adjust:+.1f}",
                )

        state = (market_context or {}).get("state")
        if state == "severe":
            if strong_leader:
                score -= 5
                warnings.append("大盘宽度严重偏弱，强趋势票降级观察")
            else:
                score -= 8
                warnings.append("大盘宽度严重偏弱，趋势票只能降级观察")
        elif state == "weak":
            score -= 4
            warnings.append("大盘宽度偏弱，趋势票仓位要轻")
        elif state == "supportive":
            score += 3

        if gap_ma10 >= 18:
            score -= 4
            warnings.append(f"偏离MA10 {gap_ma10:.1f}%，追高风险高")
        if gap_ma20 >= 28:
            score -= 6
            warnings.append(f"偏离MA20 {gap_ma20:.1f}%，趋势过热")

        score += risk_adjust
        if score < 50:
            return None
        return {
            "score": round(max(0.0, min(100.0, score)), 1),
            "setup": "强趋势延续观察",
            "exit_rule": "趋势票只做回踩承接或盘中换手确认；跌破MA10或放量长阴退出",
            "reasons": reasons[:8],
            "warnings": warnings[:8],
        }

    @staticmethod
    def _breakout_ignition_profile(
        *,
        df: pd.DataFrame,
        close: float,
        ma10: float,
        ma20: float,
        ma30: float,
        pct_change: float,
        day_range_pct: float,
        close_position: float,
        amount: float,
        market_context: dict[str, Any] | None,
        sector_metrics: dict[str, Any],
        sector_adjust: float,
        event_metrics: dict[str, Any],
        event_adjust: float,
        risk_adjust: float,
        quote_bar_appended: bool,
    ) -> dict[str, Any] | None:
        if len(df) < 25 or close <= 0:
            return None
        if not (2.5 <= pct_change <= 10.5):
            return None
        if close_position < 0.65:
            return None

        highs = pd.to_numeric(df["high"], errors="coerce")
        volumes = pd.to_numeric(df["volume"], errors="coerce")
        prev_high_20 = float(highs.iloc[-21:-1].max()) if len(highs) >= 21 else float(highs.iloc[:-1].max())
        current_high = float(highs.iloc[-1])
        if prev_high_20 <= 0 or max(close, current_high) < prev_high_20 * 1.01:
            return None
        avg_volume_5 = float(volumes.iloc[-6:-1].mean()) if len(volumes) >= 6 else 0.0
        volume_ratio_5 = float(volumes.iloc[-1]) / avg_volume_5 if avg_volume_5 > 0 else 1.0
        if volume_ratio_5 < 1.15:
            return None
        strong_breakout = (
            volume_ratio_5 >= 1.5
            and amount >= 200_000_000
            and max(close, current_high) >= prev_high_20 * 1.03
        )

        score = 0.0
        reasons: list[str] = []
        warnings: list[str] = []
        if quote_bar_appended:
            warnings.append("日线末端使用实时行情临时补齐")
        warnings.append("放量突破首日允许追1-2天，但必须等分时换手承接")

        if ma10 > ma20 > ma30:
            score += 14
            reasons.append("突破首日：MA10/20/30 多头排列")
        elif ma10 >= ma20 * 0.995 and ma20 >= ma30 * 0.99:
            score += 10
            reasons.append("突破首日：均线底座接近多头")
        else:
            return None

        if close >= ma10:
            score += 8
            reasons.append("收盘站上MA10")
        else:
            return None

        score += 12
        reasons.append("收盘突破20日新高")
        if 3.0 <= pct_change <= 8.5:
            score += 8
            reasons.append(f"涨幅{pct_change:.1f}%，属于可追踪突破强度")
        else:
            score += 5
            warnings.append(f"涨幅{pct_change:.1f}%，接近涨停追高区")

        if volume_ratio_5 >= 1.5:
            score += 8
            reasons.append(f"成交量放大至5日均量{volume_ratio_5:.1f}倍")
        else:
            score += 5
            reasons.append("成交量温和放大")

        if amount >= 1_000_000_000:
            score += 7
            reasons.append("成交额超过10亿，突破有效性较强")
        elif amount >= 200_000_000:
            score += 5
            reasons.append("成交额满足短线追踪")

        score += 5
        reasons.append("收盘位于日内高位")
        if day_range_pct >= 8.0:
            warnings.append("日内振幅偏大，追高后回撤会很快")
            score -= 6
        elif day_range_pct >= 7.0:
            warnings.append("突破日振幅偏大，隔日容易先冲后砸")
            score -= 3

        if sector_adjust > 0:
            score += min(6.0, sector_adjust)
            best_sector = sector_metrics.get("best_sector")
            best_pct = float(sector_metrics.get("best_sector_pct") or 0.0)
            if best_sector:
                reasons.append(f"所属主题{best_sector}获得盘面确认({best_pct:+.2f}%)")
        elif sector_adjust <= -8:
            if strong_breakout:
                score -= 3
                warnings.append("所属主题偏弱，但放量突破强，按独立突破小幅降权")
            else:
                score -= 8
                warnings.append("所属主题偏弱，突破延续确认不足")

        if event_adjust > 0:
            event_cap = 3.0
            if day_range_pct >= 8.0 or pct_change >= 8.5:
                event_cap = 0.0
                warnings.append("新闻题材热但突破日波动/涨幅偏大，不再额外加新闻分")
            event_points = min(event_cap, event_adjust)
            score += event_points
            event_factors = event_metrics.get("event_factors") or []
            top = event_factors[0] if event_factors else {}
            topic = str(top.get("topic") or top.get("event_tag") or "")
            if topic:
                text = f"新闻催化确认：{topic} 热度{float(event_metrics.get('event_boost') or 0):+.1f}，突破确认{event_points:+.1f}"
                if event_points > 0:
                    reasons.insert(0, text)
                else:
                    warnings.insert(0, text)
        elif event_adjust < 0:
            score += event_adjust
            event_factors = event_metrics.get("event_factors") or []
            top = event_factors[0] if event_factors else {}
            topic = str(top.get("topic") or top.get("event_tag") or "")
            if topic:
                warnings.insert(
                    0,
                    f"新闻/事件偏负：{topic} 热度{float(event_metrics.get('event_boost') or 0):+.1f}，短线降权{event_adjust:+.1f}",
                )

        state = (market_context or {}).get("state")
        if state == "severe":
            if strong_breakout:
                score -= 4
                warnings.append("大盘宽度严重偏弱，放量突破只能小仓快进快出")
            else:
                score -= 6
                warnings.append("大盘宽度严重偏弱，追涨只能小仓快进快出")
        elif state == "weak":
            score -= 3
            warnings.append("大盘宽度偏弱，追涨仓位要轻")
        elif state == "supportive":
            score += 3

        score += risk_adjust
        if score < 50:
            return None
        return {
            "score": round(max(0.0, min(100.0, score)), 1),
            "setup": "放量突破首日",
            "exit_rule": "允许追1-2天，但只在分时回踩不破均价线时介入；跌破当日低点或次日冲高失败退出",
            "reasons": reasons[:8],
            "warnings": warnings[:8],
        }

    @classmethod
    def _historical_reference(
        cls,
        df: pd.DataFrame,
        *,
        setup: str,
        current_close: float,
        target_1: float,
        stop_loss: float,
    ) -> dict[str, Any]:
        base = {
            "setup": setup,
            "lookback_days": 120,
            "sample_count": 0,
            "target_hit_rate_pct": 0.0,
            "stop_hit_rate_pct": 0.0,
            "both_hit_rate_pct": 0.0,
            "win_rate_pct": 0.0,
            "avg_next_return_pct": 0.0,
            "median_next_return_pct": 0.0,
            "score_adjust": 0.0,
            "confidence": "none",
        }
        if df is None or len(df) < 36 or current_close <= 0:
            return base

        data = cls._historical_feature_frame(df)
        if len(data) < 36:
            return base

        target_pct = max(target_1 / current_close - 1.0, 0.005)
        stop_pct = min(stop_loss / current_close - 1.0, -0.005)
        start = max(30, len(data) - int(base["lookback_days"]) - 1)
        trades: list[dict[str, float | bool]] = []
        for idx in range(start, len(data) - 1):
            if not cls._historical_setup_matches(data, idx, setup):
                continue
            row = data.iloc[idx]
            next_row = data.iloc[idx + 1]
            buy = float(row.get("close") or 0.0)
            if buy <= 0:
                continue
            target_price = buy * (1.0 + target_pct)
            stop_price = buy * (1.0 + stop_pct)
            next_high = float(next_row.get("high") or 0.0)
            next_low = float(next_row.get("low") or 0.0)
            next_close = float(next_row.get("close") or 0.0)
            if next_high <= 0 or next_low <= 0 or next_close <= 0:
                continue
            raw_target_hit = next_high >= target_price
            stop_hit = next_low <= stop_price
            target_hit = raw_target_hit and not stop_hit
            both_hit = raw_target_hit and stop_hit
            if target_hit:
                realized = target_pct
            elif stop_hit:
                realized = stop_pct
            else:
                realized = next_close / buy - 1.0
            trades.append(
                {
                    "target_hit": target_hit,
                    "raw_target_hit": raw_target_hit,
                    "stop_hit": stop_hit,
                    "both_hit": both_hit,
                    "return_pct": realized * 100,
                }
            )

        sample_count = len(trades)
        if sample_count == 0:
            return base
        returns = pd.Series([float(item["return_pct"]) for item in trades])
        target_hit_rate = sum(bool(item["target_hit"]) for item in trades) / sample_count * 100
        stop_hit_rate = sum(bool(item["stop_hit"]) for item in trades) / sample_count * 100
        both_hit_rate = sum(bool(item.get("both_hit")) for item in trades) / sample_count * 100
        win_rate = float((returns > 0).sum()) / sample_count * 100
        avg_return = float(returns.mean())
        median_return = float(returns.median())
        confidence = "high" if sample_count >= 8 else "medium" if sample_count >= 4 else "low"
        score_adjust = 0.0
        if sample_count >= 3:
            if target_hit_rate >= 60 and avg_return >= 0.3 and stop_hit_rate <= 40:
                score_adjust += 2.0
            if target_hit_rate >= 70 and avg_return >= 0.8 and stop_hit_rate <= 35:
                score_adjust += 1.0
            if target_hit_rate >= 70 and median_return > 0 and stop_hit_rate <= 50:
                score_adjust += 1.0
            if avg_return <= -0.8 or stop_hit_rate >= 65:
                score_adjust -= 5.0
            elif avg_return <= -0.4 or stop_hit_rate >= 50:
                score_adjust -= 3.0
            elif target_hit_rate <= 35 and avg_return <= 0.2:
                score_adjust -= 2.0

        return {
            **base,
            "sample_count": sample_count,
            "target_hit_rate_pct": round(target_hit_rate, 1),
            "stop_hit_rate_pct": round(stop_hit_rate, 1),
            "both_hit_rate_pct": round(both_hit_rate, 1),
            "win_rate_pct": round(win_rate, 1),
            "avg_next_return_pct": round(avg_return, 3),
            "median_next_return_pct": round(median_return, 3),
            "score_adjust": round(score_adjust, 1),
            "confidence": confidence,
        }

    @staticmethod
    def _historical_feature_frame(df: pd.DataFrame) -> pd.DataFrame:
        data = df.copy()
        for column in ("open", "high", "low", "close", "volume", "amount", "turn", "turnover"):
            if column in data.columns:
                data[column] = pd.to_numeric(data[column], errors="coerce")
        data = data.dropna(subset=["open", "high", "low", "close", "volume"])
        if len(data) < 36:
            return data

        closes = pd.to_numeric(data["close"], errors="coerce")
        highs = pd.to_numeric(data["high"], errors="coerce")
        lows = pd.to_numeric(data["low"], errors="coerce")
        volumes = pd.to_numeric(data["volume"], errors="coerce")
        prev_close = closes.shift(1)
        pct = (closes / prev_close - 1.0) * 100
        ma10 = closes.rolling(10).mean()
        ma20 = closes.rolling(20).mean()
        ma30 = closes.rolling(30).mean()
        nearest_gap = pd.concat(
            [
                (closes - ma10).abs(),
                (closes - ma20).abs(),
                (closes - ma30).abs(),
            ],
            axis=1,
        ).min(axis=1) / closes * 100
        recent_peak_volume = volumes.shift(1).rolling(20, min_periods=1).max()
        recent_pct = closes.pct_change() * 100
        prior_impulse = recent_pct.shift(1).rolling(20, min_periods=1).max()
        high_20 = highs.rolling(20).max()
        prev_high_20 = highs.shift(1).rolling(20, min_periods=1).max()
        avg_volume_5 = volumes.shift(1).rolling(5, min_periods=1).mean()
        ret_5 = (closes / closes.shift(5) - 1.0) * 100
        ret_10 = (closes / closes.shift(10) - 1.0) * 100
        limit_like_count = (recent_pct >= 9.2).rolling(10, min_periods=1).sum()

        data["_hist_ma10"] = ma10
        data["_hist_ma20"] = ma20
        data["_hist_ma30"] = ma30
        data["_hist_ma_bullish"] = (ma10 > ma20) & (ma20 > ma30) | (ma10 > ma20)
        data["_hist_pct_change"] = pct
        data["_hist_close_position"] = (closes - lows) / (highs - lows).replace(0, pd.NA)
        data["_hist_close_position"] = data["_hist_close_position"].fillna(0.5)
        data["_hist_nearest_ma_gap_pct"] = nearest_gap
        data["_hist_prior_impulse_pct"] = prior_impulse
        data["_hist_volume_to_peak"] = volumes / recent_peak_volume.replace(0, pd.NA)
        data["_hist_volume_to_peak"] = data["_hist_volume_to_peak"].fillna(1.0)
        data["_hist_ret_5"] = ret_5
        data["_hist_ret_10"] = ret_10
        data["_hist_limit_like_count"] = limit_like_count
        data["_hist_high_20_position"] = closes / high_20.replace(0, pd.NA)
        data["_hist_high_20_position"] = data["_hist_high_20_position"].fillna(0.0)
        data["_hist_prev_high_20"] = prev_high_20
        data["_hist_volume_ratio_5"] = volumes / avg_volume_5.replace(0, pd.NA)
        data["_hist_volume_ratio_5"] = data["_hist_volume_ratio_5"].fillna(1.0)
        if "amount" not in data.columns:
            data["amount"] = closes * volumes
        else:
            data["amount"] = pd.to_numeric(data["amount"], errors="coerce").fillna(closes * volumes)
        return data

    @classmethod
    def _historical_setup_matches(cls, df: pd.DataFrame, idx: int, setup: str) -> bool:
        features = cls._historical_features_at(df, idx)
        if not features:
            return False
        if setup.startswith("强趋势"):
            return (
                bool(features["ma_bullish"])
                and features["close"] >= features["ma10"]
                and (
                    features["ret_10"] >= 18
                    or features["ret_5"] >= 10
                    or features["limit_like_count"] >= 1
                )
                and features["high_20_position"] >= 0.90
            )
        if setup.startswith("放量突破"):
            return (
                2.5 <= features["pct_change"] <= 10.5
                and features["close_position"] >= 0.65
                and features["close"] >= features["ma10"]
                and features["prev_high_20"] > 0
                and max(features["close"], features["high"]) >= features["prev_high_20"] * 1.01
                and features["volume_ratio_5"] >= 1.15
            )
        return (
            features["ma10"] > features["ma20"] >= features["ma30"] * 0.995
            and -4.0 <= features["pct_change"] <= 1.2
            and features["nearest_ma_gap_pct"] <= 3.5
            and features["close_position"] >= 0.35
            and features["prior_impulse_pct"] >= 4.0
            and features["volume_to_peak"] <= 0.8
            and features["amount"] >= 50_000_000
        )

    @staticmethod
    def _historical_features_at(df: pd.DataFrame, idx: int) -> dict[str, float | bool] | None:
        if idx < 30 or idx >= len(df):
            return None
        row = df.iloc[idx]
        close = float(row.get("close") or 0.0)
        high = float(row.get("high") or 0.0)
        volume = float(row.get("volume") or 0.0)
        if close <= 0 or high <= 0 or volume <= 0:
            return None
        ma10 = float(row.get("_hist_ma10") or 0.0)
        ma20 = float(row.get("_hist_ma20") or 0.0)
        ma30 = float(row.get("_hist_ma30") or 0.0)
        if any(math.isnan(value) for value in (ma10, ma20, ma30)):
            return None
        amount = float(row.get("amount") or close * volume)
        return {
            "close": close,
            "high": high,
            "ma10": ma10,
            "ma20": ma20,
            "ma30": ma30,
            "ma_bullish": bool(row.get("_hist_ma_bullish")),
            "pct_change": float(row.get("_hist_pct_change") or 0.0),
            "close_position": float(row.get("_hist_close_position") or 0.5),
            "nearest_ma_gap_pct": float(row.get("_hist_nearest_ma_gap_pct") or 0.0),
            "prior_impulse_pct": float(row.get("_hist_prior_impulse_pct") or 0.0),
            "volume_to_peak": float(row.get("_hist_volume_to_peak") or 1.0),
            "ret_5": float(row.get("_hist_ret_5") or 0.0),
            "ret_10": float(row.get("_hist_ret_10") or 0.0),
            "limit_like_count": float(row.get("_hist_limit_like_count") or 0.0),
            "high_20_position": float(row.get("_hist_high_20_position") or 0.0),
            "prev_high_20": float(row.get("_hist_prev_high_20") or 0.0),
            "volume_ratio_5": float(row.get("_hist_volume_ratio_5") or 1.0),
            "amount": amount,
        }

    @staticmethod
    def _quote_snapshot(symbols: list[str]) -> dict[str, dict[str, Any]]:
        if not symbols:
            return {}
        try:
            from modules.data_source_manager import DataSourceManager

            manager = DataSourceManager()
            out: dict[str, dict[str, Any]] = {}
            chunk_size = 40
            for index in range(0, len(symbols), chunk_size):
                chunk = symbols[index : index + chunk_size]
                quotes = manager.fetch_quotes(chunk)
                out.update({normalize_symbol(to_full_code(code)): quote for code, quote in quotes.items() if quote})
            return out
        except Exception as exc:
            _logger.warning("quote snapshot unavailable for swing context: %s", exc)
            return {}

    def _risk_score(self, symbol: str, *, check_risk: bool, warnings: list[str]) -> float:
        if not check_risk:
            return 4.0
        score = 4.0
        try:
            risk = self._sd.check_financial_risk(symbol)
            risk_score = float(risk.get("risk_score") or 0.0)
            if risk_score >= 30:
                warnings.append(f"财务/公告风险分 {risk_score:.0f}，不适合短线低吸")
                return -25.0
            score += 4.0
        except Exception as exc:
            warnings.append(f"财务风险检查失败: {exc}")

        try:
            announcement = self._announcement_scanner.scan(symbol, lookback_days=30, limit=20)
            announcement_score = float(announcement.get("risk_score") or 0.0)
            if announcement_score >= 30 or not bool(announcement.get("is_buyable", True)):
                warnings.append(f"公告硬风险 {announcement_score:.0f}，剔除")
                return -25.0
            score += 3.0
        except Exception as exc:
            warnings.append(f"公告风险检查失败: {exc}")
        return score

    @staticmethod
    def _action(score: float, warnings: list[str]) -> str:
        if any("剔除" in item or "不适合" in item or "不符合不追涨口径" in item for item in warnings):
            return "skip"
        if score >= 70:
            return "candidate"
        if score >= 55:
            return "watch"
        return "skip"

    @staticmethod
    def _empty_plan(symbol: str, *, name: str = "", warning: str = "") -> dict[str, Any]:
        return LowAbsorbSwingPlan(
            symbol=symbol,
            name=name or symbol,
            trade_date=datetime.now().strftime("%Y-%m-%d"),
            score=0.0,
            action="skip",
            setup="低吸隔日冲高",
            plan={"entry_low": 0.0, "entry_high": 0.0, "target_1": 0.0, "target_2": 0.0, "stop_loss": 0.0, "exit_rule": ""},
            reasons=[],
            warnings=[warning] if warning else [],
            metrics={},
        ).to_dict()

    @staticmethod
    def _trade_date(df: pd.DataFrame) -> str:
        index_value = df.index[-1]
        try:
            return pd.Timestamp(index_value).strftime("%Y-%m-%d")
        except Exception:
            return datetime.now().strftime("%Y-%m-%d")
