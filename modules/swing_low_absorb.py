"""Low-absorb next-day swing scanner.

The model looks for main-board stocks that pulled back into a compact
MA10/MA20/MA30 support zone after a recent impulse move. It is a short-term
volatility setup, not a long-term buy rating.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from datetime import datetime
import math
import logging
from pathlib import Path
from typing import Any

import pandas as pd

from modules.announcement_risk import AnnouncementRiskScanner
from modules.config import tomllib
from modules.market_filters import filter_selection_universe, is_excluded_selection_board
from modules.utils import normalize_symbol, to_full_code


_logger = logging.getLogger("moatx.swing_low_absorb")


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
            risk_adjust=risk_adjust,
            quote_bar_appended=quote_bar_appended,
        )
        if trend_profile:
            profiles.append(trend_profile)
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

        score = round(max(0.0, min(100.0, score)), 1)
        action = self._action(score, warnings)
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
                **sector_metrics,
            },
        ).to_dict()

    def candidates(
        self,
        *,
        limit: int = 20,
        pool_limit: int = 80,
        check_risk: bool = False,
        workers: int = 8,
    ) -> list[dict[str, Any]]:
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
        df = self._rank_prefilter_pool(df, sector_context)
        review_df = self._select_prefilter_pool(df, pool_limit)
        candidates = []
        for _, row in review_df.iterrows():
            code = normalize_symbol(str(row.get("code") or ""))
            if not code:
                continue
            candidates.append((code, str(row.get("name") or code)))

        quotes = self._quote_snapshot([code for code, _ in candidates])
        rows: list[dict[str, Any]] = []
        max_workers = max(1, min(int(workers or 1), 4, len(candidates) or 1))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    self.analyze,
                    code,
                    name=name,
                    quote=quotes.get(code),
                    check_risk=check_risk,
                    market_context=market_context,
                    sector_context=sector_context,
                ): code
                for code, name in candidates
            }
            for future in as_completed(futures):
                try:
                    plan = future.result()
                except Exception as exc:
                    _logger.warning("low-absorb scan failed for %s: %s", futures[future], exc)
                    continue
                if plan["action"] != "skip":
                    rows.append(plan)

        rows.sort(key=lambda item: item["score"], reverse=True)
        return rows[:limit]

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
    def _rank_prefilter_pool(df: pd.DataFrame, sector_context: dict[str, Any] | None = None) -> pd.DataFrame:
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

        ranked["_prefilter_score"] = (
            amount_score * 0.55
            + setup_score * 0.25
            + turnover_score * 0.12
            + sector_score * 0.08
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
            "win_rate_pct": 0.0,
            "avg_next_return_pct": 0.0,
            "median_next_return_pct": 0.0,
            "score_adjust": 0.0,
            "confidence": "none",
        }
        if df is None or len(df) < 36 or current_close <= 0:
            return base

        data = df.copy()
        for column in ("open", "high", "low", "close", "volume", "amount", "turn", "turnover"):
            if column in data.columns:
                data[column] = pd.to_numeric(data[column], errors="coerce")
        data = data.dropna(subset=["open", "high", "low", "close", "volume"])
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
            target_hit = next_high >= target_price
            stop_hit = next_low <= stop_price
            if target_hit and not stop_hit:
                realized = target_pct
            elif stop_hit:
                realized = stop_pct
            else:
                realized = next_close / buy - 1.0
            trades.append(
                {
                    "target_hit": target_hit,
                    "stop_hit": stop_hit,
                    "return_pct": realized * 100,
                }
            )

        sample_count = len(trades)
        if sample_count == 0:
            return base
        returns = pd.Series([float(item["return_pct"]) for item in trades])
        target_hit_rate = sum(bool(item["target_hit"]) for item in trades) / sample_count * 100
        stop_hit_rate = sum(bool(item["stop_hit"]) for item in trades) / sample_count * 100
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
            if avg_return <= -0.4 or stop_hit_rate >= 50:
                score_adjust -= 3.0
            elif target_hit_rate <= 35 and avg_return <= 0.2:
                score_adjust -= 2.0

        return {
            **base,
            "sample_count": sample_count,
            "target_hit_rate_pct": round(target_hit_rate, 1),
            "stop_hit_rate_pct": round(stop_hit_rate, 1),
            "win_rate_pct": round(win_rate, 1),
            "avg_next_return_pct": round(avg_return, 3),
            "median_next_return_pct": round(median_return, 3),
            "score_adjust": round(score_adjust, 1),
            "confidence": confidence,
        }

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
        prev = df.iloc[idx - 1]
        close = float(row.get("close") or 0.0)
        high = float(row.get("high") or 0.0)
        low = float(row.get("low") or 0.0)
        prev_close = float(prev.get("close") or 0.0)
        volume = float(row.get("volume") or 0.0)
        if close <= 0 or high <= 0 or low <= 0 or prev_close <= 0 or volume <= 0:
            return None

        closes = pd.to_numeric(df["close"].iloc[: idx + 1], errors="coerce")
        highs = pd.to_numeric(df["high"].iloc[: idx + 1], errors="coerce")
        volumes = pd.to_numeric(df["volume"].iloc[: idx + 1], errors="coerce")
        ma10 = float(closes.rolling(10).mean().iloc[-1])
        ma20 = float(closes.rolling(20).mean().iloc[-1])
        ma30 = float(closes.rolling(30).mean().iloc[-1])
        if any(math.isnan(value) for value in (ma10, ma20, ma30)):
            return None

        pct_change = (close / prev_close - 1.0) * 100
        close_position = (close - low) / (high - low) if high > low else 0.5
        nearest_ma = min((ma10, ma20, ma30), key=lambda value: abs(close - value))
        nearest_ma_gap_pct = abs(close - nearest_ma) / close * 100
        recent = df.iloc[max(0, idx - 20) : idx].copy()
        recent_peak_volume = float(pd.to_numeric(recent.get("volume"), errors="coerce").max()) if not recent.empty else 0.0
        volume_to_peak = volume / recent_peak_volume if recent_peak_volume > 0 else 1.0
        recent_pct = closes.pct_change() * 100
        prior_impulse_pct = float(recent_pct.iloc[max(0, len(recent_pct) - 21) : len(recent_pct) - 1].max(skipna=True))
        limit_like_count = int((recent_pct.tail(10) >= 9.2).sum())
        ret_5 = (close / float(closes.iloc[-6]) - 1.0) * 100 if len(closes) >= 6 and closes.iloc[-6] else 0.0
        ret_10 = (close / float(closes.iloc[-11]) - 1.0) * 100 if len(closes) >= 11 and closes.iloc[-11] else 0.0
        high_20 = float(highs.tail(20).max())
        high_20_position = close / high_20 if high_20 > 0 else 0.0
        prev_high_20 = float(highs.iloc[max(0, len(highs) - 21) : len(highs) - 1].max())
        avg_volume_5 = float(volumes.iloc[max(0, len(volumes) - 6) : len(volumes) - 1].mean())
        volume_ratio_5 = volume / avg_volume_5 if avg_volume_5 > 0 else 1.0
        amount = float(row.get("amount") or close * volume)
        return {
            "close": close,
            "high": high,
            "ma10": ma10,
            "ma20": ma20,
            "ma30": ma30,
            "ma_bullish": ma10 > ma20 > ma30 or ma10 > ma20,
            "pct_change": pct_change,
            "close_position": close_position,
            "nearest_ma_gap_pct": nearest_ma_gap_pct,
            "prior_impulse_pct": prior_impulse_pct,
            "volume_to_peak": volume_to_peak,
            "ret_5": ret_5,
            "ret_10": ret_10,
            "limit_like_count": float(limit_like_count),
            "high_20_position": high_20_position,
            "prev_high_20": prev_high_20,
            "volume_ratio_5": volume_ratio_5,
            "amount": amount,
        }

    @staticmethod
    def _quote_snapshot(symbols: list[str]) -> dict[str, dict[str, Any]]:
        if not symbols:
            return {}
        try:
            from modules.datasource import QuoteManager

            manager = QuoteManager(source_names=["sina", "tencent", "eastmoney"], mode="single")
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
