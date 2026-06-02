"""Unified strategy fusion scanner."""

from __future__ import annotations

import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, time as day_time
from typing import Any

import pandas as pd

from modules.config import cfg
from modules.db import DatabaseManager
from modules.market_filters import filter_selection_universe
from modules.scoring_engine import ScoringEngine
from modules.stock_data import StockData
from modules.swing_low_absorb import LowAbsorbSwingEngine
from modules.utils import normalize_symbol


@dataclass
class FusionCandidate:
    symbol: str
    name: str
    score: float
    action: str
    suggested_weight: float
    components: dict[str, float] = field(default_factory=dict)
    strategy_hits: list[str] = field(default_factory=list)
    reasons: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class FusionScore:
    symbol: str
    total: float
    quality: float = 0.0
    timing: float = 0.0
    sentiment: float = 0.0
    action: str = "watch"
    suggested_weight: float = 0.0
    vetoed: bool = False
    veto_reason: str = ""


class StrategyFusionEngine:
    """Fuse short-term, event, multifactor, and technical strategy signals."""

    _STRATEGY_UNITS: tuple[dict[str, str], ...] = (
        {"id": "swing_low_absorb", "group": "短线形态", "name": "低吸隔日冲高"},
        {"id": "swing_trend_continuation", "group": "短线形态", "name": "强趋势延续"},
        {"id": "swing_breakout_ignition", "group": "短线形态", "name": "放量突破首日"},
        {"id": "swing_bearish_reversal", "group": "短线形态", "name": "阴线低吸反包"},
        {"id": "tail_close_buy", "group": "短线执行", "name": "尾盘收盘买入"},
        {"id": "multi_quality", "group": "综合多因子", "name": "质量/估值"},
        {"id": "multi_timing", "group": "综合多因子", "name": "择时/技术"},
        {"id": "multi_sentiment", "group": "综合多因子", "name": "情绪/资金"},
        {"id": "news_event", "group": "新闻事件", "name": "新闻事件机会"},
        {"id": "ma_cross", "group": "经典技术", "name": "均线金叉"},
        {"id": "mean_reversion", "group": "经典技术", "name": "布林均值回归"},
        {"id": "trend_following", "group": "经典技术", "name": "趋势跟踪"},
        {"id": "n_day_breakout", "group": "经典技术", "name": "N日突破"},
        {"id": "ma_volume", "group": "经典技术", "name": "均线成交量确认"},
        {"id": "contrarian_rebound", "group": "经典技术", "name": "逆向反弹"},
        {"id": "sector_rotation", "group": "经典技术", "name": "行业轮动"},
        {"id": "momentum_reversal", "group": "经典技术", "name": "动量反转双模"},
        {"id": "intraday_radar", "group": "盘中雷达", "name": "盘中异动雷达"},
    )

    def __init__(
        self,
        *,
        stock_data: Any | None = None,
        swing_engine: LowAbsorbSwingEngine | None = None,
        scoring_engine: ScoringEngine | None = None,
    ) -> None:
        self._sd = stock_data or StockData()
        self._swing = swing_engine or LowAbsorbSwingEngine(stock_data=self._sd)
        self._scoring = scoring_engine or ScoringEngine()

    def scan(
        self,
        *,
        limit: int = 10,
        pool_limit: int = 120,
        score_pool_limit: int = 80,
        min_score: float = 35.0,
        workers: int = 8,
        include_intraday: bool = False,
        use_event_context: bool = False,
        deep_score: bool = False,
        deadline_seconds: float | None = 120.0,
        allow_breakout: bool = True,
        mode: str = "fast",
        intraday_limit: int | None = None,
    ) -> dict[str, Any]:
        mode = self._normalize_mode(mode)
        if mode in {"tail", "full"}:
            include_intraday = True
        if mode == "full":
            use_event_context = True
            deep_score = True
        intraday_limit = self._intraday_limit(mode, intraday_limit)

        started = time.monotonic()
        last_mark = started
        timings: dict[str, float] = {}

        def mark(name: str) -> None:
            nonlocal last_mark
            now = time.monotonic()
            timings[name] = round(now - last_mark, 3)
            last_mark = now

        base_pool = self._base_pool(max(pool_limit, score_pool_limit))
        mark("base_pool")
        swing_rows = self._swing.candidates(
            limit=max(int(limit or 1) * 5, 50),
            pool_limit=pool_limit,
            check_risk=False,
            workers=workers,
            deadline_seconds=deadline_seconds,
            network_daily_fallback=False,
            allow_breakout=allow_breakout,
            use_event_context=use_event_context,
        )
        mark("swing")
        swing_meta = dict(getattr(self._swing, "_last_candidates_meta", {}) or {})
        event_rows = self._event_opportunities(limit=max(50, int(limit or 1) * 5))
        mark("event")
        symbols = self._candidate_symbols(base_pool, swing_rows, event_rows, score_pool_limit=score_pool_limit)

        spot_by_code = self._spot_by_code(base_pool)
        score_map = self._multi_factor_scores(symbols, spot_by_code=spot_by_code, deep_score=deep_score)
        mark("multi_factor")
        event_map = self._event_map(event_rows)
        swing_map = {normalize_symbol(str(row.get("symbol") or "")): row for row in swing_rows}
        technical_map = self._technical_votes(symbols)
        mark("technical")
        intraday_targets = self._intraday_targets(
            symbols,
            score_map,
            swing_map,
            event_map,
            technical_map,
            min_score=min_score,
            limit=intraday_limit,
        )
        intraday_map = self._intraday_scores(intraday_targets, include_intraday=include_intraday, workers=workers)
        mark("intraday")

        rows: list[FusionCandidate] = []
        for symbol in symbols:
            name = self._name_for(symbol, spot_by_code, swing_map, event_map)
            score_breakdown = score_map.get(symbol)
            swing = swing_map.get(symbol, {})
            event = event_map.get(symbol, {})
            technical = technical_map.get(symbol, {})
            intraday = intraday_map.get(symbol, {})
            components = self._components(score_breakdown, swing, event, technical, intraday)
            total = round(sum(components.values()), 1)
            if total < min_score:
                continue
            reasons, warnings, hits = self._explain(score_breakdown, swing, event, technical, intraday)
            rows.append(
                FusionCandidate(
                    symbol=symbol,
                    name=name,
                    score=total,
                    action=self._action(total, warnings),
                    suggested_weight=self._weight(total, warnings),
                    components=components,
                    strategy_hits=hits[:10],
                    reasons=reasons[:10],
                    warnings=warnings[:8],
                    metrics={
                        "multi_factor_total": round(float(getattr(score_breakdown, "total", 0.0) or 0.0), 3),
                        "swing_score": round(float(swing.get("score") or 0.0), 3),
                        "event_score": round(float(event.get("opportunity_score") or 0.0), 3),
                        "technical_score": round(float(technical.get("score") or 0.0), 3),
                        "intraday_score": round(float(intraday.get("score") or 0.0), 3),
                    },
                )
            )

        rows.sort(key=lambda item: (item.score, item.suggested_weight), reverse=True)
        selected = rows[: max(1, int(limit or 1))]
        return {
            "engine": "strategy_fusion_v1",
            "generated_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "strategy_scope": self._strategy_scope(mode=mode, include_intraday=include_intraday),
            "summary": {
                "mode": mode,
                "count": len(selected),
                "source_symbols": len(symbols),
                "base_pool": len(base_pool),
                "swing_source": len(swing_rows),
                "event_source": len(event_rows),
                "min_score": float(min_score),
                "include_intraday": bool(include_intraday),
                "intraday_targets": len(intraday_targets) if include_intraday else 0,
                "score_mode": "deep" if deep_score else "fast_proxy",
                "elapsed_seconds": round(time.monotonic() - started, 3),
                "timings": timings,
                "swing_scan": swing_meta,
            },
            "candidates": [item.to_dict() for item in selected],
        }

    @classmethod
    def _strategy_scope(cls, *, mode: str, include_intraday: bool) -> dict[str, Any]:
        enabled: list[dict[str, str]] = []
        disabled: list[dict[str, str]] = []
        for unit in cls._STRATEGY_UNITS:
            item = dict(unit)
            if item["id"] == "intraday_radar" and not include_intraday:
                disabled.append(item)
            elif item["id"] == "tail_close_buy" and mode not in {"tail", "full"}:
                disabled.append(item)
            else:
                enabled.append(item)
        groups: dict[str, list[str]] = {}
        for item in enabled:
            groups.setdefault(item["group"], []).append(item["name"])
        return {
            "total_units": len(cls._STRATEGY_UNITS),
            "enabled_units": len(enabled),
            "disabled_units": len(disabled),
            "units": enabled,
            "disabled": disabled,
            "groups": groups,
            "fused_groups": [
                "短线低吸/趋势/突破/反包" + ("/尾盘执行" if mode in {"tail", "full"} else ""),
                "综合多因子评分",
                "新闻事件机会",
                "经典技术策略投票",
                "盘中异动雷达" if include_intraday else "盘中异动雷达(本次未启用)",
            ],
            "classic_technical_models": [
                "均线金叉",
                "布林均值回归",
                "趋势跟踪",
                "N日突破",
                "均线成交量确认",
                "逆向反弹",
                "行业轮动",
                "动量反转双模",
            ],
        }

    @staticmethod
    def _normalize_mode(mode: str) -> str:
        value = str(mode or "fast").strip().lower()
        if value in {"fast", "tail", "full"}:
            return value
        return "fast"

    @staticmethod
    def _intraday_limit(mode: str, value: int | None) -> int:
        if value is not None:
            return max(1, int(value))
        if mode == "full":
            return 24
        return 8

    def _base_pool(self, pool_limit: int) -> pd.DataFrame:
        spot = self._spot_cache_snapshot(max_age_seconds=600)
        if spot.empty and self._is_off_live_window():
            spot = self._spot_cache_snapshot(max_age_seconds=None)
        try:
            if spot.empty:
                spot = self._sd.get_spot(use_cache=True)
        except Exception:
            spot = self._spot_cache_snapshot(max_age_seconds=None)
        if spot is None or spot.empty:
            return pd.DataFrame()
        df = spot.copy()
        df = filter_selection_universe(df, code_col="code")
        if "code" in df.columns:
            df["code"] = df["code"].map(normalize_symbol)
        for column in ("price", "pct_change", "amount", "turnover", "pe", "pb"):
            if column in df.columns:
                df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0.0)
        if "price" in df.columns:
            df = df[df["price"] > 0]
        if "amount" in df.columns:
            df = df[df["amount"] >= 50_000_000]
        sort_cols = [col for col in ("amount", "pct_change") if col in df.columns]
        if sort_cols:
            df = df.sort_values(sort_cols, ascending=[False] * len(sort_cols))
        return df.head(max(1, int(pool_limit or 1))).copy()

    @staticmethod
    def _spot_cache_snapshot(*, max_age_seconds: int | None) -> pd.DataFrame:
        try:
            from modules.crawler import cache

            cached = cache.read_df_cache("spot_sina", max_age_seconds=max_age_seconds)
            if cached.data is not None and not cached.data.empty:
                return cached.data.copy()
        except Exception:
            pass
        return pd.DataFrame()

    @staticmethod
    def _is_off_live_window() -> bool:
        now = datetime.now().time()
        return now < day_time(9, 20) or now >= day_time(15, 5)

    @staticmethod
    def _spot_by_code(spot: pd.DataFrame) -> dict[str, dict[str, Any]]:
        if spot is None or spot.empty or "code" not in spot.columns:
            return {}
        out: dict[str, dict[str, Any]] = {}
        for _, row in spot.iterrows():
            code = normalize_symbol(str(row.get("code") or ""))
            if code:
                out[code] = row.to_dict()
        return out

    def _event_opportunities(self, *, limit: int) -> pd.DataFrame:
        try:
            db = DatabaseManager(cfg().data.warehouse_path)
            return db.event().list_opportunities(limit=limit)
        except Exception:
            return pd.DataFrame()

    @staticmethod
    def _candidate_symbols(
        base_pool: pd.DataFrame,
        swing_rows: list[dict[str, Any]],
        event_rows: pd.DataFrame,
        *,
        score_pool_limit: int,
    ) -> list[str]:
        symbols: list[str] = []

        def add(raw: Any) -> None:
            code = normalize_symbol(str(raw or ""))
            if code and code not in symbols:
                symbols.append(code)

        if base_pool is not None and not base_pool.empty:
            for code in base_pool.get("code", []).head(max(1, int(score_pool_limit or 1))):
                add(code)
        for row in swing_rows:
            add(row.get("symbol"))
        if event_rows is not None and not event_rows.empty:
            for code in event_rows.get("symbol", []):
                add(code)
        return symbols

    def _multi_factor_scores(
        self,
        symbols: list[str],
        *,
        spot_by_code: dict[str, dict[str, Any]],
        deep_score: bool,
    ) -> dict[str, Any]:
        if deep_score:
            try:
                return self._scoring.score_symbols(symbols)
            except Exception:
                return {}
        return {symbol: self._fast_score(symbol, spot_by_code.get(symbol, {})) for symbol in symbols}

    @staticmethod
    def _fast_score(symbol: str, row: dict[str, Any]) -> FusionScore:
        price = StrategyFusionEngine._num(row.get("price"))
        pe = StrategyFusionEngine._num(row.get("pe"))
        pb = StrategyFusionEngine._num(row.get("pb"))
        pct = StrategyFusionEngine._num(row.get("pct_change"))
        amount = StrategyFusionEngine._num(row.get("amount"))
        turnover = StrategyFusionEngine._num(row.get("turnover"))

        quality = 0.0
        if 0 < pe <= 25:
            quality += 24
        elif 25 < pe <= 45:
            quality += 16
        elif pe > 0:
            quality += 8
        if 0 < pb <= 3:
            quality += 18
        elif 3 < pb <= 6:
            quality += 10
        elif pb > 0:
            quality += 4

        timing = 0.0
        if -4.0 <= pct <= 1.5:
            timing += 18
        elif 1.5 < pct <= 5.5:
            timing += 16
        elif 5.5 < pct <= 8.5:
            timing += 10
        elif pct < -6 or pct > 9:
            timing -= 6
        if 0.5 <= turnover <= 8:
            timing += 8
        elif turnover > 8:
            timing += 4

        sentiment = 0.0
        if amount >= 1_000_000_000:
            sentiment += 15
        elif amount >= 300_000_000:
            sentiment += 11
        elif amount >= 80_000_000:
            sentiment += 7
        if price <= 0:
            return FusionScore(symbol=symbol, total=0.0, vetoed=True, veto_reason="价格无效")

        total = min(100.0, max(0.0, quality + timing + sentiment))
        if total >= 70:
            action, weight = "candidate", 0.08
        elif total >= 50:
            action, weight = "watch", 0.03
        else:
            action, weight = "no_buy", 0.0
        return FusionScore(
            symbol=symbol,
            total=round(total, 1),
            quality=round(quality, 1),
            timing=round(timing, 1),
            sentiment=round(sentiment, 1),
            action=action,
            suggested_weight=weight,
        )

    @staticmethod
    def _num(value: Any) -> float:
        try:
            if pd.isna(value):
                return 0.0
            return float(value)
        except Exception:
            return 0.0

    @staticmethod
    def _event_map(event_rows: pd.DataFrame) -> dict[str, dict[str, Any]]:
        if event_rows is None or event_rows.empty:
            return {}
        out: dict[str, dict[str, Any]] = {}
        for _, row in event_rows.iterrows():
            code = normalize_symbol(str(row.get("symbol") or ""))
            if not code:
                continue
            current = out.get(code)
            item = row.to_dict()
            if current is None or float(item.get("opportunity_score") or 0.0) > float(current.get("opportunity_score") or 0.0):
                out[code] = item
        return out

    def _technical_votes(self, symbols: list[str]) -> dict[str, dict[str, Any]]:
        daily_map = self._swing._daily_cache_snapshot(symbols)
        out: dict[str, dict[str, Any]] = {}
        for symbol, daily in daily_map.items():
            out[symbol] = self._technical_score(daily)
        return out

    @staticmethod
    def _technical_score(daily: pd.DataFrame | None) -> dict[str, Any]:
        if daily is None or daily.empty or len(daily) < 30:
            return {"score": 0.0, "hits": [], "warnings": ["日线不足，经典技术策略未评分"]}
        df = daily.copy()
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df = df.dropna(subset=["date"]).set_index("date")
        for column in ("open", "high", "low", "close", "volume"):
            if column in df.columns:
                df[column] = pd.to_numeric(df[column], errors="coerce")
        df = df.dropna(subset=["close"]).sort_index()
        if len(df) < 30:
            return {"score": 0.0, "hits": [], "warnings": ["日线不足，经典技术策略未评分"]}
        close = df["close"]
        high = pd.to_numeric(df.get("high", close), errors="coerce")
        low = pd.to_numeric(df.get("low", close), errors="coerce")
        volume = pd.to_numeric(df.get("volume", pd.Series(0, index=df.index)), errors="coerce").fillna(0.0)
        last = float(close.iloc[-1])
        ma5 = float(close.rolling(5).mean().iloc[-1])
        ma10 = float(close.rolling(10).mean().iloc[-1])
        ma20 = float(close.rolling(20).mean().iloc[-1])
        ma20_prev = float(close.rolling(20).mean().iloc[-5]) if len(close) >= 25 else ma20
        std20 = float(close.rolling(20).std().iloc[-1])
        upper = ma20 + 2 * std20
        lower = ma20 - 2 * std20
        rsi = StrategyFusionEngine._rsi(close)
        ret10 = (last / float(close.iloc[-11]) - 1.0) * 100 if len(close) >= 11 and close.iloc[-11] else 0.0
        high20 = float(high.tail(20).max())
        vol_ma20 = float(volume.rolling(20).mean().iloc[-1] or 0.0)
        vol_ratio = float(volume.iloc[-1]) / vol_ma20 if vol_ma20 > 0 else 0.0
        bb_width = upper - lower
        bb_pos = (last - lower) / bb_width if bb_width > 0 else 0.5
        hits: list[str] = []
        warnings: list[str] = []
        score = 0.0
        if ma5 > ma20 and last >= ma5:
            score += 12
            hits.append("均线金叉/多头短线")
        if last <= lower * 1.01 or rsi <= 30:
            score += 10
            hits.append("布林/RSI均值回归")
        if ma20 > ma20_prev and last > ma20:
            score += 12
            hits.append("趋势跟踪")
        if high20 > 0 and last >= high20 * 0.98:
            score += 10
            hits.append("N日突破")
        if ma5 > ma20 and vol_ratio >= 1.2:
            score += 10
            hits.append("均线成交量确认")
        if rsi <= 35 and bb_pos <= 0.45:
            score += 10
            hits.append("逆向反弹")
        if ret10 >= 5.0 and rsi < 70:
            score += 10
            hits.append("动量模式")
        elif rsi <= 45 and bb_pos <= 0.4:
            score += 8
            hits.append("反转模式")
        if last < ma20 and ma20 < ma20_prev:
            warnings.append("中期均线向下，技术票需防弱反弹")
        return {"score": round(min(100.0, score), 1), "hits": hits, "warnings": warnings}

    @staticmethod
    def _rsi(close: pd.Series, period: int = 14) -> float:
        delta = close.diff()
        gain = delta.clip(lower=0).rolling(period).mean()
        loss = (-delta.clip(upper=0)).rolling(period).mean()
        rsi = 100 - (100 / (1 + gain / loss.replace(0, 1e-10)))
        value = rsi.iloc[-1] if len(rsi) else 50
        return float(value) if pd.notna(value) else 50.0

    def _intraday_scores(self, symbols: list[str], *, include_intraday: bool, workers: int) -> dict[str, dict[str, Any]]:
        if not include_intraday or not symbols:
            return {}
        try:
            from modules.intraday_radar.service import IntradayRadarService

            payload = IntradayRadarService().scan(symbols=symbols, workers=workers)
            out: dict[str, dict[str, Any]] = {}
            for signal in payload.get("signals") or []:
                code = normalize_symbol(str(signal.get("symbol") or ""))
                if code:
                    out[code] = signal
            return out
        except Exception:
            return {}

    @classmethod
    def _intraday_targets(
        cls,
        symbols: list[str],
        score_map: dict[str, Any],
        swing_map: dict[str, dict[str, Any]],
        event_map: dict[str, dict[str, Any]],
        technical_map: dict[str, dict[str, Any]],
        *,
        min_score: float,
        limit: int,
    ) -> list[str]:
        ranked: list[tuple[float, str]] = []
        for symbol in symbols:
            base_components = cls._components(
                score_map.get(symbol),
                swing_map.get(symbol, {}),
                event_map.get(symbol, {}),
                technical_map.get(symbol, {}),
                {},
            )
            ranked.append((sum(base_components.values()), symbol))
        ranked.sort(key=lambda item: item[0], reverse=True)
        selected: list[str] = []
        for base_score, symbol in ranked:
            if len(selected) >= limit:
                break
            if base_score >= float(min_score) - 10.0 or len(selected) < max(5, limit // 2):
                selected.append(symbol)
        return selected

    @staticmethod
    def _name_for(
        symbol: str,
        spot_by_code: dict[str, dict[str, Any]],
        swing_map: dict[str, dict[str, Any]],
        event_map: dict[str, dict[str, Any]],
    ) -> str:
        for source in (spot_by_code.get(symbol), swing_map.get(symbol), event_map.get(symbol)):
            if source:
                name = str(source.get("name") or "")
                if name:
                    return name
        return symbol

    @staticmethod
    def _components(score_breakdown: Any, swing: dict[str, Any], event: dict[str, Any], technical: dict[str, Any], intraday: dict[str, Any]) -> dict[str, float]:
        multi = min(100.0, max(0.0, float(getattr(score_breakdown, "total", 0.0) or 0.0))) / 100.0 * 35.0
        swing_score = max(0.0, float(swing.get("score") or 0.0))
        swing_action = str(swing.get("action") or "")
        if swing_action == "candidate":
            swing_component = min(25.0, swing_score / 100.0 * 25.0)
        elif swing_action == "watch":
            swing_component = min(22.0, swing_score / 100.0 * 25.0)
        else:
            swing_component = 0.0
        event_component = min(15.0, max(0.0, float(event.get("opportunity_score") or 0.0)) / 100.0 * 15.0)
        technical_component = min(15.0, max(0.0, float(technical.get("score") or 0.0)) / 100.0 * 15.0)
        intraday_component = min(10.0, max(0.0, float(intraday.get("score") or 0.0)) / 100.0 * 10.0)
        return {
            "multi_factor": round(multi, 2),
            "swing": round(swing_component, 2),
            "event": round(event_component, 2),
            "technical": round(technical_component, 2),
            "intraday": round(intraday_component, 2),
        }

    @staticmethod
    def _explain(score_breakdown: Any, swing: dict[str, Any], event: dict[str, Any], technical: dict[str, Any], intraday: dict[str, Any]) -> tuple[list[str], list[str], list[str]]:
        reasons: list[str] = []
        warnings: list[str] = []
        hits: list[str] = []
        total = float(getattr(score_breakdown, "total", 0.0) or 0.0)
        if total > 0:
            reasons.append(f"综合多因子评分 {total:.1f}，动作 {getattr(score_breakdown, 'action', 'unknown')}")
            hits.append("综合多因子")
        if bool(getattr(score_breakdown, "vetoed", False)):
            warnings.append(f"多因子硬否决：{getattr(score_breakdown, 'veto_reason', '')}")
        if swing:
            reasons.append(f"短线形态：{swing.get('setup', '')}，分 {float(swing.get('score') or 0):.1f}")
            hits.append(str(swing.get("setup") or "短线模型"))
            warnings.extend(str(item) for item in (swing.get("warnings") or [])[:2])
        if event:
            reasons.append(f"新闻事件机会分 {float(event.get('opportunity_score') or 0):.1f}，{event.get('recommendation', '')}")
            hits.append("新闻事件")
        tech_hits = [str(item) for item in technical.get("hits", [])]
        if tech_hits:
            reasons.append("经典技术投票：" + "、".join(tech_hits[:4]))
            hits.extend(tech_hits)
        warnings.extend(str(item) for item in technical.get("warnings", [])[:2])
        if intraday:
            reasons.append(f"盘中异动雷达 {intraday.get('level', '')}，分 {float(intraday.get('score') or 0):.1f}")
            hits.append("盘中异动雷达")
            warnings.extend(str(item) for item in (intraday.get("warnings") or [])[:2])
        return reasons, list(dict.fromkeys(warnings)), list(dict.fromkeys(hits))

    @staticmethod
    def _action(score: float, warnings: list[str]) -> str:
        text = "；".join(warnings)
        if "硬否决" in text:
            return "不买"
        if score >= 75:
            return "强候选"
        if score >= 60:
            return "候选"
        if score >= 35:
            return "观察"
        return "不买"

    @staticmethod
    def _weight(score: float, warnings: list[str]) -> float:
        if "硬否决" in "；".join(warnings):
            return 0.0
        if score >= 75:
            return 0.10
        if score >= 60:
            return 0.05
        if score >= 45:
            return 0.02
        if score >= 35:
            return 0.01
        return 0.0
