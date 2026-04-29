"""Multi-factor scoring engine for A-share stock selection.

Four-layer architecture:
  Layer 0: Risk veto (ST, financial risk >= 30 → score 0)
  Layer 1: Quality (50 pts) — valuation + profitability + financial health
  Layer 2: Timing (35 pts) — MA + MACD + KDJ + Bollinger/RSI
  Layer 3: Sentiment (15 pts) — fund flow + momentum
  Layer 4: Event multiplier (reserved, default 1.0)

P0 fixes baked in:
  - Negative PE/PB → valuation score forced to 0
  - Portfolio concentration → sector overlap penalty
  - Market regime → adaptive weights based on 000300 20-day MA
"""

from __future__ import annotations

import json
import logging
import pathlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd

from modules.config import cfg
from modules.indicators import IndicatorEngine
from modules.market_filters import filter_selection_universe
from modules.sector_tags import SectorTagProvider
from modules.stock_data import StockData

_logger = logging.getLogger("moatx.scoring")


@dataclass
class ScoreBreakdown:
    symbol: str
    total: float
    quality: float
    timing: float
    sentiment: float
    event_multiplier: float = 1.0
    reasons: list[str] = field(default_factory=list)
    vetoed: bool = False
    veto_reason: str = ""
    action: str = "no_buy"
    suggested_weight: float = 0.0


# ──────────────────────────────────────────
# P2: Feedback Learning
# ──────────────────────────────────────────

class ScoringFeedback:
    """Record trade outcomes to build factor performance statistics.

    Stores buy-score + realized-PnL pairs, computes per-factor win rate
    and average return, and returns adaptive weight adjustments.
    """

    def __init__(self, data_dir: str | None = None):
        import pathlib
        from modules.config import cfg
        self._path = pathlib.Path(data_dir or cfg().data.warehouse_path).parent / "scoring_feedback.json"
        self._trades: list[dict] = []
        self._factor_stats: dict[str, dict] = {}
        self._load()

    def _load(self) -> None:
        """Load existing feedback records."""
        if self._path.exists():
            try:
                data = json.loads(self._path.read_text(encoding="utf-8"))
                self._trades = data.get("trades", [])
                self._factor_stats = data.get("factor_stats", {})
            except Exception:
                self._trades = []
                self._factor_stats = {}

    def _save(self) -> None:
        """Persist feedback records to disk."""
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            self._path.write_text(
                json.dumps({
                    "trades": self._trades,
                    "factor_stats": self._factor_stats,
                }, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception as e:
            _logger.warning("反馈学习记录保存失败: %s", e)

    def record_buy(self, symbol: str, score_breakdown: ScoreBreakdown, buy_price: float) -> None:
        """Record the score at buy time for future comparison with realized PnL."""
        self._trades.append({
            "symbol": symbol,
            "buy_date": "",
            "sell_date": "",
            "buy_price": buy_price,
            "sell_price": None,
            "quality": score_breakdown.quality,
            "timing": score_breakdown.timing,
            "sentiment": score_breakdown.sentiment,
            "total": score_breakdown.total,
            "reasons": score_breakdown.reasons,
            "closed": False,
            "pnl_pct": None,
        })

    def record_sell(self, symbol: str, sell_price: float, sell_date: str) -> dict | None:
        """Close a buy record for symbol and compute realized PnL.

        Returns the updated trade record or None if no open buy found.
        """
        # Find the oldest unclosed buy for this symbol
        for t in self._trades:
            if t["symbol"] == symbol and not t["closed"]:
                t["sell_price"] = sell_price
                t["sell_date"] = sell_date
                t["closed"] = True
                if t["buy_price"] > 0:
                    t["pnl_pct"] = round((sell_price - t["buy_price"]) / t["buy_price"] * 100, 3)
                self._update_factor_stats()
                self._save()
                return t
        return None

    def _update_factor_stats(self) -> None:
        """Recompute per-factor win rate and average return from closed trades."""
        closed = [t for t in self._trades if t["closed"]]
        if len(closed) < 5:
            return  # need minimum sample

        # Group by rough factor buckets
        factor_buckets = {
            "quality_high": lambda t: t["quality"] >= 35,   # quality score >= 70% of max
            "quality_low":  lambda t: t["quality"] < 35,
            "timing_high":  lambda t: t["timing"] >= 24,   # timing >= 70% of max
            "timing_low":   lambda t: t["timing"] < 24,
            "sentiment_high": lambda t: t["sentiment"] >= 10,  # sentiment >= 70% of max
            "sentiment_low": lambda t: t["sentiment"] < 10,
        }

        stats: dict[str, dict] = {}
        for name, predicate in factor_buckets.items():
            subset = [t for t in closed if predicate(t)]
            if not subset:
                continue
            pnls = [t["pnl_pct"] for t in subset if t["pnl_pct"] is not None]
            if not pnls:
                continue
            wins = sum(1 for p in pnls if p > 0)
            stats[name] = {
                "count": len(pnls),
                "avg_pnl": round(sum(pnls) / len(pnls), 2),
                "win_rate": round(wins / len(pnls) * 100, 1),
            }

        self._factor_stats = stats
        _logger.info("因子表现更新: %s", stats)

    def get_adaptive_weights(self) -> dict[str, float]:
        """Return adaptive quality/timing/sentiment weight adjustments based on history.

        Reduces weight for underperforming factors (< 45% win rate or < 0 avg PnL).
        Returns adjustment dict to apply on top of _BASE_WEIGHTS.
        """
        if not self._factor_stats:
            return {"quality": 0.0, "timing": 0.0, "sentiment": 0.0}

        adjustments = {"quality": 0.0, "timing": 0.0, "sentiment": 0.0}

        # Quality factor bucket → quality weight
        if "quality_high" in self._factor_stats:
            s = self._factor_stats["quality_high"]
            if s["win_rate"] < 45 or s["avg_pnl"] < 0:
                adjustments["quality"] -= 0.05
            elif s["win_rate"] >= 65 and s["avg_pnl"] > 3:
                adjustments["quality"] += 0.05

        # Timing factor bucket → timing weight
        if "timing_high" in self._factor_stats:
            s = self._factor_stats["timing_high"]
            if s["win_rate"] < 45 or s["avg_pnl"] < 0:
                adjustments["timing"] -= 0.05
            elif s["win_rate"] >= 65 and s["avg_pnl"] > 3:
                adjustments["timing"] += 0.05

        # Sentiment factor bucket → sentiment weight
        if "sentiment_high" in self._factor_stats:
            s = self._factor_stats["sentiment_high"]
            if s["win_rate"] < 45 or s["avg_pnl"] < 0:
                adjustments["sentiment"] -= 0.05
            elif s["win_rate"] >= 65 and s["avg_pnl"] > 3:
                adjustments["sentiment"] += 0.05

        return adjustments

    def record_buy_score(self, symbol: str, score_breakdown: ScoreBreakdown, buy_price: float) -> None:
        """Alias for record_buy — exposed for simulation.py."""
        self.record_buy(symbol, score_breakdown, buy_price)


# Module-level shared feedback instance
_feedback: ScoringFeedback | None = None


def _get_feedback() -> ScoringFeedback:
    global _feedback
    if _feedback is None:
        _feedback = ScoringFeedback()
    return _feedback


class ScoringEngine:
    """Multi-factor scoring engine with P0 fixes baked in."""

    # Default weights (before market regime adjustment)
    _BASE_WEIGHTS = {"quality": 0.50, "timing": 0.35, "sentiment": 0.15}

    def __init__(self, sim_cfg=None):
        self._cfg = sim_cfg or cfg().simulation
        self._sd = StockData()
        self._ind = IndicatorEngine()
        self._sector_provider = SectorTagProvider()
        self._regime = None  # cached market regime
        self._industry_map = None  # cached {code: industry} reverse map
        self._northbound_set: set[str] | None = None   # HSGT northbound held stocks
        self._limitup_set: set[str] | None = None       # today's limit-up stocks

    # ──────────────────────────────────────────
    # P2: Feedback Learning — public entry points for simulation.py
    # ──────────────────────────────────────────

    def record_buy(self, symbol: str, score_row: dict, buy_price: float) -> None:
        """Record buy signal scores for future feedback analysis.

        Args:
            symbol: stock code
            score_row: dict with keys quality, timing, sentiment, total, reasons
            buy_price: execution price
        """
        try:
            sb = ScoreBreakdown(
                symbol=symbol,
                total=float(score_row.get("total", 0)),
                quality=float(score_row.get("quality", 0)),
                timing=float(score_row.get("timing", 0)),
                sentiment=float(score_row.get("sentiment", 0)),
                reasons=list(score_row.get("reasons", [])) if isinstance(score_row.get("reasons"), list) else [],
            )
            _get_feedback().record_buy(symbol, sb, buy_price)
        except Exception as e:
            _logger.warning("record_buy [%s] failed: %s", symbol, e)

    def record_sell(self, symbol: str, sell_price: float) -> dict | None:
        """Record sell execution to close the buy record and update factor stats."""
        try:
            return _get_feedback().record_sell(symbol, sell_price, datetime.now().strftime("%Y-%m-%d"))
        except Exception as e:
            _logger.warning("record_sell [%s] failed: %s", symbol, e)
            return None

    # ──────────────────────────────────────────
    # Public API
    # ──────────────────────────────────────────

    def score_batch(
        self, candidates: pd.DataFrame, existing_holdings: list[str] | None = None
    ) -> pd.DataFrame:
        """Score a batch of candidates. Returns DataFrame with score columns.

        Args:
            candidates: DataFrame with columns: code, name, price, pct_change, pe, pb, turnover
            existing_holdings: list of stock codes already held
        """
        if candidates.empty:
            return pd.DataFrame()

        df = candidates.copy()
        df["code"] = df["code"].astype(str)
        df = filter_selection_universe(df, code_col="code")
        if df.empty:
            return pd.DataFrame()

        # Determine market regime (cached)
        regime = self._detect_regime()

        # Layer 0: Veto + quality scoring (uses spot data only, fast)
        df = self._score_quality_batch(df, regime)

        # Remove vetoed stocks
        active = df[df["vetoed"] == False].copy()
        if active.empty:
            return self._attach_action_columns(self._finalize_score_output(df))

        # Layer 2: Timing (needs daily data, slow — parallelize)
        active = self._score_timing_batch(active, regime)

        # Layer 3: Sentiment
        active = self._score_sentiment_batch(active, regime)

        # Total — normalized ratio × regime weights, scaled to 0-100
        weights = self._regime_weights(regime)
        active["total"] = (
            (active["quality"] / 50 * weights["quality"] +
             active["timing"] / 35 * weights["timing"] +
             active["sentiment"] / 15 * weights["sentiment"]) * 100
        )

        # Layer 4: Event multiplier (macro news + individual announcements)
        # Apply before clip so high-conviction signals (total near 100) remain distinguishable
        active = self._apply_event_multiplier(active)

        active["total"] = active["total"].clip(0, 140).round(1)  # allow scores >100 for strong events

        # P0: Portfolio concentration penalty
        active = self._apply_concentration_penalty(active, existing_holdings)

        # Merge back, set vetoed total to 0 for stable sort
        df.loc[df["vetoed"] == True, "total"] = 0.0
        final = pd.concat(
            [df[df["vetoed"] == True], active], ignore_index=True
        ).sort_values("total", ascending=False, na_position="last")

        final = self._finalize_score_output(final)

        return self._attach_action_columns(final)

    @staticmethod
    def _finalize_score_output(df: pd.DataFrame) -> pd.DataFrame:
        """Ensure score_batch always returns a stable output schema."""
        defaults = {
            "quality": 0.0,
            "timing": 0.0,
            "sentiment": 0.0,
            "event_multiplier": 1.0,
            "total": 0.0,
            "vetoed": False,
            "veto_reason": "",
            "quality_reasons": "",
        }
        for col, default in defaults.items():
            if col not in df.columns:
                df[col] = default
            df[col] = df[col].fillna(default)

        veto_mask = df["vetoed"].astype(bool)
        if veto_mask.any():
            df.loc[veto_mask, ["quality", "timing", "sentiment", "total"]] = 0.0
            df.loc[veto_mask, "event_multiplier"] = 1.0

        return df

    @staticmethod
    def _attach_action_columns(df: pd.DataFrame) -> pd.DataFrame:
        """Attach final action protocol columns from the stable total/veto fields."""
        action_vals = []
        weight_vals = []
        for _, row in df.iterrows():
            act, wgt = ScoringEngine._action_and_weight(
                row.get("total", 0),
                bool(row.get("vetoed", False)),
            )
            action_vals.append(act)
            weight_vals.append(wgt)
        df["action"] = action_vals
        df["suggested_weight"] = weight_vals
        return df

    def score_single(self, symbol: str) -> ScoreBreakdown:
        """Deep scoring for a single held stock (used for sell decisions)."""
        try:
            q = self._sd.get_realtime_quote(symbol)
            price = float(q.get("price") or 0)
            name = q.get("name", symbol)
        except Exception:
            price = 0.0
            name = symbol

        regime = self._detect_regime()
        weights = self._regime_weights(regime)

        # Layer 0
        vetoed, veto_reason = self._check_veto(symbol)
        if vetoed:
            return ScoreBreakdown(symbol=symbol, total=0, quality=0, timing=0,
                                  sentiment=0, vetoed=True, veto_reason=veto_reason)

        # Layer 1: Quality (approximate — full fundamentals are expensive per-stock)
        quality, q_reasons = self._quality_single(symbol, price)

        # Layer 2: Timing
        timing, t_reasons = self._timing_single(symbol)

        # Layer 3: Sentiment
        sentiment, s_reasons = self._sentiment_single(symbol, price)

        total = (quality / 50 * weights["quality"] +
                 timing / 35 * weights["timing"] +
                 sentiment / 15 * weights["sentiment"]) * 100

        # Layer 4: Event multiplier
        from modules.event_driver import EventDriver
        try:
            driver = EventDriver()
            boost = driver.score_single(symbol)
            multiplier = 1.0 + boost / 100
            total *= multiplier
        except Exception:
            multiplier = 1.0

        total = max(0.0, total)  # allow >100 for strong event signals
        act, wgt = ScoringEngine._action_and_weight(total, False)

        return ScoreBreakdown(
            symbol=symbol, total=total,
            quality=quality, timing=timing, sentiment=sentiment,
            event_multiplier=multiplier,
            reasons=q_reasons + t_reasons + s_reasons,
            action=act,
            suggested_weight=wgt,
        )

    @staticmethod
    def _action_and_weight(total: float, vetoed: bool) -> tuple[str, float]:
        """Map total score to action label and suggested weight per design spec."""
        if vetoed or total <= 0:
            return "no_buy", 0.0
        elif total < 41:
            return "watch", 0.0
        elif total < 56:
            return "probe", 0.05
        elif total < 71:
            return "normal", 0.10
        elif total < 86:
            return "heavy", 0.15
        else:
            return "max_heavy", 0.20

    @staticmethod
    def _cheapness_score(market_values: pd.Series, current_value: float, max_points: float) -> tuple[float, float]:
        """Score valuation so lower positive PE/PB receives a higher score."""
        clean = pd.to_numeric(market_values, errors="coerce")
        clean = clean.where((clean > 0) & pd.notna(clean)).dropna()
        if clean.empty or current_value is None or pd.isna(current_value) or current_value <= 0:
            return 0.0, 0.0
        cheaper_or_equal_rank = float((clean <= float(current_value)).mean())
        cheapness_pct = max(0.0, min(1.0, 1.0 - cheaper_or_equal_rank))
        return round(cheapness_pct * max_points, 1), cheapness_pct

    # ──────────────────────────────────────────
    # Layer 0: Veto
    # ──────────────────────────────────────────

    def _check_veto(self, symbol: str) -> tuple[bool, str]:
        """Returns (vetoed, reason)."""
        try:
            risk = self._sd.check_financial_risk(symbol)
            if risk.get("risk_score", 0) >= 30:
                return True, f"高风险 {risk['risk_score']}分 {risk.get('risk_level','')}"
            name = risk.get("symbol", symbol)
            if "ST" in str(name) or "*ST" in str(name):
                return True, "ST 股票"
        except Exception:
            pass
        return False, ""

    # ──────────────────────────────────────────
    # Layer 1: Quality (50 pts)
    # ──────────────────────────────────────────

    def _score_quality_batch(self, df: pd.DataFrame, regime: str) -> pd.DataFrame:
        """Veto check + quality scoring (valuation + fundamentals, max 50 pts)."""
        vetoed_list = []
        veto_reasons = []

        for _, row in df.iterrows():
            sym = row["code"]
            vetoed, reason = self._check_veto(sym)
            vetoed_list.append(vetoed)
            veto_reasons.append(reason if vetoed else "")

        df["vetoed"] = vetoed_list
        df["veto_reason"] = veto_reasons

        # Only score non-vetoed stocks
        active_mask = ~df["vetoed"]
        if active_mask.sum() == 0:
            df["quality"] = 0.0
            df["timing"] = 0.0
            df["sentiment"] = 0.0
            df["event_multiplier"] = 1.0
            df["total"] = 0.0
            df["action"] = "no_buy"
            df["suggested_weight"] = 0.0
            df["quality_reasons"] = ""
            return df

        active_idx = df[active_mask].index

        # ── Valuation score (max 25 pts) — full-market ranking, same as _quality_single ──
        # Fetch full spot for market-wide percentile rank (not just candidate-internal)
        try:
            spot = self._sd.get_spot()
            spot_pe = spot["pe"].where(lambda x: (x > 0) & pd.notna(x))
            spot_pb = spot["pb"].where(lambda x: (x > 0) & pd.notna(x))
            candidate_codes = df.loc[active_mask, "code"].tolist()
            spot_index = {str(c): i for i, c in enumerate(spot["code"].astype(str))}
        except Exception:
            spot = None

        pe_values = df.loc[active_mask, "pe"].copy()
        pb_values = df.loc[active_mask, "pb"].copy()

        val_scores = pd.Series(0.0, index=df.index)
        quality_reasons = [""] * len(df)
        for idx in active_idx:
            score = 0.0
            r = []
            sym = str(df.at[idx, "code"])

            pe_val = pe_values.get(idx)
            pb_val = pb_values.get(idx)

            if pd.notna(pe_val) and pe_val > 0 and spot is not None:
                # Full-market cheapness: lower positive PE gets a higher score.
                spot_idx = spot_index.get(sym)
                if spot_idx is not None:
                    pe_market = float(spot_pe.iloc[spot_idx])
                    pe_pts, pe_pct = self._cheapness_score(spot_pe, pe_market, 15)
                    score += pe_pts
                    if pe_pts >= 10:
                        r.append(f"PE排前{int(pe_pct*100)}%")
                else:
                    # Fallback: candidate-internal cheapness.
                    pe_clean = pe_values.where(lambda x: (x > 0) & pd.notna(x), other=np.nan)
                    pe_pts, _ = self._cheapness_score(pe_clean, float(pe_val), 15)
                    score += pe_pts
            elif pd.notna(pe_val) and pe_val <= 0:
                r.append("PE为负")
            else:
                r.append("无PE数据")

            if pd.notna(pb_val) and pb_val > 0 and spot is not None:
                spot_idx = spot_index.get(sym)
                if spot_idx is not None:
                    pb_market = float(spot_pb.iloc[spot_idx])
                    pb_pts, _ = self._cheapness_score(spot_pb, pb_market, 10)
                    score += pb_pts
                else:
                    pb_clean = pb_values.where(lambda x: (x > 0) & pd.notna(x), other=np.nan)
                    pb_pts, _ = self._cheapness_score(pb_clean, float(pb_val), 10)
                    score += pb_pts
            elif pd.notna(pb_val) and pb_val <= 0:
                if r and r[0] != "无PE数据":
                    r.append("PB为负")

            val_scores[idx] = score
            quality_reasons[idx] = " / ".join(r) if r else ""

        # ── Profitability score (max 25 pts) via parallel fundamental fetches ──
        prof_scores = self._score_profitability_batch(df, active_idx)

        # ── Combine ──
        df["quality"] = (val_scores + prof_scores).clip(0, 50).round(1)
        df["quality_reasons"] = quality_reasons

        # P3: Liquidity penalty — stocks with daily turnover < 5000万 get 30% quality penalty
        if "amount" in df.columns:
            for idx in active_idx:
                amt = df.at[idx, "amount"]
                try:
                    amt = float(amt) if amt is not None and not pd.isna(amt) else None
                except Exception:
                    amt = None
                if amt is not None and amt < 50_000_000:  # 5000万 = 50,000,000 元
                    df.at[idx, "quality"] *= 0.7
                    prev = df.at[idx, "quality_reasons"]
                    df.at[idx, "quality_reasons"] = (prev + " / 流动性不足").strip(" /")

        return df

    def _score_profitability_batch(self, df: pd.DataFrame, active_idx) -> pd.Series:
        """Fetch fundamentals in parallel and score ROE/margin/cash-flow/debt (max 25 pts)."""
        symbols = df.loc[active_idx, "code"].tolist()
        prices = df.loc[active_idx, "price"].tolist()

        scores: dict[int, float] = {i: 0.0 for i in active_idx}

        with ThreadPoolExecutor(max_workers=min(len(symbols), 8)) as ex:
            futures = {
                ex.submit(self._profitability_single, sym, price): (i, sym)
                for i, (sym, price) in zip(active_idx, zip(symbols, prices))
            }
            for fut in as_completed(futures):
                idx, sym = futures[fut]
                try:
                    pts, _ = fut.result()
                    scores[idx] = pts
                except Exception:
                    scores[idx] = 0.0

        result = pd.Series(scores, dtype=float)
        return result

    def _profitability_single(self, symbol: str, price: float) -> tuple[float, str]:
        """Score ROE + margin + cash flow + debt (max 25 pts) with reason string."""
        score = 0.0
        reasons = []

        # ROE (10 pts)
        try:
            val = self._sd.get_valuation(symbol, price)
            if val:
                roe = val.get("roe")
                if roe and roe > 0:
                    if roe >= 20:
                        score += 10
                        reasons.append(f"ROE={roe:.1f}%")
                    elif roe >= 10:
                        score += 6
                    elif roe >= 5:
                        score += 3
        except Exception:
            pass

        # Gross margin (5 pts)
        try:
            ps = self._sd.get_profit_sheet_summary(symbol)
            if ps and "error" not in ps:
                gm = ps.get("gross_margin", 0)
                if gm >= 30:
                    score += 5
                    reasons.append(f"毛利率={gm:.1f}%")
                elif gm >= 15:
                    score += 3
        except Exception:
            pass

        # Free cash flow (5 pts)
        try:
            cf = self._sd.get_cash_flow_summary(symbol)
            if cf and "error" not in cf and cf.get("free_cf", 0) > 0:
                score += 5
                reasons.append("自由现金流为正")
        except Exception:
            pass

        # Debt ratio (5 pts): healthy if no warnings from risk checker
        try:
            risk = self._sd.check_financial_risk(symbol)
            if not risk.get("warnings"):
                score += 5
                reasons.append("财务健康")
        except Exception:
            pass

        return min(25.0, score), " / ".join(reasons)

    def _quality_single(self, symbol: str, price: float) -> tuple[float, list[str]]:
        """Deep quality score for a single stock (includes fundamentals)."""
        score = 0.0
        reasons = []

        # Valuation via spot — same ranking method as _score_quality_batch
        try:
            spot = self._sd.get_spot()
            row = spot[spot["code"] == symbol]
            if not row.empty:
                pe = row.iloc[0].get("pe")
                pb = row.iloc[0].get("pb")

                # P0 fix: negative PE → no valuation score
                if pd.notna(pe) and pe > 0:
                    pe_clean = spot["pe"].where(lambda x: (x > 0) & pd.notna(x))
                    pe_pts, pe_pct = self._cheapness_score(pe_clean, float(pe), 15)
                    score += pe_pts
                    if pe_pts >= 10:
                        reasons.append(f"PE排前{int(pe_pct*100)}%")
                elif pd.notna(pe) and pe <= 0:
                    reasons.append("PE为负")

                if pd.notna(pb) and pb > 0:
                    pb_clean = spot["pb"].where(lambda x: (x > 0) & pd.notna(x))
                    pb_pts, _ = self._cheapness_score(pb_clean, float(pb), 10)
                    score += pb_pts
                elif pd.notna(pb) and pb <= 0:
                    reasons.append("PB为负")
        except Exception:
            pass

        # ROE
        try:
            val = self._sd.get_valuation(symbol, price)
            if val:
                roe = val.get("roe")
                if roe and roe > 0:
                    if roe >= 20:
                        score += 10
                        reasons.append(f"ROE={roe:.1f}%")
                    elif roe >= 10:
                        score += 6
                    elif roe >= 5:
                        score += 3
        except Exception:
            pass

        # Profit margin
        try:
            ps = self._sd.get_profit_sheet_summary(symbol)
            if ps and "error" not in ps:
                gm = ps.get("gross_margin", 0)
                if gm >= 30:
                    score += 5
                    reasons.append(f"毛利率={gm:.1f}%")
                elif gm >= 15:
                    score += 3
        except Exception:
            pass

        # Free cash flow
        try:
            cf = self._sd.get_cash_flow_summary(symbol)
            if cf and "error" not in cf and cf.get("free_cf", 0) > 0:
                score += 5
                reasons.append("自由现金流为正")
        except Exception:
            pass

        # Debt ratio (already checked in risk but add quality bonus)
        try:
            risk = self._sd.check_financial_risk(symbol)
            # If risk < 30 and debt warnings empty, bonus for healthy balance sheet
            if not risk.get("warnings"):
                score += 5
                reasons.append("财务健康")
        except Exception:
            pass

        return min(50, score), reasons

    # ──────────────────────────────────────────
    # Layer 2: Timing (35 pts)
    # ──────────────────────────────────────────

    def _score_timing_batch(self, df: pd.DataFrame, regime: str) -> pd.DataFrame:
        """Score technical indicators in parallel."""
        symbols = df["code"].tolist()
        timing = {}

        with ThreadPoolExecutor(max_workers=min(len(symbols), 10)) as ex:
            futures = {ex.submit(self._timing_single, s): s for s in symbols}
            for fut in as_completed(futures):
                sym = futures[fut]
                try:
                    score, _ = fut.result()
                    timing[sym] = score
                except Exception:
                    timing[sym] = 0.0

        df["timing"] = df["code"].map(timing).fillna(0).clip(0, 35).round(1)
        return df

    def _timing_single(self, symbol: str) -> tuple[float, list[str]]:
        """Compute timing score from daily chart data."""
        try:
            df = self._sd.get_daily(symbol)
            if df.empty or len(df) < 20:
                return 0.0, ["数据不足"]
        except Exception:
            return 0.0, ["日线获取失败"]

        ind = self._ind.all_in_one(df)
        df = pd.concat([df, ind], axis=1)
        latest = df.iloc[-1]
        price = float(latest.get("close", 0))
        if price <= 0:
            return 0.0, ["价格无效"]

        score = 0.0
        reasons = []

        # MA alignment (10 pts)
        ma_score = 0
        for col in ["ma5", "ma10", "ma20", "ma60"]:
            ma_val = latest.get(col)
            if pd.notna(ma_val) and ma_val > 0:
                if price > ma_val:
                    ma_score += 2.5 if col in ("ma20", "ma60") else 1.5
        # Trend bonus: ma5 > ma10 > ma20
        ma5, ma10, ma20 = latest.get("ma5"), latest.get("ma10"), latest.get("ma20")
        if all(pd.notna(x) for x in (ma5, ma10, ma20)):
            if ma5 > ma10 > ma20:
                ma_score += 2
                reasons.append("多头排列")
        score += min(10, ma_score)

        # MACD (8 pts)
        dif = latest.get("dif")
        dea = latest.get("dea")
        macd_val = latest.get("macd")
        if all(pd.notna(x) for x in (dif, dea, macd_val)):
            macd_score = 0
            if dif > dea:
                macd_score += 3
            if macd_val > 0:
                macd_score += 2
            if len(df) >= 2:
                prev = df.iloc[-2]
                prev_dif, prev_dea = prev.get("dif"), prev.get("dea")
                if all(pd.notna(x) for x in (prev_dif, prev_dea)):
                    if prev_dif <= prev_dea and dif > dea:
                        macd_score += 3
                        reasons.append("MACD金叉")
                    elif prev_dif >= prev_dea and dif < dea:
                        macd_score -= 3
            score += max(0, min(8, macd_score))

        # KDJ (7 pts)
        j = latest.get("j")
        k = latest.get("k")
        if all(pd.notna(x) for x in (j, k)):
            kdj_score = 0
            if j < 0:
                kdj_score += 7
                reasons.append(f"KDJ深度超卖 J={j:.0f}")
            elif j < 20:
                kdj_score += 5
                reasons.append(f"KDJ超卖 J={j:.0f}")
            elif j < 40:
                kdj_score += 3
            elif j > 85:
                kdj_score -= 5
            elif j > 70:
                kdj_score -= 2
            score += max(0, min(7, kdj_score))

        # Bollinger + RSI (10 pts)
        bb_score = 0
        boll_u = latest.get("boll_upper")
        boll_l = latest.get("boll_lower")
        if all(pd.notna(x) for x in (boll_u, boll_l)) and boll_u != boll_l:
            boll_pos = (price - boll_l) / (boll_u - boll_l)
            if boll_pos < 0.1:
                bb_score += 5
                reasons.append("触及布林下轨")
            elif boll_pos < 0.3:
                bb_score += 3
            elif boll_pos > 0.9:
                bb_score -= 3

        rsi12 = latest.get("rsi12")
        if pd.notna(rsi12):
            if rsi12 < 30:
                bb_score += 5
                reasons.append(f"RSI超卖 {rsi12:.0f}")
            elif rsi12 < 40:
                bb_score += 3
            elif rsi12 > 70:
                bb_score -= 3

        score += max(0, min(10, bb_score))

        return round(min(35, score), 1), reasons

    # ──────────────────────────────────────────
    # Layer 3: Sentiment (15 pts)
    # ──────────────────────────────────────────

    def _score_sentiment_batch(self, df: pd.DataFrame, regime: str) -> pd.DataFrame:
        """Score sentiment: fund flow (parallel) + turnover + momentum (from spot).

        Per design spec:
          fund flow: >5% → +8, >0% → +4  (max 8 pts)
          turnover:  3-15% → +6, 1-3% → +3  (max 6 pts)
          momentum:  2-5% → +4, 1-2% → +2, <-5% → -3  (max 7 pts)
          A-share signals: northbound held → +2, limit-up today → +3
          Total max: 23 pts (capped at 15 for scoring consistency)
        """
        symbols = df["code"].tolist()

        # Fund flow in parallel (8 pts)
        fund_scores: dict[str, float] = {}
        with ThreadPoolExecutor(max_workers=min(len(symbols), 8)) as ex:
            futures = {ex.submit(self._fund_flow_score, s): s for s in symbols}
            for fut in as_completed(futures):
                sym = futures[fut]
                try:
                    fund_scores[sym] = fut.result()
                except Exception:
                    fund_scores[sym] = 0.0

        # Pre-cache A-share signals (session-level, reused across all candidates)
        northbound = self._ensure_northbound_set()
        limitup = self._ensure_limitup_set()

        scores = []
        for _, row in df.iterrows():
            sym = row["code"]
            code = sym.split(".")[0] if "." in sym else sym
            s = 0.0

            # Fund flow (8 pts)
            s += fund_scores.get(sym, 0.0)

            # Turnover health (6 pts)
            turnover = float(row.get("turnover") or 0)
            if 3 <= turnover <= 15:
                s += 6
            elif 1 <= turnover < 3:
                s += 3

            # Momentum (7 pts)
            pct = float(row.get("pct_change") or 0)
            if 2 <= pct <= 5:
                s += 4
            elif 1 <= pct < 2:
                s += 2
            elif pct < -5:
                s -= 3

            # A-share specific signals (P1)
            if code in northbound:
                s += 2   # 北向资金持股
            if code in limitup:
                s += 3   # 今日涨停

            scores.append(round(min(15, max(0, s)), 1))

        df["sentiment"] = scores
        return df

    def _fund_flow_score(self, symbol: str) -> float:
        """Get main fund net inflow score (0-8 pts)."""
        try:
            mf = self._sd.get_money_flow(symbol)
            inflow_pct = mf.get("main_net_inflow_pct", 0)
            if inflow_pct > 5:
                return 8.0
            elif inflow_pct > 0:
                return 4.0
        except Exception:
            pass
        return 0.0

    def _sentiment_single(self, symbol: str, price: float) -> tuple[float, list[str]]:
        """Deep sentiment score for single stock.

        Per design spec:
          fund flow: >5% → +8, >0% → +4  (max 8 pts)
          turnover:  3-15% → +6, 1-3% → +3  (max 6 pts)
          momentum:  2-5% → +4, 1-2% → +2, <-5% → -3  (max 7 pts)
          A-share signals: northbound held → +2, limit-up today → +3
        """
        score = 0.0
        reasons = []
        code = symbol.split(".")[0] if "." in symbol else symbol

        # Fund flow (8 pts)
        try:
            mf = self._sd.get_money_flow(symbol)
            inflow_pct = mf.get("main_net_inflow_pct", 0)
            if inflow_pct > 5:
                score += 8
                reasons.append(f"主力大幅流入 {inflow_pct:.1f}%")
            elif inflow_pct > 0:
                score += 4
                reasons.append("主力净流入")
        except Exception:
            pass

        # Turnover (6 pts)
        try:
            q = self._sd.get_realtime_quote(symbol)
            turnover = float(q.get("turnover") or 0)
            if 3 <= turnover <= 15:
                score += 6
                reasons.append(f"换手率健康 {turnover:.1f}%")
            elif 1 <= turnover < 3:
                score += 3
        except Exception:
            pass

        # Momentum (7 pts)
        try:
            pct = float(q.get("pct_change") or 0)
            if 2 <= pct <= 5:
                score += 4
            elif 1 <= pct < 2:
                score += 2
            elif pct < -5:
                score -= 3
        except Exception:
            pass

        # A-share specific signals (P1)
        if self._is_northbound(code):
            score += 2
            reasons.append("北向资金持股")
        if self._is_limitup(code):
            score += 3
            reasons.append("今日涨停")

        return min(15, score), reasons

    # ──────────────────────────────────────────
    # Market Regime (P0-2)
    # ──────────────────────────────────────────

    def _detect_regime(self) -> str:
        """Detect market regime from 000300 20-day MA direction."""
        if self._regime is not None:
            return self._regime

        try:
            df = self._sd.get_daily("000300", adjust="qfq")
            if df.empty or len(df) < 20:
                self._regime = "neutral"
                return self._regime

            close = df["close"]
            ma20 = close.rolling(20).mean()
            if len(ma20) < 20:
                self._regime = "neutral"
                return self._regime

            # Slope of last 5 days of MA20
            slope = (ma20.iloc[-1] - ma20.iloc[-5]) / ma20.iloc[-5]
            price_vs_ma = close.iloc[-1] / ma20.iloc[-1]

            if slope > 0.005 and price_vs_ma > 1.0:
                self._regime = "bull"
            elif slope < -0.005 and price_vs_ma < 1.0:
                self._regime = "bear"
            else:
                self._regime = "neutral"

            _logger.info("市场状态: %s (MA20斜率=%.4f 价格/MA20=%.2f)",
                         self._regime, slope, price_vs_ma)
        except Exception as e:
            _logger.warning("市场状态检测失败: %s", e)
            self._regime = "neutral"

        return self._regime

    def _regime_weights(self, regime: str) -> dict[str, float]:
        """Adaptive weights: market regime + P2 feedback learning.

        Layer order: base → regime → feedback.
        """
        w = dict(self._BASE_WEIGHTS)

        # P0-2: Market regime adjustment
        if regime == "bull":
            w["timing"] += 0.08
            w["quality"] -= 0.08
        elif regime == "bear":
            w["quality"] += 0.10
            w["timing"] -= 0.10

        # P2: Feedback learning adjustment
        try:
            fb = _get_feedback()
            adj = fb.get_adaptive_weights()
            for key in w:
                w[key] = max(0.05, w[key] + adj.get(key, 0.0))
        except Exception:
            pass

        return w

    # ──────────────────────────────────────────
    # Portfolio Concentration (P0-1)
    # ──────────────────────────────────────────

    # ──────────────────────────────────────────
    # Layer 4: Event Driver
    # ──────────────────────────────────────────

    def _apply_event_multiplier(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply event-based score multiplier from EventDriver."""
        from modules.event_driver import EventDriver
        try:
            driver = EventDriver()
            symbols = df["code"].tolist()
            boosts = driver.score_batch(symbols)
            for idx in df.index:
                sym = str(df.at[idx, "code"])
                boost = boosts.get(sym, 0)
                multiplier = 1.0 + boost / 100
                df.at[idx, "total"] *= multiplier
                df.at[idx, "event_multiplier"] = round(multiplier, 2)
        except Exception as e:
            _logger.warning("事件驱动评分失败: %s", e)
            df["event_multiplier"] = 1.0
        return df

    def _apply_concentration_penalty(
        self, df: pd.DataFrame, existing_holdings: list[str] | None
    ) -> pd.DataFrame:
        """Reduce score for stocks in already-concentrated sectors.

        Builds a full {code: industry} reverse map once (parallel THS API calls),
        then O(1) lookups per stock. Cached per ScoringEngine instance.
        """
        if not existing_holdings or df.empty:
            return df

        code_to_industry = self._build_industry_map()
        if not code_to_industry:
            return df

        # Map each held stock to its industry via O(1) lookup
        stock_industry_map: dict[str, str] = {}
        for sym in existing_holdings:
            code = sym.split(".")[0] if "." in sym else sym
            stock_industry_map[sym] = code_to_industry.get(code) or (
                "上海" if code.startswith(("6", "5", "9")) else "深圳"
            )

        # Count occurrences per industry
        from collections import Counter
        sector_count: dict[str, int] = dict(Counter(stock_industry_map.values()))

        if not sector_count:
            return df

        dominant = max(sector_count, key=sector_count.get)
        dominant_count = sector_count[dominant]

        if dominant_count >= 2:
            for idx in df.index:
                sym = str(df.at[idx, "code"])
                code = sym.split(".")[0] if "." in sym else sym
                candidate_industry = code_to_industry.get(code) or (
                    "上海" if code.startswith(("6", "5", "9")) else "深圳"
                )
                if candidate_industry == dominant:
                    penalty = dominant_count * 0.03
                    df.at[idx, "total"] *= max(0.7, 1.0 - penalty)
                    df.at[idx, "total"] = round(df.at[idx, "total"], 1)

        return df

    def _build_industry_map(self) -> dict[str, str]:
        """Build {stock_code: industry_name} reverse map via parallel THS lookups.

        Cached on self._industry_map for the lifetime of the ScoringEngine instance.
        """
        if hasattr(self, "_industry_map") and self._industry_map is not None:
            return self._industry_map

        self._industry_map = self._sector_provider.build_code_to_industry()
        _logger.info("行业映射表已构建: %d 只股票 → %d 个行业",
                     len(self._industry_map), len(set(self._industry_map.values())))
        return self._industry_map

    def _fetch_industry_members(self, industry_name: str) -> dict[str, str] | None:
        """Compatibility wrapper. Prefer SectorTagProvider.build_code_to_industry()."""
        members = self._sector_provider.get_members(industry_name, "industry")
        if members.empty or "code" not in members.columns:
            return None
        return {str(code): industry_name for code in members["code"].astype(str)}

    # ──────────────────────────────────────────
    # A-Share Specific Signals (P1)
    # ──────────────────────────────────────────

    def _ensure_northbound_set(self) -> set[str]:
        """Build and cache the set of HSGT northbound-held stock codes (O(1) lookup)."""
        if self._northbound_set is not None:
            return self._northbound_set

        try:
            import akshare as ak
            df = ak.stock_hsgt_stock_statistics_em(symbol="北向持股")
            if df is not None and not df.empty:
                self._northbound_set = set(df["股票代码"].astype(str).tolist())
                _logger.info("北向持股列表已缓存: %d 只", len(self._northbound_set))
            else:
                self._northbound_set = set()
        except Exception as e:
            _logger.warning("北向持股列表获取失败: %s", e)
            self._northbound_set = set()

        return self._northbound_set

    def _ensure_limitup_set(self) -> set[str]:
        """Build and cache today's limit-up stock codes (O(1) lookup)."""
        if self._limitup_set is not None:
            return self._limitup_set

        try:
            df = self._sd.get_limit_up()
            if df is not None and not df.empty:
                codes = df["code"].astype(str).tolist()
                self._limitup_set = set(codes)
                _logger.info("今日涨停股列表已缓存: %d 只", len(self._limitup_set))
            else:
                self._limitup_set = set()
        except Exception as e:
            _logger.warning("涨停股列表获取失败: %s", e)
            self._limitup_set = set()

        return self._limitup_set

    def _is_northbound(self, code: str) -> bool:
        """Return True if stock is held by northbound (HSGT) investors."""
        return code in self._ensure_northbound_set()

    def _is_limitup(self, code: str) -> bool:
        """Return True if stock hit limit-up today."""
        return code in self._ensure_limitup_set()
