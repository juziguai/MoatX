"""Generate A-share opportunities from macro event states."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from modules.config import cfg
from modules.db import DatabaseManager
from modules.market_filters import is_excluded_selection_board
from modules.sector_tags import SectorTagProvider
from modules.stock_data import StockData

from .models import EventOpportunity
from .transmission import EventTransmissionMap

_logger = logging.getLogger("moatx.event_intelligence.opportunity")


class EventOpportunityScanner:
    """Scan event-driven A-share opportunities from active event states."""

    def __init__(
        self,
        db: DatabaseManager | None = None,
        transmission_map: EventTransmissionMap | None = None,
        sector_provider: SectorTagProvider | None = None,
    ):
        self._db = db or DatabaseManager(cfg().data.warehouse_path)
        self._map = transmission_map or EventTransmissionMap()
        self._sector_provider = sector_provider or SectorTagProvider()
        self._sd = StockData()
        self._spot_cache: pd.DataFrame | None = None

    def scan(
        self,
        min_probability: float = 0.35,
        per_effect_limit: int = 20,
    ) -> dict[str, Any]:
        """Generate and persist stock opportunities for active event states."""
        states = self._db.event().list_states(limit=100)
        stats = {"states": len(states), "opportunities": 0, "refreshed_events": 0, "deleted_old": 0, "errors": []}
        if states.empty:
            return stats

        for _, state in states.iterrows():
            probability = float(state.get("probability") or 0)
            if probability < min_probability:
                continue

            event_id = str(state.get("event_id"))
            definition = self._map.get(event_id)
            if definition is None:
                continue

            old_count = self._db.event().delete_opportunities(event_id)
            stats["deleted_old"] += old_count
            stats["refreshed_events"] += 1
            best: dict[tuple[str, str], EventOpportunity] = {}
            for effect in definition.effects:
                if effect.direction != "bullish":
                    continue
                if effect.target_type not in ("sector", "concept"):
                    continue
                try:
                    members = self._fetch_members(effect.target, effect.target_type)
                    for member in members.head(per_effect_limit).to_dict(orient="records"):
                        opportunity = self._build_opportunity(
                            event_id=event_id,
                            event_name=str(state.get("name") or definition.name),
                            probability=probability,
                            impact=float(effect.impact or 0),
                            target=effect.target,
                            member=member,
                        )
                        if opportunity is not None:
                            key = (opportunity.symbol, ",".join(opportunity.sector_tags))
                            current = best.get(key)
                            if current is None or opportunity.opportunity_score > current.opportunity_score:
                                best[key] = opportunity
                except Exception as exc:
                    _logger.warning("scan opportunity [%s/%s] failed: %s", event_id, effect.target, exc)
                    stats["errors"].append(f"{event_id}/{effect.target}: {exc}")
            for opportunity in sorted(best.values(), key=lambda item: item.opportunity_score, reverse=True):
                self._db.event().insert_opportunity(opportunity)
                stats["opportunities"] += 1

        return stats

    def _build_opportunity(
        self,
        event_id: str,
        event_name: str,
        probability: float,
        impact: float,
        target: str,
        member: dict,
    ) -> EventOpportunity | None:
        code = str(member.get("code") or member.get("代码") or "").zfill(6)
        if not code or code == "000000":
            return None
        if is_excluded_selection_board(code):
            return None

        name = str(member.get("name") or member.get("名称") or "")
        spot = self._spot_row(code)

        amount = self._num(spot.get("amount") if spot else member.get("成交额"))
        pct_change = self._num(spot.get("pct_change") if spot else member.get("涨跌幅"))
        turnover = self._num(spot.get("turnover") if spot else member.get("换手率"))
        price = self._num(spot.get("price") if spot else member.get("最新价"))

        if price <= 0:
            return None

        elasticity = self._elasticity_score(event_id, code)
        event_score = probability * 32 + impact * 18 + elasticity["score"]
        exposure_score = impact * 20
        underpricing_score = self._underpricing_score(pct_change)
        liquidity_score = self._liquidity_score(amount, turnover)
        timing_score = self._timing_proxy(pct_change, turnover)
        risk_penalty = self._risk_penalty(amount, pct_change) + elasticity["penalty"]

        total = event_score + exposure_score + underpricing_score + liquidity_score + timing_score - risk_penalty
        total = round(max(0.0, min(100.0, total)), 1)

        if total < 40:
            return None

        recommendation = "重点关注" if total >= 75 else "观察"
        if pct_change >= 7:
            recommendation = "谨慎追高"

        return EventOpportunity(
            event_id=event_id,
            symbol=code,
            name=name,
            sector_tags=[target],
            opportunity_score=total,
            event_score=round(event_score, 1),
            exposure_score=round(exposure_score, 1),
            underpricing_score=round(underpricing_score, 1),
            timing_score=round(timing_score, 1),
            risk_penalty=round(risk_penalty, 1),
            recommendation=recommendation,
            evidence={
                "event_name": event_name,
                "target": target,
                "probability": probability,
                "impact": impact,
                "pct_change": pct_change,
                "amount": amount,
                "turnover": turnover,
                "elasticity_score": elasticity["score"],
                "elasticity_penalty": elasticity["penalty"],
                "elasticity_sample_count": elasticity["sample_count"],
                "elasticity_avg_forward_return": elasticity["avg_forward_return"],
                "elasticity_win_rate": elasticity["win_rate"],
            },
        )

    def _fetch_members(self, target: str, target_type: str) -> pd.DataFrame:
        return self._sector_provider.get_members(target, target_type)

    @staticmethod
    def _normalize_members(df: pd.DataFrame) -> pd.DataFrame:
        return SectorTagProvider.normalize_members(df)

    def _spot_row(self, code: str) -> dict:
        if self._spot_cache is None:
            try:
                self._spot_cache = self._sd.get_spot()
            except Exception:
                self._spot_cache = pd.DataFrame()
        if self._spot_cache.empty or "code" not in self._spot_cache.columns:
            return {}
        row = self._spot_cache[self._spot_cache["code"].astype(str) == code]
        return row.iloc[0].to_dict() if not row.empty else {}

    @staticmethod
    def _num(value: Any) -> float:
        try:
            if value is None or pd.isna(value):
                return 0.0
            return float(str(value).replace("%", "").replace(",", ""))
        except Exception:
            return 0.0

    @staticmethod
    def _underpricing_score(pct_change: float) -> float:
        if pct_change <= 0:
            return 15.0
        if pct_change <= 3:
            return 11.0
        if pct_change <= 7:
            return 6.0
        return 1.0

    @staticmethod
    def _liquidity_score(amount: float, turnover: float) -> float:
        if amount >= 300_000_000:
            return 10.0
        if amount >= 50_000_000:
            return 7.0
        if turnover >= 2:
            return 4.0
        return 0.0

    @staticmethod
    def _timing_proxy(pct_change: float, turnover: float) -> float:
        score = 0.0
        if 0 <= pct_change <= 5:
            score += 6
        elif 5 < pct_change <= 9:
            score += 3
        elif pct_change < -3:
            score -= 2
        if 1 <= turnover <= 12:
            score += 4
        return max(0.0, score)

    @staticmethod
    def _risk_penalty(amount: float, pct_change: float) -> float:
        penalty = 0.0
        if amount and amount < 50_000_000:
            penalty += 8
        if pct_change >= 9:
            penalty += 10
        return penalty

    def _elasticity_score(self, event_id: str, symbol: str) -> dict[str, float]:
        """Use historical event-window samples as a small opportunity prior."""
        try:
            samples = self._db.event().list_elasticity_samples(event_id=event_id, limit=1000)
        except Exception:
            samples = pd.DataFrame()
        if samples.empty:
            return {
                "score": 0.0,
                "penalty": 0.0,
                "sample_count": 0.0,
                "avg_forward_return": 0.0,
                "win_rate": 0.0,
            }

        subset = samples[samples["symbol"].astype(str).str.zfill(6) == str(symbol).zfill(6)]
        if subset.empty:
            subset = samples
        returns = pd.to_numeric(subset.get("forward_return"), errors="coerce").dropna()
        success = pd.to_numeric(subset.get("success"), errors="coerce").dropna()
        if returns.empty:
            return {
                "score": 0.0,
                "penalty": 0.0,
                "sample_count": 0.0,
                "avg_forward_return": 0.0,
                "win_rate": 0.0,
            }

        avg_return = float(returns.mean())
        win_rate = float(success.mean()) if not success.empty else 0.0
        sample_count = float(len(returns))
        score = max(0.0, min(8.0, avg_return * 0.8 + win_rate * 4.0))
        penalty = max(0.0, min(8.0, -avg_return * 0.8 + (0.45 - win_rate) * 6.0))
        return {
            "score": round(score, 2),
            "penalty": round(penalty, 2),
            "sample_count": sample_count,
            "avg_forward_return": round(avg_return, 4),
            "win_rate": round(win_rate, 4),
        }


def scan_event_opportunities() -> dict[str, Any]:
    """Convenience entry point for scheduler/CLI."""
    return EventOpportunityScanner().scan()
