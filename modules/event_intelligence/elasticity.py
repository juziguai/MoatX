"""Daily event elasticity backtesting for macro event intelligence."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

import pandas as pd

from modules.config import cfg
from modules.db import DatabaseManager
from modules.sector_tags import SectorTagProvider
from modules.stock_data import StockData

from .history import EventHistoryRegistry
from .transmission import EventTransmissionMap


class EventElasticityBacktester:
    """Measure forward daily returns after event trigger dates."""

    def __init__(
        self,
        db: DatabaseManager | None = None,
        stock_data: StockData | None = None,
        sector_provider: SectorTagProvider | None = None,
        transmission_map: EventTransmissionMap | None = None,
        history_registry: EventHistoryRegistry | None = None,
    ):
        self._db = db or DatabaseManager(cfg().data.warehouse_path)
        self._sd = stock_data or StockData()
        self._sector_provider = sector_provider or SectorTagProvider()
        self._map = transmission_map or EventTransmissionMap()
        self._history = history_registry or EventHistoryRegistry()

    def run(
        self,
        *,
        event_id: str = "",
        windows: list[int] | None = None,
        limit: int = 100,
        per_event_limit: int = 20,
    ) -> dict[str, Any]:
        """Run a daily event-window elasticity backtest."""
        windows = sorted({int(w) for w in (windows or [1, 3, 5, 10]) if int(w) > 0})
        triggers = self._trigger_points(event_id=event_id, limit=limit)
        run_id = self._db.event().insert_elasticity_run(
            event_id=event_id,
            windows=windows,
            trigger_count=len(triggers),
            sample_count=0,
            summary={},
        )
        result: dict[str, Any] = {
            "run_id": run_id,
            "event_id": event_id,
            "windows": windows,
            "triggers": len(triggers),
            "samples": 0,
            "summary": [],
            "errors": [],
        }
        if not triggers:
            return result

        samples: list[dict[str, Any]] = []
        for trigger in triggers:
            targets = self._targets_for_event(trigger["event_id"], trigger=trigger, limit=per_event_limit)
            for target in targets:
                daily = self._load_daily(
                    target["symbol"],
                    start_date=trigger["trigger_date"],
                    max_window=max(windows),
                )
                if daily.empty:
                    result["errors"].append(f"{trigger['event_id']}/{target['symbol']}: no daily data")
                    continue
                for window in windows:
                    sample = self._build_sample(
                        run_id=run_id,
                        trigger=trigger,
                        target=target,
                        daily=daily,
                        window=window,
                    )
                    if sample is None:
                        continue
                    self._db.event().insert_elasticity_sample(sample)
                    samples.append(sample)

        summary = self._summarize(samples)
        self._db.event().update_elasticity_run_summary(
            run_id,
            trigger_count=len(triggers),
            sample_count=len(samples),
            summary={"rows": summary},
        )
        result["samples"] = len(samples)
        result["summary"] = summary
        return result

    def _trigger_points(self, *, event_id: str, limit: int) -> list[dict[str, str]]:
        signals = self._db.event().list_signals(event_id=event_id or None, limit=limit)
        rows: list[dict[str, str]] = []
        seen: set[tuple[str, str]] = set()
        for _, row in signals.iterrows():
            eid = str(row.get("event_id") or "")
            trigger_date = self._date_part(row.get("created_at"))
            key = (eid, trigger_date)
            if eid and trigger_date and key not in seen:
                rows.append({"event_id": eid, "trigger_date": trigger_date, "source": "signals"})
                seen.add(key)

        states = self._db.event().list_states(limit=limit)
        for _, row in states.iterrows():
            eid = str(row.get("event_id") or "")
            if event_id and eid != event_id:
                continue
            trigger_date = self._date_part(row.get("updated_at"))
            key = (eid, trigger_date)
            if eid and trigger_date and key not in seen:
                rows.append({"event_id": eid, "trigger_date": trigger_date, "source": "states"})
                seen.add(key)
        for row in self._history.list(event_id=event_id, limit=limit):
            key = (row["event_id"], row["trigger_date"])
            if key not in seen:
                rows.append(
                    {
                        "event_id": row["event_id"],
                        "trigger_date": row["trigger_date"],
                        "source": "history",
                    }
                )
                seen.add(key)
        return rows

    def _targets_for_event(self, event_id: str, *, trigger: dict[str, str] | None = None, limit: int) -> list[dict[str, str]]:
        opportunities = self._db.event().list_opportunities(event_id=event_id, limit=limit)
        targets: list[dict[str, str]] = []
        seen: set[str] = set()
        for _, row in opportunities.iterrows():
            symbol = SectorTagProvider.normalize_code(str(row.get("symbol") or ""))
            if symbol and symbol not in seen:
                targets.append(
                    {
                        "symbol": symbol,
                        "name": str(row.get("name") or ""),
                        "source": "opportunity",
                    }
                )
                seen.add(symbol)
        if targets:
            return targets

        history_targets = self._history_related_targets(event_id, trigger_date=(trigger or {}).get("trigger_date", ""), limit=limit)
        if history_targets:
            return history_targets

        definition = self._map.get(event_id)
        if definition is None:
            return []
        for effect in definition.effects:
            if effect.direction != "bullish" or effect.target_type not in ("sector", "concept"):
                continue
            members = self._sector_provider.get_members(effect.target, effect.target_type)
            for _, row in members.head(limit).iterrows():
                symbol = SectorTagProvider.normalize_code(str(row.get("code") or ""))
                if symbol and symbol not in seen:
                    targets.append(
                        {
                            "symbol": symbol,
                            "name": str(row.get("name") or ""),
                            "source": str(row.get("source") or effect.target),
                        }
                    )
                    seen.add(symbol)
        return targets

    def _history_related_targets(self, event_id: str, *, trigger_date: str = "", limit: int) -> list[dict[str, str]]:
        rows = self._history.list(event_id=event_id, limit=10_000)
        if trigger_date:
            exact = [row for row in rows if row.get("trigger_date") == trigger_date]
            rows = exact or rows
        targets: list[dict[str, str]] = []
        seen: set[str] = set()
        for row in rows:
            for sector in row.get("related_sectors", []):
                members = self._sector_provider.get_members(str(sector), "sector")
                if members.empty:
                    members = self._sector_provider.get_members(str(sector), "concept")
                for _, member in members.head(limit).iterrows():
                    symbol = SectorTagProvider.normalize_code(str(member.get("code") or ""))
                    if symbol and symbol not in seen:
                        targets.append(
                            {
                                "symbol": symbol,
                                "name": str(member.get("name") or ""),
                                "source": f"history:{sector}",
                            }
                        )
                        seen.add(symbol)
                    if len(targets) >= limit:
                        return targets
        return targets

    def _load_daily(self, symbol: str, *, start_date: str, max_window: int) -> pd.DataFrame:
        end_date = (
            datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days=max_window * 4 + 14)
        ).strftime("%Y-%m-%d")
        trigger_ready_date = datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days=max_window)
        df = pd.DataFrame()
        try:
            df = self._db.price().load_daily(symbol, start_date=start_date, end_date=end_date)
        except Exception:
            df = pd.DataFrame()

        if trigger_ready_date.date() > datetime.now().date():
            return df if not df.empty else pd.DataFrame()

        if df.empty or len(df) <= max_window:
            try:
                fetched = self._sd.get_daily(
                    symbol,
                    start_date=start_date.replace("-", ""),
                    end_date=end_date.replace("-", ""),
                    adjust="qfq",
                )
                if fetched is not None and not fetched.empty:
                    fetched = fetched.reset_index() if "date" not in fetched.columns else fetched.copy()
                    try:
                        self._db.price().save_daily_batch(fetched, symbol)
                    except Exception:
                        pass
                    df = fetched
            except Exception:
                return pd.DataFrame()

        if "date" not in df.columns or "close" not in df.columns:
            return pd.DataFrame()
        out = df.copy()
        out["date"] = pd.to_datetime(out["date"])
        out["close"] = pd.to_numeric(out["close"], errors="coerce")
        out = out.dropna(subset=["date", "close"]).sort_values("date").reset_index(drop=True)
        return out[out["date"] >= pd.Timestamp(start_date)].reset_index(drop=True)

    def _build_sample(
        self,
        *,
        run_id: int,
        trigger: dict[str, str],
        target: dict[str, str],
        daily: pd.DataFrame,
        window: int,
    ) -> dict[str, Any] | None:
        if len(daily) <= window:
            return None
        entry = daily.iloc[0]
        exit_row = daily.iloc[window]
        entry_close = float(entry["close"])
        exit_close = float(exit_row["close"])
        if entry_close <= 0:
            return None

        path = daily.iloc[: window + 1]["close"].astype(float)
        forward_return = (exit_close / entry_close - 1.0) * 100
        max_drawdown = (path.min() / entry_close - 1.0) * 100
        benchmark_return = 0.0
        excess_return = forward_return - benchmark_return
        return {
            "run_id": run_id,
            "event_id": trigger["event_id"],
            "symbol": target["symbol"],
            "name": target.get("name", ""),
            "trigger_date": trigger["trigger_date"],
            "entry_date": str(entry["date"].date()),
            "window_days": window,
            "entry_close": entry_close,
            "exit_date": str(exit_row["date"].date()),
            "exit_close": exit_close,
            "forward_return": round(forward_return, 4),
            "benchmark_return": round(benchmark_return, 4),
            "excess_return": round(excess_return, 4),
            "max_drawdown": round(max_drawdown, 4),
            "success": forward_return > 0,
            "source": target.get("source", ""),
        }

    @staticmethod
    def _summarize(samples: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not samples:
            return []
        df = pd.DataFrame(samples)
        grouped = df.groupby(["event_id", "window_days"], as_index=False)
        rows: list[dict[str, Any]] = []
        for (event_id, window_days), group in grouped:
            rows.append(
                {
                    "event_id": str(event_id),
                    "window_days": int(window_days),
                    "sample_count": int(len(group)),
                    "avg_forward_return": round(float(group["forward_return"].mean()), 4),
                    "avg_excess_return": round(float(group["excess_return"].mean()), 4),
                    "win_rate": round(float(group["success"].mean()), 4),
                    "avg_max_drawdown": round(float(group["max_drawdown"].mean()), 4),
                }
            )
        return rows

    @staticmethod
    def _date_part(value: Any) -> str:
        text = str(value or "")
        return text[:10] if len(text) >= 10 else ""


def run_event_elasticity(
    event_id: str = "",
    windows: list[int] | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    """Convenience entry point for CLI."""
    return EventElasticityBacktester().run(event_id=event_id, windows=windows, limit=limit)
